from queue import Queue

# from PyQt6 import QtWidgets, QtCore, QtGui
# from PyQt6.QtCore import pyqtSlot as Slot, pyqtSignal as Signal
from dabbler.common import FromLangServer, ToLangServer

# from PySide6 import QtWidgets, QtCore, QtGui
# from PySide6.QtCore import Slot, Signal
from qtpy import QtWidgets, QtCore, QtGui
from qtpy.QtCore import Slot, Signal
from IPython import get_ipython
import pickle
import time
import zmq
import duckdb
from dabbler.db_stuff import get_db_data_new

margins = 0


class Shortcut:
    def __init__(self, Seq, Parent, target):
        if not isinstance(Seq, list):
            Seq = [Seq]
        for i in Seq:
            sc = QtGui.QShortcut(QtGui.QKeySequence(i), Parent)
            sc.activated.connect(target)


class Hbox(QtWidgets.QHBoxLayout):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setSpacing(margins)
        self.setContentsMargins(margins, margins, margins, margins)


class Vbox(QtWidgets.QVBoxLayout):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setSpacing(margins)
        self.setContentsMargins(margins, margins, margins, margins)


class TreeItem(QtWidgets.QTreeWidgetItem):
    def __init__(self, *args, item_type=None):
        super().__init__(*args)
        self.item_type = item_type


class VarWatcher(object):
    def __init__(self, ip, parent: "ZmqServer", q: Queue = None):
        self.shell = ip
        self.server = parent
        self.q = q

    # def pre_execute(self):
    #     self.server.pre_exec = True
    #     self.last_x = self.shell.user_ns.get('x', None)

    def pre_run_cell(self, info):
        # if info:
        # print(f'pre-run-cell event')
        self.q.put("cell_start")
        # self.server.cell_running = False

    def post_run_cell(self, result):
        if result.info.cell_id:
            self.q.put("cell_end")
            # print('cell executed event')
            # self.server.send_db_data()
            # msg = {
            #     'cmd':'ip',
            #     'data':str(result)
            # }
            # self.server.respond(msg)
            # self.server.pre_exec = False


class ExMonitor(QtCore.QObject):
    trigger = Signal(str)

    def __init__(self, q: Queue = None) -> None:
        super().__init__()
        # self.trigger.connect(signal_to_emit)
        self.q = q

    def run(self):
        item = None
        # print('ex monitor started')
        while True:
            while not self.q.empty() or item is None:
                item = self.q.get()
            time.sleep(0.05)
            # print(f'{item}')
            if item == "cell_end":
                time.sleep(0.05)
                if self.q.empty():
                    # print('trigger')
                    self.trigger.emit("send_udpate")
                else:
                    pass
                    # print(f'not empty')
            else:
                pass
                # print(f'not cell_end:{item}')
            item = None


class RepChannel(QtCore.QObject):
    trigger = Signal(object)
    # set_connected = Signal(bool)

    def __init__(self, socket: zmq.Socket) -> None:
        super().__init__()
        self.socket = socket
        # self.connected = False

    def run(self):
        # print('socket thread started')
        while True:
            buff = self.socket.recv()
            # if not self.connected:
            # self.set_connected.emit(True)
            # self.connected = True
            msg = pickle.loads(buff)
            # print(f'rep channel rec {msg}')
            self.trigger.emit(msg)


class ZmqServer(QtCore.QObject):
    def __init__(self, parent=None, db: duckdb.DuckDBPyConnection = None):
        super().__init__(parent)
        self.app = parent
        self.db = db.cursor()
        self.connected = False
        self.connection_id: int = None
        self.new_data = False
        self.data_sent = False
        self.db_data = get_db_data_new(self.db)

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PAIR)
        self.socket.bind("tcp://127.0.0.1:55557")

        # print(f'poll server {self.socket.poll(100)}')

        if self.socket.poll(100):
            self.rec_manual()

        self.rep_server = RepChannel(self.socket)
        self.rep_thread = QtCore.QThread()
        self.rep_server.moveToThread(self.rep_thread)
        self.rep_thread.started.connect(self.rep_server.run)
        self.rep_server.trigger.connect(self.socket_reply)

        self.context2 = zmq.Context()
        self.handeshake_socket = self.context2.socket(zmq.PAIR)
        self.handeshake_socket.connect("tcp://127.0.0.1:55558")
        self.handshake_server = RepChannel(self.handeshake_socket)
        self.handshake_thread = QtCore.QThread()
        self.handshake_server.moveToThread(self.handshake_thread)
        self.handshake_thread.started.connect(self.handshake_server.run)
        self.handshake_server.trigger.connect(self.socket_reply)

        if self.data_sent is False:
            self.handshake_send({"cmd": "ip_python_started", "data": 1})

        # self.rep_server.set_connected.connect(self.set_connected)
        self.rep_thread.start()
        self.handshake_thread.start()

        ip = get_ipython()
        q = Queue()
        self.monitor_ip = VarWatcher(ip, self, q)
        ip.events.register("pre_run_cell", self.monitor_ip.pre_run_cell)
        ip.events.register("post_run_cell", self.monitor_ip.post_run_cell)

        self.ex_monitor = ExMonitor(q)
        self.t = QtCore.QThread()
        self.ex_monitor.moveToThread(self.t)
        self.t.started.connect(self.ex_monitor.run)
        self.ex_monitor.trigger.connect(self.msg_routing)
        self.t.start()

        # if self.data_sent == False:
        #     self.send_db_data(no_block=True)
        #     self.data_sent = True

    def handshake_send(self, msg: ToLangServer):
        buff = pickle.dumps(msg)
        self.handeshake_socket.send(buff)

    @Slot(bool)
    def set_connected(self, connected):
        self.connected = connected

    @Slot(str)
    def msg_routing(self, item):
        # print(f'msg routing {item}')
        self.check_for_update()

    def check_for_update(self):
        data2 = get_db_data_new(self.db)

        # qq = (self.db_data['data'] != data2['data']
        # or self.db_data['dataframes'] != data2['dataframes'])
        # print(f'checking for updated db data {qq}')

        if (
            self.db_data["data"] != data2["data"]
            or self.db_data["dataframes"] != data2["dataframes"]
        ):
            self.new_data = True
            self.db_data = data2
            # print('data changed')
            self.send_db_data()
            self.app.update_tables()
            # print('updated tables')

    def respond(self, msg, no_block=False):
        buff = pickle.dumps(msg)

        if no_block:
            try:
                # print(f'responding_no_block {msg}')
                self.socket.send(buff, zmq.NOBLOCK)
            except:  # noqa: E722
                pass
            return

        if self.connected:
            # print(f'responding {msg}')
            self.socket.send(buff)
        else:
            pass
            # print('not connected')

    def rec_manual(self):
        buff = self.socket.recv()
        data = pickle.loads(buff)
        # print(f'zmq server rec at start {data}')
        self.socket_reply(data)

    @Slot(object)
    def socket_reply(self, msg: FromLangServer):
        if not self.connected:
            self.connected = True
            # print('connected')

        if msg["cmd"] == "connection_id":
            self.connection_id = msg["data"]

        if msg["cmd"] == "run_sql":
            self.respond({"cmd": "run_sql", "msg": "success"})
            self.app.msg_routing(msg["sql"])
            return

        if msg["cmd"] == "db_data_update":
            self.send_db_data()
            return

        if msg["cmd"] == "check_for_update":
            if self.new_data is True:
                self.send_db_data()
                # print('sent db data - check for update')
                self.new_data = False
            else:
                self.respond({"cmd": "no_update"})
            return

        if msg["cmd"] == "heartbeat":
            self.hb = time.time()
            return

        # print(f'req not handled: {msg}')
        self.respond({"cmd": "not_handled"})

    def send_db_data(self, no_block=False):
        # print('sending db data')
        reply = {"cmd": "db_data", "data": self.db_data}
        self.respond(reply, no_block=no_block)
