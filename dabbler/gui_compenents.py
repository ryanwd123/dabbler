from queue import Queue
import pprint

# from PyQt6 import QtWidgets, QtCore, QtGui
# from PyQt6.QtCore import pyqtSlot as Slot, pyqtSignal as Signal
from dabbler.common import FromLangServer, ToLangServer, KeyFile
import logging

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


# class RepChannel(QtCore.QThread):
#     trigger = Signal(object)

#     def __init__(self, poller: zmq.Poller) -> None:
#         super().__init__()
#         self.thead_active = True
#         self.poller = poller

#     def run(self):
#         while self.thead_active:
#             socks:dict[zmq.Socket,str] = dict(self.poller.poll(500))
#             for socket in socks:
#                 buff = socket.recv()
#                 msg = pickle.loads(buff)
#                 self.trigger.emit(msg)

#     def stop(self):
#         self.thead_active = False
#         self.wait()


class RepChannel(QtCore.QThread):
    trigger = Signal(object)

    def __init__(self, socket: zmq.Socket) -> None:
        super().__init__()
        self.thead_active = True
        self.socket = socket

    def run(self):
        while self.thead_active:
            if self.socket.poll(500):
                buff = self.socket.recv()
                msg = pickle.loads(buff)
                self.trigger.emit(msg)

    def stop(self):
        self.thead_active = False
        self.quit()
        self.wait()
        self.finished.emit()
        self.deleteLater()


class HBChannel(QtCore.QThread):
    trigger = Signal(object)

    def __init__(self, socket: zmq.Socket, log: logging.Logger = None) -> None:
        super().__init__()
        self.thead_active = True
        self.socket = socket

        self.logger = log
        self.log("hb thread init")

    def log(self, msg):
        if self.logger:
            self.logger.debug(msg)

    def run(self):
        while self.thead_active:
            self.send_msg({"cmd": "heartbeat"})
            if not self.socket.poll(5000):
                self.log("lost connection to lang server")
                self.trigger.emit("reset_sockets")
                # reset sockets
                break
            buff = self.socket.recv()
            msg: FromLangServer = pickle.loads(buff)
            self.log(f"heartbeat channel got message {msg}")
            if msg["cmd"] != "heartbeat":
                self.log.error(f"heartbeat channel got wrong message {msg}")
                break
            time.sleep(3)
        self.quit()
        self.wait()
        self.finished.emit()
        self.deleteLater()

    def send_msg(self, msg: ToLangServer):
        buff = pickle.dumps(msg)
        self.socket.send(buff)

    def stop(self):
        self.thead_active = False
        self.quit()
        self.wait()
        self.finished.emit()
        self.deleteLater()


class ZmqServer(QtCore.QObject):
    def __init__(self, parent=None, db: duckdb.DuckDBPyConnection = None):
        super().__init__(parent)
        self.app = parent
        self.db = db.cursor()
        self.log: logging.Logger = self.app.log.getChild("gui_zmq")
        self.log.info("zmq server init")

        self.sockets_created = False
        self.connected = False
        self.connection_id: int = None
        self.new_data = False
        self.data_sent = False
        self.db_data = get_db_data_new(self.db, self.app.file_search_path)

        self.hb_channels: list[HBChannel] = []
        self.connect_sockets()

        ip = get_ipython()
        q = Queue()
        self.monitor_ip = VarWatcher(ip, self, q)
        ip.events.register("pre_run_cell", self.monitor_ip.pre_run_cell)
        ip.events.register("post_run_cell", self.monitor_ip.post_run_cell)

        self.ex_monitor = ExMonitor(q)
        self.cell_monitor_thread = QtCore.QThread()
        self.ex_monitor.moveToThread(self.cell_monitor_thread)
        self.cell_monitor_thread.started.connect(self.ex_monitor.run)
        self.ex_monitor.trigger.connect(self.msg_routing)
        self.cell_monitor_thread.start()

    def clear_conn_info(self,delete=False):
        if delete:
            self.key_file.delete_connection(self.connection["workspace_path"])
        self.log.debug("deleted connection info from keyfile")
        self.connection = None
        self.key_file = None
        self.main_port = None
        self.handshake_port = None

    def connect_sockets(self):
        if self.sockets_created:
            return

        self.key_file = KeyFile()
        self.log.debug(self.key_file)

        self.connection = self.key_file.get_connection(self.app.py_file)
        self.log.debug(["connection_info_from_keyfile", self.connection])

        if self.connection:
            self.main_port = self.connection["main_port"]
            self.handshake_port = self.connection["handshake_port"]
        else:
            self.log.debug("connection info not found in keyfile")
            return

        self.log.debug(f"connecting to main port {self.main_port}")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PAIR)
        self.socket.bind(f"tcp://127.0.0.1:{self.main_port}")

        self.log.debug(f"connecting to handshake port {self.handshake_port}")
        self.context2 = zmq.Context()
        self.handeshake_socket = self.context2.socket(zmq.PAIR)
        self.handeshake_socket.connect(f"tcp://127.0.0.1:{self.handshake_port}")

        handshake_msg: ToLangServer = {
            "cmd": "connection_id",
            "data": self.connection["client_id"],
        }

        self.handeshake_socket.send(pickle.dumps(handshake_msg))

        if not self.handeshake_socket.poll(1000):
            self.log.error("handshake socket not connected")
            self.clear_conn_info(True)
            self.connect_sockets()
            return

        handshake_response_buff = self.handeshake_socket.recv()
        handshake_response: FromLangServer = pickle.loads(handshake_response_buff)

        if (
            not handshake_response["cmd"] == "connection_id"
            and handshake_response["data"] == self.connection["server_id"]
        ):
            self.log.error("handshake response not correct")
            # self.key_file.delete_connection(self.connection["workspace_path"])
            # self.clear_conn_info()
            return

        self.log.debug("handshake sockets connected")
        # self.poller = zmq.Poller()
        # self.poller.register(self.socket, zmq.POLLIN)
        # self.poller.register(self.handeshake_socket, zmq.POLLIN)

        # if self.socket.poll(100):
        #     self.rec_manual()
        #     self.start_lsp_logger()

        self.rep_server = RepChannel(self.socket)
        self.rep_server.trigger.connect(self.socket_reply)
        self.rep_server.start()
        self.log.debug("rep thread started")

        hb = HBChannel(self.handeshake_socket, log=self.log)
        self.log.debug("hb channel init1")
        self.hb_channels.append(hb)
        # self.hb_channel.

        # self.hb_channel = hb
        self.log.debug("hb channel init")
        self.hb_channels[-1].trigger.connect(self.heartbeat_signal_handler)
        self.log.debug("hb channel trigger connected")
        self.hb_channels[-1].start()
        self.log.debug("hb thread started")

        self.sockets_created = True
        self.log.debug(
            f"sockets created, main port {self.main_port}, handshake port {self.handshake_port}"
        )

        print(
            f'connected to language server, workspace {self.connection["workspace_path"]}'
        )

        if self.data_sent is False:
            self.handshake_send({"cmd": "ip_python_started", "data": 1})
            if self.app.debug:
                self.handshake_send({"cmd": "debug", "data": True})

        self.log.debug("sockets connected")

    def restart_sockets(self):
        self.log.debug("restarting sockets")
        self.rep_server.stop()
        self.log.debug("rep thread stopped")
        self.hb_channels[-1].stop()
        self.log.debug("hb thread stopped")
        self.sockets_created = False
        self.connection_id = None
        self.connected = False
        self.connection = None
        self.key_file = None
        self.socket.close()
        self.log.debug("main socket closed")
        self.handeshake_socket.close()
        self.log.debug("handshake socket closed")
        self.connect_sockets()

    def handshake_send(self, msg: ToLangServer):
        if not self.sockets_created:
            self.connect_sockets()
            if not self.sockets_created:
                self.log.warning(f"handshake send failed, sockets not created {msg}")
                return

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
        data2 = get_db_data_new(self.db, self.app.file_search_path)
        self.log.debug(["checking for updated db data", data2])
        # qq = (self.db_data['data'] != data2['data']
        # or self.db_data['dataframes'] != data2['dataframes'])
        # print(f'checking for updated db data {qq}')

        need_update = False

        for k, v in data2.items():
            if self.db_data[k] != v:
                need_update = True
                break

        if need_update:
            self.new_data = True
            self.db_data = data2
            # print('data changed')
            self.send_db_data()
            self.app.update_tables()
            # print('updated tables')

    def start_lsp_logger(self):
        if self.app.debug:
            self.log.info("tell lang server debug mode")
            self.respond({"cmd": "debug", "data": True})

    def respond(self, msg: ToLangServer, no_block=False):
        if not self.sockets_created:
            self.connect_sockets()
            if not self.sockets_created:
                self.log.warning(f"msg send failed, sockets not created {msg}")
                return

        buff = pickle.dumps(msg)

        # if no_block:
        if self.connected:
            try:
                # print(f'responding_no_block {msg}')
                self.socket.send(buff, zmq.NOBLOCK)
            except:  # noqa: E722
                self.log.exception(f"lost server when sending message {msg['cmd']}")
                self.restart_sockets()
            return
            # self.log.debug(msg)
            # self.socket.send(buff)
        else:
            self.log.error(f"try to send message when not connected {msg['cmd']}")

    def rec_manual(self):
        buff = self.socket.recv()
        data = pickle.loads(buff)
        self.log.debug(f"zmq server rec at start {data}")
        self.socket_reply(data)

    @Slot(object)
    def heartbeat_signal_handler(self, msg):
        if msg == "reset_sockets":
            self.restart_sockets()
            return

    @Slot(object)
    def socket_reply(self, msg: FromLangServer):
        if not self.connected:
            self.connected = True
            self.start_lsp_logger()
            # print('connected')

        if msg["cmd"] == "connection_id":
            self.connection_id = msg["data"]

        if msg["cmd"] == "run_sql":
            self.respond({"cmd": "run_sql", "msg": "success"})
            self.app.msg_routing(msg["sql"])
            return

        if msg["cmd"] == "db_data_update":
            self.send_db_data()
            self.start_lsp_logger()
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
