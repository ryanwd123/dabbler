from typing import Union
from dabbler.common import FromLangServer, ToLangServer, KeyFile
import logging
from qtpy import QtWidgets, QtCore, QtGui
from qtpy.QtCore import Slot, Signal  # type: ignore
from IPython.core.getipython import get_ipython
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
            sc = QtGui.QShortcut(QtGui.QKeySequence(i), Parent)  # type: ignore
            sc.activated.connect(target)


# class Hbox(QtWidgets.QHBoxLayout):
#     def __init__(self, parent):
#         super().__init__(parent)  
#         self.setSpacing(margins)
#         self.setContentsMargins(margins, margins, margins, margins)


# class Vbox(QtWidgets.QVBoxLayout):
#     def __init__(self, parent):
#         super().__init__(parent)
#         self.setSpacing(margins)
#         self.setContentsMargins(margins, margins, margins, margins)


class TreeItem(QtWidgets.QTreeWidgetItem):
    def __init__(self, *args, item_type=None):
        super().__init__(*args)
        self.item_type = item_type
        


class IPythonExecutionMonitor(object):
    def __init__(self, ip, parent: "ZmqServer", timer: QtCore.QTimer):
        self.shell = ip
        self.parent = parent
        self.timer = timer

    def pre_run_cell(self, info):
        self.timer.start(1_000_000)

    def post_run_cell(self, result):
        if result.info.cell_id:
            self.timer.start(200)


class RepChannel(QtCore.QThread):
    trigger = Signal(object)

    def __init__(self, socket: zmq.Socket) -> None:
        super().__init__()
        self.thead_active = True
        self.socket = socket
        self.logger = logging.getLogger("dabbler_rep_thread")

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
        self.logger.debug("rep thread stopped")
        self.finished.emit()
        self.deleteLater()


class HBChannel(QtCore.QThread):
    trigger = Signal(object)

    def __init__(self, socket: zmq.Socket) -> None:
        super().__init__()
        self.thead_active = True
        self.socket = socket
        self.logger = logging.getLogger("dabbler_hb_thread")
        self.logger.debug("hb thread init")



    def run(self):
        while self.thead_active:
            self.send_msg({"cmd": "heartbeat","con_id": 0, "data": 1})
            if not self.socket.poll(5000):
                self.logger.debug("lost connection to lang server")
                self.trigger.emit("reset_sockets")
                break
            buff = self.socket.recv()
            msg: FromLangServer = pickle.loads(buff)
            # self.log(f"heartbeat channel got message {msg}")
            if msg["cmd"] != "heartbeat":
                self.logger.error(f"heartbeat channel got wrong message {msg}")
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
    def __init__(self, parent=None, db: Union[duckdb.DuckDBPyConnection,None] = None):
        super().__init__(parent)
        self.app = parent
        if db:
            self.db = db.cursor()
        else:
            self.db = None
        self.log: logging.Logger = logging.getLogger("dabbler_zmq_server")
        self.log.info("zmq server init")

        self.sockets_created = False
        self.connected = False
        self.connection_id: int = 0
        self.new_data = False
        self.data_sent = False
        self.context = zmq.Context()
        if self.db and self.app:
            self.db_data = get_db_data_new(self.db, self.app.file_search_path)

        self.hb_channels: list[HBChannel] = []
        self.connect_sockets()

        ip = get_ipython()
        self.ipython_cell_timer = QtCore.QTimer()
        self.ipython_cell_timer.setSingleShot(True)
        self.ipython_cell_timer.timeout.connect(self.check_for_update)
        self.monitor_ip = IPythonExecutionMonitor(ip, self, self.ipython_cell_timer)
        if ip:
            ip.events.register("pre_run_cell", self.monitor_ip.pre_run_cell)
            ip.events.register("post_run_cell", self.monitor_ip.post_run_cell)

    def clear_conn_info(self, delete=False):
        if delete and self.connection and self.key_file:
            self.key_file.delete_connection(self.connection["workspace_path"])
            self.log.debug("deleted connection info from keyfile")
        self.connection = None
        self.key_file = None
        self.main_port = None
        self.handshake_port = None

    def attempt_connection(self):
        self.clear_conn_info()
        self.key_file = KeyFile()
        if self.app:
            self.connection = self.key_file.get_connection(self.app.py_file)
        if not self.connection:
            self.log.debug("Connection info not found in keyfile")
            return False

        self.main_port = self.connection["main_port"]
        self.handshake_port = self.connection["handshake_port"]

        try:
            self.create_sockets()
            self.initialize_channels()
            return True
        except Exception as e:
            self.log.error(f"Failed to connect_: {e}")
            return False

    def connect_sockets(self):
        if self.sockets_created:
            return

        if self.attempt_connection():
            self.sockets_created = True
            self.log.debug(f"Sockets created, main port {self.main_port}, handshake port {self.handshake_port}")
            if self.connection:
                print(f'Connected to language server, workspace {self.connection["workspace_path"]}')

            if self.data_sent is False:
                self.handshake_send({"cmd": "ip_python_started", "data": 1, "con_id": 0})
                if self.app and self.app.debug:
                    if self.connection:
                        id = self.connection["client_id"]
                    else:
                        id = 0
                    self.handshake_send({"cmd": "debug", "data": True, "con_id": id})
        else:
            self.log.debug("Failed to connect, will retry later")

    def reconnect(self):
        self.log.debug("Attempting to reconnect")
        while not self.attempt_connection():
            time.sleep(5)  # Wait for 5 seconds before trying again
        self.sockets_created = True
        self.connected = True
        self.log.debug(f"Reconnected successfully, {self.sockets_created}")
        self.send_db_data()

    def create_sockets(self):
        self.log.debug(f"connecting to main port {self.main_port}")
        self.socket = self.context.socket(zmq.PAIR)
        self.socket.bind(f"tcp://127.0.0.1:{self.main_port}")

        self.log.debug(f"connecting to handshake port {self.handshake_port}")
        self.handeshake_socket = self.context.socket(zmq.PAIR)
        self.handeshake_socket.connect(f"tcp://127.0.0.1:{self.handshake_port}")

        if self.connection:
            handshake_msg: ToLangServer = {
                "cmd": "connection_id",
                "data": self.connection["client_id"],
                "con_id": 0
            }


            self.log.debug(f"sending handshake message {handshake_msg}")
            self.handeshake_socket.send(pickle.dumps(handshake_msg))
            self.log.debug("sent handshake message")


        if not self.handeshake_socket.poll(1000):
            raise Exception("Handshake socket not connected")

        handshake_response_buff = self.handeshake_socket.recv()
        handshake_response: FromLangServer = pickle.loads(handshake_response_buff)

        if (
            handshake_response["cmd"] != "connection_id"
            or handshake_response["data"] != self.connection["server_id"]   # type: ignore
        ):
            raise Exception("Handshake response not correct")

    def initialize_channels(self):
        self.rep_server = RepChannel(self.socket)
        self.rep_server.trigger.connect(self.socket_reply)
        self.rep_server.start()
        self.log.debug("rep thread started")

        hb = HBChannel(self.handeshake_socket)
        self.hb_channels.append(hb)
        self.hb_channels[-1].trigger.connect(self.heartbeat_signal_handler)
        self.hb_channels[-1].start()
        self.log.debug("hb thread started")

    def restart_sockets(self):
        self.log.debug("Restarting sockets")
        self.stop_channels()
        self.close_sockets()
        self.sockets_created = False
        self.connection_id = 0
        self.connected = False
        self.reconnect()

    def stop_channels(self):
        if self.rep_server:
            self.rep_server.stop()
        for hb in self.hb_channels:
            hb.stop()
        self.hb_channels = []

    def close_sockets(self):
        # if hasattr(self, 'socket'):
        self.socket.close()
        # if hasattr(self, 'handeshake_socket'):
        self.handeshake_socket.close()

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
        self.check_for_update()

    def check_for_update(self):
        if not self.app or not self.db:
            return
        data2 = get_db_data_new(self.db, self.app.file_search_path)
        self.log.debug(["checking for updated db data", data2])

        need_update = False

        for k, v in data2.items():
            if self.db_data[k] != v:
                need_update = True
                break

        if need_update:
            self.new_data = True
            self.db_data = data2
            self.send_db_data()
            self.app.update_tables()

    def start_lsp_logger(self):
        if not self.app:
            return
        if self.app.debug:
            self.log.info("tell lang server debug mode")
            self.respond({"cmd": "debug", "data": True, "con_id": 0})

    def respond(self, msg: ToLangServer, no_block=False):
        if not self.sockets_created:
            self.connect_sockets()
            if not self.sockets_created:
                self.log.warning(f"Message send failed, sockets not created {msg}")
                return

        try:
            buff = pickle.dumps(msg)
            if self.connected:
                self.socket.send(buff, zmq.NOBLOCK if no_block else 0)
            else:
                self.log.error(f"Tried to send message when not connected {msg['cmd']}")
        except zmq.ZMQError as e:
            self.log.error(f"ZMQ error when sending message: {e}")
            self.restart_sockets()
        except Exception as e:
            self.log.error(f"Unexpected error when sending message: {e}")
            self.restart_sockets()

    def rec_manual(self):
        buff = self.socket.recv()
        data = pickle.loads(buff)
        self.log.debug(f"zmq server rec at start {data}")
        self.socket_reply(data)

    @Slot(object)
    def heartbeat_signal_handler(self, msg):
        if msg == "reset_sockets":
            self.restart_sockets()

    @Slot(object)
    def socket_reply(self, msg: FromLangServer):
        try:
            if not self.connected:
                self.connected = True
                self.start_lsp_logger()

            if msg["cmd"] == "connection_id":
                self.connection_id = msg["data"] # type: ignore

            elif msg["cmd"] == "run_sql":
                self.respond({"cmd": "run_sql_complete", "data": "success", "con_id": self.connection_id})
                if self.app:
                    self.app.msg_routing(msg["data"])

            elif msg["cmd"] == "db_data_update":
                self.send_db_data()
                self.start_lsp_logger()

            elif msg["cmd"] == "check_for_update":
                if self.new_data is True:
                    self.send_db_data()
                    self.new_data = False
                else:
                    self.respond({"cmd": "no_update", "data": "1", "con_id": self.connection_id})

            elif msg["cmd"] == "heartbeat":
                self.hb = time.time()

            else:
                self.respond({"cmd": "not_handled", "con_id": self.connection_id, "data": 1})

        except Exception as e:
            self.log.error(f"Error in socket_reply: {e}")
            self.restart_sockets()

    def send_db_data(self, no_block=False):
        reply:ToLangServer = {"cmd": "db_data", "data": self.db_data, "con_id": self.connection_id}
        self.respond(reply, no_block=no_block)