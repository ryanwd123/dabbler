import os
import logging
from pathlib import Path
from functools import partial
import zmq
from zmq.asyncio import Context, Socket as AsyncSocket, Poller
from pygls.server import LanguageServer
import pickle
import asyncio
from dabbler.common import FromLangServer, ToLangServer, KeyFile, PprintSocketHandler
from dabbler.lsp.completer import SqlCompleter
from dabbler.db_stuff import get_default_db_data
from typing import Union
# from dabbler.lsp.completer import CmpItem



# %%


class InlineSqlLangServer(LanguageServer):
    CMD_SEND_SQL_TO_GUI = "sendSqlToDbDabbler"
    CMD_FORMAT_CURRENT_STATEMENT = "dbDabblerFormatCurrentStatement"
    CONFIGURATION_SECTION = "pygls.jsonServer"

    def __init__(self, *args):
        super().__init__(*args)

        self.log = logging.getLogger("dabbler_lsp")
        self.debug = False
        self.ctx = Context()   # type: ignore
        self.create_sockets2()

        self.completer: Union["SqlCompleter",None] = None
        self.socket_connected = False
        self.socket_created = False
        self.pathlibs_paths = {}
        self.key_file: Union[KeyFile,None] = None

    def create_default_compelter(self):
        if not self.completer is None:
            return
        blank_db_data = get_default_db_data()
        blank_db_data['cwd'] = self.workspace.root_path
        self.completer = SqlCompleter(blank_db_data)

    def start_io(self, stdin = None, stdout = None):

        self.loop.create_task(self.zmq_recv(self.poller))
        # self.loop.run_until_complete(self.zmq_recv(self.poller))
        super().start_io(stdin, stdout)
        
    def start_logging(self):
        if self.debug:
            return
        self.debug = True
        self.log.setLevel(1)  # to send all records to cutelog
        socket_handler = PprintSocketHandler(
            "127.0.0.1", 19996
        )  # default listening address
        self.log.addHandler(socket_handler)
        self.log.info("logging started")
        self.log.debug(os.environ)

    def log_workspace_info(self):
        if self.debug is False:
            return
        if self.workspace is None:
            self.log.debug(f"workspace is None")
            return
        workspace_info = {
            "root_uri": self.workspace.root_uri,
            "root_path": self.workspace.root_path,
            "folders": self.workspace.folders,
            "documents": self.workspace.documents,
        }
        self.log.debug(["workspace_info", workspace_info])
        
    def find_port(self):
        # ctx = zmq.Context()
        socket = self.ctx.socket(zmq.PAIR)
        port = socket.bind_to_random_port("tcp://127.0.0.1")
        socket.close()
        return port
    


    def create_sockets2(self):
        
        # ctx1 = Context().instance()
        self.socket = self.ctx.socket(zmq.PAIR)
        self.main_port = self.find_port()
        self.socket.connect(f"tcp://127.0.0.1:{self.main_port}")

        # ctx2 = Context().instance()
        self.handshake_socket = self.ctx.socket(zmq.PAIR)
        self.handshake_port = self.find_port()
        self.handshake_socket.bind(f"tcp://127.0.0.1:{self.handshake_port}")

        self.poller = Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.handshake_socket, zmq.POLLIN)
        # self.loop.create_task(self.zmq_recv(self.poller))
        
        self.log.debug(f"main_port {self.main_port}, handshake_port {self.handshake_port}")

    def save_key_file(self):
        if self.key_file:
            return
        if self.workspace is None:
            self.log.debug(f"workspace is None, keyfile not saved")
            return
        self.key_file = KeyFile()
        
        workpace_path = str(self.workspace.root_path)
        
        connection = self.key_file.add_connection(
            workpace_path,
            {
                "workspace_path": workpace_path,
                "main_port": self.main_port,
                "handshake_port": self.handshake_port,
            },
        )
        self.connection_info = connection
        self.show_message_log(f"main_port {self.main_port}, handshake_port {self.handshake_port}")
        self.log.debug(f"key_file {self.key_file.connections}")

        self.zmq_send({"cmd": "db_data_update","con_id":self.connection_info['server_id'], "data": 1})

    def initialize_sql_completer(self, data: dict):
        self.completer = SqlCompleter(data)
        self.pathlibs_paths = {x[0]:x[1] for x in data["paths"]}
        self.show_message_log("SqlCompleter initialized")

    async def handle_zmq_message(self, msg: ToLangServer):
        if msg["cmd"] == "db_data":
            self.show_message_log("received db_data")
            await self.loop.run_in_executor(
                self.thread_pool_executor,
                partial(self.initialize_sql_completer, msg["data"])  # type: ignore
            )
        elif msg["cmd"] == "ip_python_started":
            self.show_message_log(f"ip_python_started = {msg['data']}")
            self.zmq_send({"cmd": "db_data_update", "con_id": self.connection_info["server_id"], "data": 1})
        elif msg["cmd"] == "ip":
            self.show_message_log(f"ipython event = {msg['data']}")
        elif msg["cmd"] == "no_update":
            self.show_message_log("check update: no update")
        elif msg["cmd"] == "debug":
            if msg["data"]:
                self.show_message_log("started in debug mode")
                self.start_logging()
        elif msg["cmd"] == "heartbeat":
            await self.handshake_socket.send(pickle.dumps({"cmd": "heartbeat"}))
        elif msg["cmd"] == "connection_id":
            con_id = msg["data"]
            if con_id == self.connection_info["client_id"]:
                await self.handshake_socket.send(pickle.dumps({"cmd": "connection_id", "data": self.connection_info["server_id"]}))
            else:
                self.log.debug(f"connection_id {con_id} != {self.connection_info['client_id']}")
                self.show_message_log(f"connection_id {con_id} != {self.connection_info['client_id']}")


    async def zmq_recv(self, poller: Poller):
        while self._stop_event is None:
            await asyncio.sleep(0.1)
        stop = self._stop_event

        while not stop.is_set():
            try:
                socks: dict[AsyncSocket, AsyncSocket] = dict(await poller.poll())  # type: ignore
                for socket in socks:
                    buff = await socket.recv()

                    if not self.socket_connected:
                        self.socket_connected = True
                        self.show_message_log(f"connected to IPython")
                        self.log.info("connected to IPython")
                    
                    msg: ToLangServer = pickle.loads(buff)
                    await self.handle_zmq_message(msg)
            
            except Exception as e:
                self.log.error(f"Error in zmq_recv: {str(e)}", exc_info=True)
                await asyncio.sleep(1)  # Prevent tight error loop


    def zmq_send(self, msg:FromLangServer, no_block=False):
        if no_block:
            try:
                msg = pickle.dumps(msg)   # type: ignore
                self.socket.send(msg, zmq.NOBLOCK)
            except:  # noqa: E722
                self.show_message_log("zmq send failed")
            return
        if self.socket_connected:
            # self.show_message_log(f"zmq send {msg}")
            msg = pickle.dumps(msg)    # type: ignore
            self.socket.send(msg)

    def zmq_check_for_update(self):
        if self.socket_connected:
            self.zmq_send({"cmd": "check_for_update", "con_id": self.connection_info["server_id"], "data": 1})
        else:
            self.show_message_log("zmq not connected")





