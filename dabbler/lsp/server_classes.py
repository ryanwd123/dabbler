import os
import sys
import logging
from pathlib import Path
from logging.handlers import SocketHandler
from functools import partial
import re as regex
import zmq
from dataclasses import dataclass
from zmq.asyncio import Context, Socket as AsyncSocket, Poller
from pygls.server import LanguageServer
import pickle
import asyncio
from dabbler.lsp.db_data import make_db, make_completion_map
from dabbler.lsp.sql_utils import strip_sql_whitespace, SelectNode, CmpItem
from dabbler.lsp.completion import (
    PathCompleter,
    duckdb_extensions,
    duckdb_settings,
    duckdb_pragmas,
    duckdb_types,
    file_path_completion_regex,
)
from dabbler.lsp.parser import SqlParserNew
from dabbler.common import FromLangServer, ToLangServer, KeyFile, PprintSocketHandler, grammer_kw

# from dabbler.lsp.completer import CmpItem
from lsprotocol.types import (
    CompletionItem,
    CompletionList,
    CompletionItemKind,
)


# %%


class InlineSqlLangServer(LanguageServer):
    CMD_SEND_SQL_TO_GUI = "sendSqlToDbDabbler"
    CMD_FORMAT_CURRENT_STATEMENT = "dbDabblerFormatCurrentStatement"
    CONFIGURATION_SECTION = "pygls.jsonServer"

    def __init__(self, *args):
        super().__init__(*args)

        self.log = logging.getLogger("dabbler_lsp")
        self.debug = False
        self.ctx = Context()
        self.create_sockets2()

        self.completer: "SqlCompleter" = None
        self.socket_connected = False
        self.socket_created = False
        self.pathlibs_paths = {}
        self.key_file: KeyFile = None

    def create_default_compelter(self):
        if not self.completer is None:
            return
        blank_db_data = {
            'data':[],
            'dataframes':[],
            'databases':[],
            'functions':[],
            'paths':[],
            'schemas':[],
            'current_schema':'memory.main',
            'cwd':self.workspace.root_path,
            'file_search_path':None,
        }
        self.completer = SqlCompleter(blank_db_data, self)

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

        self.zmq_send({"cmd": "db_data_update","con_id":self.connection_info['server_id']})

    def initialize_sql_completer(self, data: dict):
        self.completer = SqlCompleter(data, self)
        self.pathlibs_paths = {x[0]:x[1] for x in data["paths"]}
        self.show_message_log("SqlCompleter initialized")

    async def handle_zmq_message(self, msg: ToLangServer):
        if msg["cmd"] == "db_data":
            self.show_message_log("received db_data")
            await self.loop.run_in_executor(
                self.thread_pool_executor,
                partial(self.initialize_sql_completer, msg["data"])
            )
        elif msg["cmd"] == "ip_python_started":
            self.show_message_log(f"ip_python_started = {msg['data']}")
            self.zmq_send({"cmd": "db_data_update"})
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
                socks: dict[AsyncSocket, AsyncSocket] = dict(await poller.poll())
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
                msg = pickle.dumps(msg)
                self.socket.send(msg, zmq.NOBLOCK)
            except:  # noqa: E722
                self.show_message_log("zmq send failed")
            return
        if self.socket_connected:
            # self.show_message_log(f"zmq send {msg}")
            msg = pickle.dumps(msg)
            self.socket.send(msg)

    def zmq_check_for_update(self):
        if self.socket_connected:
            self.zmq_send({"cmd": "check_for_update"})
        else:
            self.show_message_log("zmq not connected")


table_types = set(["table", "database", "schema", "cte"])


@dataclass
class PasredItemsCache:
    age: int
    last_line: int
    rng_start: int
    spaces_on_cur_line: int
    items: dict[str, list[CmpItem]]


class SqlCompleter:
    def __init__(self, db_data, ls: InlineSqlLangServer = None) -> None:
        self.db = make_db(db_data)
        self.completion_map = make_completion_map(self.db, db_data)
        self.db_data = db_data
        if Path(db_data["cwd"]).is_dir():
            os.chdir(db_data["cwd"])
        self.path_completer = PathCompleter(db_data["cwd"], db_data["file_search_path"],ls.log.getChild('path_completer'))
        self.ls = ls
        self.file_search_path = db_data["file_search_path"]
        self.log = ls.log.getChild("completer")
        self.log_comp_map = self.log.getChild("comp_map")
        # self.parsed_times_cache = PasredItemsCache(99,0,0,0,{})
        self.parser2 = SqlParserNew(self.db, self.ls, ls.log,self.file_search_path)

    def show_message_log(self, msg):
        if self.ls:
            self.ls.show_message_log(msg)

    def get_queries(self, pos, sql):
        queries, choices_pos = self.parser2.parse_sql(sql, pos)
        if not queries:
            return None, None, choices_pos
        queries.queries_list.sort(key=lambda x: x.end_pos - x.start_pos)
        if len(queries.queries_list) == 0:
            return None, None, choices_pos
        filtered = [x for x in queries.queries_list if x.start_pos <= pos <= x.end_pos]
        if len(filtered) == 0:
            return queries.queries_list[-1], queries, choices_pos
        q = filtered[0]
        return q, queries, choices_pos

    def parse_sql2(self, pos, sql: str):
        comp_map = {}
        comp_map["root_namespace"] = []

        q, queries, choices_pos = self.get_queries(pos, sql)
        
        if choices_pos:
            kw_comps = []
            for c in choices_pos:
                if c in grammer_kw:
                    kw_comps.append(
                        CmpItem(c, CompletionItemKind.Keyword, None, "keyword", "3", "keyword")
                    )
            
            comp_map["root_namespace"].extend(kw_comps)
        
        
        if q is None:
            # self.log_comp_map.debug(comp_map)
            return comp_map
        for k, v in q.from_refs.items():
            if v.kind.name == "subquery":
                projection = queries.queries[v.start_pos].projection
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "sub query", "1", "table")
                )
                continue
            
            if v.kind.name == "table_function":
                projection = v.projection
                if not projection:
                    continue
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "table function", "1", "table")
                )
                continue
            
            # self.show_message_log(f'{v}')
            if q.ctes and v.name in q.ctes.map:
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in q.ctes.map[v.name].projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
                )
                continue
            elif v.name in q.cte_sibblings:
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in q.cte_sibblings[v.name].projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
                )
                continue
            else:
                if v.name not in self.completion_map:
                    continue
                comp_map[k] = self.completion_map[v.name]
                comp_map["root_namespace"].append(
                    CmpItem(
                        k, CompletionItemKind.File, None, "table_alias", "1", "table"
                    )
                )

        if q.ctes:
            for k, v in q.ctes.map.items():
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in v.projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
                )

        for k, v in q.cte_sibblings.items():
            comp_map[k] = [
                CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                for x in v.projection
            ]
            comp_map["root_namespace"].append(
                CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
            )

        # self.show_message_log(f'cte_sibblings {q.cte_sibblings}')

        col_to_add = []
        labels_added = []

        for k in comp_map["root_namespace"]:
            if k in q.from_refs and k in comp_map:
                col_to_add.extend(
                    [x for x in comp_map[k] if x.label not in labels_added]
                )
                labels_added.extend([x.label for x in comp_map[k]])

        comp_map["root_namespace"].extend(col_to_add)

        # self.log_comp_map.debug(comp_map)
        

        
        return comp_map

    # def get_comp_map(self, cursor_pos, sql_rng: SelectNode):
    #     parsed_items = self.parse_sql2(cursor_pos, sql_rng.txt)
    #     comp_map: dict[str, list[CmpItem]] = {}
    #     comp_map.update(parsed_items)
    #     comp_map.update(self.completion_map)
    #     return comp_map

    def route_completion2(
        self,
        cursor_pos: int,
        sql_rng: SelectNode,
        trigger: str,
        current_line: int,
        current_line_txt: str,
    ):
        # self.show_message_log(f'route_completion pos:{cursor_pos}, trigger:{trigger}, line:{current_line}, line_txt{current_line_txt}')
        # self.parsed_times_cache.age += 1
        sql_left_of_cur = strip_sql_whitespace(sql_rng.txt[:cursor_pos])
        # self.show_message_log(f'comp_map_size: {sys.getsizeof(self.completion_map)} parser: {sys.getsizeof(self.parser)}, trigger: {trigger}')

        if sql_left_of_cur[-2:] == "::":
            return CompletionList(is_incomplete=False, items=duckdb_types)

        if regex.match(
            "(^| )(load |install )$", sql_left_of_cur, flags=regex.IGNORECASE
        ):
            return CompletionList(is_incomplete=False, items=duckdb_extensions)

        if regex.match("(^| )(pragma )$", sql_left_of_cur, flags=regex.IGNORECASE):
            return CompletionList(is_incomplete=False, items=duckdb_pragmas)

        if regex.match("(^| )(set |reset )$", sql_left_of_cur, flags=regex.IGNORECASE):
            return CompletionList(
                is_incomplete=False,
                items=duckdb_settings
                + [CompletionItem(label=c) for c in ["LOCAL", "SESSION", "GLOBAL"]],
            )

        if regex.match(
            "(^| )(set |reset )(local | session | global )$",
            sql_left_of_cur,
            flags=regex.IGNORECASE,
        ):
            return CompletionList(is_incomplete=False, items=duckdb_settings)

        file_comp = file_path_completion_regex.match(sql_left_of_cur)
        if file_comp:
            items = self.path_completer.get_items(file_comp.group(2))
            return CompletionList(is_incomplete=False, items=items)


        try:
            parsed_items: dict[str, list[CmpItem]] = self.parse_sql2(
                cursor_pos, sql_rng.txt
            )
        except Exception as e:
            # self.log.info(["parsed_items_error", e,sys.exception().__traceback__])
            # self.log.debug('parsed_items_error')

            parsed_items = {"root_namespace": []}
        # self.show_message_log(f'{parsed_items}')
        # comp_map:dict[str,list[CmpItem]] = {}
        # comp_map.update(parsed_items)
        # comp_map.update(self.completion_map.copy())
        # comp_map['root_namespace'].extend(parsed_items['root_namespace'])
        comp_map = self.completion_map

        if regex.match(
            ".*(^| )(join |from |pivot |unpivot |alter table |insert into )(\w+( \w+)?, )*\w?$",
            sql_left_of_cur,
            flags=regex.IGNORECASE,
        ):
            tbls = [
                x.comp for x in comp_map["root_namespace"] if x.obj_type in table_types
            ]
            tbls += [
                x.comp
                for x in parsed_items["root_namespace"]
                if x.obj_type in table_types
            ]
            # ls.show_message_log(f'join {tbls}')
            return CompletionList(is_incomplete=False, items=tbls)

        if trigger == " ":
            return

        m = regex.match(
            ".*(^| )(join |from |pivot |unpivot |alter table |insert into )(\w+( \w+)?, )*(?P<dotitems>(\w+\.)+)$",
            sql_left_of_cur,
            flags=regex.IGNORECASE,
        )
        if m and trigger == ".":
            key = m.group("dotitems").strip(".")
            items = []
            if key in comp_map:
                items = [x.comp for x in comp_map[key] if x.obj_type in table_types]
            if key in parsed_items:
                items += [
                    x.comp for x in parsed_items[key] if x.obj_type in table_types
                ]
            return CompletionList(is_incomplete=False, items=items)

        m = regex.match(
            ".*(^| |\()(?P<dotitems>(\w+\.)+)$", sql_left_of_cur, flags=regex.IGNORECASE
        )
        if m and trigger == ".":
            key = m.group("dotitems").strip(".")
            items = []
            if key in comp_map:
                items += [x.comp for x in comp_map[key]]
            if key in parsed_items:
                items += [x.comp for x in parsed_items[key]]
            if len(items) > 0:
                return CompletionList(is_incomplete=False, items=items)

        m = regex.match(
            ".*(^| |\()(?P<char>(\w+))$", sql_left_of_cur, flags=regex.IGNORECASE
        )
        if m:
            # comp_items = []
            # comp_items.extend(self.completion_map['root_namespace'])
            # comp_items.extend(parsed_items['root_namespace'])
            chars = m.group("char").lower()
            comp_items = [x for x in comp_map["root_namespace"]]
            comp_items += parsed_items["root_namespace"]
            return CompletionList(
                is_incomplete=True,
                items=[x.comp for x in comp_items if x.label.lower().startswith(chars)],
            )

        return None
