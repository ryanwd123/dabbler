from pathlib import Path
import tempfile
import json
import uuid
import duckdb
from typing import Literal, TypedDict, Union
from logging.handlers import SocketHandler
import pprint
import pickle
import struct
import re
from dabbler.lsp.parser import lark_file

def get_grammer_kw():
    grammer_txt = lark_file.read_text()
    # reg = re.compile(r'''([A-Z_]+)\s*:\s*"[A-Z_]+"i''')
    reg = re.compile(r'''([A-Z_]+(?:\.\d+)?)\s*:\s*"[A-Z_]+"i''')
    return reg.findall(grammer_txt)


duckdb_keyworkds = set(x[0].upper() for x in duckdb.execute("select keyword_name from duckdb_keywords() where keyword_category = 'reserved'").fetchall())
# duckdb_keyworkds_all = set(x[0].upper() for x in duckdb.execute("select keyword_name from duckdb_keywords()").fetchall())
duckdb_keyworkds.update([
    'DATE',
    ])


grammer_kw = get_grammer_kw()



def check_name(col:str):
    if col:
        if (' ' in col or
            '-' in col or
            '/' in col or
            '.' in col or
            col.upper() in duckdb_keyworkds):
            return f'"{col}"'
        return col
    return ''



class PprintSocketHandler(SocketHandler):
    def makePickle(self, record):
        """
        Pickles the record in binary format with a length prefix, and
        returns it ready for transmission across the socket.
        """
        ei = record.exc_info
        if ei:
            # just to get traceback text into record.exc_text ...
            dummy = self.format(record)
        # See issue #14436: If msg or args are objects, they may not be
        # available on the receiving end. So we convert the msg % args
        # to a string, save it as msg and zap the args.
        d = dict(record.__dict__)
        # d['msg'] = record.getMessage()
        d['msg'] = pprint.pformat(record.msg)
        d['args'] = None
        d['exc_info'] = None
        # Issue #25685: delete 'message' if present: redundant with 'msg'
        d.pop('message', None)
        s = pickle.dumps(d, 1)
        slen = struct.pack(">L", len(s))
        return slen + s

class FromLangServer(TypedDict):
    cmd: Literal['run_sql', 'db_data_update', 'check_for_update','connection_id','heartbeat']
    data: Union[str,dict,int]
    con_id: int
    

class ToLangServer(TypedDict):
    cmd: Literal['db_data','ip','no_update','connection_id','ip_python_started','debug','heartbeat', 'run_sql_complete','not_handled']
    data: Union[str,dict,int,bool]
    con_id: int
    
class ConnInfo(TypedDict):
    workspace_path: str
    main_port: int
    handshake_port: int
    server_id: int
    client_id: int

class ConnInfoFromServer(TypedDict):
    workspace_path: str
    main_port: int
    handshake_port: int


class Connections(TypedDict):
    connections: dict[str,ConnInfo]


class KeyFile:
    def __init__(self) -> None:
        self.file = Path(tempfile.gettempdir()) / 'dabbler_key.json'
        if self.file.exists():
            self.read()
        else:
            self.connections = {}
            self.save()
    
    def save(self):
        self.file.write_text(json.dumps(self.connections))
    
    def read(self):
        self.connections = json.loads(self.file.read_text())
        
    def add_connection(self, name: str, conn_info_from_server: ConnInfoFromServer):
        con_info:ConnInfo = {
            "workspace_path": conn_info_from_server['workspace_path'],
            "main_port": conn_info_from_server['main_port'],
            "handshake_port": conn_info_from_server['handshake_port'],
            "client_id": uuid.uuid4().int,
            "server_id": uuid.uuid4().int,
        }
        self.connections[name] = con_info
        self.save()
        return con_info

    def delete_connection(self, name: str):
        del self.connections[name]
        self.save()
        
    def get_connection(self, file: Path) -> Union[ConnInfo,None]:
        workspaces = [Path(x) for x in self.connections if Path(x).exists() and Path(x) in file.parents]
        if len(workspaces) == 0:
            return None
        workspaces.sort(key=lambda x: file.parents.index(x))
        workspace = workspaces[0]
        return self.connections[str(workspace)]

    