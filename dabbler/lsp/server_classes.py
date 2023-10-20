import re as regex
import zmq
from dataclasses import dataclass
from zmq.asyncio import Context, Socket as AsyncSocket
from pygls.server import LanguageServer
import pickle
import asyncio
from dabbler.lsp.db_data import make_db, make_completion_map
from dabbler.lsp.sql_utils import strip_sql_whitespace, SelectNode, CmpItem
from dabbler.lsp.completion import duckdb_extensions,duckdb_settings,duckdb_pragmas,duckdb_types
from dabbler.lsp.parser import SqlParserNew
from dabbler.common import FromLangServer, ToLangServer

# from dabbler.lsp.completer import CmpItem 
from lsprotocol.types import (
    CompletionItem,
    CompletionList,
    CompletionItemKind,
)



#%%

class InlineSqlLangServer(LanguageServer):
    CMD_SEND_SQL_TO_GUI = "sendSqlToDbDabbler"
    CMD_FORMAT_CURRENT_STATEMENT = "dbDabblerFormatCurrentStatement"
    CONFIGURATION_SECTION = "pygls.jsonServer"
    

    def __init__(self, *args):
        super().__init__(*args)
        
        self.create_sockets2()
        self.completer:'SqlCompleter' = None
        self.socket_connected = False
        self.socket_created = False
  
        
    
    def create_sockets2(self):
        # self.msg_q = janus.Queue().sync_q
        ctx1 = Context().instance()
        self.socket = ctx1.socket(zmq.PAIR)
        self.socket.connect("tcp://127.0.0.1:55557")

        ctx2 = Context().instance()
        self.handshake_socket = ctx2.socket(zmq.PAIR)    
        self.handshake_socket.bind("tcp://127.0.0.1:55558")
    
        self.loop.create_task(self.zmq_recv(self.socket))
        self.loop.create_task(self.zmq_recv(self.handshake_socket))
        
        self.zmq_send({'cmd':'db_data_update'},True)
    
    
    
    async def zmq_recv(self,socket:AsyncSocket):
        # self.show_message_log('zmq_recv started')
        while self._stop_event is None:
            # self.show_message_log('waiting for stop event to be created')
            asyncio.sleep(0.1)
        stop = self._stop_event
        # self.show_message_log(f'{type(self._stop_event)}')
        
        while not stop.is_set():
            # buff = await self.socket.recv()
            
            if not await socket.poll(100):
                continue
            buff = await socket.recv()
            
            
            if not self.socket_connected:
                self.socket_connected = True    
                self.show_message_log('connected to IPython')
            msg:ToLangServer = pickle.loads(buff)
            # self.show_message_log(f"zmq recv {msg}")
            if msg['cmd'] == 'db_data':
                # self.db_data = read_db_data(msg['data'])
                self.completer = SqlCompleter(msg['data'],self)
                # self.comp_thread_put({'cmd':'new_completer','data':SqlCompleter(msg['data'],None)},0)
                self.show_message_log("recieved db_data")
            
            if msg['cmd'] == 'ip_python_started':
                self.show_message_log(f"ip_python_started = {msg['data']}")
                self.zmq_send({'cmd':'db_data_update'})
            
            if msg['cmd'] == 'ip':
                self.show_message_log(f"ipython event = {msg['data']}")
                
            if msg['cmd'] == 'no_update':
                self.show_message_log("check update: no update")
                
            
    
    def zmq_send(self,msg,no_block=False):
        if no_block:
            try:
                msg = pickle.dumps(msg)
                self.socket.send(msg,zmq.NOBLOCK)
            except:  # noqa: E722
                self.show_message_log("zmq send failed")
            return            
        if self.socket_connected:
            # self.show_message_log(f"zmq send {msg}")
            msg = pickle.dumps(msg)
            self.socket.send(msg)
        
        
    def zmq_check_for_update(self):
        if self.socket_connected:
            self.zmq_send({'cmd':'check_for_update'})
        else:
            self.show_message_log("zmq not connected")



table_types = set(['table','database','schema','cte'])

@dataclass
class PasredItemsCache:
    age:int
    last_line:int
    rng_start:int
    spaces_on_cur_line:int
    items:dict[str,list[CmpItem]]



class SqlCompleter:
      
    def __init__(self,db_data, ls:InlineSqlLangServer = None) -> None:
        self.db = make_db(db_data).cursor()
        self.completion_map = make_completion_map(self.db,db_data)
        self.db_data = db_data
        self.ls = ls
        # self.parsed_times_cache = PasredItemsCache(99,0,0,0,{})
        self.parser2 = SqlParserNew(self.db, self.ls)

  
    def show_message_log(self,msg):
        if self.ls:
            self.ls.show_message_log(msg)
    
    
    
    def get_queries(self, pos, sql):
        queries = self.parser2.parse_sql(sql)
        queries.queries_list.sort(key=lambda x: x.end_pos - x.start_pos)
        if len(queries.queries_list) == 0:
            return None, None
        filtered = [x for x in queries.queries_list if x.start_pos <= pos <= x.end_pos]
        if len(filtered) == 0:
            return None, None
        q = filtered[0]
        return q, queries
    
    
    def parse_sql2(self, pos, sql:str):

        comp_map = {}
        comp_map['root_namespace'] = []
        
        q, queries = self.get_queries(pos, sql)

        for k,v in q.from_refs.items():
            
            if v.kind.name == 'subquery':
                projection = queries.queries[v.start_pos].projection
                comp_map[k] = [CmpItem(x[0],CompletionItemKind.Field,None,x[1],'1','column') for x in projection]
                comp_map['root_namespace'].append(CmpItem(k,CompletionItemKind.File,None,'sub query','1','table'))
                continue
            
            # self.show_message_log(f'{v}')
            if q.ctes and v.name in q.ctes.map:
                comp_map[k] = [CmpItem(x[0],CompletionItemKind.Field,None,x[1],'1','column') for x in q.ctes.map[v.name].projection]
                comp_map['root_namespace'].append(CmpItem(k,CompletionItemKind.File,None,'cte','1','cte'))
                continue
            elif v.name in q.cte_sibblings:
                comp_map[k] = [CmpItem(x[0],CompletionItemKind.Field,None,x[1],'1','column') for x in q.cte_sibblings[v.name].projection]
                comp_map['root_namespace'].append(CmpItem(k,CompletionItemKind.File,None,'cte','1','cte'))
                continue
            else:
                if v.name not in self.completion_map:
                    continue
                comp_map[k] = self.completion_map[v.name]
                comp_map['root_namespace'].append(CmpItem(k,CompletionItemKind.File,None,'table_alias','1','table'))
        
        if q.ctes:    
            for k,v in q.ctes.map.items():
                comp_map[k] = [CmpItem(x[0],CompletionItemKind.Field,None,x[1],'1','column') for x in v.projection]
                comp_map['root_namespace'].append(CmpItem(k,CompletionItemKind.File,None,'cte','1','cte'))
        
        for k, v in q.cte_sibblings.items():
            comp_map[k] = [CmpItem(x[0],CompletionItemKind.Field,None,x[1],'1','column') for x in v.projection]
            comp_map['root_namespace'].append(CmpItem(k,CompletionItemKind.File,None,'cte','1','cte'))

        # self.show_message_log(f'cte_sibblings {q.cte_sibblings}')

        col_to_add = []
        labels_added = []
        
        for k in comp_map['root_namespace']:
            if k in q.from_refs and k in comp_map:
                col_to_add.extend([x for x in comp_map[k] if x.label not in labels_added])
                labels_added.extend([x.label for x in comp_map[k]])
                
        comp_map['root_namespace'].extend(col_to_add)
                
        return comp_map

    def get_comp_map(self,cursor_pos,sql_rng:SelectNode):
        parsed_items = self.parse_sql2(cursor_pos,sql_rng.txt)
        comp_map:dict[str,list[CmpItem]] = {}
        comp_map.update(parsed_items)
        comp_map.update(self.completion_map)
        return comp_map

    
    def route_completion2(self,cursor_pos:int,sql_rng:SelectNode,trigger:str,current_line:int,current_line_txt:str):
        # self.show_message_log(f'route_completion pos:{cursor_pos}, trigger:{trigger}, line:{current_line}, line_txt{current_line_txt}')
        # self.parsed_times_cache.age += 1
        sql_left_of_cur = strip_sql_whitespace(sql_rng.txt[:cursor_pos])
        # self.show_message_log(f'comp_map_size: {sys.getsizeof(self.completion_map)} parser: {sys.getsizeof(self.parser)}, trigger: {trigger}')
        
        if sql_left_of_cur[-2:] == '::':
            return CompletionList(is_incomplete=False, items=duckdb_types)
        
        if regex.match('(^| )(load |install )$',sql_left_of_cur,flags=regex.IGNORECASE):
            return CompletionList(is_incomplete=False, items=duckdb_extensions)

        if regex.match('(^| )(pragma )$',sql_left_of_cur,flags=regex.IGNORECASE):
            return CompletionList(is_incomplete=False, items=duckdb_pragmas)

        if regex.match('(^| )(set |reset )$',sql_left_of_cur,flags=regex.IGNORECASE):
            return CompletionList(is_incomplete=False, items=duckdb_settings+[CompletionItem(label=c) for c in ['LOCAL', 'SESSION', 'GLOBAL']])

        if regex.match('(^| )(set |reset )(local | session | global )$',sql_left_of_cur,flags=regex.IGNORECASE):
            return CompletionList(is_incomplete=False, items=duckdb_settings)

        try:
            parsed_items:dict[str,list[CmpItem]] = self.parse_sql2(cursor_pos,sql_rng.txt)
        except Exception as e:
            # self.show_message_log(f'parser_error {e}')
            parsed_items = {'root_namespace':[]}
        # self.show_message_log(f'{parsed_items}')
        # comp_map:dict[str,list[CmpItem]] = {}
        # comp_map.update(parsed_items)
        # comp_map.update(self.completion_map.copy())
        # comp_map['root_namespace'].extend(parsed_items['root_namespace'])
        comp_map = self.completion_map
        
        if regex.match('.*(^| )(join |from |pivot |unpivot |alter table )(\w+( \w+)?, )*\w?$',sql_left_of_cur,flags=regex.IGNORECASE):
            tbls = [x.comp for x in comp_map['root_namespace'] if x.obj_type in table_types]
            tbls += [x.comp for x in parsed_items['root_namespace'] if x.obj_type in table_types]
            # ls.show_message_log(f'join {tbls}')
            return CompletionList(is_incomplete=False, items=tbls)
        
        if trigger == ' ':
            return
        
        m = regex.match('.*(^| )(join |from |pivot |unpivot |alter table )(\w+( \w+)?, )*(?P<dotitems>(\w+\.)+)$',sql_left_of_cur,flags=regex.IGNORECASE)
        if m and trigger == '.':
            key = m.group('dotitems').strip('.')
            if key not in comp_map:
                return None
            items = [x.comp for x in comp_map[key] if x.obj_type in table_types]
            items += [x.comp for x in parsed_items[key] if x.obj_type in table_types]
            return CompletionList(is_incomplete=False, items=items)
        
        

        m = regex.match('.*(^| |\()(?P<dotitems>(\w+\.)+)$',sql_left_of_cur,flags=regex.IGNORECASE)
        if m and trigger == '.':
            key = m.group('dotitems').strip('.')
            items = []
            if key in comp_map:
                items += [x.comp for x in comp_map[key]]
            if key in parsed_items:
                items += [x.comp for x in parsed_items[key]]
            if len(items) > 0:
                return CompletionList(is_incomplete=False, items=items)
        
        
        m = regex.match('.*(^| |\()(?P<char>(\w+))$',sql_left_of_cur,flags=regex.IGNORECASE)
        if m:
            # comp_items = []
            # comp_items.extend(self.completion_map['root_namespace'])
            # comp_items.extend(parsed_items['root_namespace'])
            chars = m.group('char').lower()
            comp_items = [x for x in comp_map['root_namespace']]
            comp_items += parsed_items['root_namespace']
            return CompletionList(is_incomplete=True, items=[x.comp for x in comp_items if x.label.lower().startswith(chars)])
        
        return None
    
    
    
    
    
    
    
    
    
    