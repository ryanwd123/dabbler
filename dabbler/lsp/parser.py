import os
import re
from dataclasses import dataclass
from pathlib import Path
from enum import Enum
from lark import Lark, Transformer, v_args, Discard, Visitor, Tree, UnexpectedToken, Token
import duckdb
import logging

lark_cache = Path(__file__).parent.joinpath('lark_cache')
lark_file = Path(__file__).parent.joinpath('sql3b.lark')

def parser_error_handler(e:UnexpectedToken):
    # assert isinstance(e, UnexpectedToken)

    
    if '_AS' in e.accepts and e.token.type == 'RPAREN':
        e.interactive_parser.feed_token(Token('_AS', 'AS'))
        e.interactive_parser.feed_token(Token('NAME', 'placeholder'))
        e.interactive_parser.feed_token(e.token)
        return True
    
    if e.token == Token('$END', '') and 'NAME' in e.accepts:
        e.interactive_parser.feed_token(Token('NAME', 'xyz'))
        return True
    
    if 'NAME' in e.accepts:
        e.interactive_parser.feed_token(Token('NAME', 'xyz'))
        e.interactive_parser.feed_token(e.token)
        return True
    
    # print(e.token)
    # e.interactive_parser.feed_eof() 
    # e.interactive_parser.feed_token(Token('NAME', 'xyz'))
    # e.interactive_parser.feed_token(e.token)
    return False


def get_parser():
    sql_grammer = lark_file.read_text()
    sql_parser = Lark(
        sql_grammer,
        parser="lalr",
        cache=str(lark_cache),
        propagate_positions=True,
        maybe_placeholders=True,
        debug=False,
    )
    return sql_parser

sql_parser = get_parser()

@dataclass
class Cte:
    name: str
    sql: str
    projection: list[str]
    cte_start: int
    self_start: int


@dataclass
class Ctes:
    sql: str
    map: dict[str, Cte]
    start_pos: int
    end_pos: int
    parent: "Query" = None


@dataclass
class Query:
    ctes: Ctes
    sql: str
    projection: list[str]
    from_refs: dict[str, 'FromRef']
    start_pos: int
    end_pos: int
    cte_sibblings: dict[str, Cte]
    set_operation:bool


class FromRefKind(Enum):
    table = 0
    table_function = 1
    subquery = 2


@dataclass
class FromRef:
    kind: FromRefKind
    alias: str
    name: str
    sql: str = None
    start_pos: int = None
    projection: list[tuple[str,str]] = None


class TransformFromClause(Transformer):
    def __init__(self, sql: str, parser:'SqlParserNew') -> None:
        super().__init__()
        self.sql = sql
        self.parser = parser
        self.map = {}

    def db_table(self, value):
        if value[1]:
            alias = value[1]
        else:
            alias = value[0]

        self.map[alias] = FromRef(kind=FromRefKind.table, alias=alias, name=value[0])
        # return {alias:FromRef(kind=FromRefKind.table,alias=alias,name=value[0])}

    def join_type(self, value):
        return Discard

    def table_name(self, value):
        tbl = ".".join([x.value for x in value if x])
        return tbl

    def alias(self, value):
        return value[0].value

    def table_function(self, value):
        if value[1]:
            
            sql = self.sql[value[0].meta.start_pos : value[0].meta.end_pos]
            
            self.map[value[1]] = FromRef(
                kind=FromRefKind.table_function,
                alias=value[1],
                name=value[0],
                sql=sql,
                projection=self.parser.db_describe_columns(f'select * from {sql}')
            )
            # self.sql[value[0].meta.start_pos:value[0].meta.end_pos]

    @v_args(tree=True)
    def subquery(self, tree: Tree):
        value = tree.children
        if value[1]:
            alias = value[1]
            sql = value[0]
            self.map[alias] = FromRef(
                kind=FromRefKind.subquery,
                alias=alias,
                name=sql,
                sql=sql,
                start_pos=tree.meta.start_pos+1,
            )
            # return {alias:sql}
        else:
            return Discard


class QueryToSql(Transformer):
   def __init__(self, sql: str) -> None:
      super().__init__()
      self.sql = sql

   @v_args(tree=True)
   def query(self, value: Tree):
      sql = self.sql[value.meta.start_pos : value.meta.end_pos]
      return sql


class GetQueries(Visitor):
   def __init__(self, sql: str, parser:'SqlParserNew',pos) -> None:
      super().__init__()
      self.sql = sql
      self.parser = parser
      self.queries: dict[int, Query] = {}
      self.queries_list: list[Query] = []
      self.q_start_end:list[tuple[int,int]] = []
      self.pos = pos

   def get_from_tables(self, tree: Tree):
      s1 = QueryToSql(self.sql).transform(tree)
      fr = TransformFromClause(self.sql,self.parser)
      fr.transform(s1).pretty()
      return fr.map

   def get_projections(self, tree: Tree):
      col_names:list[str] = []
      for col in tree.children:
         # print(col)
         if len(col.children) < 2:
            continue
         if col.children[1]:
            col_names.append(col.children[1].children[0].value)
            continue
         if col.children[0].data == 'col_ref':
            col_names.append(col.children[0].children[1].value)
            continue
      return [(x,None) for x in col_names]

   def get_ctes(self, cte: Tree):
      if not cte:
         return
      if cte.data != "cte":
        #  print(cte.pretty())
         raise Exception("not cte")

      cte_map = {}

      for x in cte.children:
         if not isinstance(x, Tree):
               continue
         name = x.children[0].value

         q = self.get_query(x.children[2])

         c = Cte(
               name=name,
               sql=self.sql[x.children[2].meta.start_pos : x.children[2].meta.end_pos],
               projection=q.projection,
               cte_start=cte.meta.start_pos,
               self_start=x.children[2].meta.start_pos,
         )

         cte_map[name] = c

      cte_sql_txt = self.sql[cte.meta.start_pos : cte.meta.end_pos]

      return Ctes(
         sql=cte_sql_txt,
         map=cte_map,
         start_pos=cte.meta.start_pos,
         end_pos=cte.meta.end_pos,
      )

   def get_query(self, tree: Tree):
      cte_data = None
      set_op = False
      cols = []
      if tree.children[0]:
         if tree.children[0].data == "query" and tree.data == 'query':
            q = self.get_query(tree.children[0])
            return q
         if tree.data == "set_operation":
            set_op = True
            q1 = self.get_query(tree.children[0])
            cols = q1.projection
            
      
         if tree.children[0].data == "cte":
            cte_data = self.get_ctes(tree.children[0])
            # print(tree.pretty())
            # raise Exception("not cte")
      # print(cte_data)
      sql_txt = self.sql[tree.meta.start_pos : tree.meta.end_pos]
      from_refs = {}
      for x in tree.children:
         # print(x)
         if not isinstance(x, Tree):
               continue
         if x.data == "from_clause":
               # print(2,x.pretty())
               from_refs = self.get_from_tables(x)
               # print(2,from_refs)
         if x.data == "select_clause":
            cols = self.get_projections(x.children[4])
      start = tree.meta.start_pos
      end = tree.meta.end_pos
      q = Query(
         ctes=cte_data,
         sql=sql_txt,
         projection=cols,
         from_refs=from_refs,
         start_pos=start,
         end_pos=end,
         cte_sibblings={},
         set_operation=set_op,
      )
      
      return q
   
   def set_operation(self, tree: Tree):
      self.query(tree)
      
      
   def query(self, tree: Tree):

      q = self.get_query(tree)
      if not q:
         return
      if (q.start_pos,q.end_pos) in self.q_start_end:
         return
      self.q_start_end.append((q.start_pos,q.end_pos))
      self.queries_list.append(q)
      self.queries[q.start_pos] = q


incomplete_col_ref = re.compile('(\w+[.])([\n\s),])')

class SqlParserNew:
    
    def __init__(self, db: duckdb.DuckDBPyConnection = None, ls = None, logger:logging.Logger = None, file_search_path:str = None) -> None:
        self.db = db
        self.projection_cache = {}
        self.ls = ls
        self.file_search_path = file_search_path
        self.log = logger.getChild('sql_parser')
        self.log_describe = logger.getChild('sql_describe')
        self.log_query_output = logger.getChild('query_output')
        self.log_interactive_parser = logger.getChild('interactive_parser')
        

    def show_message(self,msg):
        if self.ls:
            self.ls.show_message_log(msg)



    def parse_sql(self, sql: str, pos:int):

        
        try:
            tree, choices_pos = interactive_parse(sql,pos,self.log_interactive_parser)
            # tree = sql_parser.parse(sql,on_error=parser_error_handler)
        except UnexpectedToken as e:
            self.log.info(['failed to parse, Unexpected Token',sql,e,e.token,e.accepts])
            return None, None
        except Exception as e:
            self.log.info(['failed to parse',sql,e])
            return None, None
            # noqa: E722
        if not tree:
            return None, choices_pos
        queries = GetQueries(sql,self,pos)
        queries.visit(tree)


        #add cte sibblings
        subq_lu = {}
        cte_lu = {}
        for q in queries.queries_list:
            if not q.ctes:
                continue
            for k, v in q.ctes.map.items():
                cte_lu[v.self_start] = v.cte_start
            for k, v in q.from_refs.items():
                if v.kind == FromRefKind.subquery:
                    subq_lu[v.start_pos] = q.start_pos

        for q in queries.queries_list:
            if q.start_pos not in cte_lu:
                continue
            parent_map = queries.queries[cte_lu[q.start_pos]].ctes.map
            sibblings = {k: v for k, v in parent_map.items() if v.self_start < q.start_pos}
            q.cte_sibblings = sibblings

        for q in queries.queries_list:
            if q.start_pos not in subq_lu:
                continue
            q.cte_sibblings = queries.queries[subq_lu[q.start_pos]].ctes.map

        for q in queries.queries_list:
            if q.set_operation:
                for q_in_set in [x for x in queries.queries_list if x.start_pos >= q.start_pos and x.end_pos <= q.end_pos]:
                    q_in_set.cte_sibblings = q.cte_sibblings
        
        
        
        if not self.db:
            # self.show_message(f'no db connection')
            return queries
        
        # self.show_message(f'running db queries')
        
        # cte_and_subq = [cte for k,cte in q.ctes.map.items() for q in queries.queries_list if q.ctes]
        # [subq for k,subq in q.from_refs.items() for q in queries.queries_list if subq.kind == FromRefKind.subquery] 
        
        for q in queries.queries_list:
            cte_sql = ''
            if q.cte_sibblings:
                sibblings = ',\n'.join([f'{k} as ({v.sql})' for k,v in q.cte_sibblings.items()])
                cte_sql = f'with {sibblings}'
            sql = f'{cte_sql}\n{q.sql}'
            if pos >= q.start_pos and pos <= q.end_pos:
                continue
            projection = self.db_describe_columns(sql)
            if projection:
                q.projection = projection
        
        
        for q in queries.queries_list:
            if q.ctes:
                for k,v in q.ctes.map.items():
                    if v.self_start in queries.queries:
                        v.projection = queries.queries[v.self_start].projection
        
        
        self.log_query_output.debug(queries.queries_list)
        return queries, choices_pos
    

    def db_describe_columns(self, sql):
        if sql in self.projection_cache:
            return self.projection_cache[sql]
        
        if incomplete_col_ref.search(f'{sql} '):
            self.log.debug(['skipping describe col, incomplete col ref',sql])
            return

        try:
            db = self.db.cursor()
            if self.file_search_path and len(self.file_search_path)>0:
                db.execute(f"set file_search_path = '{self.file_search_path}';")
            rec = db.execute(f'describe ({sql})').fetchall()
            data = [(x[0],x[1]) for x in rec]
            self.projection_cache[sql] = data
            return data
        except Exception as e:  # noqa: E722
            self.log.debug(['failed to run describe',sql,e,os.getcwd()])
            return
        


check_choices = (
    ('RPAREN', ')'),
    ('NAME', 'placeholder'),
)


def find_end(p):
    choices = list(p.choices().keys())
    if '$END' in choices:
        try:
            return p.feed_eof()
        except:
            pass
    # for typ, value in check_choices:
    #     if typ in choices:
    #         t = Token(typ, value)
    #         print(f'feeding {t}')
    #         p.feed_token(t)
    #         return find_end(p)


def interactive_parse(sql:str,pos:int,logger:logging.Logger):

    p = sql_parser.parse_interactive(sql)
    tokens = p.iter_parse()
    token_history = []
    # tk = next(lex)
    choices_pos = []
    logger.debug(['interactive_parse',sql,pos])
        
    while True:
        token = None
        try:
            token = next(tokens)
        except StopIteration:
            break
        except UnexpectedToken as e:
            print('unexpected token', e.token, e.token.type)
            if e.token == '$END':
                print('end')
                break
            choices = p.choices().keys()
            if 'col_replace' in choices and e.token == ')':
                if not '_AS' in choices:
                    p.feed_token(Token('NAME', 'placeholder'))
                p.feed_token(Token('_AS', 'as'))
                p.feed_token(Token('NAME', 'placeholder'))
                p.feed_token(e.token)
                continue
            
            if 'col_exclude' in choices and e.token == ')':
                p.feed_token(Token('NAME', 'placeholder'))
                p.feed_token(e.token)
                continue
            
        except Exception as e:
            logger.exception(['failed to parse',sql,e])
        
        if not choices_pos and token and token.end_pos == pos:
            choices_pos = list(p.choices().keys())
            print(f'choices, {token}')
        token_history.append(token)
        
    if not choices_pos:
        choices_pos = list(p.choices().keys())
    tree = find_end(p)
    logger.debug({'choices_pos':choices_pos})
    logger.debug({'tree':tree})
    return tree, choices_pos