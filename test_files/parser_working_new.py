#%%
from tkinter import N
import duckdb
from dabbler.lsp.new_parser import interactive_parse_new
from dabbler.lsp.parser import SqlParserNew
from dabbler.lsp.server_classes import SqlCompleter
from dabbler.db_stuff import get_db_data_new

from pathlib import Path
import logging
logger = logging.getLogger(__name__)
db = duckdb.connect()
db.sql("create or replace table potato(a int, b int, c int)")
#!%load_ext dabbler.ext_debug

sql = """--sql
with tst_cte as (
    select
        p.a,
        p.b,
        p.c,
    from potato p
    join zz z on z.a = p.a
 )
from tst_cte t
SELECT t.a, t.b, t.c """
sql[len(sql)-1]

class FakeLs:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = duckdb.connect()
        self.db_data = get_db_data_new(self.db)
        self.log = logging.getLogger(__name__)

ls = FakeLs()
p = SqlParserNew(db,None,logger,r'C:\test')
db_data = get_db_data_new(db)
db_data['file_search_path'] = r'C:\test'
c = SqlCompleter(db_data,ls)
# interactive_parse_new(sql, 121)
# %%
r = p.parse_sql(sql,127)
r[0].queries
#%%
len(sql)
a,b,g = c.get_queries(len(sql)-1,sql)
b.queries_list[0].end_pos
#%%
if b.queries_list[1].ctes:
    print('yes')

b.queries_list[1].ctes
#%%
c.parse_sql2(127,sql)
