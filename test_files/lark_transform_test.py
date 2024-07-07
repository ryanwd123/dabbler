#%%
from itertools import tee
from dabbler.lsp.parser import interactive_parse
from pprint import pprint
from lark import Visitor, Transformer, v_args, Tree, Discard, ast_utils
from dataclasses import dataclass
import logging
logger = logging.getLogger()

import duckdb
db = duckdb.connect()
#!%load_ext dabbler.ext
#%%

sql = """--sql
with 
cte_aaa as MATERIALIZED (select aa, b * 5 as gg, cc, id from table1),
cte_bbb as (
        (select i, j, k from table2)
        union all 
        (select i2, j2, k2 from table3)
        )
from cte_aaa a
SELECT a.aa, a.gg

"""

tree, choices = interactive_parse(sql,len(sql)-1,logger)
# %%
print(tree.pretty())



#%%
@dataclass
class Query:
    query:Tree
    start:int
    end:int
    sql:str

class GetColumns(Transformer):
    def __init__(self):
        pass

    @v_args(tree=True)
    def col_exp(self, tree:Tree):
        print(tree.pretty())
        return Discard

class GetAlias(Visitor):
    def alias(self, tree:Tree):
        print(tree.pretty())

class GetQueries(Visitor):
    def __init__(self,sql:str):
        self.queries:list[Query] = []
        self.sql = sql
    # @v_args(meta=True)
    def query(self, tree:Tree):
        start = tree.meta.start_pos
        end = tree.meta.end_pos
        self.queries.append(Query(tree, start, end, self.sql[start:end]))
        # GetColumns().transform(tree)

    def col_exp(self, tree:Tree):
        # print(tree.pretty())
        print([x for x in tree.find_data('alias')])

    def cte_expression(self, tree:Tree):
        print(str(tree.children[0]))
        print(str(tree.children[3].meta.start_pos))



c = GetQueries(sql)
c.visit(tree)
c.queries[0].end
c.queries[0].start
c.queries[0].sql

#%%
for q in c.queries:
    print(f'{q.start}:{q.end}', q.sql[:25])

# %%
