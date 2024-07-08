#%%
from typing import Union
from calendar import c
from pathlib import Path
from lark import Lark, Token, Tree, Visitor, Transformer, v_args
from dataclasses import dataclass, field
import logging


#%%
from regex import R
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)

# lark_file = Path(__file__).parent.parent / 'dabbler/lsp/duckdb_2.lark'
lark_file = Path(__file__).parent.parent / 'dabbler/lsp/duckdb.lark'

def get_parser():
    sql_grammer = lark_file.read_text()
    sql_parser = Lark(
        sql_grammer,
        parser="lalr",
        # cache=str(lark_cache),
        propagate_positions=True,
        maybe_placeholders=True,
        debug=False,
    )
    return sql_parser

sql_parser = get_parser()
log.debug('Parser loaded')
#%%

sql = """--sql
with cte_with_set_operation as (
    select cte1_c1, cte1_c2, cte1_c3 from zzz
    union all
    select a2,b2,c2 from zzz2
),
cte_with_set_operation2 as MATERIALIZED (
    ((select p1, p2, p3 from pp1)
    union all
    (select p3,p5,p6 from pp2))
),
cte_with_alias_col_names(alias1, alias2, alias3) as (
    select zc1, zc2, zc3 from zzz
), 
cte_with_nested_ctes as (
    with nested_cte as (
        select nc1, nc2, nc3 from nnn
    )
    select c3c1, c3c2, c3c3 from nested_cte
)
select
    root_query_column_ref1,
    def as root_query_col_alias2,
    sum(abc) FILTER (WHERE gg in (select subq_in_select_col_ref1, aaa as subq_in_select_col_alias2 from k)) as root_query_col_alias3,
    ghi as root_query_col_alias4,
    sum(asdasda),
    p.*
from potato p
    join (select join_subq_column_ref1, banana join_subq_column_alias1 from tomato) subquery1 on p.id = subquery1.id

"""



def get_projection(tree:Tree):
    columns:list[str] = []
    for c in tree.find_data('col_exp'):
        aliases = list(c.find_data('alias'))
        if aliases:
            tok = aliases[0].children[0]
            if isinstance(tok, Token):
                columns.append(tok.value)
        else:
            if c.children[0].data == 'col_ref':
                col_ref = c.children[0]
                last_child = col_ref.children[-1]
                if isinstance(last_child, Token):
                    columns.append(last_child.value)
    return columns

class PromoteQueriesInParentheses(Transformer):
    def __init__(self):
        pass

    @v_args(tree=True)
    def query(self, tree:Tree):
        if len(tree.children) == 1 and tree.children[0].data in ['query','set_operation']:
            return tree.children[0]
        return tree

@dataclass
class Cte:
    name:str
    columns:list[str]
    query:'Query'
    sql:str

    def __str__(self):
        return f'{self.name} -> query={self.query}'

@dataclass
class Query:
    start_pos:int
    end_pos:int
    columns:list[str]
    sql:str
    item:int
    ctes:dict[str, Cte] = field(default_factory=dict)

    def __str__(self):
        result = f'Query {self.item} [{self.start_pos}:{self.end_pos}] {self.columns=}'
        if self.ctes:
            for k, v in self.ctes.items():
                result += f'\n\t{v}'
        return result

def drill_down_to_query(tree:Union[Tree, Query]):
    if isinstance(tree, Query):
        return tree
    if isinstance(tree, Tree):
        return drill_down_to_query(tree.children[0])
    raise ValueError(f'Expected Tree or Query, but got: {tree}')


class MakeTree(Transformer):
    def __init__(self, sql:str):
        self.queries:list[Query] = []
        self.i = 0
        self.sql = sql
    
    @v_args(tree=True)
    def query(self, tree:Tree):
        columns = get_projection(tree)

        ctes = self.parse_ctes(tree)        
                    # columns.append(col_refs[0].children[-1].value)
        q = Query(
            start_pos=tree.meta.start_pos,
            end_pos=tree.meta.end_pos,
            columns=columns,
            # sql=self.sql[tree.meta.start_pos:tree.meta.end_pos],
            sql='',
            item=self.i,
            ctes=ctes,
        )
        self.queries.append(q)
        self.i += 1
        return q   
    
    def parse_ctes(self, tree:Tree):
        ctes = {}
        for cte in tree.find_data('cte_expression'):
            cte_name_tok = cte.children[0]
            projection = []
            if isinstance(cte_name_tok, Token):
                cte_name = cte_name_tok.value
            else:
                error_txt = f'CTE name not found for tree starting at position: {cte.meta.start_pos}\n'
                error_txt += f'exected first child to be a Token, but got: {cte_name_tok}\n'
                error_txt += f'cte_expression details: {cte.pretty()}'
                log.error(error_txt)
                continue

            cte_start = cte.meta.start_pos
            cte_end = cte.meta.end_pos
            cte_sql = self.sql[cte_start:cte_end]

            cte_col_alias_token = cte.children[1]
            if isinstance(cte_col_alias_token, Tree) and cte_col_alias_token.data == 'cte_col_alias':
                projection = [a.value for a in cte_col_alias_token.children]    # type: ignore
            else:
                cte_query = cte.children[-1]
                if isinstance(cte_query, Tree):
                    cte_query = drill_down_to_query(cte_query)
                if isinstance(cte_query, Query):
                    query = cte_query
                    projection = cte_query.columns
                else:
                    log.error(f'Problem finding "Query" for cte {cte_name} Expected "Query", but got: {cte_query}')
                    query = None
            
            cte = Cte(
                name=cte_name,
                columns=projection,
                query=query,
                sql=cte_sql
            )

            ctes[cte_name] = cte
        return ctes
        
    
tree = sql_parser.parse(sql)
print(tree.pretty())

query_promoter = PromoteQueriesInParentheses()
mt = MakeTree(sql)
new_tree = query_promoter.transform(tree)
# print(new_tree.pretty())
new_tree = mt.transform(new_tree)   
# new_tree = mt.transform(tree)

for q in mt.queries:
    print(q)
# list(tree.find_data('col_exp'))[6].children[0].data
# %%

