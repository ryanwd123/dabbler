#%%
from typing import Union, Literal
from pathlib import Path
from lark import Lark, Token, Tree, Transformer, v_args
from dataclasses import dataclass, field
import logging
import duckdb
import json

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
    select cte1_c1, cte1_c2, cte1_c3 from zzz z(a,b,c)
    union all
    select a2,b2,c2 from tbl_macro() q join tbl_macro2(a,b,c) q2(a,b,c) on q.id = q2.id
),
cte_with_set_operation2 as MATERIALIZED (
    ((select p1, p2, p3 from pp1)
    union all
    (select p3,p5,p6 from pp2))
    union all
    select p3b ,p5b ,p6b from pp3
),
cte_with_alias_col_names(alias1, alias2, alias3) as (
    select zc1, zc2, zc3, col_not_aliases from zzz
), 
cte_with_nested_ctes as (
    with nested_cte as (
        select nc1, nc2, nc3 from nnn
    )
    select c3c1, c3c2, c3c3 from nested_cte
),
normal_cte as (
    select nc1, nc2, nc3 from nnn
),
star_cte as (
    select * from normal_cte
),
star_cte2 as (
    select * from star_cte
)
select
    root_query_column_ref1,
    def as root_query_col_alias2,
    sum(abc) FILTER (WHERE gg in (select subq_in_select_col_ref1, aaa as subq_in_select_col_alias2 from k)) as root_query_col_alias3,
    ghi as root_query_col_alias4,
    sum(asdasda),
    p.*,
from potato p 
    join cte_with_nested_ctes c on c.c3c1 = p.id
    join duckdb_columns() d on d.id = p.id
    join normal_cte n on n.nc1 = p.id
    join (select join_subq_column_ref1, banana join_subq_column_alias1 from tomato) subquery1(a,b) on p.id = subquery1.id
    join ((select join_subq_column_ref2, banana join_subq_column_alias2 from tomato2)) subquery2 on p.id = subquery2.id
where 
    exists (SELECT c.* EXCLUDE (alias1) FROM cte_with_alias_col_names c WHERE c.alias1 = p.id)
    
"""

r = duckdb.execute("select json_serialize_sql(?::VARCHAR)", [sql]).fetchone()[0] # type: ignore
r = json.loads(r)
if 'error' not in r or r['error'] == True:
    raise Exception(f'Error parsing SQL: {r}')

@dataclass
class FromRef:
    alias:str
    names:Union[list[str], None]
    subquery:Union['Query', None]
    ref_type:Literal['table', 'subquery', 'table_macro', 'values']
    col_alias:Union[list[str],None] = None

    def projection(self, db=None):
        if self.subquery:
            return self.subquery.projection(db)
        return ['get_from_sqlite']


@dataclass
class Alias:
    ident:str
    end_pos:int


class PromoteQueriesInParentheses(Transformer):
    def __init__(self):
        pass

    @v_args(tree=True)
    def query(self, tree:Tree):
        if len(tree.children) == 1 and tree.children[0].data in ['query','set_operation']:
            return tree.children[0]
        return tree

@dataclass
class Query:
    start_pos:int
    end_pos:int
    columns:list['ColRef']
    aliases:list[Alias]
    from_refs:list[FromRef]
    where_subq:list['Query']
    ctes_before:list['Query']
    item:int
    parent:Union['Query', None] = None
    col_aliases:Union[list[str], None] = None
    is_subquery:bool = False
    sub_query_alias:Union[str, None] = None
    is_cte:bool = False
    cte_start:int = 0
    cte_end:int = 0
    cte_name:Union[str, None] = None
    set_operation_sibling:Union['Query', None] = None
    ctes:dict[str, 'Query'] = field(default_factory=dict)

    def __str__(self):
        result = f'Query {self.item} [{self.start_pos}:{self.end_pos}] {self.columns=}'
        if self.ctes:
            for k, v in self.ctes.items():
                result += f'\n\t{v}'
        return result
    
    @property
    def tbl_completions(self):
        comps:dict[str,Union[Query,FromRef]] = {}
        for k, v in self.ctes.items():
            comps[k] = v
        if self.parent:
            for x in self.parent.from_refs:
                if x.alias in comps:
                    continue
                comps[x.alias] = x
            for k, v in self.parent.ctes.items():
                if k in comps:
                    continue
                comps[k] = v

        if self.ctes_before:
            for cte in self.ctes_before:
                if cte.cte_name is None or cte.cte_name in comps:
                    continue
                comps[cte.cte_name] = cte
        
        return comps
    
    def projection(self, db=None):
        cols = [c.name for c in self.columns if not c.star and c.name]
        stars = [c for c in self.columns if c.star]
        for star in stars:
            comps = self.tbl_completions
            if star.star_table_ref:
                tbl = [x for x in self.from_refs if x.alias == star.star_table_ref]
                if not tbl:
                    continue
                tbl = tbl[0]
                if tbl.alias in comps and tbl.names:
                    projection = comps[tbl.names[-1]].projection(db)
                    cols.extend([x for x in projection if x not in star.star_exlude])
            else:
                for tbl in self.from_refs:
                    if tbl.alias in comps:
                        projection = comps[tbl.alias].projection(db)
                        cols.extend([x for x in projection if x not in star.star_exlude])
                    else:
                        projection = tbl.projection(db)
                        cols.extend([x for x in projection if x not in star.star_exlude])  
        if self.col_aliases:
            for i, col_alias in enumerate(self.col_aliases):
                if i < len(cols):
                    cols[i] = col_alias                  

        return cols

    

    
@dataclass
class ColRef:
    name:Union[str, None] = None
    star:bool = False
    star_table_ref:Union[FromRef, None] = None
    star_exlude:list[str] = field(default_factory=list)

    def __repr__(self):
        if self.star:
            if self.star_table_ref:
                return f'{self.star_table_ref}.*'
            return '*'
        if self.name:
            return self.name
        return ''
        

def get_projection(tree:Tree):
    columns:list[ColRef] = []
    aliases_return:list[Alias] = []

    for c in tree.find_data('col_exp'):
        aliases = list(c.find_data('alias'))
        if aliases:
            tok = aliases[0].children[0]
            alias = aliases[0]
            alias.meta.end_pos
            if isinstance(tok, Token):
                columns.append(tok.value)
                if isinstance(alias, Tree):
                    aliases_return.append(Alias(ident=tok.value, end_pos=alias.meta.end_pos))
                    columns.append(ColRef(name=tok.value))
                    continue
        if c.children[0].data == 'col_ref':
            col_ref = c.children[0]
            last_child = col_ref.children[-1]
            if isinstance(last_child, Token):
                columns.append(ColRef(name=last_child.value))
                continue

        if c.children[0].data == 'star_options':
            star_table_ref=None
            excludes = []
            star_opt = c.children[0]
            if star_opt.children[0]:
                table_ref_tok = star_opt.children[0]
                if isinstance(table_ref_tok, Token):
                    star_table_ref = table_ref_tok.value

            for c in star_opt.find_data('col_exclude'):
                if c.children:
                    tok = c.children[0]
                    if isinstance(tok, Token):
                        excludes.append(tok.value)
            columns.append(ColRef(star=True, star_exlude=excludes, star_table_ref=star_table_ref))


    return columns, aliases_return

def drill_down_to_query(tree:Union[Tree, Query]):
    if isinstance(tree, Query):
        return tree
    if isinstance(tree, Tree):
        return drill_down_to_query(tree.children[0])
    raise ValueError(f'Expected Tree or Query, but got: {tree}')


def get_alias_value(tree:Tree):
    if tree is None:
        return None
    if isinstance(tree, Tree) and tree.data == 'alias':
        if isinstance(tree.children[0], Token):
            return tree.children[0].value
    return None


def get_cte(cte:Tree):
    cte_name_tok = cte.children[0]
    query = None
    cte_name = None

    if isinstance(cte_name_tok, Token):
        cte_name = cte_name_tok.value
    else:
        error_txt = f'CTE name not found for tree starting at position: {cte.meta.start_pos}\n'
        error_txt += f'exected first child to be a Token, but got: {cte_name_tok}\n'
        error_txt += f'cte_expression details: {cte.pretty()}'
        log.error(error_txt)
        return None

    cte_start = cte.meta.start_pos
    cte_end = cte.meta.end_pos

    cte_query = cte.children[-1]
    if isinstance(cte_query, Tree):
        cte_query = drill_down_to_query(cte_query)
    if isinstance(cte_query, Query):
        query = cte_query
    else:
        log.error(f'Problem finding "Query" for cte {cte_name} Expected "Query", but got: {cte_query}')
        query = None

    col_alias = None
    cte_col_alias_token = cte.children[1]
    if isinstance(cte_col_alias_token, Tree) and cte_col_alias_token.data == 'cte_col_alias':
        col_alias = [a.value for a in cte_col_alias_token.children if isinstance(a, Token)]    # type: ignore
    if query is None:
        return None
    

    query.is_cte = True
    query.cte_name = cte_name
    query.cte_start = cte_start
    query.cte_end = cte_end
    if col_alias:
        query.col_aliases = col_alias


    return query


def parse_ctes(tree:Tree):
        ctes:dict[str,Query] = {}
        cte_names:list[str] = []
        for cte in tree.find_data('cte_expression'):
            cte = get_cte(cte)
            if cte and cte.cte_name:
                ctes[cte.cte_name] = cte
                cte_names.append(cte.cte_name)

        cte_names = [c for c in cte_names if c in ctes]

        for i, cte_name in enumerate(cte_names):
            if i == 0:
                continue
            cte_before = cte_names[:i]
            ctes[cte_name].ctes_before = [ctes[cte_name] for cte_name in cte_before]

        return ctes


def get_table_names(tree:Tree):
    names:list[str] = []
    for tlb in tree.find_data('table_name'):
        for name in tlb.children:
            if isinstance(name, Token):
                names.append(name.value)
    return names


def get_table_ref(tree:Tree, ref_type:Literal['table', 'table_macro']):
    tbl_alias = get_alias_value(tree.children[1])
    tbl_names = [x.value for x in tree.children[0].children[:3] if isinstance(x, Token)]
    col_alias = None
    if isinstance(tree.children[2], Tree) and tree.children[2].data == 'col_alias':
        col_alias = [x.value for x in tree.children[2].children if isinstance(x, Token)]
    if not tbl_alias and not tbl_names: 
        return
    if not isinstance(tbl_alias, str):
        tbl_alias = tbl_names[-1]
    return FromRef(alias=tbl_alias, names=tbl_names, subquery=None, col_alias=col_alias, ref_type=ref_type)


class MakeTree(Transformer):
    def __init__(self, sql:str):
        self.queries:list[Query] = []
        self.i = 0
        self.sql = sql
    
    @v_args(tree=True)
    def query(self, tree:Tree):
        # print(tree.pretty())
        columns, aliases = get_projection(tree)
        from_refs:list[FromRef] = []
        where_subq:list[Query] = []
        for c in tree.children:
            if isinstance(c, list) and len(c) > 0 and isinstance(c[0], FromRef):
                from_refs = c

        for sq in tree.find_data('subquery'):
            if isinstance(sq.children[0], Query):
                where_subq.append(sq.children[0])

        for sq in tree.find_data('exists'):
            if isinstance(sq, Tree):
                for c in sq.children:
                    if isinstance(c, Query):
                        where_subq.append(c)


        ctes = parse_ctes(tree)        
                    # columns.append(col_refs[0].children[-1].value)
        q = Query(
            start_pos=tree.meta.start_pos,
            end_pos=tree.meta.end_pos,
            columns=columns,
            aliases=aliases,
            from_refs=from_refs,
            where_subq=where_subq,
            ctes_before=[],
            # sql=self.sql[tree.meta.start_pos:tree.meta.end_pos],
            item=self.i,
            ctes=ctes,
        )

        for ref in q.from_refs:
            if ref.subquery:
                subquery = ref.subquery
                subquery.parent = q

        for sq in q.where_subq:
            sq.parent = q

        self.queries.append(q)
        self.i += 1
        return q   
    
    @v_args(tree=True)
    def set_operation(self, tree:Tree):
        queries = [drill_down_to_query(q) for q in tree.children if isinstance(q, Query) or isinstance(q, Tree)]
        queries = [q for q in queries if isinstance(q, Query)]
        if len(queries) == 2:
            queries[1].set_operation_sibling = queries[0]
        return tree
    
    @v_args(tree=True)
    def from_clause(self, tree:Tree):
        refs:list[FromRef] = []
        # print(tree.pretty())
        for sq in tree.find_data('subquery'):
            # print(sq.pretty())
            sq_alias = get_alias_value(sq.children[1])
            
            if sq_alias and isinstance(sq.children[0],Query):
                sub_q = sq.children[0]
                sub_q.is_subquery = True
                sub_q.sub_query_alias = sq_alias
                refs.append(FromRef(alias=sq_alias, names=None, subquery=sub_q, ref_type='subquery'))

        for tbl in tree.find_data('db_table'):
            tbl_ref = get_table_ref(tbl,'table')
            if tbl_ref:
                refs.append(tbl_ref)

        for tbl_macro in tree.find_data('table_function'):
            tbl_ref = get_table_ref(tbl_macro,'table_macro')
            if tbl_ref:
                refs.append(tbl_ref)

        for values in tree.find_data('values'):
            pass

        return refs

tree = sql_parser.parse(sql)

query_promoter = PromoteQueriesInParentheses()
mt = MakeTree(sql)
new_tree = query_promoter.transform(tree)
new_tree = mt.transform(new_tree)   

# new_tree = mt.transform(tree)
for q in mt.queries:
    if q.set_operation_sibling:
        q.parent = q.set_operation_sibling.parent
        q.ctes_before = q.set_operation_sibling.ctes_before

for q in mt.queries:
    print(q)
# list(tree.find_data('col_exp'))[6].children[0].data
# %%
mt.queries[-1].ctes.keys()
mt.queries[13].columns[0].star_exlude
#%%
mt.queries[14].projection()

#%%

sql[1265:1343]
#%%

sql2 = (
"""--sql
SELECT a.b, a.* EXCLUDE (d,e,f) from abc a
""")

tree2 = sql_parser.parse(sql2)
print(tree2.pretty())

#%%
duckdb.sql(
"""--sql
with v1(k1, k2, k3) as (
    SELECT * from (VALUES (1, 2, 3), (4,5,6)) v(a,b,c)
),
v2 as (
    SELECT * from v1 
),
v3 as (
    SELECT * from v2


)


SELECT * from v3 v(a)

"""
)


#%%
