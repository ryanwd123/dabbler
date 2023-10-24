#%%
from pathlib import Path
import time
from dabbler.lsp.parser import get_parser, SqlParserNew
import duckdb

from lark import Lark, Token, UnexpectedToken
db = duckdb.connect()


db.read_csv(
    "./../../sample_data/austin/Issued_Tree_Permits.csv", header=True, normalize_names=True
).create("tree_permits")


sql_parser = get_parser()
parse2 = sql_parser.parse

pass_test = 0
fail_test = 0


tst_files = list(Path("./sql_tst").glob("*.sql"))

for f in tst_files[:]:
    txt = f.read_text()
    # print(f.name, duckdb_parse(txt)["error"])
    try:
        start = time.time()
        parse2(txt)
        pass_test += 1
        # print(f'{time.time() - start:.4f} seconds')
    except Exception as e:
        print(f.name,e)
        fail_test += 1

print(f"pass: {pass_test}, fail: {fail_test}")
# %%

db.sql("select 'aaa'[(2-1):(3-1)]")
#%%

sql = """
with cte1 as (from b select a),cte2 as ((((select a from cte1))) union all select b from jjj union all select ggg from hhh),
   cte3 as (select a,b,c from my_table join (select a,b from dsd) dd on (a = b) where a = 1),
   cte4 as (Pivot abc on f using sum(g))

select x, y, z 
from xy_zjy as x 
   join cat.scm.qst as q on (x.id = q.id)
   join my_table_macro('a') tm on (x.id = tm.id)
   join (select * from ddd) z on (x.id = z.id)
, abc
"""

import pprint
parser = SqlParserNew(db)
queries = parser.parse_sql(sql)
print(len(queries.queries_list))
pprint.pprint(queries.queries_list)

#%%

# sql = "select a,b,c from my_table join (Pivot abc on f using sum(g)) dd on (a = b) where a = 1"

#%%
import sqlparse
print(sqlparse.format(sql,reindent=True,keyword_case='upper'))

#%%

#%%
## !%%timeit

#%%


sql =   """--sql
    with qq as (select *, project_id as gg from tree_permits)
    from qq q select *"""
parser = SqlParserNew(db)
queries = parser.parse_sql(sql)
print(len(queries.queries_list))
pprint.pprint(queries.queries_list)
# pprint.
#%%

pos = 155

queries.queries_list.sort(key=lambda x: x.end_pos - x.start_pos)
q = [x for x in queries.queries_list if x.start_pos <= pos <= x.end_pos][0]

q.cte_sibblings
q.from_refs['dd'].kind.name

queries.queries[175].projection

# %%

def parser_error_handler(e):
    # print(e.token)
    # e.interactive_parser.feed_eof() 
    # e.interactive_parser.feed_token(Token('NAME', 'xyz'))
    # e.interactive_parser.feed_token(e.token)
    return True

sql_grammer = Path("./../dabbler/lsp/sql3b.lark").read_text()

test_parser = Lark(
    sql_grammer,
    parser="lalr",
    # cache=str(lark_cache),
    propagate_positions=True,
    maybe_placeholders=True,
    debug=False,
)
#%%
sql2 = """

from jjj j
select
    * replace (a)



"""

try:
    sql_parser.parse(sql2)
except Exception as e:
    ee = e
    print(e)

#%%
dir(ee)
print(ee.__str__().split('\n')[0])
type(ee)
if isinstance(ee, UnexpectedToken):
    print(f'Unexpected Token "{ee.token}"')

print(ee.token)


#%%
ee.token_history
#%%
def parser_error_handler(e:UnexpectedToken):
    assert isinstance(e, UnexpectedToken)
    print(e.token,e.token.type)
    print(e.accepts)   

    
    if '_AS' in e.accepts and e.token.type == 'RPAREN':
        e.interactive_parser.feed_token(Token('_AS', 'AS'))
        e.interactive_parser.feed_token(Token('NAME', 'placeholder'))
        e.interactive_parser.feed_token(e.token)
        
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


# q = test_parser.parse_interactive(sql2)
# q.feed_eof() 
p = test_parser.parse(sql2,on_error=parser_error_handler)
# p = test_parser.parse(sql2,on_error=parser_error_handler)
print(p.pretty())
#%%
Token('NAME', 'xyz').type






#%%
test_parser.parse(sql2)
#%%
i = test_parser.parse_interactive(sql2)

for t in i.iter_parse():
    if not t.type in i.accepts():
        break
    print(t,t.type)

print(i.choices())
i.parser_state
#%%
test_parser.get_terminal('NAME')
#%%
import re

pat = re.compile(r'\b(?!QUALIFY\b|AUTHORIZATION\b|DO\b|CAST\b|ISNULL\b|LIKE\b|REFERENCES\b|ASC\b|ILIKE\b|ALL\b|WINDOW\b|FOR\b|INNER\b|GRANT\b|INITIALLY\b|OR\b|LEFT\b|ANALYZE\b|COLLATE\b|CASE\b|FETCH\b|IN\b|CONCURRENTLY\b|SEMI\b|UNPIVOT\b|BINARY\b|FROM\b|VARIADIC\b|ARRAY\b|IS\b|ANALYSE\b|AND\b|HAVING\b|UNION\b|COLUMN\b|ON\b|GLOB\b|SOME\b|AS\b|VERBOSE\b|TRAILING\b|END\b|ANY\b|TABLESAMPLE\b|INTERSECT\b|NATURAL\b|CREATE\b|OFFSET\b|CROSS\b|ELSE\b|FULL\b|SELECT\b|USING\b|CONSTRAINT\b|FOREIGN\b|OVERLAPS\b|ORDER\b|INTO\b|NOT\b|TABLE\b|ANTI\b|CHECK\b|ASYMMETRIC\b|LATERAL\b|DEFERRABLE\b|BOTH\b|RIGHT\b|COLLATION\b|UNIQUE\b|SYMMETRIC\b|TO\b|RETURNING\b|SIMILAR\b|NOTNULL\b|LIMIT\b|ASOF\b|JOIN\b|THEN\b|ONLY\b|FREEZE\b|WITH\b|DESC\b|WHERE\b|OUTER\b|WHEN\b|DISTINCT\b|LEADING\b|EXCEPT\b)([a-zA-Z_]\w*|\d+[a-zA-Z_]+\w*)\b')

pat.findall("""SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)""")

#%%

