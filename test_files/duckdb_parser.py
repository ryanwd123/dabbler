#%%
import duckdb
db = duckdb.connect()
import json
import threading
import sqlparse
import sqlfluff
from sqloxide import parse_sql
#%%



from sqlglot import parse, parse_one, Generator, generator
g = Generator(pretty=True, indent=4, max_text_width=200)

#%%
for i in range(9,6,-1):
    print(i)


# %%
sql = (
"""--sql,
from fruites
select apple as a, banana as b, candy

"""
)
parse_one(sql, read='duckdb')


#%%
p = parse(sql, read='duckdb')
p[0]
for q in p:
    print(q.sql(dialect='duckdb',pretty=True, indent=4, max_text_width=2000))
#%%
print(sqlparse.format(sql, reindent=True, keyword_case='upper'))
#%%
print(sqlfluff.fix(sql))
#%%
db.sql(
"""--sql,
select
    d.column_default,
    d.column_name
from duckdb_columns d
"""
)
#%%
duckdb.tokenize(sql)
db.sql(sql)
# %%
sql = (
"""--sql,
with fruites as (
    select a,b,c from zzz
    union all
    select a2,b2,c2 from zzz2
)
from fruites
select 
    apple as a, 
    banana as b, 
    candy,
    lpad(apple,'0',2) as s2
order by apple

"""
)
sql = (
"""SELECT 
t.
from trees t

"""
)

j = db.execute("""SELECT json_serialize_sql(?::VARCHAR)""",[sql]).fetchone()[0]
p = json.loads(j)

duckdb.tokenize(sql)
p
# %%
p['statements'][2]