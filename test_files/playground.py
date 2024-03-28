#%%
import duckdb
db = duckdb.connect()
#!%load_ext dabbler.ext_debug

# %%

db.sql(
"""--sql,
CREATE SCHEMA v;
CREATE TABLE v.t (a int, b int);
"""
)

#%%
db.execute(
"""--sql,
SELECT *
FROM duckdb_keywords() k
WHERE k.keyword_name like $1

""",
['%and%']
).fetchall()


#%%
import logging
log = logging.getLogger('dabbler')
from dabbler.lsp.parser import SqlParserNew
p = SqlParserNew(db,None,log)

sql_txt = (
"""--sql,
with abc as (
    select 1 as a, 2 as b
)
select
    g.a,
    g.b,
from abc g 
"""
)

p.parse_sql(sql_txt,len(sql_txt)-1)
#%%
import tempfile
tempfile.gettempdir()


#%%
db.sql(
"""--sql,
with num as (
select
    l."Draw Date", unnest(split(l."Winning Numbers",' ')) as numbers
from './../../sample_data/Lottery.csv' l)
select
    numbers,
    count(*) as freq,
    sum(1) as total,
    sum(1) over () as total2,
    sum(1) over (partition by numbers) as total3,
    sum(1) over (partition by numbers order by numbers) as total4,
    sum(1) over (partition by numbers order by numbers rows between 1 preceding and 1 following) as total5
    from information_schema.tables
from num
GROUP by AL   
order by freq desc
"""
)

#%%
txt = """--sql
create table abc (
    wbs VARCHAR,
    name VARCHAR,
)
"""




#%%