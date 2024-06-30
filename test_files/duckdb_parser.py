#%%
import duckdb
db = duckdb.connect()
import json
import threading
#%%


# %%
sql = (
"""--sql,
with qq as (SELECT * from (VALUES (1, 2),, (3, 4)) t(a, b))
select q.a from qq q;


"""
)
#%%
duckdb.tokenize(sql)
db.sql(sql)
# %%

j = db.execute("""SELECT json_serialize_sql(?::VARCHAR)""",[sql]).fetchone()[0]
p = json.loads(j)
p

# %%
p['statements'][2]