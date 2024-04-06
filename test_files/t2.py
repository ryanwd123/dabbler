#%%
import duckdb
db = duckdb.connect()
#!%load_ext dabbler.ext
#%%
db.sql(
"""--sql
select unnest([
    [1,2,3],
    [1,2,3],
    [1,2,3],
])
"""
)
# %%
