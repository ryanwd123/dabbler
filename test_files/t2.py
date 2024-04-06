#%%
import duckdb
db = duckdb.connect()
#!%load_ext dabbler.ext
#%%
# %%
db.sql(
"""--sql,
CREATE SCHEMA _SYS_BIC;
CREATE TABLE _SYS_BIC."finance.labor/actuals_cost_script"(
    ALKJDSD VARCHAR(10));
CREATE TABLE _SYS_BIC.tst123(
    ALKJDSD VARCHAR)
"""
)
#%%
db.sql(
"""--sql,
CREATE SCHEMA abc;
CREATE TABLE abc."finance.labor/actuals_cost_script"(
    ALKJDSD VARCHAR)
"""
)

# %%
db.sql(
"""--sql,
select
    t.schema_name, t.table_name
from duckdb_tables t
"""
)

#%%
db.sql(
"""--sql,
select
    t
from _SYS_BIC."finance.labor/actuals_cost_script" t
"""
)
#%%
db.sql(
"""--sql,
select
    j.ALKJDSD
from jjj j
"""
)
#%%
from dabbler.lsp.db_data import make_db, make_completion_map
from dabbler.db_stuff import get_db_data_new


dbd = get_db_data_new(db)

db2 = make_db(dbd)

comp_map = make_completion_map(db2, dbd)

# %%
comp_map['_SYS_BIC']
'_SYS_BIC' in comp_map['root_namespace']


[k for k in comp_map.keys() if 'BIC' in k]