#%%
import duckdb
db = duckdb.connect()
#!%load_ext dabbler.ext
#%%

db.sql(
"""--sql,
select database_name, schema_name, type_name, type_category from duckdb_types
WHERE database_name = 'memory'
and schema_name = 'main'
 """
)
#%%

db.sql(
"""--sql,
select database_name, schema_name, table_name, column_name, data_type, data_type_id from duckdb_columns
"""
)
# %%

db.sql(
"""--sql,
CREATE OR REPLACE table struck_example as
SELECT {'x': 1, 'y': 2, 'z': 3} as a;
"""
)
# %%
