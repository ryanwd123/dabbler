#%%
from pathlib import Path
from re import S
import duckdb
db = duckdb.connect('../../sample_data/test.db')   
#!%load_ext dabbler.ext b


f = Path(__file__).parent

# %%

db.sql(
"""--sql,
create or REPLACE TABLE books as
select
    *
from './../../sample_data/amazon_books.csv'
"""
)
#%%
db.sql(
"""--sql,
create or REPLACE TABLE trees as
select
    *
from './../../sample_data/austin/Issued_Tree_Permits.csv'
"""
)
# %%

db.close()