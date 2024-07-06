#%%
import polars as pl
import sys
from pathlib import Path
import pandas as pd
sys.path.append(str(Path(__file__).parent.parent))
import duckdb
db = duckdb.connect()
import tempfile
print(tempfile.gettempdir())
# import paths_z as pp
# db.execute("set file_search_path to 'C:\\scripts'")
##!%load_ext dabbler.ext_debug
#!%load_ext dabbler.ext

df1 = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]})

files =  list(Path('./../../sample_data/austin').glob("*.csv"))
f = files[0]
for f in files:
    t_name = f'{f.name}'.replace('.csv','').replace('-','')
    db.sql(
        f"""--sql
        create or replace table {t_name} as
        select * from read_csv_auto('{f}',header=true)
        """)

files =  list(Path('./../../sample_data').glob("*.csv"))
f = files[0]
for f in files:
    t_name = f'{f.name}'.replace('.csv','').replace('-','')
    db.sql(
        f"""--sql
        create or replace table {t_name} as
        select * from read_csv_auto('{f}',header=true)
        """)

db.sql("""--sql
           attach './../../sample_data/imdb.db' (READ_ONLY TRUE);
           """)

#%%
df1 = pl.DataFrame({'a':[1,2,3,0],'b':[4,5,6,0]})

#%%
aa = db.sql(
"""--sql,
select
    i.COUNCIL_DISTRICT,
    i.APPENDIX_F_REMOVED


from Issued_Tree_Permits i
"""
).execute()




#%%
aa.select('PERMIT_STATUS').distinct().fetchall()

#%%
try:
    j = db.sql(
    """--sql,
    select
        t.ended
    from imdb.main.titles t
    limit 100
    """
    ).pl()
except Exception as e:
    print(f'{type(e).__module__}.{type(e).__name__}:\n{e}')

#%%
aa = Path(__file__).parent.parent.parent.parent
ab = Path(__file__).parent.parent.parent.parent
ac = Path(__file__).parent.parent.parent.parent

#%%

db.sql(
"""--sql,
CREATE SCHEMA v;
drop TYPE if EXISTS permit_status;
CREATE TYPE permit_status as ENUM (SELECT DISTINCT PERMIT_STATUS from Issued_Tree_Permits);
ALTER TABLE Issued_Tree_Permits ALTER COLUMN permit_status TYPE PERMIT_STATUS;
"""
)
#%%
db.sql(
"""--sql,
CREATE OR REPLACE FUNCTION addition(a, b) as a + b;
CREATE OR REPLACE FUNCTION v.add(a, b) as a + b;
CREATE OR REPLACE MACRO v.tbl_macro() as table select 1;
CREATE OR REPLACE MACRO tbl_macro() as table select 1;
CREATE or replace MACRO tbl_macro2(p) as table select * from Issued_Tree_Permits i where lower(i.PERMIT_STATUS) = lower(p);
CREATE OR REPLACE VIEW v.test_view as select * from Issued_Tree_Permits;
CREATE OR REPLACE VIEW test_view as select * from Issued_Tree_Permits;
"""
)

#%%
db.sql(
"""--sql,
select
    t
from v.tbl_macro()
"""
)


#%%

db.sql(
"""--sql,
with kk as (SELECT 
    j.APPLICATION_TYPE, 
    j.PROPERTY_ID
from v.test_view j)
from kk k
SELECT k.APPLICATION_TYPE,
    k.PROPERTY_ID

"""
)
#%%
db.sql(
"""--sql,
select
    i
from Issued_Tree_Permits i
"""
)


#%%

#%%
db.sql(
"""--sql,
drop type IF EXISTS permit;
CREATE type permit as ENUM (select distinct i.PERMIT_STATUS from Issued_Tree_Permits i order by all);
alter table Issued_Tree_Permits alter column PERMIT_STATUS set data type permit;
select 
    * EXCLUDE (PERMIT_ADDRESS) REPLACE (ISSUED_DATE::DATE as ISSUED_DATE),
    list(DISTINCT t.PERMIT_STATUS) over (partition by t.FEE_REQUIRED) as list_col,
    struct_pack(t.PERMIT_STATUS, t.FEE_REQUIRED) as struct_col,
from Issued_Tree_Permits t
LIMIT 100
"""
).to_parquet('../dabbler_py_tests/test_data.parquet')

#%%
db.sql(
"""--sql,
select
    count(i.CROWN_REMOVAL) FILTER (WHERE i.PERMIT_STATUS ~ '%losed') OVER (PARTITION BY i.SPECIES) as crown_removal_count,
from Issued_Tree_Permits i
"""
)

#%%
db.sql(
    """--sql
    with aaa as (
        SELECT
            i.CROWN_REMOVAL,
            i.ENCROACHMENT_OF_ROOT_ZONE,
            i.JURISDICTION,
            i.SPECIES
        from Issued_Tree_Permits i
        WHERE
            i.ENCROACHMENT_OF_ROOT_ZONE = true
    ),
    ggg as (SELECT
        z.* EXCLUDE (CROWN_REMOVAL)
    from aaa z
    ),
    xyz as (select 
        *
    from Issued_Tree_Permits i
        join ggg g on g.SPECIES = i.SPECIES
    ), t123 as (
    select 
        CASE 
            when j.PERMIT_ADDRESS ILIKE '%grover%' then 'grover'
            when j.PERMIT_ADDRESS ILIKE '%gor%' then 'grover'
            when j.PERMIT_ADDRESS ILIKE '%oak%' then 'grover'
            else 'not grover'
        END as j7,
        j.*
    from xyz j
    WHERE j.ISSUED_DATE > '2020-01-01'
    ),
    t1234 as (
    SELECT
        t.j7,
        t.JURISDICTION,
        t.Combined_Geo,
        t.TRUNK_DIAMETER,
        t.PERMIT_STATUS,
        t.APPENDIX_F_REMOVED,
        t.PERMIT_CLASS,
        t.APPENDIX_F_REMOVED,
        t.PROJECT_ID,
        t.PERMIT_NUMBER,
    from t123 t
    ), g0a9s8d as (pivot t1234 on j7 using max(TRUNK_DIAMETER))
    SELECT 
        g.APPENDIX_F_REMOVED,
        g.JURISDICTION,
        g.APPENDIX_F_REMOVED,
        g.APPENDIX_F_REMOVED
    from t1234 g 
    """
)



#%%
db.sql(
"""--sql,
ATTACH ':memory:' as mem2;
CREATE table mem2.main.test1 (a INTEGER, b VARCHAR);
INSERT into mem2.main.test1 values (1,'a'),(2,'b'),(3,'c');
"""
)

#%%
db.sql(
"""--sql,
set search_path to 'mem2.information_schema,memory';
"""
)

#%%
db.sql(
"""--sql,
SELECT current_setting('search_path');

"""
)