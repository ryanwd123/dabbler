#%%
import sys
from pathlib import Path
import pandas as pd
sys.path.append(str(Path(__file__).parent.parent))
import duckdb
db = duckdb.connect()
# db.execute("set file_search_path to 'C:\\scripts'")

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
db.execute("force checkpoint")
# db.sql("create or replace view db_sql as select * from t_medicare")
# db.sql("create or replace view db_sql_with_alias as select * from t_medicare",'abc')
# db.execute("create or replace view db_execute_with_params as select * from t_medicare",[1,2,3])
# db.execute("create or replace view db_execute_with_params2 as select * from t_medicare", [1,2,3])
# db.execute("create or replace view db_execute_with_params3 as select * from t_medicare" , [1,2,3])
# db.executemany("create or replace view db_executemany as select * from t_medicare")
# db.sql('create or replace view sql_single_quote as select * from t_medicare')
# db.execute("attach './../../sample_data/imdb.duckdb'")
#%load_ext dabbler.ext_debug
#!%load_ext dabbler.ext
# from dabbler.lsp.db_data import get_db_data_new,make_db,make_completion_map
#%%

db.sql(
    """--sql
    create or REPLACE TABLE piv_tree as
    PIVOT Issued_Tree_Permits ON PERMIT_STATUS USING max(ISSUED_DATE) GROUP BY APPLICATION_TYPE
    """
)

#%%
j = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]})
import os
os.chdir(r'C:\scripts')
#%%

db.sql(
    """--sql
    from Issued_Tree_Permits i
    SELECT i.PERMIT_ADDRESS, COLUMNS ((j) -> j like 'P%'), i.REASON_FOR_REQUEST 
    """
)


#%%
db.sql(
    """--sql
    UNPIVOT piv_tree ON * exclude (APPLICATION_TYPE) INTO NAME qq VALUE jj 
    """
)




#%%

db.execute("force checkpoint")

import duckdb
db = duckdb.connect()

db.sql(
    """--sql
    CREATE or REPLACE TABLE t1 as
    from (VALUES
        ('a',1),
        ('b',2),
        ('c',3),
        ('d',4),
        ('e',5),
        (NULL,6),
        ) as a(c1,c2);
    
    CREATE OR REPLACE TABLE t2 as
    from (VALUES
        ('a',1),
        ('b',2),
        ('c',3),
        ('j',4),
        ) as a(c1,c2);
    
    from t2 t
    SELECT t.c1
    WHERE t.c1 not in (SELECT DISTINCT c1 from t1)
        
    """
)
#%%
db.sql(
    """--sql
    from read_csv_auto('./../../../../Scripts/bases.csv',header=true) b 
    select b.base_ce, b.oh_ce
           
    """
)

#%%

db.sql(
    """--sql
    with qq as (from Issued_Tree_Permits i
    select
        i.PERMIT_NUMBER,
        i.APPENDIX_F_REMOVED,
        i.PROJECT_ID)
    from qq q
        SELECT
            q.PERMIT_NUMBER,
            q.PERMIT_NUMBER,
            q.PROJECT_ID,
        
           
    """
)



#%%
db.sql(
    """--sql
    from Issued_Tree_Permits i
    SELECT 
        CASE lower(i.PERMIT_STATUS) 
            WHEN 'approved' THEN 'a' 
            ELSE 
                CASE i.PERMIT_ADDRESS
                    WHEN 'g' THEN 'c' 
                    ELSE i.PERMIT_STATUS 
                END 
        END as a,
        i.COUNCIL_DISTRICT as b,
        
        
    """
)



#%%
import logging
log = logging.getLogger('test')
txt = """--sql

    
    """
from dabbler.lsp.parser import interactive_parse
find_txt = 'a as j w'
pos = txt.find(find_txt)+len(find_txt)
txt.find(find_txt)
txt[:pos]

interactive_parse(txt,pos,log)


#%%

db.execute("force checkpoint")

db.execute("""--sql
    FORCE CHECKPOINT;
    DROP VIEW if EXISTS my_view2;
    DROP TABLE if EXISTS my_table2;
    CREATE OR REPLACE TABLE my_table (
        id INTEGER PRIMARY KEY,
        wbs VARCHAR UNIQUE,
        amt DOUBLE,
        description VARCHAR,
        gen GENERATED ALWAYS AS (1),
        CHECK(amt > 0),
    ) ;
    INSERT INTO my_table VALUES (1, 'wbs1', 1.9, 'abc'), (2, 'wbs2', 2.9, 'def');
    SELECT * FROM my_table;
    CREATE or REPLACE VIEW my_view AS (SELECT * from my_table);
    SELECT v.description, v.amt FROM my_view v;
    ALTER VIEW my_view RENAME TO my_view2;
    CREATE OR REPLACE SEQUENCE my_seq;
    CREATE or REPLACE TABLE imdb.main.my_tb (
        id INTEGER,
        name VARCHAR,
    );
    CREATE SCHEMA IF NOT EXISTS my_schema;
    ALTER TABLE my_table RENAME TO my_table2;
    ALTER TABLE my_table2 RENAME id to id2;
    ALTER TABLE imdb.main.my_tb ALTER COLUMN id TYPE VARCHAR;
    CHECKPOINT;
    CHECKPOINT imdb;
    SELECT * from Issued_Tree_Permits;

    
    """)

#%%
db.sql("""--sql
    select * from medicare USING SAMPLE 2
    
    """)
#%%
db.execute("""--sql
    create table t_test as select * from 'txtb.csv';
    
    """)

#%%

txt = """--sql
    

    
    """
db.sql(txt)

#%%
from dabbler.lsp.parser import interactive_parse
find_txt = 'abc s'
pos = txt.find(find_txt)+len(find_txt)
txt.find(find_txt)
txt[:pos]

interactive_parse(txt,pos,log)




#%%

#%%
globals()['__vsc_ipynb_file__']

#%%

import os
os.chdir(r'C:\scripts')

df9 = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]})
df10 = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]})

db.sql(
    """--sql
    from read_csv_auto('./rates.csv',header=true,normalize_names=true) z
    select z.oh_ce
    """)
#%%
from urllib.parse import urlparse, unquote
uri = 'file:///c%3A/Projects/db_dabbler/src/test_files/tst_load_ext2.py'

unquote(uri)


path = Path(unquote(uri[8:]))
path.is_file()

import pprint
pprint.pprint(globals())

#%%
db.sql("show tables")
sql_txt = (
     """--sql
    with cine as (select distinct
        c.person_id,
        p.name,
        c.category,
        c.title_id,
        t.primary_title,
        r.averageRating,
        r.numVotes
    from imdb.main.crew c
        join imdb.main.people p on p.person_id = c.person_id
        join imdb.main.titles t on t.title_id = c.title_id
        join imdb.main.akas a on a.title_id = t.title_id
        join imdb.main.ratings r on r.tconst = t.title_id
    where
        t.type = 'movie'
        --and a.region = 'US'
        and c.category = 'cinematographer'
    --group by all
    --order by title_count desc
    ), cc as (
    select 
        c.person_id,
        c.name,
        c.category,
        c.person_id, c.numVotes,
        c.person_id,
        count() as title_count,
        sum(c.numVotes) as total_votes,
        avg(c.averageRating) as avg_rating
        
    from cine c
    group by all
    order by title_count desc)
    select
        k.numVotes,
        sum(k.averageRating),
        k.averageRating,
    from cine k 
    """)


db.sql(
    """--sql
    with qq as (from athlete_events select *),
    qq2 as (from qq select *),
    qq3 as (from qq2 select * exclude (Age, Sex, ID, Height))
    from qq3 q select q.City, q.Medal, q.NOC
    """)

#%%

db.sql(
    """--sql
    with crew_count as (
        from imdb.main.crew c
        select
            c.title_id,
            count() as crew_count
        group by all
    ),
    prin_count as (
        from imdb.main.principals p
        select
            p.tconst as title_id,
            count() as prin_count
        group by all
    ),
    t_count as (
        select * from crew_count
        union all
        select * from prin_count
    )
    from imdb.main.titles t
        join imdb.main.ratings r on r.tconst = t.title_id
        join crew_count c on c.title_id = t.title_id
        join prin_count p on p.title_id = t.title_id    
    select
        t.title_id,
        t.primary_title,
        r.averageRating,
        r.numVotes,
        c.crew_count,
        c.crew_count,
        p.prin_count,
        c.crew_count + p.prin_count as total_count
    order by r.numVotes desc
    
    """)
#%%
db.execute("set file_search_path to 'C:\\scripts'")
df7 = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]})
db.sql("select current_setting('file_search_path')").fetchone()[0]
#%%



#%%

db.sql(
    """--sql
    with exp as (from Issued_Tree_Permits i
    select
        i.PERMIT_NUMBER,
        i.TRUNK_DIAMETER,
        unnest(regexp_extract_all(i.TRUNK_DIAMETER,'\d+([.]\d+)?'))::DOUBLE as trunk,
    ),
    tree_info as (
        from exp e
        select 
            e.PERMIT_NUMBER,
            sum(e.trunk) as total_trees_diameter,
            count() as tree_count
        group by all
    )
    from Issued_Tree_Permits i
        join tree_info t on t.PERMIT_NUMBER = i.PERMIT_NUMBER
    select 
        t.total_trees_diameter, 
        t.tree_count,
        i.*
    order by
        t.total_trees_diameter desc
        
        --regexp_replace(q.trunk,'(\d+([.]\d)*)',''),
    
    """)

#%%

db.sql(
    """--sql
    from (VALUES 
            (1,2,3),
            (4,4,5),
            (4,4,5),
            (4,4,5),
            (4,4,5),
        ) a(a,b,c)
    
    
    """)

#%%



#%%
from dabbler.common import KeyFile
# %%
k = KeyFile()
# %%
print(k.file.read_text())
#%%
import sys
sys.executable == 'c:\\Projects\\db_dabbler\\db_dabbler_env\\Scripts\\python.exe'