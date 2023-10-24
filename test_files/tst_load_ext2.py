#%%
import sys
from pathlib import Path
import pandas as pd
sys.path.append(str(Path(__file__).parent.parent))
import duckdb
db = duckdb.connect()

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

# db.sql("create or replace view db_sql as select * from t_medicare")
# db.sql("create or replace view db_sql_with_alias as select * from t_medicare",'abc')
# db.execute("create or replace view db_execute_with_params as select * from t_medicare",[1,2,3])
# db.execute("create or replace view db_execute_with_params2 as select * from t_medicare", [1,2,3])
# db.execute("create or replace view db_execute_with_params3 as select * from t_medicare" , [1,2,3])
# db.executemany("create or replace view db_executemany as select * from t_medicare")
# db.sql('create or replace view sql_single_quote as select * from t_medicare')
db.execute("attach './../../sample_data/imdb.duckdb'")
#!%load_ext dabbler.ext_debug
# from dabbler.lsp.db_data import get_db_data_new,make_db,make_completion_map

class FakeLS:
    def show_message_log(a,b):
        pass

# db_data = get_db_data_new(db)
# db2 = make_db(db_data)
# comp_map = make_completion_map(db2,db_data)
# parser = SqlParser(db2)
# completer = SqlCompleter(db_data,FakeLS())
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
        k.category
    from cine k 
    """)


db.sql(
    """--sql
    with qq as (from athlete_events select *),
    qq2 as (from qq select *),
    qq3 as (from qq2 select * exclude (Age, Sex, ID, Height))
    from qq3 q select q.City
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
        p.prin_count,
        c.crew_count + p.prin_count as total_count
    order by r.numVotes desc
    
    """)



#%%



db.sql(
    """--sql
    from Issued_Tree_Permits i
    select 
        i.
    
    """)
#%%
from dabbler.common import KeyFile
# %%
k = KeyFile()
# %%
print(k.file.read_text())
