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
#!%load_ext dabbler.ext
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
            
    from cine k 
    """)


db.sql(
    """--sql
    with qq as (from athlete_events select *),
    qq2 as (from qq select *),
    qq3 as (from qq2 select * exclude (Age, Sex, ID, Height))
    from qq q select q.City
    """)

#%%




db.sql(
    """--sql
    from Issued_Tree_Permits i
    select 
        i.
    
    """)
