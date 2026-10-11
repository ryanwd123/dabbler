#%%
import duckdb
import os
from typing import Union
from IPython.core.getipython import get_ipython
from pathlib import Path
import logging
log = logging.getLogger(__name__)


def get_loaded_extensions(db:duckdb.DuckDBPyConnection):
    return [x[0] for x in db.sql(
        """--sql,
        select e.extension_name
        from duckdb_extensions() e
        where e.loaded

        """
        ).fetchall()]



def get_pathlib_paths():
    """pathlib.Path variables in the IPython namespace (and in the user's own modules),
    for path completions in python code."""
    paths = []

    ipython = get_ipython()
    if not ipython:
        return paths

    for item in (ipython.ev('dir()')):
        i_type = str(type(ipython.ev(item)))

        if 'WindowsPath' in i_type or 'PosixPath' in i_type:
            paths.append(
                (item, str(ipython.ev(item)))
            )

        if i_type == "<class 'module'>":
            if '__file__' not in ipython.ev(f'dir({item})'):
                continue
            if 'site-packages' in ipython.ev(item).__file__:
                continue
            if Path(ipython.ev(item).__file__).parent.name == 'Lib':
                continue
            if Path(ipython.ev(item).__file__).parent.parent.name == 'Lib':
                continue

            for item2 in ipython.ev(f'dir({item})'):
                item_item2 = f'{item}.{item2}'
                i_type2 = str(type(ipython.ev(item_item2)))

                if 'WindowsPath' in i_type2 or 'PosixPath' in i_type2:
                    paths.append(
                        (item_item2, str(ipython.ev(item_item2)))
                    )

    return paths


def get_db_data_new(db:duckdb.DuckDBPyConnection, file_search_path:Union[str,None]=None, skip_ipython = False):
    """gets the data to send to the language server"""
    db_items = db.execute("""--sql
            with
        cols as (
            select 
                database_name||'.'||schema_name as db_scm,
                table_name, 
                list([column_name, data_type]) as cols
            from duckdb_columns
            group by all
        ), db_data as (
            (select database_name||'.'||schema_name as db_scm, table_name, 'table' from duckdb_tables)
            union all
            (select database_name||'.'||schema_name, view_name, 'view' from duckdb_views order by view_oid)
            union all
            (select database_name||'.'||schema_name, sequence_name, sql from duckdb_sequences())
        )
        select
            d.*,
            c.cols
        from db_data d
            left join cols c using (db_scm, table_name)
        """).fetchall()

    db_functions = db.sql(
            """--sql,
            select
                database_name||'.'||schema_name as db_scm,
                function_name,
                function_type,
                -- a table macro's body lets the language server work out its columns
                case when function_type in ('macro', 'table_macro') then macro_definition end,
                parameters,
            from duckdb_functions()
            WHERE internal = false
            """
            ).fetchall()


    paths = []
    if not skip_ipython:
        try:
            paths = get_pathlib_paths()
        except Exception:
            log.exception('Error collecting pathlib.Path variables')

    paths.sort(key=lambda x: f'{x[0]}-{x[1]}')
    current_schema:str = db.execute("select current_database()||'.'||current_schema()").fetchone()[0]  # type: ignore
    databases = [x[0] for x in db.execute("select database_name from duckdb_databases()").fetchall()]
    schemas = [x[0] for x in db.execute("select database_name ||'.'|| schema_name from duckdb_schemas()").fetchall()]

    

    db_data = {'data':db_items,
            'databases':databases,
            'functions':db_functions,
            'paths':paths,
            'schemas':schemas,
            'current_schema':current_schema,
            'cwd':os.getcwd(),
            'file_search_path':file_search_path,
            'loaded_extensions':get_loaded_extensions(db),
            }

    return db_data



def get_default_db_data():
    db = duckdb.connect()
    db_data = get_db_data_new(db)
    db.close()
    return db_data







# def get_db_data(db:duckdb.DuckDBPyConnection):
#     records = db.execute(
#         """
#         select
#             c.table_catalog,
#             c.table_schema,
#             c.table_name,
#             t.table_type,
#             list([column_name, data_type]),
#             sql
#         from duckdb_columns c
#             join information_schema.tables t using (table_catalog, table_schema, table_name)
#         group by all
        
#         union all
        
#         select distinct
#             d.database_name,
#             d.schema_name,
#             d.function_name,
#             'table_function',
#             [],
#             null
#         from duckdb_functions() d
#         where d.function_type in ('table','table_macro')
#             --and d.function_name not in ('force_checkpoint')
#         group by all
        
        
#         """).fetchall()
    
#     functions = db.execute(
#         """
#         select distinct
#             d.database_name,
#             d.schema_name,
#             d.function_name,
#             d.function_type,
#             [],
#         from duckdb_functions() d
#         where d.function_type in ('aggregate','macro','scalar')
#             and d.function_name not similar to '.*\W.*'
#             and not d.internal
#             and database_name != 'system'
#         group by all
        
#         union all
        
#         select distinct
#             null,
#             null,
#             d.function_name,
#             d.function_type,
#             [],
#         from duckdb_functions() d
#         where d.function_type in ('aggregate','macro','scalar')
#             and d.function_name not similar to '.*\W.*'
#             and database_name = 'system'
#         group by all
#         """).fetchall()

#     table_names = [x[2] for x in records]

#     ipython = get_ipython()
#     for item in (ipython.ev('dir()')):
#         i_type = str(type(ipython.ev(item)))
        
#         if i_type in capture_types and item[0] != '_' and item not in table_names:
#             locals().__setitem__(item,ipython.ev(item))
#             cols = [[x[0],x[1]] for x in  db.sql("describe select * from df limit 1").fetchall()]
#             records.append([None,None,item,'python',cols])

#     current_schema = db.execute("select current_schema()").fetchone()[0]
#     current_db = db.execute("select current_database()").fetchone()[0]
#     sequences = db.execute("select database_name, schema_name, sequence_name, from duckdb_sequences()").fetchall()
    
#     db_data = {
#         'tbl_col':records,
#         'sequences':sequences,
#         'functions':functions,
#         'current_db':current_db,
#         'current_schema':current_schema,
#     }
    
#     return db_data






#%%
