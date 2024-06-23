from filecmp import cmp
from math import log
from lsprotocol.types import (
    CompletionItemKind,
    MarkupContent,
)
from pathlib import Path
from dabbler.lsp.sql_utils import CmpItem
import duckdb
import json
from dabbler.common import check_name
import logging
logger = logging.getLogger(__name__)


get_records_sql = """--sql
    with
        cols as (
            select 
                database_name||'.'||schema_name as db_scm,
                table_name, 
                list([column_name, data_type]) as cols
            from duckdb_columns()
            group by all
        ),
    tables_views as (
        select 
            database_name||'.'||schema_name as db_scm,
            table_name, 
            'table' as object_type,
            'table' as table_type,
            case
                when not internal then sql
                else null
            end as sql
        from duckdb_tables()
        union all
        select 
            database_name||'.'||schema_name as db_scm,
            view_name, 
            'table' as object_type,
            'view' as table_type,
            case
                when not internal then sql
                else 'internal'
            end as sql
        from duckdb_views()
        union all
            select distinct
            database_name||'.'||schema_name as db_scm,
            d.function_name,
            'table' as object_type,
            'table_macro',
            case
                when not internal then 'external'
                else 'internal'
            end as sql
        from duckdb_functions() d
        where d.function_type in ('table','table_macro')
            --and d.function_name not in ('force_checkpoint')
        union all
        select distinct
            database_name||'.'||schema_name as db_scm,
            d.function_name,
            'function' as object_type,
            d.function_type,
            null,
        from duckdb_functions() d
        where d.function_type in ('aggregate','macro','scalar')
            and d.function_name not similar to '.*\W.*'
            and not d.internal
            and database_name != 'system'
        group by all
        union all
        select distinct
            null,
            d.function_name,
            'function' as object_type,
            d.function_type,
            'internal',
        from duckdb_functions() d
        where d.function_type in ('aggregate','macro','scalar')
            and d.function_name not similar to '.*\W.*'
            and database_name = 'system'
        group by all
    )
    select 
        t.*,
        case when object_type = 'table' then c.cols end as cols
    from tables_views t
        left join cols c using (db_scm, table_name)
    """




def make_db(db_data:dict):
    """
    makes a duckdb in memory database from mirroring the tables 
    and columns in the IPython database.
    
    This DB is used by the language server to get column completion items
    for subqueries and CTEs.
    """
    db2 = duckdb.connect()
    databases = [x[0] for x in db2.execute("select database_name from duckdb_databases()").fetchall()]

    for db_to_add in db_data['databases']:
        if db_to_add not in databases:
            try:
                db2.execute(f"attach ':memory:' as {db_to_add}")
            except Exception as e:
                logger.error(f'Error attaching dummy memory database {db_to_add}, {e}')

    schemas = [x[0] for x in db2.execute("select database_name ||'.'|| schema_name from duckdb_schemas()").fetchall()]

    for schema in db_data['schemas']:
        if schema not in schemas:
            try:
                db2.execute(f"create schema {schema}")
            except Exception as e:
                logger.error(f'Error creating schema {schema}, {e}')
            
    db2.execute(f"use {db_data['current_schema']};")

    for df in db_data['dataframes']:
        try:
            db2.execute(df[1])
        except Exception as e:
            logger.error(f'Error creating table for dataframe {df[0]}, {e}')


    for schema, item, sql, cols in db_data['data']:
        if not cols:
            continue
        col_names = []
        col_items = []
        for c, d in cols:
            if c in col_names:
                continue
            col_names.append(c)
            col_items.append([c, d])

        col_txt = ',\n'.join([f'"{c[0]}" {c[1]}' for c in col_items])
        sql2 = f'create table {check_name(item)}({col_txt})'
        try:
            db2.execute(f'use {schema}; {sql2}')
        except Exception as e:
            logger.error(f'Error creating table {item} in schema {schema}, {e}')

    db2.execute(f"use {db_data['current_schema']};")
    if db_data['file_search_path']:
        try:
            db2.execute(f"""set file_search_path to '{db_data['file_search_path']}';""")
        except Exception as e:
            logger.error(f'Error setting file_search_path to {db_data["file_search_path"]}, {e}')
    
    return db2



def make_completion_map(db:duckdb.DuckDBPyConnection,db_data):
    """makes a map of completion items for the language server"""
    records = db.execute(get_records_sql).fetchall()
    function_docs = json.loads(Path(__file__).parent.joinpath('functions.json').read_text())
    kind_map = {
        'table':CompletionItemKind.File,
        'table_macro':CompletionItemKind.File,
        'function':CompletionItemKind.Function,
    }

    sort_map = {
        'table':"2",
        'function':"8"
    }


    item_map:dict[str,list[CmpItem]] = {}
    item_map['root_namespace'] = []

    cur_db = db_data['current_schema'].split('.')[0]

    for db_scm, fn_name, fn_type in db_data['functions']:
        if fn_type == 'table_macro':
            obj_type = 'table_macro'
        else:
            obj_type = 'function'

        comp_item = CmpItem(
                label=fn_name,
                kind=kind_map[obj_type],
                detail=None,
                typ='function',
                sort='9',
                obj_type=obj_type,
                doc=None)
        if db_scm not in item_map:
            item_map[db_scm] = []
        item_map[db_scm].append(comp_item)


    for db_scm, item, obj_type, comp_detial, sql, cols in records:

        
        item = check_name(item)

        if not db_scm:
            db_scm = 'root_namespace'

        db_schema_split = db_scm.split('.')
        if len(db_schema_split) == 2:
            db, schema = db_schema_split
        else:
            db = 'aa'
            schema = 'aabb'

        if db_scm not in item_map:
            item_map[db_scm] = []

        if db == cur_db:
            if schema not in item_map:
                item_map[schema] = []
        
        fn_doc = None
        fn_detail = None
        
        sort = sort_map[obj_type]
        if sql == 'internal':
            sort = '9'
        
        if obj_type == 'function' or comp_detial == 'table_macro':
            if item in function_docs:
                fn_doc = MarkupContent(
                    kind='markdown',
                    value=function_docs[item]['documentation']['documentation']) 
                fn_detail = function_docs[item]['documentation']['detail'] 
        
        comp_item = CmpItem(
                label=item,
                kind=kind_map[obj_type],
                detail=fn_detail,
                typ=comp_detial,
                sort=sort,
                obj_type=obj_type,
                doc=fn_doc)
        

        item_map[db_scm].append(comp_item)
        if db == cur_db:
            item_map[schema].append(comp_item)

        if db_scm == db_data['current_schema']:
            item_map['root_namespace'].append(comp_item)
        
        
        if cols:
            col_completions = [CmpItem(
                label=c[0],
                kind=CompletionItemKind.Field,
                detail=None,
                typ=c[1],
                sort='1',
                obj_type='column',
                doc=None) for c in cols]
            item_map[f'{db_scm}.{item}'] = col_completions
            if db == cur_db:
                item_map[f'{schema}.{item}'] = col_completions
            if db_scm == db_data['current_schema']:
                if item in item_map:
                    item_map[item].extend(col_completions)
                else:
                    item_map[item] = col_completions



    for cat_schema in db_data['schemas']:
        cat, schema = cat_schema.split('.')
        cat_comp = CmpItem(
            label=cat,
            kind=CompletionItemKind.Folder,
            detail=None,
            typ='database',
            sort='3',
            obj_type='database',
            doc=None)
        
        schema_comp = CmpItem(
            label=schema,
            kind=CompletionItemKind.Folder,
            detail=None,
            typ='schema',
            sort='3',
            obj_type='schema',
            doc=None)
        
        if cat == cur_db:
            if schema not in item_map['root_namespace']:
                # if not cat_schema in item_map:
                    # item_map[cat_schema] = []
                # item_map[schema] = item_map[cat_schema]
                item_map['root_namespace'].append(schema_comp)

        if cat not in item_map:
            item_map[cat] = []
            item_map['root_namespace'].append(cat_comp)
            
        item_map[cat].append(schema_comp)
        
        root_labels = [x.label for x in item_map['root_namespace']]
        item_map['root_namespace'].extend([x for x in item_map['system.main'] if x.label not in root_labels])
    
    # for cat_schema in db_data['schemas']:
    #     cat, schema = cat_schema.split('.')
    #     if cat == cur_db:
    #         if schema not in item_map and schema not in item_map['root_namespace']:
    #             if not cat_schema in item_map:
    #                 item_map[cat_schema] = []
    #             item_map[schema] = item_map[cat_schema]
    #             item_map['root_namespace'].append(schema_comp)
    
    return item_map


        
   