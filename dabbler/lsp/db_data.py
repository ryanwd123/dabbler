import enum
import functools
import attrs

# from lsprotocol.types import (
#     CompletionItemKind,
#     MarkupContent,
# )
from dabbler.lsp.sql_utils import CmpItem
from dabbler.lsp.functions import builtin_functions
from dabbler.lsp.parser import set_search_path_sql
import duckdb
from dabbler.common import check_name
import logging
logger = logging.getLogger(__name__)

@enum.unique
class CompletionItemKind(int, enum.Enum):
    """The kind of a completion entry."""

    Text = 1
    Method = 2
    Function = 3
    Constructor = 4
    Field = 5
    Variable = 6
    Class = 7
    Interface = 8
    Module = 9
    Property = 10
    Unit = 11
    Value = 12
    Enum = 13
    Keyword = 14
    Snippet = 15
    Color = 16
    File = 17
    Reference = 18
    Folder = 19
    EnumMember = 20
    Constant = 21
    Struct = 22
    Event = 23
    Operator = 24
    TypeParameter = 25

@enum.unique
class MarkupKind(str, enum.Enum):
    """Describes the content type that a client supports in various
    result literals like `Hover`, `ParameterInfo` or `CompletionItem`.

    Please note that `MarkupKinds` must not start with a `$`. This kinds
    are reserved for internal usage."""

    PlainText = "plaintext"
    """Plain text is supported as a content format"""
    Markdown = "markdown"
    """Markdown is supported as a content format"""

@attrs.define
class MarkupContent:
    """A `MarkupContent` literal represents a string value which content is interpreted base on its
    kind flag. Currently the protocol supports `plaintext` and `markdown` as markup kinds.

    If the kind is `markdown` then the value can contain fenced code blocks like in GitHub issues.
    See https://help.github.com/articles/creating-and-highlighting-code-blocks/#syntax-highlighting

    Here is an example how such a string can be constructed using JavaScript / TypeScript:
    ```ts
    let markdown: MarkdownContent = {
     kind: MarkupKind.Markdown,
     value: [
       '# Header',
       'Some text',
       '```typescript',
       'someCode();',
       '```'
     ].join('\n')
    };
    ```

    *Please Note* that clients might sanitize the return markdown. A client could decide to
    remove HTML from the markdown to avoid script execution."""

    kind: MarkupKind = attrs.field()
    """The type of the Markup"""

    value: str = attrs.field(validator=attrs.validators.instance_of(str))
    """The content itself"""

get_records_sql = r"""--sql
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




def search_path_for(db_data:dict):
    """The session's file_search_path plus its working directory.

    Relative paths in queries (read_csv('./x.csv')) then resolve the way they do
    in the IPython session without changing this process's working directory,
    which matters when one language server serves several notebooks.
    """
    paths = [p for p in (db_data.get('file_search_path') or '').split(',') if p]
    if db_data.get('cwd'):
        paths.append(db_data['cwd'])
    return ','.join(paths)


def make_engine(db_data:dict):
    """A plain DuckDB connection for describing table functions, e.g. read_csv('x.csv').

    It has the session's extensions and file search path, but none of its
    tables: everything about the session's schema comes from the snapshot.
    """
    engine = duckdb.connect()
    for ext in db_data.get('loaded_extensions') or []:
        try:
            engine.load_extension(ext)
        except Exception as e:
            logger.info(f'could not load extension "{ext}": {e}')
    search_path = search_path_for(db_data)
    if search_path:
        try:
            engine.execute(set_search_path_sql(search_path))
        except Exception as e:
            logger.info(f'could not set file_search_path to {search_path}: {e}')
    return engine


@functools.lru_cache(maxsize=8)
def builtin_records(extensions: tuple[str, ...]):
    """DuckDB's own functions, table functions and system views, with the given extensions loaded."""
    con = duckdb.connect()
    for ext in extensions:
        try:
            con.load_extension(ext)
        except Exception as e:
            logger.info(f'could not load extension "{ext}": {e}')
    try:
        return con.execute(get_records_sql).fetchall()
    finally:
        con.close()


def user_records(db_data:dict):
    """The session's tables and views from the snapshot, shaped like get_records_sql rows."""
    for db_scm, name, kind, cols in db_data['data']:
        if not cols:
            continue  # sequences
        table_type = 'view' if kind == 'view' else 'table'
        yield (db_scm, name, 'table', table_type, None, cols)


def make_completion_map(db_data):
    """makes a map of completion items for the language server"""
    extensions = tuple(sorted(db_data.get('loaded_extensions') or []))
    records = builtin_records(extensions) + list(user_records(db_data))
    function_docs = builtin_functions(extensions)
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

    for cat_schema in db_data['schemas']:
        item_map[cat_schema] = []


    try:
        cur_db = db_data['current_schema'].split('.')[0]
    except:
        cur_db = 'memory'

    for db_scm, fn_name, fn_type, *_ in db_data['functions']:
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

        # if db_scm == db_data['current_schema']:
        #     item_map['root_namespace'].append(comp_item)


    for db_scm, name, obj_type, comp_detial, sql, cols in records:
        item = check_name(name)

        if not db_scm:
            db_scm = 'root_namespace'
        
        if item in [x.label for x in item_map['root_namespace']]:
            continue

        db_schema_split = db_scm.split('.')
        if len(db_schema_split) == 2:
            database, schema = db_schema_split
        else:
            database = 'memory'
            schema = 'main'

        if db_scm not in item_map:
            item_map[db_scm] = []

        if database == cur_db:
            if schema not in item_map:
                item_map[schema] = []
        
        fn_doc = None
        fn_detail = None
        
        sort = sort_map[obj_type]
        if sql == 'internal':
            sort = '9'
        
        if obj_type == 'function' or comp_detial == 'table_macro':
            info = function_docs.get(name.lower())
            if info is not None:
                fn_doc = MarkupContent(kind=MarkupKind.Markdown, value=info.markdown())
                fn_detail = info.detail
        
        comp_item = CmpItem(
                label=item,
                kind=kind_map[obj_type],
                detail=fn_detail,
                typ=comp_detial,
                sort=sort,
                obj_type=obj_type,
                doc=fn_doc) # type: ignore
        

        item_map[db_scm].append(comp_item)
        if database == cur_db:
            item_map[schema].append(comp_item)

        # if db_scm == db_data['current_schema']:
        #     item_map['root_namespace'].append(comp_item)
        
        
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
            if database == cur_db:
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
                item_map['root_namespace'].append(schema_comp)
            if cat_schema in item_map:
                item_map[schema] = item_map[cat_schema]

        if cat not in item_map:
            item_map[cat] = []
            item_map['root_namespace'].append(cat_comp)
            
        item_map[cat].append(schema_comp)
        
    
    
    
    
    root_labels = set([f'{x.label}-{x.obj_type}' for x in item_map['root_namespace']])

    curr_schema = db_data['current_schema']
    if curr_schema not in item_map:
        item_map[curr_schema] = []

    for item in item_map[curr_schema]:
        identifier = f'{item.label}-{item.obj_type}'
        if identifier not in root_labels:
            item_map['root_namespace'].append(item)
            root_labels.add(identifier)
    
    for item in item_map['system.main']: 
        identifier = f'{item.label}-{item.obj_type}'
        if identifier not in root_labels:
            item_map['root_namespace'].append(item)
            root_labels.add(identifier)
    # for cat_schema in db_data['schemas']:
    #     cat, schema = cat_schema.split('.')
    #     if cat == cur_db:
    #         if schema not in item_map and schema not in item_map['root_namespace']:
    #             if not cat_schema in item_map:
    #                 item_map[cat_schema] = []
    #             item_map[schema] = item_map[cat_schema]
    #             item_map['root_namespace'].append(schema_comp)
    
    return item_map


        
   