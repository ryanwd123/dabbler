import duckdb
# from dabbler.lsp.server_classes import InlineSqlLangServer
from lsprotocol.types import (
    CompletionItem,
)

duckdb_types = [CompletionItem(label=f'{x[0]}') for x in 
                duckdb.execute("select distinct logical_type from duckdb_types()").fetchall()]


duckdb_extensions = [CompletionItem(label=f"'{x[0]}'") for x in 
                duckdb.execute("select distinct extension_name from duckdb_extensions()").fetchall()]


duckdb_settings = [CompletionItem(label=f'{x[0]}') for x in 
                duckdb.execute("select distinct name from duckdb_settings()").fetchall()]

duckdb_kw_comp = [CompletionItem(label=f'{x[0]}',sort_text="99") for x in 
                duckdb.execute("select upper(keyword_name) from duckdb_keywords() where keyword_category = 'reserved'").fetchall()]

duckdb_pragmas = [CompletionItem(label=f'{x[0]}',sort_text="99") for x in 
                duckdb.execute("select distinct function_name from duckdb_functions() where function_type ='pragma'").fetchall()]


