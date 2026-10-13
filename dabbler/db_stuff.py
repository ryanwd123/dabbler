import functools
import logging
import os
import re
import sys
import sysconfig
import types
from dataclasses import dataclass
from pathlib import PurePath
from typing import Union

import duckdb

log = logging.getLogger(__name__)


def get_loaded_extensions(db:duckdb.DuckDBPyConnection):
    return [x[0] for x in db.sql(
        """--sql,
        select e.extension_name
        from duckdb_extensions() e
        where e.loaded

        """
        ).fetchall()]



def _is_user_module(module) -> bool:
    """A module of the user's own, not the standard library or an installed package."""
    file = getattr(module, "__file__", None)
    if not file:
        return False
    path = os.path.normcase(os.path.abspath(file))
    return not any(path.startswith(prefix) for prefix in _library_prefixes())


@functools.lru_cache(maxsize=1)
def _library_prefixes() -> tuple[str, ...]:
    paths = sysconfig.get_paths()
    dirs = {paths[k] for k in ("stdlib", "platstdlib", "purelib", "platlib") if k in paths}
    dirs |= {os.path.dirname(os.__file__), sys.prefix, sys.base_prefix}
    return tuple(os.path.normcase(os.path.abspath(d)) + os.sep for d in dirs)


def get_pathlib_paths(user_ns: Union[dict, None] = None):
    """pathlib.Path variables in the IPython namespace (and one level into the user's
    own modules), for path completions in python code.

    Reads values directly; nothing is evaluated, so properties and lazy module
    attributes aren't triggered.
    """
    if user_ns is None:
        from IPython.core.getipython import get_ipython

        ipython = get_ipython()
        if not ipython:
            return []
        user_ns = ipython.user_ns
    paths = []
    for name, value in list(user_ns.items()):
        if name.startswith("_"):
            continue
        if isinstance(value, PurePath):
            paths.append((name, str(value)))
        elif isinstance(value, types.ModuleType) and _is_user_module(value):
            for attr, attr_value in list(vars(value).items()):
                if not attr.startswith("_") and isinstance(attr_value, PurePath):
                    paths.append((f"{name}.{attr}", str(attr_value)))
    return paths


re_session_change = re.compile(
    r"\buse\s|\bset\s+(?:session\s+|local\s+|global\s+)?(?:search_path|schema|file_search_path)\b", re.IGNORECASE
)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass
class SessionState:
    """Settings of the user's connection that a cursor doesn't share: the database and
    schema chosen with USE, search_path and file_search_path."""

    database: Union[str, None] = None
    schema: Union[str, None] = None
    search_path: Union[str, None] = None
    file_search_path: Union[str, None] = None

    @classmethod
    def read(cls, con: duckdb.DuckDBPyConnection) -> "SessionState":
        """Reads the state of a connection. On the user's own connection this drops an
        unfetched result, so only do it right after a cell that changed the state."""
        row = con.execute(
            "select current_database(), current_schema(), current_setting('search_path'), "
            "current_setting('file_search_path')"
        ).fetchone()
        return cls(*row) if row else cls()  # type: ignore

    def apply(self, cur: duckdb.DuckDBPyConnection) -> None:
        if self.database and self.schema:
            cur.execute(f"use {quote_ident(self.database)}.{quote_ident(self.schema)}")
        if self.search_path:
            cur.execute(f"set search_path = {quote_literal(self.search_path)}")
        if self.file_search_path:
            cur.execute(f"set file_search_path = {quote_literal(self.file_search_path)}")


def get_db_data_new(db:duckdb.DuckDBPyConnection, file_search_path:Union[str,None]=None, skip_ipython = False) -> dict:
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



def file_databases(db: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    """(catalog, path) of the DuckDB database files attached to a connection."""
    return db.execute(
        "select database_name, path from duckdb_databases() where path is not null and not internal and type = 'duckdb'"
    ).fetchall()


def database_file_db_data(db_data: dict, catalog: str, path: str) -> dict:
    """The part of a session's db_data in one attached database file, as if the
    file had been opened on its own: the catalog renamed to the file's stem (the
    name DuckDB gives it then) and its main schema current."""
    name = PurePath(path).stem

    def in_catalog(db_scm: str) -> bool:
        return db_scm.partition(".")[0] == catalog

    def rename(db_scm: str) -> str:
        return name + "." + db_scm.partition(".")[2]

    return {
        **db_data,
        "data": [(rename(r[0]), *r[1:]) for r in db_data.get("data") or [] if in_catalog(r[0])],
        "functions": [(rename(r[0]), *r[1:]) for r in db_data.get("functions") or [] if in_catalog(r[0])],
        "schemas": [rename(s) for s in db_data.get("schemas") or [] if in_catalog(s)],
        "databases": [name],
        "paths": [],
        "current_schema": f"{name}.main",
    }


def get_default_db_data():
    db = duckdb.connect()
    db_data = get_db_data_new(db)
    db.close()
    return db_data
