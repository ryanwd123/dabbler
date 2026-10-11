"""Completion items for one source's schema, queried from the schema cache on demand.

User objects (databases, schemas, tables, views, functions, columns) are read
from the SQLite rows the IPython extension saved, on every request. DuckDB's
own catalog (built-in functions, table functions, system views) doesn't change
between requests, so it's built once per set of extensions and kept in memory.
"""
import functools
import threading
from contextlib import contextmanager
from typing import Union

import duckdb

from dabbler.common import check_name
from dabbler.lsp.db_data import CompletionItemKind, MarkupContent, MarkupKind, builtin_records, make_completion_map
from dabbler.lsp.functions import user_function_info
from dabbler.lsp.parser import Columns, norm_name
from dabbler.lsp.sql_utils import CmpItem
from dabbler.schema_cache import ObjectRow, SchemaCache

TABLE_KINDS = ("table", "view")
SCHEMA_MEMBER_KINDS = ("table", "view", "function", "table_macro")


@functools.lru_cache(maxsize=8)
def builtin_schemas(extensions: tuple[str, ...]) -> list[str]:
    con = duckdb.connect()
    try:
        for ext in extensions:
            try:
                con.load_extension(ext)
            except Exception:
                pass
        return [r[0] for r in con.execute(
            "select database_name || '.' || schema_name from duckdb_schemas() where database_name in ('system', 'temp')"
        ).fetchall()]
    finally:
        con.close()


@functools.lru_cache(maxsize=8)
def builtin_completion_map(extensions: tuple[str, ...]):
    """DuckDB's built-in catalog as a completion map, and its keys by normalized name."""
    builtin_records(extensions)  # warm the shared cache
    data = {
        "data": [],
        "functions": [],
        "schemas": builtin_schemas(extensions),
        "current_schema": "memory.main",
        "loaded_extensions": list(extensions),
    }
    completion_map = make_completion_map(data)
    keys: dict[str, str] = {}
    for k in completion_map:
        keys.setdefault(norm_name(k), k)
    return completion_map, keys


def object_item(row: ObjectRow) -> CmpItem:
    if row.kind == "database":
        return CmpItem(row.name, CompletionItemKind.Folder, None, "database", "3", "database")
    if row.kind == "schema":
        return CmpItem(row.name, CompletionItemKind.Folder, None, "schema", "3", "schema")
    if row.kind in TABLE_KINDS:
        return CmpItem(check_name(row.name), CompletionItemKind.File, None, row.kind, "2", "table")
    info = user_function_info(row.name, row.detail or "function", list(row.parameters or []), row.definition)
    doc = MarkupContent(kind=MarkupKind.Markdown, value=info.markdown())
    if row.kind == "table_macro":
        return CmpItem(row.name, CompletionItemKind.File, info.detail, "function", "9", "table_macro", doc)
    return CmpItem(row.name, CompletionItemKind.Function, info.detail, "function", "9", "function", doc)


def column_item(name: str, typ: Union[str, None]) -> CmpItem:
    return CmpItem(name, CompletionItemKind.Field, None, typ, "1", "column")


def merge(*groups: list[CmpItem]) -> list[CmpItem]:
    """Concatenates item lists, keeping the first item for each (label, object type)."""
    seen, out = set(), []
    for group in groups:
        for item in group:
            key = (item.label, item.obj_type)
            if key not in seen:
                seen.add(key)
                out.append(item)
    return out


class Catalog:
    def __init__(self, cache: SchemaCache, source_file: str):
        self.cache = cache
        self.source = source_file
        info = cache.source_info(source_file)
        if info is None:
            raise ValueError(f"no schema saved for {source_file}")
        self.info = info
        catalog, _, schema = info.current_schema.partition(".")
        self.current_catalog, self.current_schema = catalog, schema or "main"
        self.builtins, self.builtin_keys = builtin_completion_map(tuple(sorted(info.extensions)))
        self._local = threading.local()  # completions run on several threads

    @contextmanager
    def request(self):
        """One completion request: a consistent read of the cache, and lookups memoized for its duration."""
        with self.cache.read_transaction():
            outer = getattr(self._local, "memo", None)
            self._local.memo = outer if outer is not None else {}
            try:
                yield
            finally:
                self._local.memo = outer

    def _memoized(self, key, compute):
        memo = getattr(self._local, "memo", None)
        if memo is None:
            return compute()
        if key not in memo:
            memo[key] = compute()
        return memo[key]

    # -- what completions need --

    def root_items(self) -> list[CmpItem]:
        """Names usable without qualification: the current schema's objects, schemas of the
        current database, databases, and DuckDB's built-ins."""
        def compute():
            src, cc, cs = self.source, self.current_catalog, self.current_schema
            user = [object_item(r) for r in self.cache.objects(src, kinds=["database"])]
            user += [object_item(r) for r in self.cache.objects(src, catalog=cc, kinds=["schema"])]
            user += [object_item(r) for r in self.cache.objects(src, catalog=cc, schema=cs, kinds=SCHEMA_MEMBER_KINDS)]
            return merge(user, self.builtins["root_namespace"])
        return self._memoized("root", compute)

    def members(self, qualifier: str) -> Union[list[CmpItem], None]:
        """What can follow "qualifier.": a table's columns, a schema's objects, or a database's schemas."""
        def compute():
            user = self._user_members(norm_name(qualifier).split(".")) or []
            key = self.builtin_keys.get(norm_name(qualifier))
            builtin = self.builtins[key] if key else []
            return merge(user, builtin) or None
        return self._memoized(("members", norm_name(qualifier)), compute)

    def table_columns(self, name: str) -> Union[Columns, None]:
        """Columns of a table or view, for working out CTE and subquery columns."""
        def compute():
            columns = self._user_columns(norm_name(name).split("."))
            if columns:
                return columns
            key = self.builtin_keys.get(norm_name(name))
            builtin = [(x.label, x.typ) for x in self.builtins.get(key, []) if x.obj_type == "column"] if key else []
            return builtin or None
        return self._memoized(("columns", norm_name(name)), compute)

    def table_macro_sql(self, name: str) -> Union[str, None]:
        def compute():
            rows = self.cache.objects(self.source, name=norm_name(name).split(".")[-1], kinds=["table_macro"])
            rows.sort(key=lambda r: (r.catalog, r.schema) != (self.current_catalog, self.current_schema))
            return next((r.definition for r in rows if r.definition), None)
        return self._memoized(("macro", norm_name(name)), compute)

    def pathlib_paths(self) -> dict[str, str]:
        return self.cache.paths(self.source)

    # -- name resolution against the saved rows --

    def _user_columns(self, parts: list[str]) -> Union[Columns, None]:
        """parts: table, schema.table or catalog.schema.table, normalized."""
        cc, cs = self.current_catalog, self.current_schema
        if len(parts) == 1:
            target = (cc, cs, parts[0])
        elif len(parts) == 2:
            target = (cc, parts[0], parts[1])
        elif len(parts) == 3:
            target = (parts[0], parts[1], parts[2])
        else:
            return None
        return self.cache.columns(self.source, *target) or None

    def _user_members(self, parts: list[str]) -> Union[list[CmpItem], None]:
        src, cc = self.source, self.current_catalog
        columns = self._user_columns(parts)
        if columns:
            return [column_item(n, t) for n, t in columns]
        if len(parts) == 1:
            name = parts[0]
            if self.cache.objects(src, catalog=cc, schema=name, name=name, kinds=["schema"]):
                return self._schema_members(cc, name)
            if self.cache.objects(src, catalog=name, name=name, kinds=["database"]):
                return [object_item(r) for r in self.cache.objects(src, catalog=name, kinds=["schema"])]
        if len(parts) == 2:
            catalog, schema = parts
            if self.cache.objects(src, catalog=catalog, schema=schema, name=schema, kinds=["schema"]):
                return self._schema_members(catalog, schema)
        return None

    def _schema_members(self, catalog: str, schema: str) -> list[CmpItem]:
        rows = self.cache.objects(self.source, catalog=catalog, schema=schema, kinds=SCHEMA_MEMBER_KINDS)
        return [object_item(r) for r in rows]
