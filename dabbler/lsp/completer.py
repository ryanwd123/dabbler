import logging
import re
from dabbler.common import grammer_kw
from dabbler.lsp.parser import (
    CursorContext,
    FromRef,
    FromRefKind,
    ParseResult,
    Query,
    SqlParserNew,
    cursor_context,
    lookup,
    norm_name,
    rename_columns,
)
from dabbler.lsp.catalog import Catalog
from dabbler.lsp.db_data import make_engine, search_path_for
from dabbler.schema_cache import IN_MEMORY, SchemaCache
from dabbler.lsp.sql_utils import strip_sql_whitespace, CmpItem
from typing import Union
from dabbler.lsp.completion_utils import (
    PathCompleter,
    duckdb_extensions,
    duckdb_settings,
    duckdb_pragmas,
    duckdb_types,
    file_path_completion_regex,
)
from lsprotocol.types import (
    CompletionItem,
    CompletionList,
    CompletionItemKind,
)

table_types = set(["table", "database", "schema", "cte", "table_macro"])
kw_replace = {
    'ORDER': 'ORDER BY',
    'GROUP': 'GROUP BY',
    'PARTITION': 'PARTITION BY',
}
kw_adds = {
    'CREATE' : 'CREATE OR REPLACE',
    'GROUP' : 'GROUP BY ALL',
    'ORDER': 'ORDER BY ALL',
}

re_cast_as = re.compile(r"(^|\W)(try_)?cast\s*\(([^()]|\([^()]*\))*\s+as $", flags=re.IGNORECASE)


re_word_at_end = re.compile(r"\w+$")


def column_items(projection) -> list[CmpItem]:
    """Completion items for (name, type) columns; unnamed expressions are skipped."""
    return [CmpItem(name, CompletionItemKind.Field, None, typ, "1", "column") for name, typ in projection if name]


def unique_by_label(items: list[CmpItem]) -> list[CmpItem]:
    seen = set()
    result = []
    for x in items:
        if x.label not in seen:
            seen.add(x.label)
            result.append(x)
    return result


class SqlCompleter:
    """Completions for one source (an IPython session's file) in the schema cache.

    SqlCompleter(db_data) is a shortcut that saves db_data to a private
    in-memory cache first, for tests and for the blank default schema.
    """

    def __init__(
        self,
        db_data: Union[dict, None] = None,
        *,
        cache: Union[SchemaCache, None] = None,
        source_file: Union[str, None] = None,
    ) -> None:
        if db_data is not None:
            cache, source_file = SchemaCache(IN_MEMORY), "dabbler-db-data"
            cache.save_snapshot(source_file, db_data)
        if cache is None or source_file is None:
            raise ValueError("SqlCompleter needs db_data, or a cache and a source_file")
        self.catalog = Catalog(cache, source_file)
        info = self.catalog.info
        settings = {"cwd": info.cwd, "file_search_path": info.file_search_path, "loaded_extensions": info.extensions}
        self.path_completer = PathCompleter(info.cwd, info.file_search_path)  # type: ignore
        self.file_search_path = search_path_for(settings)
        self.log = logging.getLogger("completer")
        # DuckDB is only used to describe table functions (reading files); the schema comes from the cache
        self.engine = make_engine(settings)
        self.parser2 = SqlParserNew(
            self.engine,
            self.catalog.table_columns,
            self.file_search_path,
            self.catalog.table_macro_sql,
            remote_files="httpfs" in (info.extensions or []),
        )

    @property
    def pathlib_paths(self) -> dict[str, str]:
        """pathlib.Path variables in the IPython session, for completions in python code."""
        return self.catalog.pathlib_paths()

    def lookup_completion_map(self, name: str) -> Union[list[CmpItem], None]:
        return self.catalog.members(name)

    def table_columns(self, name: str):
        """Columns of a table or view, as (name, type) pairs."""
        return self.catalog.table_columns(name)

    def get_queries(self, pos, sql, repair=False):
        """The innermost query around pos, all queries, and the parse result."""
        queries, parsed = self.parser2.parse_sql(sql, pos, repair=repair)   # type: ignore
        if not queries or not queries.queries_list:
            return None, None, parsed
        queries.queries_list.sort(key=lambda x: x.end_pos - x.start_pos)
        filtered = [x for x in queries.queries_list if x.start_pos <= pos <= x.end_pos]
        if len(filtered) == 0:
            return queries.queries_list[-1], queries, parsed
        return filtered[0], queries, parsed

    def keyword_items(self, parsed: Union[ParseResult, None]) -> list[CmpItem]:
        if not parsed:
            return []
        kw_comps = []
        for c in parsed.choices:
            if c in grammer_kw:
                if c in kw_adds:
                    kw_comps.append(
                        CmpItem(kw_adds[c], CompletionItemKind.Keyword, None, "keyword", "3", "keyword")
                    )
                c = kw_replace.get(c, c)
                kw_comps.append(
                    CmpItem(c, CompletionItemKind.Keyword, None, "keyword", "3", "keyword")
                )
        return kw_comps

    def ref_columns(self, ref: FromRef, scopes: list[Query], queries) -> tuple[Union[list[CmpItem], None], str, str]:
        """Columns of a FROM item, with its detail label and object type."""
        def as_items(projection):
            projection = rename_columns(projection, ref.col_aliases)
            if not projection:
                return None
            return column_items(projection)

        if ref.kind == FromRefKind.subquery:
            subquery = queries.queries.get(ref.start_pos) if queries and ref.start_pos else None
            return as_items(subquery.projection if subquery else None), "sub query", "table"

        if ref.kind == FromRefKind.table_function:
            return as_items(ref.projection), "table function", "table"

        if ref.kind == FromRefKind.values:
            return as_items(ref.projection), "values", "table"

        for scope in scopes:
            cte = (lookup(scope.ctes.map, ref.name) if scope.ctes else None) or lookup(scope.cte_sibblings, ref.name)
            if cte:
                return as_items(cte.projection), "cte", "cte"

        columns = self.lookup_completion_map(ref.name)
        if columns is not None and ref.col_aliases:
            columns = [
                CmpItem(alias, x.kind, x.detail, x.typ, x.sort, x.obj_type, x.doc)
                for alias, x in zip(ref.col_aliases, columns)
            ] + columns[len(ref.col_aliases):]
        return columns, "table_alias", "table"

    def parse_sql2(self, pos, sql: str) -> tuple[dict[str, list[CmpItem]], Union[ParseResult, None]]:
        """Completion items from the SQL itself: aliases, CTEs, their columns, and keywords.

        Aliases from enclosing queries are in scope too (correlated subqueries,
        and DuckDB's implicit lateral joins); inner ones shadow outer ones.
        """
        q, queries, parsed = self.get_queries(pos, sql, repair=True)
        comp_map: dict[str, list[CmpItem]] = {"root_namespace": self.keyword_items(parsed)}
        if q is None or queries is None:
            return comp_map, parsed

        scopes = [x for x in queries.queries_list if x.start_pos <= q.start_pos and x.end_pos >= q.end_pos]
        root = comp_map["root_namespace"]
        refs: dict[str, tuple[Query, FromRef]] = {}
        for scope in scopes:
            for alias, ref in scope.from_refs.items():
                if lookup(refs, alias) is None:
                    refs[alias] = (scope, ref)

        in_scope_columns: list[CmpItem] = []
        for alias, (owner, ref) in refs.items():
            owner_scopes = scopes[scopes.index(owner):]
            columns, detail, obj_type = self.ref_columns(ref, owner_scopes, queries)
            if columns is None and detail == "table_alias":
                continue  # unknown table
            root.append(CmpItem(alias, CompletionItemKind.File, None, detail, "1", obj_type))
            if columns:
                comp_map[alias] = columns
                in_scope_columns.extend(columns)

        for scope in scopes:
            ctes = dict(scope.cte_sibblings)
            if scope.ctes:
                ctes.update(scope.ctes.map)
            for name, cte in ctes.items():
                if lookup(comp_map, name) is None:
                    projection = rename_columns(cte.projection, cte.col_aliases)
                    if projection:
                        comp_map[name] = column_items(projection)
                root.append(CmpItem(name, CompletionItemKind.File, None, "cte", "1", "cte"))

        root.extend(unique_by_label(in_scope_columns))
        # DuckDB lets select-list aliases be used later in the same query
        for scope in scopes:
            for name in scope.aliases_visible_at(pos):
                root.append(CmpItem(name, CompletionItemKind.Variable, None, "alias", "1", "column"))
        comp_map["root_namespace"] = unique_by_label(root)
        return comp_map, parsed

    def route_completion2(
        self,
        cursor_pos: int,
        txt: str,
        trigger: Union[str,None],
    ):
        # one consistent read of the schema cache per request, even if IPython saves meanwhile
        with self.catalog.request():
            return self._route_completion(cursor_pos, txt, trigger)

    def _route_completion(self, cursor_pos: int, txt: str, trigger: Union[str, None]):
        sql_left_of_cur = strip_sql_whitespace(txt[:cursor_pos])

        # contexts the grammar doesn't model
        if sql_left_of_cur[-2:] == "::" or re_cast_as.search(sql_left_of_cur):
            return CompletionList(is_incomplete=False, items=duckdb_types)

        if re.match(
            r"(^| )(load |install )$", sql_left_of_cur, flags=re.IGNORECASE
        ):
            return CompletionList(is_incomplete=False, items=duckdb_extensions)

        if re.match(r"(^| )(pragma )$", sql_left_of_cur, flags=re.IGNORECASE):
            return CompletionList(is_incomplete=False, items=duckdb_pragmas)

        if re.match(r"(^| )(set |reset )$", sql_left_of_cur, flags=re.IGNORECASE):
            return CompletionList(
                is_incomplete=False,
                items=duckdb_settings
                + [CompletionItem(label=c) for c in ["LOCAL", "SESSION", "GLOBAL"]],
            )

        if re.match(
            r"(^| )(set |reset )(local | session | global )$",
            sql_left_of_cur,
            flags=re.IGNORECASE,
        ):
            return CompletionList(is_incomplete=False, items=duckdb_settings)

        file_comp = file_path_completion_regex.match(sql_left_of_cur)
        if file_comp:
            items = self.path_completer.get_items(file_comp.group(2))
            return CompletionList(is_incomplete=False, items=items)

        try:
            parsed_items, parsed = self.parse_sql2(cursor_pos, txt)
        except Exception:
            self.log.exception("failed to collect completion items from the SQL")
            parsed_items, parsed = {"root_namespace": []}, None

        context = cursor_context(parsed, txt, cursor_pos) if parsed else CursorContext("unknown")
        if context.kind == "unknown":
            return self.route_by_regex(sql_left_of_cur, trigger, parsed_items)
        return self.route_by_context(context, txt[:cursor_pos], trigger, parsed_items)

    def route_by_context(self, context: CursorContext, left: str, trigger: Union[str, None], parsed_items):
        """Completions for what the parser expects at the cursor."""
        if context.kind in ("string", "comment"):
            return None

        if context.kind == "type":
            return CompletionList(is_incomplete=False, items=duckdb_types)

        typed = re_word_at_end.search(left)
        prefix = typed.group(0).lower() if typed else ""
        catalog = self.catalog.root_items()
        local = parsed_items["root_namespace"]

        def complete(items: list[CmpItem]):
            if prefix:
                return CompletionList(
                    is_incomplete=True, items=[x.comp for x in items if x.label.lower().startswith(prefix)]
                )
            return CompletionList(is_incomplete=False, items=[x.comp for x in items])

        if context.kind == "table":
            if context.qualifier:
                found = self.lookup_completion_map(".".join(context.qualifier)) or []
                return complete([x for x in unique_by_label(found) if x.obj_type in table_types])
            # space after FROM/JOIN is where the table list is wanted, so don't wait for a letter
            return complete([x for x in catalog + local if x.obj_type in table_types])

        if context.kind == "expression" and context.qualifier:
            key = ".".join(context.qualifier)
            # a query's own aliases shadow same-named tables and schemas
            found = lookup(parsed_items, key) or self.lookup_completion_map(key) or []
            return complete(unique_by_label(found))

        if context.kind == "expression":
            items = local + catalog
        else:  # keyword
            items = [x for x in local if x.obj_type == "keyword"]

        if prefix:
            return complete(items)
        if trigger is None:
            # completion invoked (ctrl+space) with nothing typed: offer everything in scope
            return CompletionList(is_incomplete=True, items=[x.comp for x in items])
        # a typed space or other trigger character: don't pop up a list in the middle of an expression
        return None

    def route_by_regex(self, sql_left_of_cur: str, trigger: Union[str, None], parsed_items):
        """The text-pattern router, for when the SQL before the cursor doesn't parse."""
        root_items = self.catalog.root_items()

        if re.match(
            r".*(^| )(join |from |pivot |unpivot |alter table |insert into )(\w+( \w+)?, )*\w?$",
            sql_left_of_cur,
            flags=re.IGNORECASE,
        ):
            tbls = [
                x.comp for x in root_items if x.obj_type in table_types
            ]
            tbls += [
                x.comp
                for x in parsed_items["root_namespace"]
                if x.obj_type in table_types
            ]
            return CompletionList(is_incomplete=False, items=tbls)

        if trigger == " ":
            return

        m = re.match(
            r".*(^| )(join |from |pivot |unpivot |alter table |insert into )(\w+( \w+)?, )*(?P<dotitems>(\w+\.)+)$",
            sql_left_of_cur,
            flags=re.IGNORECASE,
        )
        if m and trigger == ".":
            key = m.group("dotitems").strip(".")
            found = (self.lookup_completion_map(key) or []) + (lookup(parsed_items, key) or [])
            items = [x.comp for x in unique_by_label(found) if x.obj_type in table_types]
            return CompletionList(is_incomplete=False, items=items)

        m = re.match(
            r".*(^| |\()(?P<dotitems>(\w+\.)+)$", sql_left_of_cur, flags=re.IGNORECASE
        )
        if m and trigger == ".":
            key = m.group("dotitems").strip(".")
            # a query's own aliases shadow same-named tables and schemas
            found = lookup(parsed_items, key) or self.lookup_completion_map(key) or []
            items = [x.comp for x in unique_by_label(found)]
            if len(items) > 0:
                return CompletionList(is_incomplete=False, items=items)

        m = re.match(
            r".*(^| |\()(?P<char>(\w+))$", sql_left_of_cur, flags=re.IGNORECASE
        )
        if m:
            chars = m.group("char").lower()
            comp_items = [x for x in root_items]
            comp_items += parsed_items["root_namespace"]
            return CompletionList(
                is_incomplete=True,
                items=[x.comp for x in comp_items if x.label.lower().startswith(chars)],
            )

        if trigger is None and re.search(r"(^|[\s,(])$", sql_left_of_cur):
            # completion invoked (ctrl+space) with nothing typed: offer everything in scope
            comp_items = parsed_items["root_namespace"] + root_items
            return CompletionList(is_incomplete=True, items=[x.comp for x in comp_items])

        return None
