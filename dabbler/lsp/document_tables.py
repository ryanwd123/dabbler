"""Tables and views a python file creates in its own SQL, for completions elsewhere in the file.

    db.sql("create table sales (id int, total decimal(10, 2))")
    db.sql("create or replace table big_sales as select * from sales where total > 100")

Both tables complete in the file's other SQL strings, with their columns, before
the statements have run. Columns of CREATE TABLE ... AS come from the query, the
way CTE columns do (computed columns have no type). Every CREATE in the file
counts, wherever it is, since notebook cells run in any order; a table the
session or database already has keeps its real columns.

The file's SQL strings are scanned after typing pauses (with the syntax check),
not on each completion; only strings that mention CREATE ... TABLE/VIEW are parsed.
"""
import functools
import re
from dataclasses import dataclass, field
from typing import Callable, Optional

from lark import Token, Tree

from dabbler.common import check_name
from dabbler.lsp.db_data import CompletionItemKind
from dabbler.lsp.parser import Columns, norm_name, rename_columns, sql_parser
from dabbler.lsp.sql_regions import sql_regions
from dabbler.lsp.sql_utils import CmpItem

re_maybe_create = re.compile(r"\bcreate\b[\s\S]*?\b(?:table|view)\b", re.IGNORECASE)
QUERY_NODES = ("query", "set_operation")


@dataclass(frozen=True)
class CreateStatement:
    """A CREATE TABLE / VIEW as written, before its columns are worked out."""
    parts: tuple[str, ...]  # the name: (table,), (schema, table) or (catalog, schema, table)
    kind: str  # 'table' or 'view'
    columns: Optional[tuple[tuple[str, Optional[str]], ...]] = None  # CREATE TABLE t (a INT, ...)
    query: Optional[str] = None  # CREATE ... AS <query>
    column_names: tuple[str, ...] = ()  # CREATE TABLE t (a, b) AS ...


@dataclass
class DocumentTable:
    parts: tuple[str, ...]
    kind: str
    columns: Columns

    @property
    def name(self) -> str:
        return self.parts[-1]


@dataclass
class DocumentTables:
    """The tables a document creates, looked up the way the catalog looks up its own."""
    tables: list[DocumentTable] = field(default_factory=list)

    def _matches(self, table: DocumentTable, parts: list[str]) -> bool:
        # t, s.t and db.s.t all name a table created as s.t: one ends with the other
        names = [norm_name(p) for p in table.parts]
        shorter, longer = sorted((names, parts), key=len)
        return longer[-len(shorter):] == shorter

    def columns(self, name: str) -> Optional[Columns]:
        parts = norm_name(name).split(".")
        return next((t.columns for t in self.tables if self._matches(t, parts)), None)

    def root_items(self) -> list[CmpItem]:
        """Tables created without a schema, which are usable by their name alone."""
        return [table_item(t) for t in self.tables if len(t.parts) == 1]

    def members(self, qualifier: str) -> Optional[list[CmpItem]]:
        """A created table's columns, or the tables created in a schema."""
        columns = self.columns(qualifier)
        if columns is not None:
            return [CmpItem(n, CompletionItemKind.Field, None, t, "1", "column") for n, t in columns if n]
        parts = norm_name(qualifier).split(".")
        in_schema = [
            table_item(t) for t in self.tables
            if len(t.parts) > 1 and [norm_name(p) for p in t.parts[:-1]][-len(parts):] == parts
        ]
        return in_schema or None


def table_item(table: DocumentTable) -> CmpItem:
    return CmpItem(check_name(table.name), CompletionItemKind.File, "created in this file", table.kind, "2", "table")


def _text(sql: str, node) -> str:
    return sql[node.meta.start_pos:node.meta.end_pos]


def _name_parts(table_name: Tree) -> tuple[str, ...]:
    return tuple(t.value.strip('"') for t in table_name.children if isinstance(t, Token))


def _statement(sql: str, node: Tree) -> Optional[CreateStatement]:
    children = [c for c in node.children if isinstance(c, Tree)]
    name = next((c for c in children if c.data == "table_name"), None)
    if name is None:
        return None
    kind = "view" if node.data == "create_view" else "table"
    definitions = next((c for c in children if c.data == "col_defintions"), None)
    if definitions is not None:
        columns = []
        for col in definitions.children:
            if not isinstance(col, Tree) or col.data != "col_defintion":
                continue
            typ = next((c for c in col.children if isinstance(c, Tree) and c.data == "type"), None)
            columns.append((col.children[0].value, _text(sql, typ).upper() if typ is not None else None))
        return CreateStatement(_name_parts(name), kind, columns=tuple(columns))
    query = next((c for c in children if c.data in QUERY_NODES), None)
    if query is None:
        return None
    names = next((c for c in children if c.data == "col_names"), None)
    column_names = tuple(t.value for t in names.children if isinstance(t, Token)) if names is not None else ()
    return CreateStatement(_name_parts(name), kind, query=_text(sql, query), column_names=column_names)


@functools.lru_cache(maxsize=512)
def create_statements(sql: str) -> tuple[CreateStatement, ...]:
    """The CREATE TABLE / VIEW statements in one SQL string (cached by its text)."""
    if not re_maybe_create.search(sql):
        return ()
    found = []
    # a half-typed statement elsewhere in the string shouldn't hide the CREATE, so
    # fall back to its statements one at a time (a ";" in a string literal just fails)
    for piece in [sql] + (sql.split(";") if ";" in sql else []):
        if found or not re_maybe_create.search(piece):
            continue
        try:
            tree = sql_parser.parse(piece)
        except Exception:
            continue
        for node in tree.iter_subtrees_topdown():
            # "create_view" is both the statement's alias and its rule; only the rule has the table_name
            if node.data in ("create_table_statement", "create_view"):
                statement = _statement(piece, node)
                if statement is not None:
                    found.append(statement)
    return tuple(found)


def document_tables(
    source: str, query_columns: Callable[[str, "DocumentTables"], Optional[Columns]]
) -> DocumentTables:
    """The tables created by a python file's SQL strings, in file order.

    query_columns works out a query's output columns; it gets the tables found so
    far, so a CREATE ... AS can read a table created earlier in the file.
    """
    tables = DocumentTables()
    for region in sql_regions(source):
        for statement in create_statements(region.text):
            if statement.columns is not None:
                columns: Columns = list(statement.columns)
            else:
                columns = rename_columns(query_columns(statement.query, tables), list(statement.column_names) or None) or []
            tables.tables.append(DocumentTable(statement.parts, statement.kind, columns))
    return tables


def resolve_with(completer) -> Callable[[str, DocumentTables], Optional[Columns]]:
    """query_columns for document_tables, using a completer's parser and schema."""
    def query_columns(sql: str, so_far: DocumentTables) -> Optional[Columns]:
        with completer.catalog.request(so_far):
            queries, _ = completer.parser2.parse_sql(sql, len(sql))
        if not queries or not queries.queries_list:
            return None
        outer = max(queries.queries_list, key=lambda q: q.end_pos - q.start_pos)
        return outer.projection
    return query_columns
