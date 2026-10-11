import os
import re
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum
from lark import Lark, Transformer, v_args, Discard, Visitor, Tree, UnexpectedToken, Token, UnexpectedCharacters
from lark.parsers.lalr_interactive_parser import InteractiveParser
from typing import Callable, Union
import duckdb
import logging
import time

lark_cache = Path(__file__).parent.joinpath('lark_cache')
lark_file = Path(__file__).parent.joinpath('duckdb.lark')

def set_search_path_sql(search_path:str):
    # SET doesn't take bound parameters, so quote the literal
    return "set file_search_path = '{}'".format(search_path.replace("'", "''"))


def parser_error_handler(e:UnexpectedToken):
    # assert isinstance(e, UnexpectedToken)

    
    if '_AS' in e.accepts and e.token.type == 'RPAREN':
        e.interactive_parser.feed_token(Token('_AS', 'AS'))
        e.interactive_parser.feed_token(Token('NAME', 'placeholder'))
        e.interactive_parser.feed_token(e.token)
        return True
    
    if e.token == Token('$END', '') and 'NAME' in e.accepts:
        e.interactive_parser.feed_token(Token('NAME', 'xyz'))
        return True
    
    if 'NAME' in e.accepts:
        e.interactive_parser.feed_token(Token('NAME', 'xyz'))
        e.interactive_parser.feed_token(e.token)
        return True
    
    # print(e.token)
    # e.interactive_parser.feed_eof() 
    # e.interactive_parser.feed_token(Token('NAME', 'xyz'))
    # e.interactive_parser.feed_token(e.token)
    return False


def get_parser():
    sql_grammer = lark_file.read_text()
    sql_parser = Lark(
        sql_grammer,
        parser="lalr",
        cache=str(lark_cache),
        propagate_positions=True,
        maybe_placeholders=True,
        debug=False,
    )
    return sql_parser

sql_parser = get_parser()

@dataclass
class Cte:
    name: str
    sql: str
    projection: Union[list[tuple[str,Union[str,None]]], None]
    cte_start: int
    self_start: int
    col_aliases: Union[list[str], None] = None

    @property
    def definition(self):
        """The CTE as it appears in a WITH clause."""
        cols = f"({', '.join(self.col_aliases)})" if self.col_aliases else ""
        return f"{self.name}{cols} as ({self.sql})"


def with_clause(ctes: dict[str, Cte]):
    if not ctes:
        return ""
    return "with " + ",\n".join(c.definition for c in ctes.values())


def rename_columns(projection, col_aliases: Union[list[str], None]):
    """Applies a column alias list like t(a, b) to the first columns of a projection."""
    if not projection or not col_aliases:
        return projection
    renamed = [(alias, col[1]) for alias, col in zip(col_aliases, projection)]
    return renamed + list(projection[len(col_aliases):])


Columns = list[tuple[Union[str, None], Union[str, None]]]
"""(column name, type) pairs; the type is None when it isn't known. The name is
None for unaliased expressions: they don't get completions, but they keep their
position so column alias lists like t(a, b) line up."""


def norm_name(name: str) -> str:
    """DuckDB matches identifiers case-insensitively, and quoting doesn't change the name."""
    return ".".join(part.strip('"') for part in name.split(".")).lower()


def lookup(mapping: dict, name: str):
    """mapping[name], falling back to a case- and quote-insensitive match."""
    if name in mapping:
        return mapping[name]
    target = norm_name(name)
    for k, v in mapping.items():
        if norm_name(k) == target:
            return v
    return None


@dataclass
class SelectItem:
    """One entry of a select list, as far as its output column names go."""
    name: Union[str, None] = None
    """Output name: the alias, or the column of a plain column reference. None for
    unaliased expressions, which don't get completions."""
    source: Union[tuple[Union[str, None], str], None] = None
    """(qualifier, column) when the expression is a plain column reference, for its type."""
    star: bool = False
    qualifier: Union[str, None] = None
    """t in t.*"""
    exclude: list[str] = field(default_factory=list)
    replace: list[str] = field(default_factory=list)
    regex: Union[str, None] = None
    """COLUMNS('regex')"""
    aliased: bool = False
    """name comes from an explicit alias (`expr AS name`)"""
    end_pos: Union[int, None] = None


def _star_item(star: Tree, regex: Union[str, None] = None) -> SelectItem:
    item = SelectItem(star=True, regex=regex)
    for child in star.children:
        if isinstance(child, Token) and child.type == "IDENT":
            item.qualifier = child.value
        elif isinstance(child, Tree) and child.data == "exclude":
            item.exclude = [c.children[0].value for c in child.children if isinstance(c, Tree)]
        elif isinstance(child, Tree) and child.data == "replace":
            item.replace = [c.children[-1].value for c in child.children if isinstance(c, Tree)]
        elif isinstance(child, Tree) and child.data == "columns_exp":
            return _columns_item(child)
    return item


def _columns_item(columns: Tree) -> SelectItem:
    """COLUMNS('regex') or COLUMNS(* EXCLUDE (...)); lambda forms aren't expanded."""
    arg = columns.children[1] if len(columns.children) > 1 else None
    if isinstance(arg, Token) and arg.type == "STRING":
        return SelectItem(star=True, regex=arg.value[1:-1].replace("''", "'"))
    if isinstance(arg, Tree) and arg.data == "star_options":
        return _star_item(arg)
    return SelectItem()


def select_items(col_exps: Tree) -> list[SelectItem]:
    items = []
    for col in col_exps.children:
        if not isinstance(col, Tree) or not col.children:
            continue
        first = col.children[0]
        if isinstance(first, Tree) and first.data == "star_options":
            items.append(_star_item(first))
            continue
        alias = col.children[1] if len(col.children) > 1 else None
        source = None
        if isinstance(first, Tree) and first.data == "col_ref":
            qualifier, column = first.children[0], first.children[-1]
            if isinstance(column, Token):
                source = (qualifier.value if isinstance(qualifier, Token) else None, column.value)
        name = alias.children[0].value if isinstance(alias, Tree) else (source[1] if source else None)
        items.append(SelectItem(
            name=name, source=source, aliased=isinstance(alias, Tree), end_pos=getattr(col.meta, "end_pos", None)
        ))
    return items


@dataclass
class Ctes:
    sql: str
    map: dict[str, Cte]
    start_pos: int
    end_pos: int
    parent: Union["Query",None] = None


@dataclass
class Query:
    ctes: Union[Ctes,None]
    sql: str
    projection: Union[list[tuple[str, Union[str,None]]], None]
    from_refs: dict[str, 'FromRef']
    start_pos: int
    end_pos: int
    cte_sibblings: dict[str, Cte]
    set_operation:bool
    select_items: list[SelectItem] = field(default_factory=list)
    set_first: Union["Query", None] = None
    """For UNION etc.: the first branch, which names the output columns."""
    clauses: list[tuple[str, int, int]] = field(default_factory=list)
    """(clause, start, end) for select_clause, from_clause, where, group_by, ..., in text order."""

    def clause_at(self, pos: int) -> Union[str, None]:
        """The clause the position is in: the last one that starts at or before it."""
        current = None
        for name, start, _ in self.clauses:
            if start <= pos:
                current = name
        return current

    def aliases_visible_at(self, pos: int) -> list[str]:
        """Select-list aliases DuckDB lets you use at pos: earlier ones within the
        select list, all of them in WHERE, GROUP BY, HAVING, QUALIFY and ORDER BY."""
        clause = self.clause_at(pos)
        aliased = [i for i in self.select_items if i.aliased and i.name]
        if clause == "select_clause":
            return [i.name for i in aliased if i.end_pos is not None and i.end_pos < pos]  # type: ignore
        if clause in ALIAS_CLAUSES:
            return [i.name for i in aliased]  # type: ignore
        return []


QUERY_CLAUSES = ("select_clause", "from_clause", "where", "group_by", "having", "qualify", "order_by", "limit")
ALIAS_CLAUSES = ("where", "group_by", "having", "qualify", "order_by")


class FromRefKind(Enum):
    table = 0
    table_function = 1
    subquery = 2
    values = 3


@dataclass
class FromRef:
    kind: FromRefKind
    alias: str
    name: str
    sql: Union[str,None] = None
    start_pos: Union[int,None] = None
    projection: Union[list[tuple[str,str]],None] = None
    col_aliases: Union[list[str],None] = None


def col_alias_names(value) -> Union[list[str], None]:
    """Names from a col_alias / cte_col_alias tree, e.g. t(a, b) -> ['a', 'b']."""
    if not isinstance(value, Tree):
        return None
    return [x.value for x in value.children if isinstance(x, Token)] or None


class TransformFromClause(Transformer):
    def __init__(self, sql: str, parser:'SqlParserNew') -> None:
        super().__init__()
        self.sql = sql
        self.parser = parser
        self.map = {}

    def db_table(self, value):
        if value[1]:
            alias = value[1]
        else:
            alias = value[0]

        self.map[alias] = FromRef(kind=FromRefKind.table, alias=alias, name=value[0], col_aliases=col_alias_names(value[2]))
        # return {alias:FromRef(kind=FromRefKind.table,alias=alias,name=value[0])}

    def join_type(self, value):
        return Discard

    def table_name(self, value):
        tbl = ".".join([x.value for x in value if x])
        return tbl

    def alias(self, value):
        return value[0].value

    def table_function(self, value):
        sql:str = self.sql[value[0].meta.start_pos : value[0].meta.end_pos]
        # without an alias the function's name is the alias, as in DuckDB
        names = [c.value for c in value[0].children[:3] if isinstance(c, Token)]
        alias = value[1] or (names[-1] if names else None)
        if not alias:
            return
        self.map[alias] = FromRef(
            kind=FromRefKind.table_function,
            alias=alias,
            name=names[-1] if names else alias,
            sql=sql,
            projection=self.parser.table_function_columns(names[-1] if names else alias, sql),
            col_aliases=col_alias_names(value[2]),
        )

    def values(self, value):
        if value and isinstance(value[0], Token) and value[0].type == "VALUES":
            # the VALUES list itself: DuckDB names its columns col0, col1, ...
            first_row = next((v for v in value[1:] if isinstance(v, Tree)), None)
            width = len([c for c in first_row.children if c is not None]) if first_row else 0
            return [f"col{i}" for i in range(width)]
        default_names, alias, col_alias = (list(value) + [None, None, None])[:3]
        if not alias:
            return Discard
        names = col_alias_names(col_alias) or default_names or []
        self.map[alias] = FromRef(
            kind=FromRefKind.values,
            alias=alias,
            name=alias,
            projection=[(n, None) for n in names],
        )

    def _inner_query_start(self, from_pos: int):
        """Start of the query inside the first "(" at or after from_pos.

        Queries are keyed by where their text starts, so skip whitespace after the paren.
        """
        paren = self.sql.index("(", from_pos)
        m = non_space.search(self.sql, paren + 1)
        return m.start() if m else paren + 1

    @v_args(tree=True)
    def subquery(self, tree: Tree):
        value = tree.children
        if value[1]:
            alias = value[1]
            sql = value[0]
            self.map[alias] = FromRef(
                kind=FromRefKind.subquery,
                alias=alias, # type: ignore
                name=sql, # type: ignore
                sql=sql, # type: ignore
                start_pos=self._inner_query_start(tree.meta.start_pos),
                col_aliases=col_alias_names(value[2]),
            )
            # return {alias:sql}
        else:
            return Discard

    @v_args(tree=True)
    def lateral_subquery(self, tree: Tree):
        tree.children = tree.children[1:]  # drop the LATERAL token
        return self.subquery(tree)


class QueryToSql(Transformer):
   def __init__(self, sql: str) -> None:
      super().__init__()
      self.sql = sql

   @v_args(tree=True)
   def query(self, value: Tree):
      sql = self.sql[value.meta.start_pos : value.meta.end_pos]
      return sql


class GetQueries(Visitor):
   def __init__(self, sql: str, parser:'SqlParserNew',pos) -> None:
      super().__init__()
      self.sql = sql
      self.parser = parser
      self.queries: dict[int, Query] = {}
      self.queries_list: list[Query] = []
      self.q_start_end:list[tuple[int,int]] = []
      self.pos = pos

   def get_from_tables(self, tree: Tree):
      s1 = QueryToSql(self.sql).transform(tree)
      fr = TransformFromClause(self.sql,self.parser)
      fr.transform(s1).pretty()
      return fr.map


   def get_ctes(self, cte: Tree):
      if not cte:
         return
      if cte.data != "cte":
        #  print(cte.pretty())
         raise Exception("not cte")

      cte_map = {}

      for x in cte.children:
         if not isinstance(x, Tree):
               continue
         name = x.children[0].value # type: ignore

         q = self.get_query(x.children[3])
         if not q:
            continue    

         c = Cte(
               name=name,
               sql=self.sql[x.children[3].meta.start_pos : x.children[3].meta.end_pos],
               projection=q.projection,
               cte_start=cte.meta.start_pos,
               self_start=x.children[3].meta.start_pos,
               col_aliases=col_alias_names(x.children[1]),
         )

         cte_map[name] = c

      cte_sql_txt = self.sql[cte.meta.start_pos : cte.meta.end_pos]

      return Ctes(
         sql=cte_sql_txt,
         map=cte_map,
         start_pos=cte.meta.start_pos,
         end_pos=cte.meta.end_pos,
      )

   def get_query(self, tree: Tree):
      if not tree:
        return
      cte_data = None
      set_op = False
      set_first = None
      items: list[SelectItem] = []
      clauses: list[tuple[str, int, int]] = []
      if tree.children[0]:
         if tree.children[0].data == "query" and tree.data == 'query':
            q = self.get_query(tree.children[0])
            return q
         if tree.data == "set_operation":
            set_op = True
            set_first = self.get_query(tree.children[0])
            
      
         if tree.children[0].data == "cte":
            cte_data = self.get_ctes(tree.children[0])
            # print(tree.pretty())
            # raise Exception("not cte")
      # print(cte_data)
      sql_txt = self.sql[tree.meta.start_pos : tree.meta.end_pos]
      from_refs = {}
      for x in tree.children:
         # print(x)
         if not isinstance(x, Tree):
               continue
         if x.data == "from_clause":
               # print(2,x.pretty())
               from_refs = self.get_from_tables(x)
               # print(2,from_refs)
         if x.data == "select_clause":
            items = select_items(x.children[4])
         if x.data in QUERY_CLAUSES and not x.meta.empty:
            clauses.append((x.data, x.meta.start_pos, x.meta.end_pos))
      start = tree.meta.start_pos
      end = tree.meta.end_pos
      q = Query(
         ctes=cte_data,
         sql=sql_txt,
         projection=[(i.name, None) for i in items if i.name],
         from_refs=from_refs,
         start_pos=start,
         end_pos=end,
         cte_sibblings={},
         set_operation=set_op,
         select_items=items,
         set_first=set_first,
         clauses=clauses,
      )
      
      return q
   
   def set_operation(self, tree: Tree):
      self.query(tree)

   def update_statment(self, tree: Tree):
      """UPDATE target [AS alias] SET ... [FROM ...]: the target and FROM items are in scope."""
      names = [c for c in tree.children if isinstance(c, Tree) and c.data == "table_name"]
      if not names:
         return
      name = ".".join(t.value for t in names[0].children if isinstance(t, Token))
      alias = ".".join(t.value for t in names[1].children if isinstance(t, Token)) if len(names) > 1 else name
      from_refs = {alias: FromRef(kind=FromRefKind.table, alias=alias, name=name)}
      for x in tree.children:
         if isinstance(x, Tree) and x.data == "from_clause":
            from_refs.update(self.get_from_tables(x))
      self.add_statement_scope(tree, from_refs)

   def delete_statement(self, tree: Tree):
      """DELETE FROM target [AS alias] [USING ...] [WHERE ...]: the target and USING items are in scope."""
      names = [c for c in tree.children if isinstance(c, Tree) and c.data == "table_name"]
      if not names:
         return
      name = ".".join(t.value for t in names[0].children if isinstance(t, Token))
      aliases = [c for c in tree.children if isinstance(c, Tree) and c.data == "alias"]
      alias = aliases[0].children[0].value if aliases else name
      from_refs = {alias: FromRef(kind=FromRefKind.table, alias=alias, name=name)}
      for x in tree.children:
         if isinstance(x, Tree) and x.data == "from_ref":
            from_refs.update(self.get_from_tables(x))
      self.add_statement_scope(tree, from_refs)

   def add_statement_scope(self, tree: Tree, from_refs: dict):
      """Registers an UPDATE or DELETE as a query whose FROM items are from_refs."""
      q = Query(
         ctes=None,
         sql=self.sql[tree.meta.start_pos : tree.meta.end_pos],
         projection=[],
         from_refs=from_refs,
         start_pos=tree.meta.start_pos,
         end_pos=tree.meta.end_pos,
         cte_sibblings={},
         set_operation=False,
      )
      if (q.start_pos, q.end_pos) in self.q_start_end:
         return
      self.q_start_end.append((q.start_pos, q.end_pos))
      self.queries_list.append(q)
      self.queries[q.start_pos] = q
      
      
   def query(self, tree: Tree):

      q = self.get_query(tree)
      if not q:
         return
      if (q.start_pos,q.end_pos) in self.q_start_end:
         return
      self.q_start_end.append((q.start_pos,q.end_pos))
      self.queries_list.append(q)
      self.queries[q.start_pos] = q


incomplete_col_ref = re.compile(r'(\w+[.])([\n\s),])')
remote_url = re.compile(r"\b[a-z][a-z0-9+]*://", re.IGNORECASE)
DESCRIBE_TIMEOUT_SECONDS = 1.0
DESCRIBE_CACHE_SIZE = 256
non_space = re.compile(r'\S')
quoted_text = re.compile(r"""'(?:[^']|'')*'|"[^"]*\"""")

# Stands in for the word being typed when half-typed SQL needs repairing to parse.
CURSOR_PLACEHOLDER = "dabbler_cursor_zz"


def repair_candidates(sql: str, pos: int):
    """Edits at the cursor that often turn half-typed SQL into SQL that parses.

    `from |)` needs a table name and `sum(o.|` needs its closing paren, so try
    a placeholder word, the missing parens, and both.
    """
    left = quoted_text.sub("", sql[:pos])
    closing = ")" * max(left.count("(") - left.count(")"), 0)
    candidates = [CURSOR_PLACEHOLDER]
    if closing:
        candidates += [closing, CURSOR_PLACEHOLDER + closing]
    for insert in candidates:
        yield sql[:pos] + insert + sql[pos:]

class ProjectionResolver:
    """Works out queries' output columns from the SQL itself, without running it.

    Columns come from select-list names and aliases; * and COLUMNS(...) are
    expanded from what the query reads: tables (table_columns, which looks
    them up in the session's schema snapshot), CTEs, subqueries, VALUES lists
    and table functions. Types are kept for plain column references and are
    None for expressions.
    """

    def __init__(self, queries: "GetQueries", table_columns: Callable[[str], Union[Columns, None]]):
        self.queries = queries
        self.table_columns = table_columns
        self.memo: dict[int, Union[Columns, None]] = {}
        self.ref_memo: dict[int, Union[Columns, None]] = {}

    def enclosing(self, q: Query) -> list[Query]:
        """q and the queries around it, innermost first."""
        around = [
            x for x in self.queries.queries_list
            if x is not q and x.start_pos <= q.start_pos and x.end_pos >= q.end_pos
        ]
        around.sort(key=lambda x: x.end_pos - x.start_pos)
        return [q] + around

    def find_cte(self, name: str, q: Query) -> Union[Cte, None]:
        for scope in self.enclosing(q):
            cte = (lookup(scope.ctes.map, name) if scope.ctes else None) or lookup(scope.cte_sibblings, name)
            if cte:
                return cte
        return None

    def projection(self, q: Union[Query, None]) -> Union[Columns, None]:
        if q is None:
            return None
        key = id(q)
        if key in self.memo:
            return self.memo[key]
        self.memo[key] = None  # a recursive CTE refers to itself
        if q.set_first is not None:
            result = self.projection(q.set_first)
        else:
            result = []
            for item in q.select_items:
                if item.star:
                    result.extend(self.expand_star(item, q))
                else:
                    result.append((item.name, self.source_type(item, q)))
        self.memo[key] = result
        return result

    def ref_columns(self, ref: FromRef, scope: Query) -> Union[Columns, None]:
        key = id(ref)
        if key in self.ref_memo:
            return self.ref_memo[key]
        self.ref_memo[key] = None
        if ref.kind == FromRefKind.subquery:
            sub = self.queries.queries.get(ref.start_pos) if ref.start_pos is not None else None
            cols = rename_columns(self.projection(sub), ref.col_aliases)
        elif ref.kind == FromRefKind.values:
            cols = ref.projection
        elif ref.kind == FromRefKind.table_function:
            cols = rename_columns(ref.projection, ref.col_aliases)
        else:
            cols = None
            cte = self.find_cte(ref.name, scope)
            if cte is not None:
                cte_query = self.queries.queries.get(cte.self_start)
                cols = rename_columns(self.projection(cte_query), cte.col_aliases)
            if cols is None:
                # not a CTE, or a CTE named after the table it reads from
                cols = self.table_columns(ref.name)
            cols = rename_columns(cols, ref.col_aliases)
        self.ref_memo[key] = cols
        return cols

    def refs_for(self, q: Query, qualifier: Union[str, None]):
        refs = list(q.from_refs.values())
        if qualifier:
            refs = [r for a, r in q.from_refs.items() if norm_name(a) == norm_name(qualifier)]
        return refs

    def expand_star(self, item: SelectItem, q: Query) -> Columns:
        exclude = {norm_name(x) for x in item.exclude}
        replace = {norm_name(x) for x in item.replace}
        pattern = None
        if item.regex is not None:
            try:
                pattern = re.compile(item.regex)  # DuckDB matches anywhere in the name, case-sensitively
            except re.error:
                return []
        out: Columns = []
        for ref in self.refs_for(q, item.qualifier):
            for name, typ in self.ref_columns(ref, q) or []:
                key = norm_name(name) if name else None
                if key is not None and key in exclude:
                    continue
                if pattern is not None and not (name and pattern.search(name.strip('"'))):
                    continue
                out.append((name, None if key in replace else typ))
        return out

    def source_type(self, item: SelectItem, q: Query) -> Union[str, None]:
        if not item.source:
            return None
        qualifier, column = item.source
        for ref in self.refs_for(q, qualifier):
            for name, typ in self.ref_columns(ref, q) or []:
                if name and norm_name(name) == norm_name(column):
                    return typ
        return None


class SqlParserNew:
    
    def __init__(
        self,
        engine: Union[duckdb.DuckDBPyConnection, None] = None,
        table_columns: Union[Callable[[str], Union[Columns, None]], None] = None,
        file_search_path: Union[str, None] = None,
        table_macro_sql: Union[Callable[[str], Union[str, None]], None] = None,
        remote_files: bool = False,
    ) -> None:
        """engine: a plain DuckDB connection, used only to describe table functions.
        table_columns: columns of a table or view by name, from the schema snapshot.
        table_macro_sql: the body of the session's table macro with that name, if any.
        remote_files: whether to describe calls reading URLs (the session has httpfs)."""
        self.engine = engine
        self.table_columns = table_columns or (lambda name: None)
        self.table_macro_sql = table_macro_sql or (lambda name: None)
        self.projection_cache = {}
        # columns of table function calls by their text, most recently used last
        self.describe_cache: OrderedDict[str, Union[Columns, None]] = OrderedDict()
        self.remote_files = remote_files
        self.file_search_path = file_search_path
        self.log = logging.getLogger('sql_parser')
        self.log_describe = logging.getLogger('sql_describe')
        self.log_query_output = logging.getLogger('query_output')
        self.log_interactive_parser = logging.getLogger('interactive_parser')
        

    def parse_sql(self, sql: str, pos:int, repair:bool=False) -> tuple[Union["GetQueries", None], Union["ParseResult", None]]:
        """Parses sql and collects its queries.

        Returns the queries (None if the SQL couldn't be parsed) and the parse
        result, which says what the parser expected at the cursor.

        With repair=True, SQL that doesn't parse as typed is retried with small
        edits at the cursor (see repair_candidates). Only use that for
        completions: the returned queries contain the edits.
        """
        try:
            parsed = interactive_parse(sql, pos)
        except Exception as e:
            self.log.info(['failed to parse',sql,e])
            return None, None
        tree = parsed.tree
        if not tree and repair:
            for repaired in repair_candidates(sql, pos):
                try:
                    tree = sql_parser.parse(repaired)
                except Exception:
                    continue
                sql = repaired
                break
        if not tree:
            tree = parsed.partial_tree
        if not tree:
            return None, parsed
        queries = GetQueries(sql,self,pos)
        queries.visit(tree)


        #add cte sibblings
        subq_lu = {}
        cte_lu = {}
        for q in queries.queries_list:
            if not q.ctes:
                continue
            for k, v in q.ctes.map.items():
                cte_lu[v.self_start] = v.cte_start
            for k, v in q.from_refs.items():
                if v.kind == FromRefKind.subquery:
                    subq_lu[v.start_pos] = q.start_pos

        for q in queries.queries_list:
            if q.start_pos not in cte_lu:
                continue
            subq_1 = queries.queries[cte_lu[q.start_pos]]
            if not subq_1.ctes:
                continue
            parent_map = subq_1.ctes.map
            sibblings = {k: v for k, v in parent_map.items() if v.self_start < q.start_pos}
            q.cte_sibblings = sibblings

        for q in queries.queries_list:
            if q.start_pos not in subq_lu:
                continue
            subq = queries.queries[subq_lu[q.start_pos]]
            if not subq.ctes:
                continue
            q.cte_sibblings = subq.ctes.map

        for q in queries.queries_list:
            if q.set_operation:
                for q_in_set in [x for x in queries.queries_list if x.start_pos >= q.start_pos and x.end_pos <= q.end_pos]:
                    q_in_set.cte_sibblings = q.cte_sibblings
        
        
        
        for q in queries.queries_list:
            # queries closed off with filler tokens can end without a position
            if q.start_pos is None:
                q.start_pos = 0
            if q.end_pos is None:
                q.end_pos = len(sql)

        resolver = ProjectionResolver(queries, self.table_columns)
        for q in queries.queries_list:
            projection = resolver.projection(q)
            if projection is not None:
                q.projection = projection

        for q in queries.queries_list:
            if q.ctes:
                for k,v in q.ctes.map.items():
                    if v.self_start in queries.queries:
                        v.projection = rename_columns(queries.queries[v.self_start].projection, v.col_aliases)

        return queries, parsed

    def table_function_columns(self, name: str, sql: str) -> Union[Columns, None]:
        """Columns of a FROM item like read_csv('x.csv') or a table macro call."""
        macro_sql = self.table_macro_sql(name)
        if macro_sql:
            return self.table_macro_columns(name, macro_sql)
        return self.describe_table_function(sql)

    def table_macro_columns(self, name: str, macro_sql: str) -> Union[Columns, None]:
        """A table macro's columns, from parsing its body like any other query."""
        key = ("table_macro", norm_name(name))
        if key in self.projection_cache:
            return self.projection_cache[key]
        self.projection_cache[key] = None  # a macro that calls itself
        queries, _ = self.parse_sql(macro_sql, len(macro_sql))
        columns = None
        if queries and queries.queries_list:
            outermost = max(queries.queries_list, key=lambda q: q.end_pos - q.start_pos)
            columns = outermost.projection
        self.projection_cache[key] = columns
        return columns

    def describe_table_function(self, sql: str) -> Union[Columns, None]:
        """Columns of a table function call such as read_csv('x.csv').

        The one place DuckDB runs: a file's columns need a real reader. Table
        macros from the session aren't known to the engine and give None.
        """
        if sql in self.describe_cache:
            self.describe_cache.move_to_end(sql)
            return self.describe_cache[sql]
        if self.engine is None or incomplete_col_ref.search(f'{sql} '):
            return None
        if remote_url.search(sql) and not self.remote_files:
            return None  # reading it would fail without httpfs, after a network wait
        # a cursor per call: completions run on several threads
        cur = self.engine.cursor()
        # a big or remote file mustn't hold up completions
        timer = threading.Timer(DESCRIBE_TIMEOUT_SECONDS, cur.interrupt)
        timer.start()
        try:
            if self.file_search_path:
                cur.execute(set_search_path_sql(self.file_search_path))
            data = [(r[0], r[1]) for r in cur.execute(f'describe (select * from {sql})').fetchall()]
        except Exception as e:
            self.log_describe.debug(['describe failed', sql, e])
            data = None
        finally:
            timer.cancel()
            cur.close()
        self.describe_cache[sql] = data
        if len(self.describe_cache) > DESCRIBE_CACHE_SIZE:
            self.describe_cache.popitem(last=False)
        return data


# Tokens fed, in order of preference, to close off half-typed SQL so it still
# gives a tree, e.g. "update orders set am" + "= 1", "select (a +" + "x)".
END_FILLERS = (
    ("RPAREN", ")"),
    ("IDENT", CURSOR_PLACEHOLDER),
    ("NUMBER", "1"),
    ("EQUAL", "="),
    ("END", "END"),
)


def find_end(p:InteractiveParser,cur_token:Union[Token,None]=None, max_fillers=6):
    choices = list(p.choices().keys())
    if 'table_ref' in choices and cur_token and cur_token.upper() in ['FROM','JOIN']:
        try:
            p.feed_token(Token('IDENT', 'placeholder'))
            choices = list(p.choices().keys())
        except:
            pass

    for _ in range(max_fillers):
        if '$END' in choices:
            break
        filler = next((Token(t, v) for t, v in END_FILLERS if t in choices), None)
        if filler is None:
            return None
        try:
            p.feed_token(filler)
        except Exception:
            return None
        choices = list(p.choices().keys())

    if '$END' in choices:
        try:
            return p.feed_eof()
        except Exception as e:
            pass


@dataclass
class ParseResult:
    tree: Union[Tree, None]
    tokens: list[Token]
    """Every token lexed, up to a lexer or parser error."""
    tokens_before_cursor: list[Token]
    expected_before: list[frozenset[str]]
    """What the parser expected just before each token in tokens_before_cursor."""
    choices: frozenset[str]
    """Rule and terminal names the parser accepts at the cursor."""
    reached_cursor: bool
    """False when the SQL stopped parsing before the cursor; choices are then meaningless."""
    token_at_cursor: Union[Token, None]
    """A token that starts before the cursor and isn't finished there: the word being typed, or a string the cursor is inside."""
    duration: float
    cursor_parser: Union[InteractiveParser, None] = None
    partial_tree: Union[Tree, None] = None
    """The SQL up to a parse error, closed off with filler tokens."""

    def accepts(self) -> set[str]:
        """Terminals that can come next at the cursor (slow; for the parser debugging GUI)."""
        return set(self.cursor_parser.accepts()) if self.cursor_parser else set()


word_char = re.compile(r"\w")


def is_before_cursor(token: Token, pos: int):
    """Tokens left of the cursor. A word that touches the cursor is the one being typed;
    punctuation, operators and closed quotes are finished."""
    end = token.end_pos or 0
    return end < pos or (end <= pos and not word_char.match(token.value[-1:]))


def interactive_parse(sql: str, pos: int, parser=sql_parser, keep_cursor_parser=False) -> ParseResult:
    """Parses sql, recording what the parser expects at the cursor position pos.

    Lark yields each token before feeding it to the parser, so when the first
    token at or after the cursor comes out of the lexer the parser's state is
    exactly "everything left of the cursor".
    """
    start = time.time()
    p = parser.parse_interactive(sql)
    tokens = p.iter_parse()
    all_tokens: list[Token] = []
    before: list[Token] = []
    expected_before: list[frozenset[str]] = []
    cursor_choices: Union[frozenset[str], None] = None
    cursor_parser = None
    token_at_cursor = None
    failed = False
    token = None

    while True:
        try:
            token = next(tokens)
        except StopIteration:
            break
        except (UnexpectedCharacters, UnexpectedToken) as e:
            failed = True
            # The parser rejects a token when it's fed, after we've seen it; the
            # contextual lexer rejects text before it becomes a token. In the
            # second case every token seen was fed, so if the bad text is at or
            # after the cursor, the parser's current state is the cursor state.
            bad = getattr(e, "token", None)
            lexer_error = bad is None or not all_tokens or bad.start_pos != all_tokens[-1].start_pos
            if bad is not None:
                # includes the word being typed, e.g. "s" at the start of a statement
                at_or_after_cursor = not is_before_cursor(bad, pos)
            else:
                at_or_after_cursor = (getattr(e, "pos_in_stream", 0) or 0) >= pos
            if cursor_choices is None and lexer_error and at_or_after_cursor:
                cursor_choices = frozenset(p.choices())
                if keep_cursor_parser:
                    cursor_parser = p.copy()
                if bad is not None and (bad.start_pos or 0) < pos:
                    token_at_cursor = bad
            break

        all_tokens.append(token)
        if cursor_choices is not None:
            continue
        choices = frozenset(p.choices())
        if is_before_cursor(token, pos):
            before.append(token)
            expected_before.append(choices)
        else:
            cursor_choices = choices
            if keep_cursor_parser:
                cursor_parser = p.copy()
            if (token.start_pos or 0) < pos:
                token_at_cursor = token

    reached_cursor = cursor_choices is not None or not failed
    if cursor_choices is None and reached_cursor:
        # the cursor is after the last token
        cursor_choices = frozenset(p.choices())
        if keep_cursor_parser:
            cursor_parser = p.copy()

    # After an error, end the parse where it stopped, so queries before a grammar
    # gap (e.g. QUALIFY) still give their tables and aliases. Closing it off with
    # filler tokens there would drop everything after the error, so that is kept
    # separately as a last resort, after repairs at the cursor have been tried.
    partial_tree = None
    if failed:
        tree = find_end(p.copy(), cur_token=token, max_fillers=0)
        if tree is None:
            partial_tree = find_end(p, cur_token=token)
    else:
        tree = find_end(p, cur_token=token)
    return ParseResult(
        tree=tree,
        partial_tree=partial_tree,
        tokens=all_tokens,
        tokens_before_cursor=before,
        expected_before=expected_before,
        choices=cursor_choices or frozenset(),
        reached_cursor=reached_cursor,
        token_at_cursor=token_at_cursor,
        duration=time.time() - start,
        cursor_parser=cursor_parser,
    )


TABLE_RULES = frozenset(["table_name", "table_ref", "from_ref", "table_func"])
TYPE_RULES = frozenset(["type", "existing_types"])
EXPRESSION_RULES = frozenset(["expression", "col_exp", "col_exps", "_col_ref", "column_update"])
# Some states only list terminals, e.g. after "a = ". A name right after an
# operator is a value; a name after a word or ")" may be an alias or a keyword.
operator_token = re.compile(r"^[^\w\s\"')\]}]+$")
line_comment_at_end = re.compile(r"--[^\n]*$")


@dataclass
class CursorContext:
    kind: str
    """'table', 'type', 'expression', 'keyword' (only keywords can go here),
    'string', 'comment', or 'unknown' (the parser can't tell; use the regex router)."""
    qualifier: Union[list[str], None] = None
    """For "a.b.|": ['a', 'b'], as written (quotes kept)."""


def classify(expected: frozenset[str], previous: Union[Token, None] = None) -> str:
    if expected & TABLE_RULES:
        return "table"
    if expected & TYPE_RULES:
        return "type"
    if expected & EXPRESSION_RULES:
        return "expression"
    if "IDENT" not in expected:
        return "keyword"
    if previous is not None and operator_token.match(previous.value):
        return "expression"
    return "unknown"


def cursor_context(parsed: ParseResult, sql: str, pos: int) -> CursorContext:
    """What kind of thing goes at the cursor, according to the parser's state there."""
    if not parsed.reached_cursor:
        return CursorContext("unknown")

    at = parsed.token_at_cursor
    if at is not None and at.type == "STRING" and (at.start_pos or 0) < pos < (at.end_pos or 0):
        return CursorContext("string")

    # comments are skipped by the lexer, so look at the text between the last token and the cursor
    gap_start = parsed.tokens_before_cursor[-1].end_pos if parsed.tokens_before_cursor else 0
    gap_end = at.start_pos if at is not None else pos
    gap = sql[gap_start or 0:gap_end]
    if line_comment_at_end.search(gap) or gap.rfind("/*") > gap.rfind("*/"):
        return CursorContext("comment")

    before = parsed.tokens_before_cursor
    if before and before[-1].type == "DOT":
        names: list[str] = []
        i = len(before) - 1
        while i >= 1 and before[i].type == "DOT" and before[i - 1].type == "IDENT":
            names.insert(0, before[i - 1].value)
            i -= 2
        if not names:
            return CursorContext("unknown")
        # what the parser expected where the qualified name starts
        kind = classify(parsed.expected_before[i + 1], before[i] if i >= 0 and i < len(before) and before[i].type != "IDENT" else None)
        return CursorContext(kind if kind in ("table", "expression") else "unknown", names)

    return CursorContext(classify(parsed.choices, before[-1] if before else None))
