"""Hover text for names in SQL: functions, tables, CTEs, aliases and columns."""
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

from dabbler.lsp.parser import lookup, norm_name
from dabbler.lsp.sql_utils import CmpItem

if TYPE_CHECKING:
    from dabbler.lsp.completer import SqlCompleter

re_part = re.compile(r'"[^"]*"|\w+')
re_chain = re.compile(r'(?:"[^"]*"|\w+)(?:\s*\.\s*(?:"[^"]*"|\w+))*')
MAX_COLUMNS = 40
SOURCE_KINDS = {"table_alias": "table", "cte": "CTE", "sub query": "subquery", "table function": "table function", "values": "VALUES list"}


@dataclass(frozen=True)
class HoverText:
    start: int
    end: int
    markdown: str


@dataclass(frozen=True)
class NameAt:
    parts: list[str]
    """The dotted name up to and including the hovered part."""
    is_last: bool
    """The hovered part ends the dotted name."""
    start: int
    end: int
    """Span of the hovered part."""
    chain_end: int


def name_at(sql: str, pos: int) -> Union[NameAt, None]:
    """The dotted name under pos, like o.amount or main.orders."""
    for m in re_chain.finditer(sql):
        if m.start() <= pos <= m.end():
            parts = list(re_part.finditer(m.group(0)))
            for i, part in enumerate(parts):
                start, end = m.start() + part.start(), m.start() + part.end()
                if start <= pos <= end:
                    return NameAt([p.group(0) for p in parts[: i + 1]], i == len(parts) - 1, start, end, m.end())
            return None
        if m.start() > pos:
            break
    return None


def columns_table(columns) -> str:
    rows = [f"| {name} | {typ or ''} |" for name, typ in columns[:MAX_COLUMNS] if name]
    if len(columns) > MAX_COLUMNS:
        rows.append(f"| … {len(columns) - MAX_COLUMNS} more | |")
    return "| column | type |\n|---|---|\n" + "\n".join(rows) if rows else ""


def describe_source(kind: str, name: str, columns) -> str:
    text = f"**{kind}** `{name}`"
    table = columns_table(columns or [])
    return text + ("\n\n" + table if table else "")


def column_text(item: CmpItem, owner: Union[str, None]) -> str:
    text = f"**column** `{item.label}`" + (f" `{item.typ}`" if item.typ else "")
    return text + (f"\n\nfrom `{owner}`" if owner else "")


def hover(completer: "SqlCompleter", sql: str, pos: int) -> Union[HoverText, None]:
    left = sql[:pos]
    if left.count("'") % 2 == 1 or "--" in left[left.rfind("\n") + 1:]:
        return None  # in a string literal or a comment
    found = name_at(sql, pos)
    if found is None:
        return None
    parts, start, end = found.parts, found.start, found.end
    name = ".".join(parts)

    def result(markdown: Union[str, None]):
        return HoverText(start, end, markdown) if markdown else None

    if found.is_last and sql[found.chain_end:].lstrip().startswith("("):
        info = completer.catalog.function_info(name)
        return result(info.markdown() if info else None)

    comp_map, _ = completer.parse_sql2(found.chain_end, sql)
    root = comp_map.get("root_namespace", [])

    if len(parts) == 1:
        local = next((x for x in root if norm_name(x.label) == norm_name(name) and x.obj_type in ("table", "cte")), None)
        if local is not None:
            columns = [(c.label, c.typ) for c in lookup(comp_map, name) or []]
            if local.typ == "table_alias":
                table = completer.catalog.table_columns(name)
                if table is not None and not columns:
                    columns = table
            return result(describe_source(SOURCE_KINDS.get(local.typ or "", "table"), local.label, columns))
        table = completer.catalog.table_columns(name)
        if table is not None:
            return result(describe_source("table", name, table))
        for alias, items in comp_map.items():
            if alias == "root_namespace":
                continue
            match = next((c for c in items if norm_name(c.label) == norm_name(name) and c.obj_type == "column"), None)
            if match is not None:
                return result(column_text(match, alias))
        info = completer.catalog.function_info(name)
        return result(info.markdown() if info else None)

    qualifier, last = ".".join(parts[:-1]), parts[-1]
    members = lookup(comp_map, qualifier) or completer.catalog.members(qualifier) or []
    match = next((c for c in members if norm_name(c.label) == norm_name(last)), None)
    if match is None:
        return None
    if match.obj_type == "column":
        return result(column_text(match, qualifier))
    if match.obj_type in ("table", "cte"):
        return result(describe_source(match.typ or "table", name, completer.catalog.table_columns(name)))
    if match.obj_type in ("function", "table_macro"):
        info = completer.catalog.function_info(name)
        return result(info.markdown() if info else None)
    return result(f"**{match.obj_type}** `{name}`")
