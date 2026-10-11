"""Formats an SQL string of a python file with sqlglot's DuckDB dialect."""
from dataclasses import dataclass

import sqlglot
import sqlglot.errors

from dabbler.lsp.sql_regions import SqlRegion

DEFAULT_INDENT = "    "


@dataclass(frozen=True)
class FormatEdit:
    start: int
    end: int
    new_text: str


class FormatError(Exception):
    pass


def format_sql(sql: str, pretty: bool) -> str:
    try:
        statements = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
    except sqlglot.errors.SqlglotError as e:
        raise FormatError(f"sqlglot couldn't parse the SQL: {str(e).splitlines()[0]}") from e
    if not statements:
        raise FormatError("there's no SQL to format")
    out = [s.sql(dialect="duckdb", pretty=pretty, pad=4, indent=4) for s in statements]
    return (";\n\n" if pretty else "; ").join(out)


def format_region(source: str, region: SqlRegion) -> FormatEdit:
    """An edit that replaces the SQL string's text with the formatted SQL.

    Multi-line strings are formatted over several lines at the indent of the
    SQL's first line (or 4 spaces at column 0); single-line strings stay on
    one line. The whitespace around the SQL, like the newline before the
    closing quotes, is kept.
    """
    original = source[region.start:region.end]
    if region.has_fields or original != region.text:
        # formatting would lose the f-string's {fields} or the python escapes
        raise FormatError("f-strings with fields and strings with backslash escapes can't be formatted")
    text = region.text
    body = text.strip()
    leading = text[: len(text) - len(text.lstrip())]
    trailing = text[len(text.rstrip()):]

    if "\n" not in text:
        return FormatEdit(region.start, region.end, leading + format_sql(body, pretty=False) + trailing)

    prefix = leading[: leading.rfind("\n") + 1]
    indent = leading[len(prefix):] or DEFAULT_INDENT
    lines = format_sql(body, pretty=True).split("\n")
    formatted = "\n".join(indent + line if line else line for line in lines)
    if not trailing:
        trailing = "\n"
    return FormatEdit(region.start, region.end, prefix + formatted + trailing)

