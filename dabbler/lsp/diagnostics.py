"""Syntax errors in a python file's SQL strings, from DuckDB's own parser.

DuckDB parses without running anything (`extract_statements`), so whatever it
accepts is valid even where dabbler's Lark grammar (used for completions) has
gaps. `json_serialize_sql` reports where the error is.
"""
import functools
import json
import re
import threading
from dataclasses import dataclass
from typing import Union

import duckdb

from dabbler.lsp.sql_regions import sql_regions

re_near = re.compile(r'at or near "(.*)"')

_con = duckdb.connect()
_lock = threading.Lock()


@dataclass(frozen=True)
class SqlError:
    start: int
    """Offset of the error in the file."""
    end: int
    message: str


@functools.lru_cache(maxsize=512)
def check_sql(text: str) -> Union[tuple[int, int, str], None]:
    """(start, end, message) of the syntax error in text, or None if DuckDB parses it."""
    with _lock:
        try:
            _con.extract_statements(text)
            return None
        except duckdb.ParserException as e:
            message = str(e).splitlines()[0].removeprefix("Parser Error: ")
        except duckdb.Error:
            return None  # e.g. "not implemented"; not a syntax error
        try:
            # a literal, not a bound parameter: binding a python value can make DuckDB
            # import pandas while it holds a lock, and deadlock with another thread
            literal = "'" + text.replace("'", "''") + "'"
            report = json.loads(_con.execute(f"select json_serialize_sql({literal})").fetchone()[0])  # type: ignore
            position = int(report["position"])
        except Exception:
            position = None

    if position is None:
        start = len(text) - len(text.lstrip())
    else:
        start = min(position, len(text))  # DuckDB counts characters, not bytes
    near = re_near.search(message)
    length = len(near.group(1)) if near else 1
    if start >= len(text.rstrip()):
        # "at end of input": mark the last thing written
        start = max(len(text.rstrip()) - 1, 0)
        length = 1
    return start, min(start + max(length, 1), len(text)), message


def sql_errors(source: str) -> list[SqlError]:
    """Syntax errors in every SQL string of a python file.

    f-strings with replacement fields are skipped: what the fields expand to
    isn't known, so they could be flagged wrongly.
    """
    errors = []
    for region in sql_regions(source):
        if region.has_fields:
            continue
        found = check_sql(region.text)
        if found:
            start, end, message = found
            errors.append(SqlError(region.start + start, region.start + end, message))
    return errors
