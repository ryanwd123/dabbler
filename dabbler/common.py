from pathlib import Path
import logging
import re
from urllib.parse import urlparse, unquote
import duckdb
from dabbler.lsp.parser import lark_file
from dabbler.paths import get_datadir  # noqa: F401  (used to live here)


log = logging.getLogger(__name__)


def uri_to_path(uri:str):
    parsed = urlparse(unquote(uri))
    if parsed.scheme == 'file':
        # Remove leading slash for Windows paths
        path = parsed.path[1:] if parsed.path.startswith('/') and Path.cwd().drive else parsed.path
        return Path(path)
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}")


def get_grammar_kw():
    """Keywords of the completion grammar (terminals like SELECT: "SELECT"i)."""
    grammar_txt = lark_file.read_text()
    reg = re.compile(r'''([A-Z_]+(?:\.\d+)?)\s*:\s*"[A-Z_]+"i''')
    return reg.findall(grammar_txt)


def _reserved_keywords() -> set[str]:
    # a private connection: duckdb.execute() would use the default connection, which
    # is also the one a notebook's duckdb.sql(...) uses
    con = duckdb.connect()
    try:
        rows = con.execute("select keyword_name from duckdb_keywords() where keyword_category = 'reserved'").fetchall()
    finally:
        con.close()
    return {x[0].upper() for x in rows} | {"DATE"}


duckdb_keywords = _reserved_keywords()
grammar_kw = get_grammar_kw()


# what DuckDB's lexer reads as an unquoted identifier; any non-ASCII character counts as a letter
plain_identifier = re.compile(r'[A-Za-z_-􏿿][A-Za-z0-9_$-􏿿]*')


def check_name(col: str):
    """The name as written in SQL: quoted unless it's a plain identifier that isn't a reserved word."""
    if not col:
        return ''
    if plain_identifier.fullmatch(col) and col.upper() not in duckdb_keywords:
        return col
    return '"' + col.replace('"', '""') + '"'


def unquote_name(name: str):
    """The name an identifier stands for: "x""y" is x"y, and an unquoted name is itself."""
    if len(name) >= 2 and name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name
