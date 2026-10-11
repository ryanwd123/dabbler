import os
import sys
from pathlib import Path
import logging
import re
from urllib.parse import urlparse, unquote
import duckdb
from dabbler.lsp.parser import lark_file


log = logging.getLogger(__name__)


def uri_to_path(uri:str):
    parsed = urlparse(unquote(uri))
    if parsed.scheme == 'file':
        # Remove leading slash for Windows paths
        path = parsed.path[1:] if parsed.path.startswith('/') and Path.cwd().drive else parsed.path
        return Path(path)
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}")


def get_datadir() -> Path:
    """dabbler's data folder (%APPDATA%\\dabbler, ~/.local/share/dabbler, ~/Library/Application Support/dabbler)."""
    home = Path.home()
    if sys.platform == "win32":
        data_dir = Path(os.environ.get("APPDATA") or home / "AppData/Roaming") / "dabbler"
    elif sys.platform == "darwin":
        data_dir = home / "Library/Application Support/dabbler"
    else:
        data_dir = Path(os.environ.get("XDG_DATA_HOME") or home / ".local/share") / "dabbler"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


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


def check_name(col: str):
    """The name as written in SQL: quoted when it has spaces, dots, dashes or slashes, or is a reserved word."""
    if col:
        if (' ' in col or
            '-' in col or
            '/' in col or
            '.' in col or
            col.upper() in duckdb_keywords):
            return f'"{col}"'
        return col
    return ''
