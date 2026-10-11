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


def get_datadir():

    home = Path.home()
    if not home.is_dir():
        log.error("Could not find home directory")

    data_dir = None

    if sys.platform == "win32":
        data_dir = home / "AppData/Roaming/dabbler"
    if sys.platform == "linux":
        data_dir = home / ".local/share/dabbler"
    elif sys.platform == "darwin":
        data_dir = home / "Library/Application Support/dabbler"

    if data_dir:
        if not data_dir.is_dir():
            data_dir.mkdir()
        return data_dir

    log.error(f"Could not find data directory for {sys.platform=}")












def get_grammer_kw():
    grammer_txt = lark_file.read_text()
    # reg = re.compile(r'''([A-Z_]+)\s*:\s*"[A-Z_]+"i''')
    reg = re.compile(r'''([A-Z_]+(?:\.\d+)?)\s*:\s*"[A-Z_]+"i''')
    return reg.findall(grammer_txt)


duckdb_keyworkds = set(x[0].upper() for x in duckdb.execute("select keyword_name from duckdb_keywords() where keyword_category = 'reserved'").fetchall())
# duckdb_keyworkds_all = set(x[0].upper() for x in duckdb.execute("select keyword_name from duckdb_keywords()").fetchall())
duckdb_keyworkds.update([
    'DATE',
    ])




grammer_kw = get_grammer_kw()



def check_name(col:str):
    if col:
        if (' ' in col or
            '-' in col or
            '/' in col or
            '.' in col or
            col.upper() in duckdb_keyworkds):
            return f'"{col}"'
        return col
    return ''



def make_table_sql(table_name:str, cols:list[str]):
    col_txt = ",\n".join([f'"{c[0]}" {c[1]}' for c in cols])
    return f'''CREATE TABLE "{table_name}"({col_txt});'''
