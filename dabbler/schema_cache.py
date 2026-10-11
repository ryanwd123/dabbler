"""Schema snapshots shared between the IPython extension and the language server.

The IPython extension writes a snapshot of its DuckDB catalog (the `db_data`
dict from `get_db_data_new`) for the file it is running, and the language
server reads it to build completions. Snapshots persist, so completions keep
working after the IPython session ends or before it starts.

The same SQLite file carries the few messages that go the other way (the
language server asking the GUI to run a query) and a heartbeat so the server
can tell whether an IPython session is listening.

SQLite rather than a DuckDB file: DuckDB doesn't let a second process read a
database while another one has it open for writing.
"""
import hashlib
import json
import os
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Union

CACHE_FILE_NAME = "schema_cache.sqlite3"
# Tests and side-by-side installs can point both processes at another file.
CACHE_PATH_ENV = "DABBLER_SCHEMA_CACHE"

# A session that hasn't written a heartbeat for this long is treated as gone.
SESSION_TIMEOUT_SECONDS = 15
# Commands nobody picked up in time are dropped instead of running much later.
COMMAND_TIMEOUT_SECONDS = 30

_SCHEMA = """
create table if not exists snapshots(
    source_file text primary key,
    fingerprint text not null,
    updated_at real not null,
    db_data text not null
);
create table if not exists sessions(
    source_file text primary key,
    pid integer not null,
    last_seen real not null
);
create table if not exists commands(
    id integer primary key autoincrement,
    source_file text not null,
    cmd text not null,
    data text not null,
    created_at real not null
);
"""


def default_cache_path() -> Path:
    if os.environ.get(CACHE_PATH_ENV):
        return Path(os.environ[CACHE_PATH_ENV])
    from dabbler.common import get_datadir

    return Path(get_datadir()) / CACHE_FILE_NAME  # type: ignore


def normalize_path(path: Union[str, Path]) -> str:
    """One spelling per file: absolute, and on Windows lower case with backslashes."""
    return os.path.normcase(os.path.abspath(str(path)))


def fingerprint(db_data: dict) -> str:
    return hashlib.sha1(json.dumps(db_data, sort_keys=True, default=str).encode()).hexdigest()


@dataclass
class Snapshot:
    source_file: str
    fingerprint: str
    updated_at: float
    db_data: dict


@dataclass
class Command:
    cmd: str
    data: str


class SchemaCache:
    def __init__(self, path: Union[str, Path, None] = None) -> None:
        self.path = Path(path) if path else default_cache_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as con:
            con.execute("pragma journal_mode=wal")
            con.executescript(_SCHEMA)

    def _connect(self):
        # A connection per call keeps this safe to use from any thread; opening one is cheap.
        return sqlite3.connect(self.path, timeout=5, isolation_level=None)

    # -- snapshots (IPython writes, language server reads) --

    def save_snapshot(self, source_file: Union[str, Path], db_data: dict) -> bool:
        """Stores db_data for source_file. Returns False when it was already stored."""
        source = normalize_path(source_file)
        fp = fingerprint(db_data)
        if self.get_fingerprint(source) == fp:
            return False
        with closing(self._connect()) as con:
            con.execute(
                "insert or replace into snapshots values (?, ?, ?, ?)",
                (source, fp, time.time(), json.dumps(db_data, default=str)),
            )
        return True

    def get_fingerprint(self, source_file: Union[str, Path]) -> Union[str, None]:
        with closing(self._connect()) as con:
            row = con.execute(
                "select fingerprint from snapshots where source_file = ?", (normalize_path(source_file),)
            ).fetchone()
        return row[0] if row else None

    def get_snapshot(self, source_file: Union[str, Path]) -> Union[Snapshot, None]:
        with closing(self._connect()) as con:
            row = con.execute(
                "select source_file, fingerprint, updated_at, db_data from snapshots where source_file = ?",
                (normalize_path(source_file),),
            ).fetchone()
        if not row:
            return None
        return Snapshot(row[0], row[1], row[2], json.loads(row[3]))

    def resolve_source(
        self, document: Union[str, Path], workspace_root: Union[str, Path, None] = None
    ) -> Union[str, None]:
        """The snapshot to use for a document.

        The document's own snapshot if it has one, otherwise the most recently
        updated one in the same workspace (or folder, without a workspace), so
        SQL in helper modules gets the schema of the notebook that uses them.
        """
        document = normalize_path(document)
        folder = normalize_path(workspace_root) if workspace_root else os.path.dirname(document)
        with closing(self._connect()) as con:
            rows = con.execute(
                "select source_file from snapshots order by (source_file = ?) desc, updated_at desc", (document,)
            ).fetchall()
        for (source,) in rows:
            if source == document or source.startswith(folder.rstrip(os.sep) + os.sep):
                return source
        return None

    # -- sessions --

    def heartbeat(self, source_file: Union[str, Path]) -> None:
        with closing(self._connect()) as con:
            con.execute(
                "insert or replace into sessions values (?, ?, ?)",
                (normalize_path(source_file), os.getpid(), time.time()),
            )

    def end_session(self, source_file: Union[str, Path]) -> None:
        with closing(self._connect()) as con:
            con.execute("delete from sessions where source_file = ?", (normalize_path(source_file),))

    def session_active(self, source_file: Union[str, Path]) -> bool:
        with closing(self._connect()) as con:
            row = con.execute(
                "select last_seen from sessions where source_file = ?", (normalize_path(source_file),)
            ).fetchone()
        return bool(row) and time.time() - row[0] < SESSION_TIMEOUT_SECONDS

    # -- commands (language server writes, IPython reads) --

    def push_command(self, source_file: Union[str, Path], cmd: str, data: str) -> None:
        with closing(self._connect()) as con:
            con.execute(
                "insert into commands(source_file, cmd, data, created_at) values (?, ?, ?, ?)",
                (normalize_path(source_file), cmd, data, time.time()),
            )

    def take_commands(self, source_file: Union[str, Path]) -> list[Command]:
        """Removes and returns the pending commands for source_file, oldest first."""
        source = normalize_path(source_file)
        cutoff = time.time() - COMMAND_TIMEOUT_SECONDS
        with closing(self._connect()) as con:
            # IPython polls this several times a second; only take the write lock when there's work
            if not con.execute("select 1 from commands where source_file = ? limit 1", (source,)).fetchone():
                return []
            con.execute("begin immediate")
            rows = con.execute(
                "select cmd, data from commands where source_file = ? and created_at >= ? order by id",
                (source, cutoff),
            ).fetchall()
            con.execute("delete from commands where source_file = ? or created_at < ?", (source, cutoff))
            con.execute("commit")
        return [Command(cmd, data) for cmd, data in rows]
