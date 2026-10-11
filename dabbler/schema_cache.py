"""Schema information shared between the IPython extension and the language server.

The IPython extension saves its DuckDB catalog (the `db_data` dict from
`get_db_data_new`) for the file it is running, as rows in a SQLite file: one
row per database, schema, table, view, function and column. The schema of a
DuckDB database file named by a `# dabbler use '<file>'` comment is stored the
same way, keyed by the database file's path. The language server
queries those rows on every completion request instead of keeping its own copy,
and the rows persist, so completions keep working after the IPython session
ends or before it starts.

Saving compares a hash per object and only rewrites the objects that changed.

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
import threading
import time
import uuid
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Union

CACHE_FILE_NAME = "schema_cache.sqlite3"
# Tests and side-by-side installs can point both processes at another file.
CACHE_PATH_ENV = "DABBLER_SCHEMA_CACHE"
# A private cache that lives only in this process (tests, completers built from a db_data dict).
IN_MEMORY = ":memory:"

# A session that hasn't written a heartbeat for this long is treated as gone.
SESSION_TIMEOUT_SECONDS = 15
# Commands nobody picked up in time are dropped instead of running much later.
COMMAND_TIMEOUT_SECONDS = 30

SCHEMA_VERSION = 4

_SCHEMA = """
create table if not exists meta(key text primary key, value text);
create table if not exists sources(
    source_file text primary key,
    kind text not null,               -- 'session': an IPython session running this file;
                                      -- 'database': a DuckDB database file
    version integer not null,         -- increases whenever anything below changes
    updated_at real not null,
    current_schema text,              -- 'catalog.schema'
    cwd text,
    file_search_path text,
    extensions text not null,         -- JSON list of loaded extensions
    file_mtime real                   -- a database file's modification time when it was read
);
create table if not exists objects(
    source_file text not null,
    catalog text not null,
    schema text not null,             -- '' for databases
    name text not null,
    catalog_lower text not null,
    schema_lower text not null,
    name_lower text not null,
    kind text not null,               -- database, schema, table, view, function, table_macro
    detail text,                      -- function type, e.g. 'scalar', 'macro'
    definition text,                  -- a macro's body
    parameters text,                  -- JSON list of a function's parameter names
    fingerprint text not null,
    primary key (source_file, catalog, schema, name, kind)
);
create index if not exists objects_by_name on objects(source_file, name_lower);
create index if not exists objects_by_schema on objects(source_file, catalog_lower, schema_lower);
create table if not exists columns(
    source_file text not null,
    catalog_lower text not null,
    schema_lower text not null,
    table_lower text not null,
    position integer not null,
    name text not null,
    type text,
    primary key (source_file, catalog_lower, schema_lower, table_lower, position)
);
create table if not exists paths(
    source_file text not null,
    variable text not null,
    path text not null,
    primary key (source_file, variable)
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
create table if not exists database_uses(
    document text not null,           -- a python file with a `# dabbler use` comment, or an IPython session's file
    database text not null,           -- the database file, normalized like source_file
    path text not null,               -- the database file as it's shown
    used_at real not null,
    primary key (document, database)
);
"""


def default_cache_path() -> Path:
    if os.environ.get(CACHE_PATH_ENV):
        return Path(os.environ[CACHE_PATH_ENV])
    from dabbler.paths import get_datadir

    return Path(get_datadir()) / CACHE_FILE_NAME  # type: ignore


def normalize_path(path: Union[str, Path]) -> str:
    """One spelling per file: absolute, and on Windows lower case with backslashes."""
    return os.path.normcase(os.path.abspath(str(path)))


def database_mtime(path: Union[str, Path]) -> Union[float, None]:
    """When a DuckDB database file last changed, counting its write-ahead log; None if it doesn't exist."""
    try:
        mtime = os.stat(path).st_mtime
    except OSError:
        return None
    try:
        return max(mtime, os.stat(f"{path}.wal").st_mtime)
    except OSError:
        return mtime


def _hash(value) -> str:
    return hashlib.sha1(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _split_scm(db_scm: str) -> tuple[str, str]:
    catalog, _, schema = db_scm.partition(".")
    return catalog, schema


@dataclass
class SourceInfo:
    source_file: str
    version: int
    updated_at: float
    current_schema: str
    cwd: Union[str, None]
    file_search_path: Union[str, None]
    extensions: list[str]


@dataclass
class Command:
    cmd: str
    data: str


@dataclass
class ObjectRow:
    catalog: str
    schema: str
    name: str
    kind: str
    detail: Union[str, None]
    definition: Union[str, None]
    parameters: Union[tuple[str, ...], None] = None


def object_rows(db_data: dict) -> dict[tuple, tuple[ObjectRow, list[tuple[str, Union[str, None]]]]]:
    """The objects in a db_data dict, keyed by (catalog, schema, name, kind), with their columns."""
    rows: dict[tuple, tuple[ObjectRow, list]] = {}

    def add(row: ObjectRow, columns=()):
        rows.setdefault((row.catalog, row.schema, row.name, row.kind), (row, list(columns)))

    for catalog in db_data.get("databases") or []:
        add(ObjectRow(catalog, "", catalog, "database", None, None))
    for db_scm in db_data.get("schemas") or []:
        catalog, schema = _split_scm(db_scm)
        add(ObjectRow(catalog, schema, schema, "schema", None, None))
    for db_scm, name, kind, cols in db_data.get("data") or []:
        if not cols:
            continue  # sequences
        catalog, schema = _split_scm(db_scm)
        add(ObjectRow(catalog, schema, name, "view" if kind == "view" else "table", None, None),
            [(c[0], c[1]) for c in cols])
    for db_scm, name, fn_type, *rest in db_data.get("functions") or []:
        catalog, schema = _split_scm(db_scm)
        kind = "table_macro" if fn_type == "table_macro" else "function"
        definition = rest[0] if rest else None
        parameters = tuple(rest[1]) if len(rest) > 1 and rest[1] is not None else None
        add(ObjectRow(catalog, schema, name, kind, fn_type, definition, parameters))
    return rows


class SchemaCache:
    def __init__(self, path: Union[str, Path, None] = None) -> None:
        self._local = threading.local()
        self._anchor = None
        if str(path) == IN_MEMORY:
            # a shared-cache memory database lives as long as one connection to it is open
            self.path = f"file:dabbler_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._uri = True
            self._anchor = self._connect()
        else:
            self.path = Path(path) if path else default_cache_path()
            self._uri = False
            self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as con:
            if not self._uri:
                con.execute("pragma journal_mode=wal")
            con.execute("create table if not exists meta(key text primary key, value text)")
            row = con.execute("select value from meta where key = 'schema_version'").fetchone()
            if row is None or int(row[0]) < SCHEMA_VERSION:
                # Older layouts: version 1 kept one JSON blob per file, version 2 had no
                # function parameters, version 3 no database files. It's all rebuilt on
                # the next save from IPython.
                for table in ("snapshots", "sources", "objects", "columns"):
                    con.execute(f"drop table if exists {table}")
            con.executescript(_SCHEMA)
            con.execute("insert or replace into meta values ('schema_version', ?)", (str(SCHEMA_VERSION),))

    def _connect(self):
        # Writers open a connection per call, which keeps this safe from any thread.
        return sqlite3.connect(self.path, timeout=5, isolation_level=None, uri=self._uri, check_same_thread=False)

    def _reader(self) -> sqlite3.Connection:
        """A connection per thread for reads; completions run many small queries."""
        con = getattr(self._local, "con", None)
        if con is None:
            con = self._connect()
            self._local.con = con
        return con

    @contextmanager
    def read_transaction(self):
        """Reads inside see one consistent state, even if IPython saves in the meantime."""
        con = self._reader()
        if con.in_transaction:
            yield con
            return
        con.execute("begin")
        try:
            yield con
        finally:
            con.execute("commit")

    # -- saving (IPython) --

    def save_snapshot(
        self,
        source_file: Union[str, Path],
        db_data: dict,
        kind: str = "session",
        file_mtime: Union[float, None] = None,
    ) -> bool:
        """Stores the schema in db_data for source_file. Returns False when nothing changed.

        Only objects whose name, kind, columns or definition changed are rewritten.
        kind is 'session' for an IPython session's file, or 'database' for a DuckDB
        database file, whose modification time when it was read goes in file_mtime.
        """
        source = normalize_path(source_file)
        new = {
            key: (row, cols, _hash([row.detail, row.definition, row.parameters, cols]))
            for key, (row, cols) in object_rows(db_data).items()
        }
        meta = (
            db_data.get("current_schema"),
            db_data.get("cwd"),
            db_data.get("file_search_path"),
            json.dumps(sorted(db_data.get("loaded_extensions") or [])),
        )
        paths = sorted((str(p[0]), str(p[1])) for p in db_data.get("paths") or [])

        with closing(self._connect()) as con:
            con.execute("begin immediate")
            try:
                changed = self._write_objects(con, source, new)
                changed |= self._write_paths(con, source, paths)
                current = con.execute(
                    "select version, current_schema, cwd, file_search_path, extensions, kind, file_mtime "
                    "from sources where source_file = ?",
                    (source,),
                ).fetchone()
                if current is None or changed or tuple(current[1:6]) != (*meta, kind):
                    version = (current[0] + 1) if current else 1
                    con.execute(
                        "insert or replace into sources values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (source, kind, version, time.time(), *meta, file_mtime),
                    )
                    changed = True
                elif current[6] != file_mtime:
                    # the file was touched without a schema change: remember it was read
                    con.execute("update sources set file_mtime = ? where source_file = ?", (file_mtime, source))
                con.execute("commit")
            except BaseException:
                con.execute("rollback")
                raise
        return changed

    def _write_objects(self, con, source: str, new: dict) -> bool:
        existing = {
            (c, s, n, k): fp
            for c, s, n, k, fp in con.execute(
                "select catalog, schema, name, kind, fingerprint from objects where source_file = ?", (source,)
            )
        }
        stale = [key for key, fp in existing.items() if key not in new or new[key][2] != fp]
        fresh = [key for key, value in new.items() if existing.get(key) != value[2]]
        for catalog, schema, name, kind in stale:
            con.execute(
                "delete from objects where source_file = ? and catalog = ? and schema = ? and name = ? and kind = ?",
                (source, catalog, schema, name, kind),
            )
            if kind in ("table", "view"):
                con.execute(
                    "delete from columns where source_file = ? and catalog_lower = ? and schema_lower = ? and table_lower = ?",
                    (source, catalog.lower(), schema.lower(), name.lower()),
                )
        for key in fresh:
            row, cols, fp = new[key]
            con.execute(
                "insert into objects values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (source, row.catalog, row.schema, row.name, row.catalog.lower(), row.schema.lower(),
                 row.name.lower(), row.kind, row.detail, row.definition,
                 None if row.parameters is None else json.dumps(list(row.parameters)), fp),
            )
            con.executemany(
                "insert or replace into columns values (?, ?, ?, ?, ?, ?, ?)",
                [(source, row.catalog.lower(), row.schema.lower(), row.name.lower(), i, name, typ)
                 for i, (name, typ) in enumerate(cols)],
            )
        return bool(stale or fresh)

    def _write_paths(self, con, source: str, paths: list[tuple[str, str]]) -> bool:
        current = sorted(con.execute("select variable, path from paths where source_file = ?", (source,)).fetchall())
        if current == paths:
            return False
        con.execute("delete from paths where source_file = ?", (source,))
        con.executemany("insert into paths values (?, ?, ?)", [(source, v, p) for v, p in paths])
        return True

    # -- reading (language server) --

    def file_mtime(self, source_file: Union[str, Path]) -> Union[float, None]:
        """When a database file's schema was read: the file's modification time then."""
        row = self._reader().execute(
            "select file_mtime from sources where source_file = ?", (normalize_path(source_file),)
        ).fetchone()
        return row[0] if row else None

    def get_version(self, source_file: Union[str, Path]) -> Union[int, None]:
        row = self._reader().execute(
            "select version from sources where source_file = ?", (normalize_path(source_file),)
        ).fetchone()
        return row[0] if row else None

    def source_info(self, source_file: Union[str, Path]) -> Union[SourceInfo, None]:
        row = self._reader().execute(
            "select source_file, version, updated_at, current_schema, cwd, file_search_path, extensions "
            "from sources where source_file = ?",
            (normalize_path(source_file),),
        ).fetchone()
        if not row:
            return None
        return SourceInfo(row[0], row[1], row[2], row[3] or "memory.main", row[4], row[5], json.loads(row[6]))

    def objects(
        self,
        source_file: str,
        catalog: Union[str, None] = None,
        schema: Union[str, None] = None,
        name: Union[str, None] = None,
        kinds: Iterable[str] = (),
    ) -> list[ObjectRow]:
        """Objects of a source; catalog, schema and name match case-insensitively."""
        where, args = ["source_file = ?"], [normalize_path(source_file)]
        for column, value in (("catalog_lower", catalog), ("schema_lower", schema), ("name_lower", name)):
            if value is not None:
                where.append(f"{column} = ?")
                args.append(value.lower())
        kinds = list(kinds)
        if kinds:
            where.append(f"kind in ({', '.join('?' for _ in kinds)})")
            args.extend(kinds)
        rows = self._reader().execute(
            f"select catalog, schema, name, kind, detail, definition, parameters from objects "
            f"where {' and '.join(where)} order by catalog, schema, name",
            args,
        ).fetchall()
        return [ObjectRow(*r[:6], None if r[6] is None else tuple(json.loads(r[6]))) for r in rows]

    def columns(self, source_file: str, catalog: str, schema: str, table: str) -> list[tuple[str, Union[str, None]]]:
        return self._reader().execute(
            "select name, type from columns where source_file = ? and catalog_lower = ? and schema_lower = ? "
            "and table_lower = ? order by position",
            (normalize_path(source_file), catalog.lower(), schema.lower(), table.lower()),
        ).fetchall()

    def paths(self, source_file: str) -> dict[str, str]:
        return dict(
            self._reader().execute("select variable, path from paths where source_file = ?", (normalize_path(source_file),))
        )

    def resolve_source(
        self, document: Union[str, Path], workspace_root: Union[str, Path, None] = None
    ) -> Union[str, None]:
        """The IPython session whose schema applies to a document.

        The document's own if it has one, otherwise the most recently updated one
        in the same workspace (or folder, without a workspace), so SQL in helper
        modules gets the schema of the notebook that uses them. Database files
        only apply to documents that name them (`# dabbler use`).
        """
        document = normalize_path(document)
        folder = normalize_path(workspace_root) if workspace_root else os.path.dirname(document)
        rows = self._reader().execute(
            "select source_file from sources where kind = 'session' "
            "order by (source_file = ?) desc, updated_at desc",
            (document,),
        ).fetchall()
        for (source,) in rows:
            if source == document or source.startswith(folder.rstrip(os.sep) + os.sep):
                return source
        return None

    # -- database files used before --

    def record_database_use(self, document: Union[str, Path], database: Union[str, Path]) -> None:
        """Remembers that a document used a database file, for `# dabbler use '` completions."""
        with closing(self._connect()) as con:
            con.execute(
                "insert or replace into database_uses values (?, ?, ?, ?)",
                (normalize_path(document), normalize_path(database), os.path.abspath(str(database)), time.time()),
            )

    def recent_databases(self, limit: int = 20) -> list[tuple[str, int]]:
        """Database files used before, most recent first, with their number of tables and views.

        Files that no longer exist are forgotten.
        """
        rows = self._reader().execute(
            "select u.database, u.path, "
            "  (select count(*) from objects o where o.source_file = u.database and o.kind in ('table', 'view')) "
            "from database_uses u "
            "where u.used_at = (select max(used_at) from database_uses v where v.database = u.database) "
            "group by u.database order by max(u.used_at) desc"
        ).fetchall()
        recent, missing = [], []
        for database, path, tables in rows:
            if os.path.exists(path):
                recent.append((path, tables))
            else:
                missing.append(database)
        if missing:
            with closing(self._connect()) as con:
                con.executemany("delete from database_uses where database = ?", [(m,) for m in missing])
        return recent[:limit]

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
        # IPython polls this several times a second: check on a reused connection, and
        # only open one for writing and take the write lock when there's work
        if not self._reader().execute("select 1 from commands where source_file = ? limit 1", (source,)).fetchone():
            return []
        with closing(self._connect()) as con:
            con.execute("begin immediate")
            rows = con.execute(
                "select cmd, data from commands where source_file = ? and created_at >= ? order by id",
                (source, cutoff),
            ).fetchall()
            con.execute("delete from commands where source_file = ? or created_at < ?", (source, cutoff))
            con.execute("commit")
        return [Command(cmd, data) for cmd, data in rows]
