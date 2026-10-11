"""Reading the schema of DuckDB database files named by `# dabbler use` comments.

The file is opened read-only, its schema saved to the schema cache under the
file's path (the same rows an IPython session's schema becomes), and closed
again right away: while the language server has it open, no other process can
open it for writing. It's read again when the file's modification time changes.

When another process has the file open for writing, it can't be opened at all
(on Windows also not read-only). An IPython session with dabbler saves the
schema of its database files under their paths, so those rows are used then.
"""
import logging
import os
import threading
from dataclasses import dataclass
from typing import Union

import duckdb

from dabbler.db_stuff import get_db_data_new
from dabbler.schema_cache import SchemaCache, database_mtime, normalize_path

log = logging.getLogger(__name__)


@dataclass
class DatabaseSource:
    source: Union[str, None]  # key of the database's rows in the schema cache, if there are any
    error: Union[str, None] = None  # why the file couldn't be read, for a diagnostic


class DatabaseFiles:
    def __init__(self, cache: SchemaCache) -> None:
        self.cache = cache
        self.lock = threading.Lock()
        # path -> (modification time, error) of the last failed read, so it isn't retried until the file changes
        self.failed: dict[str, tuple[float, str]] = {}

    def source(self, path: str, cwd: Union[str, None] = None) -> DatabaseSource:
        """The schema cache source for a database file, read from the file first if it changed.

        cwd: the folder relative file paths in the SQL are completed from.
        """
        key = normalize_path(path)
        mtime = database_mtime(path)
        if mtime is None:
            known = self.cache.get_version(key) is not None
            return DatabaseSource(key if known else None, f"database file not found: {path}")
        with self.lock:
            if self.cache.file_mtime(key) == mtime:
                return DatabaseSource(key)
            failed = self.failed.get(key)
            if failed and failed[0] == mtime:
                return self._fallback(key, failed[1])
            try:
                self._read(path, key, mtime, cwd)
            except Exception as e:
                message = str(e).splitlines()[0] if str(e) else type(e).__name__
                log.info(f"couldn't read {path}: {message}")
                self.failed[key] = (mtime, message)
                return self._fallback(key, message)
            self.failed.pop(key, None)
            return DatabaseSource(key)

    def _fallback(self, key: str, error: str) -> DatabaseSource:
        if self.cache.get_version(key) is not None:
            # rows saved by an IPython session that has the file open, or from an earlier read
            return DatabaseSource(key)
        return DatabaseSource(None, error)

    def _read(self, path: str, key: str, mtime: float, cwd: Union[str, None]) -> None:
        con = duckdb.connect(path, read_only=True)
        try:
            db_data = get_db_data_new(con, skip_ipython=True)
        finally:
            con.close()
        db_data["cwd"] = cwd or os.path.dirname(path)
        self.cache.save_snapshot(key, db_data, kind="database", file_mtime=mtime)
