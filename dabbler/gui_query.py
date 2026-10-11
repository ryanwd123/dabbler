"""Runs the GUI's queries on a worker thread, so the window stays responsive and
a running query can be cancelled."""
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Union

import duckdb
import polars as pl

from dabbler.db_stuff import quote_ident
from dabbler.gui_qt import QtCore, Signal
from dabbler.gui_stuff import dataframe_kind

log = logging.getLogger("dabbler_gui")


def check_dtype(dt: str) -> str:
    if dt.startswith("ENUM("):
        return "ENUM"
    if dt.startswith("STRUCT("):
        return "STRUCT"
    return dt


@dataclass
class QueryResult:
    sql: str
    df: Union[pl.DataFrame, None] = None
    """None for statements without a result, e.g. CREATE TABLE."""
    dtypes: list[str] = field(default_factory=list)
    error: Union[BaseException, None] = None
    cancelled: bool = False
    seconds: float = 0.0


def register_dataframes(cur: duckdb.DuckDBPyConnection, sql: str, namespace: dict) -> list[str]:
    """Makes the DataFrames, arrow tables and relations the SQL names queryable on cur.

    DuckDB finds python variables by looking at the calling code's variables,
    which on a worker thread aren't the notebook's, so they are registered.
    """
    registered = []
    for name, value in list(namespace.items()):
        if name.startswith("_") or not re.search(rf"\b{re.escape(name)}\b", sql):
            continue
        kind = dataframe_kind(value)
        if kind is None:
            continue
        try:
            if kind == "duckdb_rel":
                cur.execute(f"create or replace temp view {quote_ident(name)} as {value.sql_query()}")
            else:
                cur.register(name, value)
            registered.append(name)
        except Exception:
            log.exception(f"couldn't make {name} queryable")
    return registered


class QueryRunner(QtCore.QObject):
    """Runs one query at a time on its own cursor; starting a query cancels the running one."""

    finished = Signal(object)  # QueryResult, for the latest query only
    _done = Signal(int, object)  # emitted on the worker thread, delivered on the GUI thread

    def __init__(self, make_cursor: Callable[[], duckdb.DuckDBPyConnection], namespace: Callable[[], dict], parent=None):
        super().__init__(parent)
        self.make_cursor = make_cursor
        self.namespace = namespace
        self.generation = 0
        self.cursor: Union[duckdb.DuckDBPyConnection, None] = None
        self._done.connect(self._deliver)

    @property
    def running(self) -> bool:
        return self.cursor is not None

    def run(self, sql: str, limit: Union[int, None]) -> None:
        self.cancel()
        self.generation += 1
        cur = self.make_cursor()
        self.cursor = cur
        registered = register_dataframes(cur, sql, self.namespace())
        threading.Thread(
            target=self._run, args=(self.generation, cur, sql, limit, registered), name="dabbler-query", daemon=True
        ).start()

    def cancel(self) -> None:
        if self.cursor is not None:
            try:
                self.cursor.interrupt()
            except Exception:
                pass

    def _run(self, generation: int, cur: duckdb.DuckDBPyConnection, sql: str, limit, registered: list[str]) -> None:
        start = time.perf_counter()
        result = QueryResult(sql)
        try:
            rel = cur.sql(sql)
            if rel is not None:
                if limit:
                    rel = rel.limit(limit)
                result.dtypes = [check_dtype(str(t)) for t in rel.dtypes]
                result.df = rel.pl()
        except duckdb.InterruptException:
            result.cancelled = True
        except Exception as e:
            result.error = e
        finally:
            result.seconds = time.perf_counter() - start
            for name in registered:
                try:
                    cur.unregister(name)
                except Exception:
                    pass
            cur.close()
        self._done.emit(generation, result)

    def _deliver(self, generation: int, result: QueryResult) -> None:
        if generation != self.generation:
            return  # a newer query replaced this one
        self.cursor = None
        self.finished.emit(result)
