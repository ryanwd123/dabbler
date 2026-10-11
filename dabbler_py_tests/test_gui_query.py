"""Running the GUI's queries on a worker thread (gui_query.QueryRunner) and exporting results."""
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import time

import duckdb
import pandas as pd
import polars as pl
import pytest

from dabbler.gui_main import export_df
from dabbler.gui_qt import QtWidgets
from dabbler.gui_query import QueryRunner


@pytest.fixture
def db():
    QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
    con = duckdb.connect()
    con.execute("create table t as select range as i from range(10)")
    return con


def run(runner, sql, limit=None, timeout=10.0):
    results = []
    runner.finished.connect(results.append)
    runner.run(sql, limit)
    deadline = time.time() + timeout
    while not results and time.time() < deadline:
        QtWidgets.QApplication.processEvents()
        time.sleep(0.01)
    runner.finished.disconnect(results.append)
    assert results, "query didn't finish"
    return results[0]


def test_query_with_limit(db):
    result = run(QueryRunner(db.cursor, dict), "select * from t order by i", limit=3)
    assert result.df["i"].to_list() == [0, 1, 2]
    assert result.dtypes == ["BIGINT"]


@pytest.mark.parametrize("sql", ["describe t", "pivot t on i % 2 using count(*)", "select * from t limit 2;"])
def test_statements_the_old_limit_append_broke(db, sql):
    assert run(QueryRunner(db.cursor, dict), sql, limit=1000).error is None


def test_ddl_has_no_rows(db):
    result = run(QueryRunner(db.cursor, dict), "create table u as select 1 as x")
    assert result.error is None and result.df is None
    assert db.execute("select count(*) from u").fetchone() == (1,)


def test_errors_are_reported(db):
    result = run(QueryRunner(db.cursor, dict), "select * from nope")
    assert "nope" in str(result.error)


def test_dataframes_from_the_namespace(db):
    namespace = {
        "pdf": pd.DataFrame({"a": [1, 2]}),
        "pldf": pl.DataFrame({"a": [3]}),
        "rel": db.sql("select 10 as a"),
        "unused": pd.DataFrame({"a": [1]}),
    }
    runner = QueryRunner(db.cursor, lambda: namespace)
    result = run(runner, "select sum(a) as s from (select * from pdf union all select * from pldf union all select * from rel)")
    assert result.error is None, result.error
    assert result.df["s"].to_list() == [16]


def test_cancel(db):
    runner = QueryRunner(db.cursor, dict)
    results = []
    runner.finished.connect(results.append)
    runner.run("select sum(i) from range(100000000000) t(i)", None)
    time.sleep(0.2)
    runner.cancel()
    deadline = time.time() + 10
    while not results and time.time() < deadline:
        QtWidgets.QApplication.processEvents()
        time.sleep(0.01)
    assert results and results[0].cancelled


def test_cancel_right_after_starting(db):
    # DuckDB's interrupt() is lost if it comes before the query starts executing
    runner = QueryRunner(db.cursor, dict)
    results = []
    runner.finished.connect(results.append)
    runner.run("select sum(i) from range(100000000000) t(i)", None)
    runner.cancel()
    runner.wait(10)
    assert not runner.thread.is_alive()
    deadline = time.time() + 5
    while not results and time.time() < deadline:
        QtWidgets.QApplication.processEvents()
        time.sleep(0.01)
    assert results and results[0].cancelled


def test_a_newer_query_replaces_the_running_one(db):
    runner = QueryRunner(db.cursor, dict)
    results = []
    runner.finished.connect(results.append)
    runner.run("select sum(i) from range(100000000000) t(i)", None)
    runner.run("select 7 as x", None)
    deadline = time.time() + 10
    while not results and time.time() < deadline:
        QtWidgets.QApplication.processEvents()
        time.sleep(0.01)
    time.sleep(0.2)
    QtWidgets.QApplication.processEvents()
    assert [r.df["x"].to_list() for r in results] == [[7]]


@pytest.mark.parametrize("name, chosen, suffix", [("out.csv", "", ".csv"), ("out", "Parquet (*.parquet)", ".parquet")])
def test_export(tmp_path, name, chosen, suffix):
    df = pl.DataFrame({"a": [1, 2]})
    path = export_df(df, str(tmp_path / name), chosen)
    assert path.suffix == suffix and path.exists()


def test_dataframe_kinds(db):
    from dabbler.gui_stuff import dataframe_kind
    import pyarrow as pa

    assert dataframe_kind(pd.DataFrame()) == "pandas"
    assert dataframe_kind(pl.DataFrame()) == "polars"
    assert dataframe_kind(pa.table({"a": [1]})) == "arrow"
    assert dataframe_kind(db.sql("select 1")) == "duckdb_rel"
    assert dataframe_kind(pl.LazyFrame()) is None
    assert dataframe_kind([1, 2]) is None
