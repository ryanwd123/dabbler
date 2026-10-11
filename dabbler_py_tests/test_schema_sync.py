"""The IPython side of the schema cache: SchemaSync, without IPython or a window."""
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import sqlite3
import time
import types
from types import SimpleNamespace

import duckdb
import pytest

from dabbler.gui_qt import QtCore, QtWidgets, Signal
from dabbler.db_stuff import get_pathlib_paths
from dabbler.gui_components import SchemaSync
from dabbler.schema_cache import CACHE_PATH_ENV, SchemaCache


class FakeApp(QtCore.QObject):
    """The parts of MyApp that SchemaSync uses."""

    aboutToQuit = Signal()

    def __init__(self, py_file):
        super().__init__()
        self.py_file = py_file
        self.file_search_path = None
        self.queries_run = []
        self.table_updates = 0

    def msg_routing(self, sql):
        self.queries_run.append(sql)

    def update_tables(self):
        self.table_updates += 1


@pytest.fixture(scope="module")
def qapp():
    return QtWidgets.QApplication.instance() or QtWidgets.QApplication([])


@pytest.fixture
def setup(qapp, tmp_path, monkeypatch):
    monkeypatch.setenv(CACHE_PATH_ENV, str(tmp_path / "cache.sqlite3"))
    db = duckdb.connect()
    db.execute("create table first_table(a int)")
    app = FakeApp(tmp_path / "nb.py")
    sync = SchemaSync(app, db)  # type: ignore
    yield app, sync, db, SchemaCache(tmp_path / "cache.sqlite3")
    sync.stop()


def table_names(cache, source):
    return {row.name for row in cache.objects(str(source), kinds=["table", "view"])}


def test_schema_is_published_on_start(setup):
    app, sync, db, cache = setup
    assert "first_table" in table_names(cache, app.py_file)
    assert cache.session_active(app.py_file)


def test_changes_are_published_after_a_cell(setup):
    app, sync, db, cache = setup
    sync.check_for_update()
    assert app.table_updates == 0  # nothing changed

    db.execute("create table second_table(b int)")
    sync.check_for_update()
    assert "second_table" in table_names(cache, app.py_file)
    assert app.table_updates == 1


def test_queries_from_the_editor_are_run(setup):
    app, sync, db, cache = setup
    cache.push_command(app.py_file, "run_sql", "select 42")
    sync.poll()
    assert app.queries_run == ["select 42"]
    sync.poll()
    assert app.queries_run == ["select 42"]


def test_stop_ends_the_session(setup):
    app, sync, db, cache = setup
    sync.stop()
    assert not cache.session_active(app.py_file)


def test_any_cell_schedules_a_check(setup):
    app, sync, db, cache = setup
    sync.monitor_ip.pre_run_cell(SimpleNamespace(raw_cell="x = 1"))
    # terminal IPython and %run have no cell_id; they used to be skipped
    sync.monitor_ip.post_run_cell(SimpleNamespace(info=SimpleNamespace(cell_id=None)))
    assert sync.ipython_cell_timer.isActive()


def test_use_in_a_cell_is_followed(setup):
    app, sync, db, cache = setup
    db.execute("attach ':memory:' as other; create table other.only_there(x int)")
    sync.monitor_ip.pre_run_cell(SimpleNamespace(raw_cell='db.sql("use other")'))
    db.execute("use other")
    sync.check_for_update()
    assert cache.source_info(app.py_file).current_schema == "other.main"
    assert sync.db.execute("select current_database()").fetchone()[0] == "other"


def test_other_cells_leave_the_users_pending_result_alone(setup):
    app, sync, db, cache = setup
    db.execute("select 42")
    sync.monitor_ip.pre_run_cell(SimpleNamespace(raw_cell="db.execute('select 42')"))
    sync.check_for_update()
    assert db.fetchall() == [(42,)]


def test_heartbeat_continues_without_qt(setup, monkeypatch):
    app, sync, db, cache = setup
    sync.stop()
    monkeypatch.setattr(SchemaSync, "HEARTBEAT_SECONDS", 0.05)
    sync2 = SchemaSync(app, db)  # type: ignore
    try:
        with sqlite3.connect(cache.path) as con:
            first = con.execute("select last_seen from sessions").fetchone()[0]
        time.sleep(0.3)  # no Qt events are processed here, like during a long cell
        with sqlite3.connect(cache.path) as con:
            assert con.execute("select last_seen from sessions").fetchone()[0] > first
    finally:
        sync2.stop()


def test_pathlib_paths_from_the_namespace(tmp_path):
    module = types.ModuleType("my_helpers")
    module.__file__ = str(tmp_path / "my_helpers.py")
    module.data_dir = tmp_path / "data"
    user_ns = {"base": tmp_path, "n": 1, "_hidden": tmp_path, "my_helpers": module, "os": os}
    paths = dict(get_pathlib_paths(user_ns))
    assert paths == {"base": str(tmp_path), "my_helpers.data_dir": str(tmp_path / "data")}
