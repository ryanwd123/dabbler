"""The IPython side of the schema cache: SchemaSync, without IPython or a window."""
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import duckdb
import pytest

from dabbler.gui_qt import QtCore, QtWidgets, Signal
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
    from dabbler.gui_compenents import SchemaSync

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
