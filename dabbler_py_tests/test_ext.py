"""Loading and unloading the IPython extension, with an offscreen Qt and a plain IPython shell."""
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import duckdb
import pytest
from IPython.core.interactiveshell import InteractiveShell

from dabbler import ext
from dabbler.ext_classes import find_connections, pick_connection, source_file
from dabbler.gui_qt import QtCore, QtWidgets
from dabbler.schema_cache import CACHE_PATH_ENV, SchemaCache


def test_pick_connection():
    a, b = duckdb.connect(), duckdb.connect()
    assert pick_connection(find_connections({"x": a})) == "x"
    assert pick_connection(find_connections({"x": a, "db": b})) == "db"
    assert pick_connection(find_connections({"x": a, "y": b})) is None
    assert find_connections({"_hidden": a, "n": 1}) == {}


def test_source_file():
    assert source_file({"__file__": "a.py", "__vsc_ipynb_file__": "b.ipynb"}) == "a.py"
    assert source_file({"__vsc_ipynb_file__": "b.ipynb"}) == "b.ipynb"
    assert source_file({}) is None


@pytest.fixture
def shell(tmp_path, monkeypatch):
    QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
    monkeypatch.setenv(CACHE_PATH_ENV, str(tmp_path / "cache.sqlite3"))
    # keep window geometry and font size out of the user's real settings
    ini = str(tmp_path / "settings.ini")
    monkeypatch.setattr("dabbler.gui_main.app_settings", lambda: QtCore.QSettings(ini, QtCore.QSettings.Format.IniFormat))
    ip = InteractiveShell.instance()
    saved = dict(ip.user_ns)
    yield ip
    ext.unload_ipython_extension(ip)
    ip.user_ns.clear()
    ip.user_ns.update(saved)


def callbacks(ip, event):
    return [getattr(cb, "__self__", None) for cb in ip.events.callbacks[event]]


def test_load_and_unload(shell, tmp_path):
    shell.user_ns["db"] = duckdb.connect()
    shell.user_ns["db"].execute("create table t(a int)")
    shell.user_ns["__file__"] = str(tmp_path / "nb.py")
    ext.load_ipython_extension(shell)
    app = ext._magics.app
    assert app is not None and len(app.windows) == 1
    cache = SchemaCache(tmp_path / "cache.sqlite3")
    assert cache.session_active(tmp_path / "nb.py")
    monitor = app.schema_sync.monitor_ip
    assert monitor in callbacks(shell, "post_run_cell")

    ext.unload_ipython_extension(shell)
    assert ext._magics is None
    assert monitor not in callbacks(shell, "post_run_cell")
    assert not cache.session_active(tmp_path / "nb.py")


def test_reload_reuses_the_qt_application(shell, tmp_path):
    shell.user_ns["db"] = duckdb.connect()
    ext.load_ipython_extension(shell)
    first = QtWidgets.QApplication.instance()
    ext.load_ipython_extension(shell)  # what %reload_ext does after unloading
    assert QtWidgets.QApplication.instance() is first
    assert len(ext._magics.app.windows) == 1


def test_without_a_source_file_the_gui_still_works(shell):
    shell.user_ns.pop("__file__", None)
    shell.user_ns["db"] = duckdb.connect()
    ext.load_ipython_extension(shell)
    assert ext._magics.app.schema_sync is None
    assert len(ext._magics.app.windows) == 1


def test_no_connection_then_dabbler_magic(shell, capsys):
    for name in list(find_connections(shell.user_ns)):
        del shell.user_ns[name]
    ext.load_ipython_extension(shell)
    assert ext._magics.app is None
    assert "no DuckDB connection" in capsys.readouterr().out
    shell.user_ns["con"] = duckdb.connect()
    shell.run_line_magic("dabbler", "")
    assert ext._magics.app is not None and ext._magics.app.db_name == "con"


def test_several_connections_need_a_name(shell, capsys):
    shell.user_ns["first"] = duckdb.connect()
    shell.user_ns["second"] = duckdb.connect()
    ext.load_ipython_extension(shell)
    assert ext._magics.app is None
    assert "%dabbler <name>" in capsys.readouterr().out
    shell.run_line_magic("dabbler", "second")
    assert ext._magics.app.db_name == "second"
    shell.run_line_magic("dabbler", "nope")
    assert "isn't a DuckDB connection" in capsys.readouterr().out
    assert ext._magics.app.db_name == "second"


def test_query_from_the_editor_shows_in_the_window(shell):
    import time

    shell.user_ns["db"] = duckdb.connect()
    shell.user_ns["frame"] = __import__("pandas").DataFrame({"a": [1, 2, 3]})
    ext.load_ipython_extension(shell)
    app = ext._magics.app
    app.msg_routing("select sum(a) as total from frame")
    window = app.windows[-1]
    deadline = time.time() + 10
    while window.table._model.df.width == 0 and time.time() < deadline:
        QtWidgets.QApplication.processEvents()
        time.sleep(0.01)
    assert window.table._model.df["total"].to_list() == [6]
    assert "frame" in window.selection_area.df_list


def test_closing_a_window_deletes_it(shell):
    shell.user_ns["db"] = duckdb.connect()
    ext.load_ipython_extension(shell)
    app = ext._magics.app
    window = app.windows[0]
    destroyed = []
    window.destroyed.connect(lambda *_: destroyed.append(True))
    window.close()
    # Qt deletes it on this thread, through the event loop, not the garbage collector
    QtWidgets.QApplication.sendPostedEvents(None, QtCore.QEvent.Type.DeferredDelete)
    assert destroyed and app.windows == []
    shell.run_line_magic("dabbler", "")
    assert len(app.windows) == 1 and app.windows[0].isVisible()
