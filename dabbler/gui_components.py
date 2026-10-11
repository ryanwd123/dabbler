import logging
import threading
import time
from typing import TYPE_CHECKING
from dabbler.gui_qt import (
    QtWidgets,
    QtCore,
    QtGui,
)
from IPython.core.getipython import get_ipython
import duckdb
from dabbler.db_stuff import (
    SessionState,
    database_file_db_data,
    file_databases,
    get_db_data_new,
    re_session_change,
)
from dabbler.schema_cache import SchemaCache, database_mtime

if TYPE_CHECKING:
    from dabbler.gui_main import MyApp

class Shortcut:
    def __init__(self, Seq, Parent, target):
        if not isinstance(Seq, list):
            Seq = [Seq]
        for i in Seq:
            sc = QtGui.QShortcut(QtGui.QKeySequence(i), Parent)  # type: ignore
            sc.activated.connect(target)


class TreeItem(QtWidgets.QTreeWidgetItem):
    def __init__(self, *args, item_type=None):
        super().__init__(*args)
        self.item_type = item_type



class IPythonExecutionMonitor(object):
    """Schedules a schema check after every cell, and notes cells that change the
    session's database, schema or search paths (USE, SET search_path, ...)."""

    def __init__(self, timer: QtCore.QTimer):
        self.timer = timer
        self.session_changed = False

    def pre_run_cell(self, info):
        # hold off while the cell runs; Qt doesn't run timers then anyway
        self.timer.stop()
        if re_session_change.search(getattr(info, "raw_cell", "") or ""):
            self.session_changed = True

    def post_run_cell(self, result):
        self.timer.start(200)


class SchemaSync(QtCore.QObject):
    """Shares this session's database schema with the language server.

    After each cell the schema is saved to the schema cache (only when it
    changed), where the language server picks it up for completions. Queries
    sent from the editor ("send SQL to GUI") come back through the same cache.
    A heartbeat thread tells the language server the session is alive, also
    while a long cell runs.
    """

    POLL_MS = 300
    HEARTBEAT_SECONDS = 5

    def __init__(self, app: "MyApp", db: duckdb.DuckDBPyConnection):
        super().__init__(app)
        self.app = app
        self.user_db = db
        make_cursor = getattr(app, "make_cursor", None)
        self.db = make_cursor() if make_cursor else db.cursor()
        self.source_file = app.py_file
        self.log = logging.getLogger("dabbler_schema_sync")
        self.cache = SchemaCache()
        self.recorded_databases: set[str] = set()
        self.publish()

        self.stopped = threading.Event()
        self.cache.heartbeat(self.source_file)  # type: ignore
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, name="dabbler-heartbeat", daemon=True)
        self.heartbeat_thread.start()

        self.ip = get_ipython()
        self.ipython_cell_timer = QtCore.QTimer()
        self.ipython_cell_timer.setSingleShot(True)
        self.ipython_cell_timer.timeout.connect(self.check_for_update)
        self.monitor_ip = IPythonExecutionMonitor(self.ipython_cell_timer)
        if self.ip:
            self.ip.events.register("pre_run_cell", self.monitor_ip.pre_run_cell)
            self.ip.events.register("post_run_cell", self.monitor_ip.post_run_cell)

        self.poll_timer = QtCore.QTimer()
        self.poll_timer.setInterval(self.POLL_MS)
        self.poll_timer.timeout.connect(self.poll)
        self.poll_timer.start()

    def publish(self) -> bool:
        """Saves the current schema; returns True if it changed."""
        start = time.perf_counter()
        db_data = get_db_data_new(self.db, self.app.file_search_path)
        extracted = time.perf_counter()
        changed = self.cache.save_snapshot(self.source_file, db_data)  # type: ignore
        self.publish_database_files(db_data)
        elapsed = time.perf_counter() - start
        message = f"schema extract {extracted - start:.3f}s, save {time.perf_counter() - extracted:.3f}s, changed={changed}"
        if elapsed > 1:
            self.log.warning(f"slow schema snapshot: {message}")
        else:
            self.log.debug(message)
        return changed

    def publish_database_files(self, db_data: dict) -> None:
        """Saves the schema of each database file the session has open under the file's
        path too. The language server can't open a file another process holds open
        (`# dabbler use '<file>'`), so it uses these rows instead."""
        try:
            for catalog, path in file_databases(self.db):
                self.cache.save_snapshot(
                    path, database_file_db_data(db_data, catalog, path), kind="database", file_mtime=database_mtime(path)
                )
                if path not in self.recorded_databases and self.source_file:
                    # offered first in `# dabbler use '` completions
                    self.cache.record_database_use(self.source_file, path)
                    self.recorded_databases.add(path)
        except Exception:
            self.log.exception("failed to save the schema of the session's database files")

    def sync_session_state(self) -> None:
        """Copies USE / search_path / file_search_path from the user's connection to dabbler's cursors."""
        state = SessionState.read(self.user_db)
        state.apply(self.db)
        if hasattr(self.app, "session"):
            self.app.session = state
        self.app.file_search_path = state.file_search_path or None

    def check_for_update(self):
        try:
            if self.monitor_ip.session_changed:
                self.monitor_ip.session_changed = False
                self.sync_session_state()
            changed = self.publish()
        except Exception:
            self.log.exception("failed to save schema for completions")
            return
        if changed:
            self.app.update_tables()

    def heartbeat_loop(self):
        while not self.stopped.wait(self.HEARTBEAT_SECONDS):
            try:
                self.cache.heartbeat(self.source_file)  # type: ignore
            except Exception:
                self.log.exception("failed to write the session heartbeat")

    def poll(self):
        try:
            for command in self.cache.take_commands(self.source_file):  # type: ignore
                if command.cmd == "run_sql":
                    self.app.msg_routing(command.data)
        except Exception:
            self.log.exception("failed to read commands from the language server")

    def stop(self):
        """Stops listening to cells and the editor, and tells the language server the session ended."""
        self.stopped.set()
        self.poll_timer.stop()
        self.ipython_cell_timer.stop()
        if self.ip:
            for event, callback in (("pre_run_cell", self.monitor_ip.pre_run_cell),
                                    ("post_run_cell", self.monitor_ip.post_run_cell)):
                try:
                    self.ip.events.unregister(event, callback)
                except ValueError:
                    pass  # already unregistered
        self.cache.end_session(self.source_file)  # type: ignore
