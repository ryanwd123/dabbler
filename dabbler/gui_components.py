import logging
import time
from typing import TYPE_CHECKING
from dabbler.gui_qt import (
    QtWidgets,
    QtCore,
    QtGui,
)
# from qtpy import QtWidgets, QtCore, QtGui
# from qtpy.QtCore import Slot, Signal  # type: ignore
from IPython.core.getipython import get_ipython
import duckdb
from dabbler.db_stuff import get_db_data_new
from dabbler.schema_cache import SchemaCache

if TYPE_CHECKING:
    from dabbler.gui_main import MyApp

margins = 0

class Shortcut:
    def __init__(self, Seq, Parent, target):
        if not isinstance(Seq, list):
            Seq = [Seq]
        for i in Seq:
            sc = QtGui.QShortcut(QtGui.QKeySequence(i), Parent)  # type: ignore
            sc.activated.connect(target)


# class Hbox(QtWidgets.QHBoxLayout):
#     def __init__(self, parent):
#         super().__init__(parent)  
#         self.setSpacing(margins)
#         self.setContentsMargins(margins, margins, margins, margins)


# class Vbox(QtWidgets.QVBoxLayout):
#     def __init__(self, parent):
#         super().__init__(parent)
#         self.setSpacing(margins)
#         self.setContentsMargins(margins, margins, margins, margins)


class TreeItem(QtWidgets.QTreeWidgetItem):
    def __init__(self, *args, item_type=None):
        super().__init__(*args)
        self.item_type = item_type
        


class IPythonExecutionMonitor(object):
    def __init__(self, ip, parent: "SchemaSync", timer: QtCore.QTimer):
        self.shell = ip
        self.parent = parent
        self.timer = timer

    def pre_run_cell(self, info):
        self.timer.start(1_000_000)

    def post_run_cell(self, result):
        if result.info.cell_id:
            self.timer.start(200)


class SchemaSync(QtCore.QObject):
    """Shares this session's database schema with the language server.

    After each cell the schema is saved to the schema cache (only when it
    changed), where the language server picks it up for completions. Queries
    sent from the editor ("send SQL to GUI") come back through the same cache.
    """

    POLL_MS = 300
    HEARTBEAT_SECONDS = 5

    def __init__(self, app: "MyApp", db: duckdb.DuckDBPyConnection):
        super().__init__(app)
        self.app = app
        self.db = db.cursor()
        self.source_file = app.py_file
        self.log = logging.getLogger("dabbler_schema_sync")
        self.cache = SchemaCache()
        self.last_heartbeat = 0.0
        self.publish()
        self.heartbeat()

        self.ip = get_ipython()
        self.ipython_cell_timer = QtCore.QTimer()
        self.ipython_cell_timer.setSingleShot(True)
        self.ipython_cell_timer.timeout.connect(self.check_for_update)
        self.monitor_ip = IPythonExecutionMonitor(self.ip, self, self.ipython_cell_timer)
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
        self.log.debug(
            f"schema extract {extracted - start:.3f}s, save {time.perf_counter() - extracted:.3f}s, changed={changed}"
        )
        return changed

    def check_for_update(self):
        try:
            changed = self.publish()
        except Exception:
            self.log.exception("failed to save schema for completions")
            return
        if changed:
            self.app.update_tables()

    def heartbeat(self):
        self.cache.heartbeat(self.source_file)  # type: ignore
        self.last_heartbeat = time.time()

    def poll(self):
        try:
            if time.time() - self.last_heartbeat > self.HEARTBEAT_SECONDS:
                self.heartbeat()
            for command in self.cache.take_commands(self.source_file):  # type: ignore
                if command.cmd == "run_sql":
                    self.app.msg_routing(command.data)
        except Exception:
            self.log.exception("failed to read commands from the language server")

    def stop(self):
        """Stops listening to cells and the editor, and tells the language server the session ended."""
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
