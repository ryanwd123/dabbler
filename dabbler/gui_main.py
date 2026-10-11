import logging
from typing import Union
from IPython.core.getipython import get_ipython

from dabbler.gui_qt import (
    QtWidgets,
    QtCore,
    QtGui,
)
# from qtpy import QtWidgets, QtCore, QtGui



import duckdb
from pathlib import Path
from pygments.formatters import HtmlFormatter
import datetime
from dabbler.gui_stuff import (
    dataframes_in,
    highlight,
)
from dabbler.db_stuff import SessionState
from dabbler.gui_components import Shortcut, TreeItem, SchemaSync
from dabbler.gui_query import QueryResult, QueryRunner
from dabbler.gui_table import DfView

class TableSelectionArea(QtWidgets.QWidget):
    def __init__(
        self,
        parent: "mainWindow",
        db: duckdb.DuckDBPyConnection,
        parent_layout,
        table: DfView,
    ):
        super().__init__(parent)
        self.main = parent
        self.db = db
        self.table = table
        self.table_list: dict[str, str] = {}
        # self.setMaximumWidth(275)
        self.lay_out = QtWidgets.QVBoxLayout()
        self.setLayout(self.lay_out)
        self.lay_out.setContentsMargins(0, 3, 0, 0)
        self.lay_out.setSpacing(5)


        self.limit_input_layout = QtWidgets.QHBoxLayout()
        self.limit_input_layout.setContentsMargins(0, 0, 0, 0)
        self.limit_input_layout.setSpacing(0)
        self.limit_input = QtWidgets.QLineEdit()
        self.limit_input.setText(self.main.app.settings.value("limit", "1000000"))  # type: ignore
        self.limit_input.setValidator(QtGui.QIntValidator())
        self.limit_input.setFixedWidth(130)
        self.limit_input.textChanged.connect(lambda: self.main.app.settings.setValue("limit", self.limit_input.text()))
        self.limit_input_label = QtWidgets.QLabel("Result Limit:   ")
        self.limit_input_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.limit_input_layout.addWidget(self.limit_input_label)
        self.limit_input_layout.addWidget(self.limit_input)
        self.lay_out.addLayout(self.limit_input_layout)
        self.query: Union[str,None] = None
        self.selected_table: Union[str,None] = None
        self.column_types: dict[str, dict[str, str]] = {}
        self.table_selection = QtWidgets.QTreeWidget()
        self.table_selection.setIndentation(10)
        self.table_selection.setColumnCount(2)
        self.table_selection.setHeaderLabels(["Name", "Type"])
        self.table_selection.setColumnWidth(0, 250)
        self.table_selection.setColumnWidth(1, 100)


        self.table_selection.itemSelectionChanged.connect(self.select_table)
        self.df_list = set()

        self.model = self.table._model
        self.lay_out.addWidget(self.table_selection)
        self.runner = QueryRunner(self.main.app.make_cursor, lambda: user_namespace(), self)
        self.runner.finished.connect(self.show_result)

        self.populate_table_list()


    def populate_table_list(self):
        self.table_selection.clear()
        self.current_schema = self.db.execute("select current_schema()").fetchone()[0] # type: ignore
        self.current_db = self.db.execute("select current_database()").fetchone()[0] # type: ignore

        tbl_data = self.db.execute(
            """--sql
            with cte as (
                select "database_name", schema_name, table_name, 'table' as table_type from duckdb_tables
                union all
                select "database_name", schema_name, view_name, 'view' as table_type from duckdb_views
                union all
                select 'system_info', schema_name, function_name || '()', 'table function'
                    from duckdb_functions()
                    where function_type = 'table' and parameters = []
                    and function_name not in ('checkpoint','test_all_types','force_checkpoint','index_scan','seq_scan')
                union all
                select distinct 'system_info', "database_name" || '.' || schema_name, table_name, 'table'
                    from duckdb_columns()
                    where schema_name in ('information_schema', 'pg_catalog')
                    and database_name not in ('system','temp')            )
            select * from cte order by "database_name", schema_name, table_name
            """
        ).fetchall()

        dbs: dict[str, TreeItem] = {}
        dbs[self.current_db] = TreeItem([f"{self.current_db}"], item_type="db")

        for db, scm, tbl, typ in tbl_data:
            if db not in dbs:
                dbs[db] = TreeItem([f"{db}"], item_type="db")
            dbs[db].addChild(TreeItem([f"{scm}.{tbl}", typ], item_type=db))

        dbs["dataframes"] = TreeItem(["dataframes"], item_type="db")
        self.df_list = set()
        for name, df_type in dataframes_in(get_ipython()).items():
            dbs["dataframes"].addChild(TreeItem([name, df_type], item_type=df_type))
            self.df_list.add(name)
            self.table_list[name] = df_type

        added_items = ["dataframes", self.current_db]
        sys_info = dbs.pop("system_info")

        items = []
        for x in added_items:
            if x in dbs:
                items += [dbs[x]]

        items += [v for k, v in dbs.items() if k not in added_items]
        items += [sys_info]

        self.table_selection.addTopLevelItems(items)
        # self.table_selection.setIndentation()
        # self.table_selection.setRootIsDecorated(True)



        for k, db in [(k, db) for k, db in dbs.items() if k != "system_info"]:
            db.setExpanded(True)

    def select_table(self):
        items = self.table_selection.selectedItems()
        if not items or len(items) == 0:
            return
        item:TreeItem = items[0]   # type: ignore
        if item.item_type == "db":
            return

        self.selected_table = item.data(0, 0)
        tbl_db = f"{item.parent().data(0,0)}."
        if tbl_db == "system_info." or tbl_db == "dataframes.":
            tbl_db = ""

        self.query_db(f"""select * from {tbl_db}{self.selected_table} """, True)

    def limit(self) -> Union[int, None]:
        try:
            return int(self.limit_input.text()) or None
        except ValueError:
            return None

    def query_db(self, sql: str, _tbl_click=False):
        """Runs sql on a worker thread; show_result displays it."""
        self.main.status.setText("running... (Ctrl+. or cancel to stop)")
        self.main.sql_doc.setHtml(highlight(sql))
        self.main.btn_cancel.setEnabled(True)
        self.runner.run(sql, self.limit())

    def show_result(self, result: QueryResult):
        self.main.btn_cancel.setEnabled(False)
        if result.cancelled:
            self.main.status.setText(f"cancelled after {result.seconds:.2f} sec")
            return
        if result.error is not None:
            error = result.error
            self.main.status.setText(f"Exception executing query\n{type(error).__module__}.{type(error).__name__}:\n{error}")
            return
        self.query = result.sql
        if result.df is None:
            self.main.status.setText(f"statement executed in {result.seconds:.2f} sec")
            self.main.app.refresh_schema()  # it may have created or dropped something
            return
        self.result = result.df
        self.main.status.setText(f"{result.seconds:.2f} sec to execute")
        self.table.set_df(result.df, result.dtypes)


class mainWindow(QtWidgets.QMainWindow):
    def __init__(
        self,
        db: duckdb.DuckDBPyConnection,
        app: "MyApp",
        parent=None,
        id: int = 0,
    ):
        super().__init__(parent)
        self.central_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.central_widget)
        self.db = db
        self.id = id
        self.app = app
        self.split = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        self.setWindowTitle("dabbler")
        self.top_layout = QtWidgets.QVBoxLayout()
        self.central_widget.setLayout(self.top_layout)
        self.top_bar = QtWidgets.QHBoxLayout()
        self.top_layout.addLayout(self.top_bar)
        self.top_layout.addWidget(self.split)
        self.top_layout.setSpacing(0)
        self.top_layout.setContentsMargins(0, 0, 0, 0)

        self.mid_layout = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.mid_layout.setContentsMargins(0, 0, 0, 0)
        self.split.addWidget(self.mid_layout)
        self.table = DfView(self,self.app)
        self.selection_area = TableSelectionArea(
            self, self.db, self.mid_layout, self.table
        )
        self.mid_layout.addWidget(self.selection_area)
        self.mid_layout.addWidget(self.table)
        self.status = QtWidgets.QLabel()
        self.sql_doc = QtGui.QTextDocument()
        self.sql_doc.setDefaultStyleSheet(HtmlFormatter().get_style_defs())
        self.sql_textbox = QtWidgets.QTextEdit()
        self.sql_textbox.setReadOnly(True)
        self.sql_textbox.setDocument(self.sql_doc)
        self.status.setText("Ready")
        self.bottom_section = QtWidgets.QTabWidget()
        self.bottom_section.setMinimumHeight(100)
        self.bottom_layout = QtWidgets.QVBoxLayout(self.bottom_section)
        self.bottom_layout.setContentsMargins(0, 0, 0, 0)
        self.bottom_layout.setSpacing(0)



        self.bottom_layout.addWidget(self.status)
        self.bottom_layout.addWidget(self.sql_textbox)
        self.split.addWidget(self.bottom_section)

        btn_new_window = QtWidgets.QPushButton()
        btn_new_window.setText("new window")
        btn_new_window.clicked.connect(self.app.new_window)
        self.table.top_layout.addWidget(btn_new_window)

        btn_export = QtWidgets.QPushButton()
        btn_export.setText("export...")
        btn_export.setToolTip("Save the rows shown to a file and open it")
        btn_export.clicked.connect(self.export)
        self.table.top_layout.addWidget(btn_export)

        self.btn_cancel = QtWidgets.QPushButton()
        self.btn_cancel.setText("cancel")
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(lambda: self.selection_area.runner.cancel())
        self.table.top_layout.addWidget(self.btn_cancel)

        btn_refresh_tables = QtWidgets.QPushButton()
        btn_refresh_tables.setText("update table list")
        btn_refresh_tables.clicked.connect(self.selection_area.populate_table_list)
        self.table.top_layout.addWidget(btn_refresh_tables)

        self.short_keys = [
            # Shortcut('Alt+Right',self,lambda:checked_radio[0].next.setChecked(True)),
            # Shortcut('Escape',self,self.hide_window),
            # Shortcut("PgDown", self, self.selection_area.next_page),
            Shortcut("Ctrl+=", self, self.increase_font_size),
            Shortcut("Ctrl+.", self, lambda: self.selection_area.runner.cancel()),
            Shortcut("Ctrl+-", self, self.decrease_font_size),
        ]





        self.table.search.setTabOrder(self.table.search, self.table.table)
        self.font_size:int = int(self.app.settings.value("font_size", 12))  # type: ignore
        self.set_font_size()

        self.set_inital_position()
        self.save_geometry_settings_timer = QtCore.QTimer()
        self.save_geometry_settings_timer.timeout.connect(self.save_geometry_settings)
        self.save_geometry_settings_timer.setSingleShot(True)
        self.activateWindow()
        width = self.width()
        tabl_sel_width = 350
        self.mid_layout.setSizes([tabl_sel_width, width - tabl_sel_width])
        self.mid_layout.setStretchFactor(1, 1)

    def increase_font_size(self):
        self.font_size += 1
        self.table.font_size = self.font_size
        self.set_font_size()

    def decrease_font_size(self):
        self.font_size -= 1
        self.table.font_size = self.font_size
        self.set_font_size()

    def set_font_size(self):
        font = self.font()
        self.app.settings.setValue("font_size", self.font_size)
        font.setPointSize(self.font_size)
        font.setFamilies(["Calibri", "Arial", "sans-serif"])
        self.setFont(font)
        self.table.set_font_size(self.font_size)



    def set_inital_position(self):
        saved_pos = self.app.settings.value("pos", QtCore.QPoint(100, 100))
        saved_size = self.app.settings.value("size", QtCore.QSize(1200, 800))
        saved_geometry = QtCore.QRect(saved_pos, saved_size)  # type: ignore
        screens = QtWidgets.QApplication.screens()

        if not any([s.geometry().contains(saved_geometry) for s in screens]):
            self.setGeometry(100,100,1200,800)
            self.save_geometry_settings()
        else:
            self.move(saved_pos)  # type: ignore
            self.resize(saved_size)  # type: ignore

    def save_geometry_settings(self):
        if self.id != 0:
            return
        self.app.settings.setValue("size", self.size())
        self.app.settings.setValue("pos", self.pos())

    def closeEvent(self, event):
        # a query left running would outlive the window (and keep a CPU busy)
        self.selection_area.runner.cancel()
        self.selection_area.runner.wait(5)
        self.table.stop_worker()
        super().closeEvent(event)

    def resizeEvent(self, event):
        self.save_geometry_settings_timer.start(2000)

    def moveEvent(self, event):
        self.save_geometry_settings_timer.start(2000)


    def export(self):
        """Saves the rows shown (after search and sort) to a file, then opens it."""
        df = self.table._model.df
        if df.width == 0:
            self.status.setText("nothing to export; run a query first")
            return
        filters = ["CSV (*.csv)", "Parquet (*.parquet)"]
        if excel_available():
            filters.insert(0, "Excel (*.xlsx)")
        default = str(Path.home() / f"query_{datetime.datetime.now():%Y%m%d_%H%M%S}")
        path, chosen = QtWidgets.QFileDialog.getSaveFileName(self, "Export rows", default, ";;".join(filters))
        if not path:
            return
        try:
            path = export_df(df, path, chosen)
        except Exception as error:
            self.status.setText(f"Export failed:\n{type(error).__name__}: {error}")
            return
        self.status.setText(f"exported {df.height:,} rows to {path}")
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(path)))


def excel_available() -> bool:
    try:
        import xlsxwriter  # noqa: F401  (polars writes .xlsx with it)
        return True
    except ImportError:
        return False


def export_df(df, path: str, chosen_filter: str = "") -> Path:
    """Writes df as .xlsx, .parquet or .csv, by the file's suffix or the chosen filter."""
    target = Path(path)
    suffix = target.suffix.lower()
    if suffix not in (".xlsx", ".parquet", ".csv"):
        suffix = ".xlsx" if "xlsx" in chosen_filter else ".parquet" if "parquet" in chosen_filter else ".csv"
        target = target.with_suffix(suffix)
    if suffix == ".xlsx":
        df.write_excel(target)
    elif suffix == ".parquet":
        df.write_parquet(target)
    else:
        df.write_csv(target)
    return target


def user_namespace() -> dict:
    ipython = get_ipython()
    return ipython.user_ns if ipython else {}


def app_settings() -> QtCore.QSettings:
    """Window geometry, font size and the row limit, kept between sessions."""
    return QtCore.QSettings("dabbler", "dabbler")


class MyApp(QtCore.QObject):
    """dabbler's windows and schema sharing for one DuckDB connection.

    Uses the process's QApplication if there is one (matplotlib's Qt backend,
    or an earlier %load_ext), and creates it otherwise.
    """

    def __init__(
        self, db: duckdb.DuckDBPyConnection, file_search_path=None, debug=False,
        file: Union[str, None] = None, db_name: Union[str, None] = None,
    ) -> None:
        existing = QtWidgets.QApplication.instance()
        self.qapp = existing or QtWidgets.QApplication([])
        super().__init__()
        if existing is None:
            self.qapp.setStyle("Fusion")  # type: ignore
        self.db = db
        self.db_name = db_name
        self.settings = app_settings()
        self.windows: list[mainWindow] = []
        self.file_search_path = file_search_path
        self.py_file = Path(file) if file else None
        # USE / search_path / file_search_path of the user's connection, applied to dabbler's cursors
        self.session = SessionState(file_search_path=file_search_path)
        self.debug = debug
        self.log = logging.getLogger("dabbler_gui")
        if self.debug:
            logging.basicConfig()
            for name in ("dabbler_gui", "dabbler_schema_sync"):
                logging.getLogger(name).setLevel(logging.DEBUG)
            self.log.info("debugging")
        self.schema_sync = SchemaSync(self, self.db) if self.py_file else None
        self.qapp.aboutToQuit.connect(self.close)  # type: ignore
        self.win_id = 0
        self.new_window()

    def make_cursor(self) -> duckdb.DuckDBPyConnection:
        """A cursor on the user's connection, with its USE / search path settings."""
        cur = self.db.cursor()
        try:
            self.session.apply(cur)
        except duckdb.Error:
            self.log.exception("failed to apply the session's settings to a cursor")
        return cur

    def msg_routing(self, sql: str):
        """Runs a query sent from the editor in the most recently used window."""
        window = self.last_window() or self.new_window()
        window.selection_area.query_db(sql)
        window.raise_()
        window.activateWindow()

    def last_window(self) -> Union["mainWindow", None]:
        self.windows = [w for w in self.windows if w.isVisible()]
        active = QtWidgets.QApplication.activeWindow()
        if active in self.windows:
            return active  # type: ignore
        return self.windows[-1] if self.windows else None

    def update_tables(self):
        self.windows = [w for w in self.windows if w.isVisible()]
        for w in self.windows:
            w.selection_area.populate_table_list()

    def refresh_schema(self):
        if self.schema_sync is not None:
            self.schema_sync.check_for_update()
        self.update_tables()

    def show(self):
        window = self.last_window() or self.new_window()
        window.showNormal()
        window.raise_()
        window.activateWindow()

    def new_window(self) -> "mainWindow":
        window = mainWindow(self.db, self, id=self.win_id)
        self.win_id += 1
        # Qt deletes a closed window on this thread. Left to Python's garbage collector,
        # it could be freed from the heartbeat or a query thread, which aborts on Linux.
        window.setAttribute(QtCore.Qt.WidgetAttribute.WA_DeleteOnClose)
        key = id(window)  # not the window itself, which would keep it alive
        window.destroyed.connect(lambda *_: self.forget_window(key))
        window.show()
        self.windows = [w for w in self.windows if w.isVisible()] + [window]
        window.status.setText(f"windows open: {len(self.windows)}")
        return window

    def forget_window(self, key: int):
        self.windows = [w for w in self.windows if id(w) != key]

    def close(self):
        """Stops sharing the schema and closes the windows (on %unload_ext, or when Qt quits)."""
        if self.schema_sync is not None:
            self.schema_sync.stop()
            self.schema_sync = None
        for w in list(self.windows):
            w.close()  # deletes it (WA_DeleteOnClose)
        self.windows = []
