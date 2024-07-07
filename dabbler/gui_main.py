import time
import logging
from typing import Union
from dabbler.common import PprintSocketHandler
from sqlglot import parse_one, exp
from IPython.core.getipython import get_ipython
from qtpy import QtWidgets, QtCore, QtGui
import duckdb
import sqlparse
from pathlib import Path
from pygments.formatters import HtmlFormatter
import datetime
from dabbler.gui_stuff import (
    check_dataframe_type,
    highlight,
)
from dabbler.gui_compenents import Shortcut, TreeItem, ZmqServer
from dabbler.gui_table import DfView

def check_dtype(dt:str):
    if dt.startswith("ENUM("):
        return "ENUM"
    if dt.startswith("STRUCT("):
        return "STRUCT"
    return dt


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
        

        self.db_size = QtWidgets.QLineEdit("Limit: ")
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

        self.populate_table_list()


    def populate_table_list(self):
        self.table_selection.clear()
        mem, mem_limit = self.db.execute(  
            "select memory_usage, memory_limit from pragma_database_size()"
        ).fetchone() # type: ignore

        self.db_size.setText(f"DB Size: {mem}/{mem_limit}")
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

        if not self.main.app.in_thread:
            dbs["dataframes"] = TreeItem(["dataframes"], item_type="db")
            ipython = get_ipython()
            if ipython:
                self.df_list = set()
                for item in ipython.ev("dir()"):
                    i_type = str(type(ipython.ev(item)))
                    df_type = check_dataframe_type(i_type)
                    
                    if df_type and item[0] != "_":
                        dbs["dataframes"].addChild(
                            TreeItem(
                                [item, df_type],
                                item_type=df_type,
                            )
                        )
                        self.df_list.add(item)
                        self.table_list[item] = df_type

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

    def query_db(_self_z, _stmt_z: str, _tbl_click=False):
        _self_z.db = _self_z.db.cursor()
        _self_z.db.execute(f"set file_search_path to '{_self_z.main.app.file_search_path}'")

        try:
            _limit = parse_one(_stmt_z, read="duckdb").find(exp.Select).find(exp.Limit)   # type: ignore
        except Exception:
            _limit = None
        
        if _limit is None:
            _limit_str = f"limit {_self_z.limit_input.text()}"
            _stmt_z = f"{_stmt_z} {_limit_str}"

        _ipython_z = get_ipython()
        if _ipython_z:
            for _item_z in _ipython_z.ev("dir()"):
                _i_type_z = str(type(_ipython_z.ev(_item_z)))
                _df_type = check_dataframe_type(_i_type_z)
                
                
                if _df_type and _item_z[0] != "_":
                    locals().__setitem__(_item_z, _ipython_z.ev(_item_z))

        try:
            _start = time.time()
            
            # _desc = _self_z.db.execute(f"Describe {_stmt_z}").fetchall()
            
            # _self_z.dtypes = [x[1] for x in _desc]
            # _self_z.htypes = [fmt_types.get(x, "STRING") for x in _self_z.dtypes]
            
            _rel = _self_z.db.sql(_stmt_z)
            d_types = _rel.dtypes
            d_types = [check_dtype(str(x)) for x in d_types]
            _self_z.result = _rel.pl()

            
            _self_z.query = _stmt_z
            _duration = time.time() - _start
            _self_z.main.status.setText(f"{_duration:.2f} sec to execute")
            _self_z.table.set_df(_self_z.result, d_types)


            _highlighted_query_text = highlight(_stmt_z)
            _self_z.main.sql_doc.setHtml(_highlighted_query_text)

        except Exception as error:
            excetion_str = 'Exception executing query\n'
            excetion_str += f'{type(error).__module__}.{type(error).__name__}:\n{error=}'
            _self_z.main.status.setText(excetion_str)
            _highlighted_query_text = highlight(
                sqlparse.format(_stmt_z, reindent_aligned=True)
            )
            _self_z.main.sql_doc.setHtml(_highlighted_query_text)



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

        btn_to_excel = QtWidgets.QPushButton()
        btn_to_excel.setText("open_in_excel")
        btn_to_excel.clicked.connect(self.to_excel)
        self.table.top_layout.addWidget(btn_to_excel)

        btn_refresh_tables = QtWidgets.QPushButton()
        btn_refresh_tables.setText("update table list")
        btn_refresh_tables.clicked.connect(self.selection_area.populate_table_list)
        self.table.top_layout.addWidget(btn_refresh_tables)

        self.short_keys = [
            # Shortcut('Alt+Right',self,lambda:checked_radio[0].next.setChecked(True)),
            # Shortcut('Escape',self,self.hide_window),
            # Shortcut("PgDown", self, self.selection_area.next_page),
            Shortcut("Ctrl+=", self, self.increase_font_size),
            Shortcut("Ctrl+-", self, self.decrease_font_size),
        ]





        self.table.search.setTabOrder(self.table.search, self.table.table)
        self.font_size:int = self.app.settings.value("font_size", 12)  # type: ignore
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
        saved_size = self.app.settings.value("size", QtCore.QPoint(1200, 800))
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

    def resizeEvent(self, event):
        self.save_geometry_settings_timer.start(2000)

    def moveEvent(self, event):
        self.save_geometry_settings_timer.start(2000)


    def to_excel(self):
        if not self.selection_area.query:
            self.status.setText("query not found")
            return
        try:
            self.status.setText("clicked to excel")
            Path(r"c:\temp_rpt").mkdir(exist_ok=True)
            self.status.setText("make_path")

            output_file = Path(r"c:\temp_rpt").joinpath(
                f"query_{datetime.datetime.now():%Y%m%d%M%S}.csv"
            )
            output = self.db.sql(self.selection_area.query)
            self.status.setText("db_run")
            output.limit(100000).to_csv(str(output_file), header=True)
            self.status.setText("csv_saved")
            import win32com.client as win

            excel = win.Dispatch("Excel.Application")
            excel.Workbooks.Open(str(output_file))
            excel.Visible = True
            self.status.setText("excel_open")
        except Exception as error:
            self.status.setText(f"Exception trying to export to excel:\n{error=}")


class MyApp(QtWidgets.QApplication):
    def __init__(
        self, argv, db=None, file_search_path = None, debug = False, file:Union[str,None] = None, db_name:Union[str,None] = None
        
    ) -> None:
        super().__init__(argv)
        self.db = db
        self.db_name = db_name
        self.settings = QtCore.QSettings("dabbler", "dabbler")
        self.windows: list[mainWindow] = []
        self.in_thread = False
        self.file_search_path = file_search_path
        if file:
            self.py_file = Path(file)
        else:
            self.py_file = None
        self.debug = debug
        self.log = logging.getLogger("dabbler_gui")
        if self.debug:
            self.log.setLevel(1)
            socket_handler = PprintSocketHandler('127.0.0.1', 19996)
            # socket_handler.setFormatter(LogFmt)
            self.log.addHandler(socket_handler)
            self.log.info("debugging")
        if file:
            self.zmq = ZmqServer(self, self.db)
        self.win_id = 0
        self.new_window()

    # def update_tables(self):
    #     self.windows = [w for w in self.windows if w.isVisible()]

    #     for w in self.windows:
    #         w.selection_area.populate_table_list()

    # @Slot(str)
    def msg_routing(self, item):
        self.windows[0].selection_area.query_db(item)

    def update_tables(self):
        self.windows = [w for w in self.windows if w.isVisible()]
        for w in self.windows:
            w.selection_area.populate_table_list()

    def new_window(self):
        if not self.db:
            return
        window = mainWindow(self.db, self, id=self.win_id)
        self.win_id += 1
        window.show()
        # window.move(QtCore.QPoint(900,200))
        self.windows.append(window)
        for w in self.windows:
            if not w.isVisible():
                w.deleteLater()
        self.windows = [w for w in self.windows if w.isVisible()]

        txt = f"windows open: {len(self.windows)}"
        # for w in self.windows:
        # txt += f'\n{w},{w.isVisible()}'
        window.status.setText(txt)
