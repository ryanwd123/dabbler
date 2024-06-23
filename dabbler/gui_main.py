from queue import Queue
import time
import logging
from dabbler.common import PprintSocketHandler
from logging.handlers import SocketHandler
from IPython import get_ipython
from qtpy import QtWidgets, QtCore, QtGui
import duckdb
from statistics import mean, quantiles
import sqlparse
from pathlib import Path
from pygments.formatters import HtmlFormatter
import datetime
from dabbler.gui_stuff import (
    check_dataframe_type,
    apply_fmt,
    format_map,
    fmt_types,
    gui_style,
    highlight,
)
from dabbler.gui_compenents import Shortcut, Vbox, Hbox, TreeItem, ZmqServer


def header_format(txt:str):
    if txt.startswith('ENUM'):
        return 'ENUM'
    return txt


class TableModel(QtCore.QAbstractTableModel):
    def __init__(
        self,
        data,
        selection_area: "TableSelectionArea",
        parent=None,
        hheaders: list[str] = None,
        htypes: list[str] = None,
    ):
        super(TableModel, self).__init__(parent)
        self._data = data
        self.selection_area = selection_area
        self.hheaders = hheaders
        self.htypes = htypes
        self.dispaly_role = QtCore.Qt.ItemDataRole.DisplayRole
        self.align_role = QtCore.Qt.ItemDataRole.TextAlignmentRole
        self.horiz = QtCore.Qt.Orientation.Horizontal

        self.alignment_map = {
            "STRING": QtCore.Qt.AlignmentFlag.AlignLeft
            + QtCore.Qt.AlignmentFlag.AlignVCenter,
            "NUMBER": QtCore.Qt.AlignmentFlag.AlignRight
            + QtCore.Qt.AlignmentFlag.AlignVCenter,
            "INT": QtCore.Qt.AlignmentFlag.AlignRight
            + QtCore.Qt.AlignmentFlag.AlignVCenter,
            "DATE": QtCore.Qt.AlignmentFlag.AlignRight
            + QtCore.Qt.AlignmentFlag.AlignVCenter,
            "DATETIME": QtCore.Qt.AlignmentFlag.AlignRight
            + QtCore.Qt.AlignmentFlag.AlignVCenter,
            "TIME": QtCore.Qt.AlignmentFlag.AlignRight
            + QtCore.Qt.AlignmentFlag.AlignVCenter,
        }

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):
        return len(self._data[0]) if self.rowCount() else 0

    def data(self, index, role):
        if role == self.dispaly_role:
            row = index.row()
            if 0 <= row < self.rowCount():
                column = index.column()
                if 0 <= column < self.columnCount():
                    return self._data[row][column]
        if role == self.align_role:
            column = index.column()
            alignment = self.alignment_map.get(
                self.htypes[column],
                QtCore.Qt.AlignmentFlag.AlignLeft
                + QtCore.Qt.AlignmentFlag.AlignVCenter,
            )
            return alignment

    def headerData(self, section, orientation, role=QtCore.Qt.ItemDataRole.DisplayRole):
        if orientation == self.horiz and role == self.dispaly_role:
            return self.hheaders[section]
        return super().headerData(section, orientation, role)


class DisplayArea(QtWidgets.QWidget):
    def __init__(self, parent=None, parent_layout=None):
        super().__init__(parent)
        self.lay_out = Vbox(self)
        self.lay_out.addWidget(QtWidgets.QLabel("Display Area"))
        self.table = QtWidgets.QTableView()

        self.lay_out.addWidget(self.table)
        # self.table.setMinimumWidth(800)
        # self.table.setMinimumHeight(450)
        self.table.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        self.table.verticalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        self.table.verticalHeader().setVisible(False)


class TableSelectionArea(QtWidgets.QWidget):
    def __init__(
        self,
        parent: "mainWindow" = None,
        db: duckdb.DuckDBPyConnection = None,
        parent_layout=None,
        table: QtWidgets.QTableView = None,
        col_selector: "ColSelectionArea" = None,
    ):
        super().__init__(parent)
        self.main = parent
        self.db = db
        self.table = table
        self.table_list: dict[str, str] = {}
        # self.setMaximumWidth(275)
        self.lay_out = Vbox(self)
        self.db_size = QtWidgets.QLabel("DB size: ")
        self.lay_out.addWidget(self.db_size)
        self.query: str = None
        self.selected_table: str = None
        self.column_types: dict[str, dict[str, str]] = {}
        self.table_selection = QtWidgets.QTreeWidget()
        self.table_selection.setColumnCount(2)
        self.table_selection.setHeaderLabels(["Name", "Type"])
        self.table_selection.setColumnWidth(0, 200)
        self.table_selection.setColumnWidth(1, 50)

        self.table_selection.itemSelectionChanged.connect(self.select_table)
        self.df_list = set()

        self.model = self.table.model()
        self.lay_out.addWidget(self.table_selection)

        self.populate_table_list()

    def populate_table_list(self):
        self.table_selection.clear()
        mem, mem_limit = self.db.execute(
            "select memory_usage, memory_limit from pragma_database_size()"
        ).fetchone()
        self.db_size.setText(f"DB Size: {mem}/{mem_limit}")
        self.current_schema = self.db.execute("select current_schema()").fetchone()[0]
        self.current_db = self.db.execute("select current_database()").fetchone()[0]

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

        added_items = [self.current_db, "dataframes"]
        sys_info = dbs.pop("system_info")

        items = []
        for x in added_items:
            if x in dbs:
                items += [dbs[x]]

        items += [v for k, v in dbs.items() if k not in added_items]
        items += [sys_info]

        self.table_selection.addTopLevelItems(items)
        self.table_selection.setIndentation(7)

        for k, db in [(k, db) for k, db in dbs.items() if k != "system_info"]:
            db.setExpanded(True)

    def select_table(self):
        item = self.table_selection.selectedItems()[0]
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

        # if _self_z.selected_table in _self_z.df_list and _tbl_click:
        #     _ip_z = get_ipython()
        #     locals().__setitem__(_self_z.selected_table,_ip_z.ev(_self_z.selected_table))
        #     if _self_z.table_list[_self_z.selected_table] == 'duckdb_rel':
        #         for _t_z_ in _self_z.table_list:
        #             locals().__setitem__(_t_z_,_ip_z.ev(_t_z_))

        _ipython_z = get_ipython()
        for _item_z in _ipython_z.ev("dir()"):
            _i_type_z = str(type(_ipython_z.ev(_item_z)))
            _df_type = check_dataframe_type(_i_type_z)
            
            
            if _df_type and _item_z[0] != "_":
                locals().__setitem__(_item_z, _ipython_z.ev(_item_z))

        try:
            _start = time.time()
            
            _desc = _self_z.db.execute(f"Describe {_stmt_z}").fetchall()
            
            # desc_stmt = f"{_self_z.main.app.db_name}.execute('''Describe {_stmt_z}''').fetchall()"
            # _self_z.main.app.log.info(desc_stmt)
            # _desc = _ipython_z.ev(desc_stmt)
            
            _self_z.headers = [f"{x[0]}\n{header_format(x[1])}" for x in _desc]
            _self_z.dtypes = [x[1] for x in _desc]
            _self_z.htypes = [fmt_types.get(x, "STRING") for x in _self_z.dtypes]
            
            _self_z.result = _self_z.db.execute(_stmt_z)
            
            # exec_stmt = f"{_self_z.main.app.db_name}.execute('''{_stmt_z}''')"
            # _self_z.result = _ipython_z.ev(exec_stmt)
            
            
            
            _self_z.query = _stmt_z
            _duration = time.time() - _start
            _self_z.main.status.setText(f"{_duration:.2f} sec to execute")

            # highlighted_query_text = highlight(sqlparse.format(_stmt_z,reindent_aligned=True))
            _highlighted_query_text = highlight(_stmt_z)
            _self_z.main.sql_doc.setHtml(_highlighted_query_text)
            _self_z.next_page()

        except Exception as e:
            _self_z.main.status.setText(f"Exception executing query:\n{e}")
            _highlighted_query_text = highlight(
                sqlparse.format(_stmt_z, reindent_aligned=True)
            )
            _self_z.main.sql_doc.setHtml(_highlighted_query_text)


    def get_col_fmt(self, col_type, col_index, data):
        try:
            fmt_type = fmt_types.get(col_type, "STRING")
            if fmt_type == "NUMBER":
                col_abs = [
                    abs(x[col_index])
                    for x in data
                    if x[col_index] is not None and x[col_index] != 0
                ]
                if len(col_abs) == 0:
                    return ""

                if len(col_abs) == 1:
                    col_avg = col_abs[0]
                else:
                    col_avg = mean(quantiles(col_abs, n=10)[1:-1])

                if col_avg < 1:
                    return ".5f"
                if col_avg < 5:
                    return ".2f"
                if col_avg < 99:
                    return ".1f"
                return ",.0f"
            if fmt_type == "INT":
                col_abs = [
                    abs(x[col_index])
                    for x in data
                    if x[col_index] is not None and x[col_index] != 0
                ]
                if len(col_abs) == 0:
                    return ""

                if len(col_abs) == 1:
                    col_avg = col_abs[0]
                else:
                    col_avg = mean(quantiles(col_abs, n=10)[1:-1])

                if col_avg < 3000:
                    return ""
                if col_avg > 3000:
                    return ","

            fmt_str = format_map.get(fmt_type, "")
        except Exception as e:
            self.main.status.setText(
                f"fmt exception:{e}\ncol_type:{col_type}\ndata:{[x[col_index] for x in data]}"
            )
        return fmt_str

    def next_page(self):
        try:
            subtract_amt = 0
            if self.table.horizontalScrollBar().isVisible():
                subtract_amt = 24
            rows_to_fetch = int((self.height() - 48 - subtract_amt) / 23)
            self.model = self.table.model()

            if self.model is not None:
                self.table.setModel(None)
                self.model.deleteLater()

            data = self.result.fetchmany(rows_to_fetch)

            if len(data) == 0:
                self.main.status.setText("end of query")
                return

            fmts = [
                self.get_col_fmt(col_type, i, data)
                for i, col_type in enumerate(self.dtypes)
            ]

            # data = [[f' {x:{fmts[c]}} ' for c,x in enumerate(row)] for row in data]
            data = [[apply_fmt(x, c, fmts) for c, x in enumerate(row)] for row in data]

            self.model = TableModel(data, self.table, self, self.headers, self.htypes)
            self.table.setModel(self.model)
            self.table.setCurrentIndex(self.table.model().index(0, 0))

        except Exception as e:
            self.main.status.setText(f"Exeption on next_page:\n{e}\nfmts:{fmts}")


class ColSelectionArea(QtWidgets.QWidget):
    def __init__(
        self,
        parent=None,
        db: duckdb.DuckDBPyConnection = None,
        parent_layout=None,
        table: QtWidgets.QTableView = None,
    ):
        super().__init__(parent)
        self.db = db
        self.table = table
        self.setMaximumWidth(150)
        self.lay_out = Vbox(self)
        self.lay_out.addWidget(QtWidgets.QLabel("Tbl Columns"))
        self.selected_table: str = None
        self.column_types: dict[str, list[(str, str)]] = {}
        self.col_sel_model = QtGui.QStandardItemModel()
        self.col_selection = QtWidgets.QListView(self)
        self.col_selection.setModel(self.col_sel_model)
        self.col_selection.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self.col_selection.selectionModel().selectionChanged.connect(
            self.populate_table
        )
        self.lay_out.addWidget(self.col_selection)


class TopBar(QtWidgets.QWidget):
    def __init__(self, parent=None, parent_layout=None):
        super().__init__(parent)
        self.lay_out = Hbox(self)


class mainWindow(QtWidgets.QWidget):
    def __init__(
        self,
        db: duckdb.DuckDBPyConnection,
        app: "MyApp",
        parent=None,
    ):
        super().__init__(parent)
        self.db = db
        self.app = app
        self.split = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        self.setWindowTitle("dabbler")
        self.top_layout = Vbox(self)
        self.top_bar = TopBar(self, self.top_layout)
        self.top_layout.addWidget(self.top_bar)
        self.top_layout.addWidget(self.split)

        self.mid_layout = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.split.addWidget(self.mid_layout)
        self.display_area = DisplayArea(self, self.mid_layout)
        self.selection_area = TableSelectionArea(
            self, self.db, self.mid_layout, self.display_area.table
        )
        self.mid_layout.addWidget(self.selection_area)
        self.mid_layout.addWidget(self.display_area)
        self.status = QtWidgets.QLabel()
        self.sql_doc = QtGui.QTextDocument()
        self.sql_doc.setDefaultStyleSheet(HtmlFormatter().get_style_defs())
        self.sql_textbox = QtWidgets.QTextEdit()
        self.sql_textbox.setReadOnly(True)
        self.sql_textbox.setDocument(self.sql_doc)
        self.status.setText("Ready")
        self.bottom_section = QtWidgets.QTabWidget()
        self.bottom_section.setMinimumHeight(100)
        self.bottom_layout = Vbox(self.bottom_section)
        self.bottom_layout.addWidget(self.status)
        self.bottom_layout.addWidget(self.sql_textbox)
        self.split.addWidget(self.bottom_section)

        btn_new_window = QtWidgets.QPushButton()
        btn_new_window.setText("new window")
        btn_new_window.clicked.connect(self.app.new_window)
        self.top_bar.lay_out.addWidget(btn_new_window)

        btn_to_excel = QtWidgets.QPushButton()
        btn_to_excel.setText("open_in_excel")
        btn_to_excel.clicked.connect(self.to_excel)
        self.top_bar.lay_out.addWidget(btn_to_excel)

        btn_refresh_tables = QtWidgets.QPushButton()
        btn_refresh_tables.setText("update table list")
        btn_refresh_tables.clicked.connect(self.selection_area.populate_table_list)
        self.top_bar.lay_out.addWidget(btn_refresh_tables)

        self.short_keys = [
            # Shortcut('Alt+Right',self,lambda:checked_radio[0].next.setChecked(True)),
            # Shortcut('Escape',self,self.hide_window),
            Shortcut("PgDown", self, self.selection_area.next_page),
        ]
        width = 960
        self.setGeometry(900, 200, width, 600)
        self.activateWindow()
        tabl_sel_width = 285
        self.mid_layout.setSizes([tabl_sel_width, width - tabl_sel_width])
        self.mid_layout.setStretchFactor(1, 1)

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
        except Exception as e:
            self.status.setText(f"Exception trying to export to excel:\n{e}")


class MyApp(QtWidgets.QApplication):
    def __init__(
        self, argv, db=None, file_search_path = None, debug = False, file:str = None, db_name:str = None
        
    ) -> None:
        super().__init__(argv)
        self.db = db
        self.db_name = db_name
        self.windows: list[mainWindow] = []
        self.in_thread = False
        self.file_search_path = file_search_path
        self.py_file:Path = Path(file)
        self.stylesheet = gui_style
        self.debug = debug
        self.log = logging.getLogger("dabbler_gui")
        if self.debug:
            self.log.setLevel(1)
            socket_handler = PprintSocketHandler('127.0.0.1', 19996)
            # socket_handler.setFormatter(LogFmt)
            self.log.addHandler(socket_handler)
            self.log.info("debugging")
        self.zmq = ZmqServer(self, self.db)
        # self.q_handler = MsgHandler(q=q,signal_to_emit=self.msg_routing)
        # self.focusWindowChanged.connect(self.update_tables)
        self.setStyleSheet(gui_style)
        self.new_window()
        # if in_thread:
        # self.exec()

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
        window = mainWindow(self.db, self)
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
