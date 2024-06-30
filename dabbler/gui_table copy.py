from ast import mod
from audioop import avg
import re
from tkinter import N
from unicodedata import numeric
from qtpy import QtWidgets, QtCore, QtGui
from qtpy.QtCore import Qt
from qtpy.QtGui import QFont, QFontMetrics
from dabbler.gui_compenents import Shortcut
import polars as pl
import time
from functools import reduce


def get_col_fmts(c: str, df: pl.DataFrame, dtype: pl.DataType, fm: QFontMetrics):

    if dtype.is_numeric():
        q = df[c].quantile(0.9)
        b = df[c].quantile(0.1)
        if q and q < 0.5:
            return ",.6f"
        if b and b > 10:
            return ",.0f"
        return ",.2f"

    if dtype.is_temporal():
        return "%Y-%m-%d"
    return ""


def get_str(val, fmt: str):
    if not val:
        return ""
    return f"{val:{fmt}}"


def get_col_width(
    fm: QFontMetrics, col: str, df: pl.DataFrame, dtype: pl.DataType, format: str
):
    if dtype.is_numeric():
        vals = df[col].unique().top_k(10).to_list()
        # print(col, vals, format)
        return max(
            [fm.horizontalAdvance(str(dtype))]
            + [fm.horizontalAdvance(col)]
            + [fm.horizontalAdvance(get_str(val, format)) for val in vals]
        )
    if dtype.is_temporal():
        return max(
            [fm.horizontalAdvance(str(dtype))]
            + [fm.horizontalAdvance(col)]
            + [
                fm.horizontalAdvance(get_str(val, format))
                for val in df[col].unique().top_k(10).to_list()
            ]
        )
    else:
        if dtype == pl.List:
            try:
                c = df[col].list.join(", ").cast(pl.String).unique()
            except:
                return 200
        elif dtype == pl.Struct:
            return 200
        elif not dtype == pl.String:
            print(col, dtype, dtype.base_type())
            c = df[col].unique().cast(pl.String)
        elif dtype == pl.String:
            c = df[col].unique()
        else:
            c = df[col].cast(pl.String).unique()
        if df.shape[0] > 100:
            q = c.str.len_chars().quantile(0.95)
            if q:
                c = c.filter(c.str.len_chars() < q)
        top = c.str.len_chars().arg_sort(descending=True)[:10].to_list()
        # print(col, q, [c[val] for val in top])
        # top = c.str.len_chars().arg_sort(descending=True)[:10].to_list()
        # print(col, [c[val] for val in top])
        w = max(
            [fm.horizontalAdvance(str(dtype))]
            + [fm.horizontalAdvance(col)]
            + [fm.horizontalAdvance(c[val]) for val in top]
        )
        w = min(w, 600)
        return w


def get_col_fmts_and_widths(font: QFontMetrics, df: pl.DataFrame):
    start = time.time()
    # fm = QFontMetrics(font)
    fm = font
    schema = df.schema
    # if df.shape[0] > 1000_000:
    # df = df.sample(800_000)

    fmts = [get_col_fmts(c, df, dt, fm) for c, dt in schema.items()]
    widths = [
        get_col_width(fm, c, df, dt, f)
        for c, dt, f in zip(schema.keys(), schema.values(), fmts)
    ]
    print(f"time to calc widths {time.time() - start:.4f}s")
    return widths, fmts


def get_alignment(dtype: pl.DataType):
    if dtype.is_numeric():
        return Qt.AlignmentFlag.AlignRight
    if dtype.is_temporal():
        return Qt.AlignmentFlag.AlignCenter
    return Qt.AlignmentFlag.AlignLeft



class CustomDelegate(QtWidgets.QStyledItemDelegate):
    def paint(self, painter, option, index):
        if option.state & QtWidgets.QStyle.StateFlag.State_Selected:
            option.palette.setColor(option.palette.Highlight, QtGui.QColor("gold"))
            option.palette.setColor(option.palette.HighlightedText, QtGui.QColor("black"))
        QtWidgets.QStyledItemDelegate.paint(self, painter, option, index)


#MARK: TableModel
class TableModel(QtCore.QAbstractTableModel):

    def __init__(self, df: pl.DataFrame, p: "DfView" = None):
        super(TableModel, self).__init__()
        self.active_cell: tuple[int,int] = None
        self.anchor_cell: tuple[int,int] = None
        self.start_row: int = None
        self.start_col: int = None
        self.end_row: int = None
        self.end_col: int = None

        self.p = p
        self.set_df(df)

    def set_active_cell(self, index: QtCore.QModelIndex, shift: bool):
        row = index.row()
        col = index.column()
        if not shift:
            self.active_cell = (row, col)
            self.anchor_cell = (row, col)
            self.update_start_end_row_col()
        else:
            self.active_cell = (row, col)
            self.update_start_end_row_col()
        # print(f"active cell {self.active_cell} anchor cell: {self.anchor_cell}",  shift)

    def update_start_end_row_col(self):
            if self.active_cell is None or self.anchor_cell is None:
                return
            self.start_row = min(self.active_cell[0], self.anchor_cell[0])
            self.start_col = min(self.active_cell[1], self.anchor_cell[1])
            self.end_row = max(self.active_cell[0], self.anchor_cell[0])
            self.end_col = max(self.active_cell[1], self.anchor_cell[1])
            # print(f"start: {self.start_row}, {self.start_col} end: {self.end_row}, {self.end_col}")
            self.layoutChanged.emit()


    def set_df(self, df: pl.DataFrame):
        self.df = df
        self.formats = ["" for d in df.dtypes]
        self.columns = df.columns
        self.alignments = [get_alignment(d) for d in df.dtypes]
        self.dtypes = [str(d) for d in df.dtypes]
        self.layoutChanged.emit()

    def rowCount(self, parent=None):
        return self.df.shape[0]

    def columnCount(self, parent=None):
        return self.df.shape[1]

    def data(self, index, role):
        if role == QtCore.Qt.ItemDataRole.DisplayRole:
            row = index.row()
            if 0 <= row < self.rowCount():
                column = index.column()
                if 0 <= column < self.columnCount():
                    value = self.df.item(row, column)
                    fmt = self.formats[column]
                    if not fmt:
                        return str(value)
                    return get_str(value, fmt)

        if role == QtCore.Qt.ItemDataRole.TextAlignmentRole:
            row = index.row()
            if 0 <= row < self.rowCount():
                column = index.column()
                if 0 <= column < self.columnCount():
                    return self.alignments[column]
                
        if role == QtCore.Qt.ItemDataRole.BackgroundRole:
            row = index.row()
            col = index.column()
            if self.start_row is None or self.end_row is None:
                return

            if row >= self.start_row and row <= self.end_row:
                if col >= self.start_col and col <= self.end_col:
                    return QtGui.QColor("lightblue")
        

    def headerData(self, section, orientation, role):
        if role == QtCore.Qt.ItemDataRole.DisplayRole:

            if orientation == QtCore.Qt.Orientation.Horizontal:
                col = self.columns[section]
                dtype = self.dtypes[section]
                return f"{col}\n{dtype}"
            if orientation == QtCore.Qt.Orientation.Vertical:
                return str(section)
        # if role == QtCore.Qt.ItemDataRole.SizeHintRole:
        #     if orientation == QtCore.Qt.Orientation.Horizontal:
        #         return QtCore.QSize(400, 45)

class Table(QtWidgets.QTableView):
    def __init__(self, parent:'DfView'=None):
        self.p = parent
        self.start_mouse_cell = None
        super(Table, self).__init__(parent)


    def mousePressEvent(self, a0: QtGui.QMouseEvent) -> None:
        self.start_mouse_cell = self.indexAt(a0.pos())
        print("mouse press", self.start_mouse_cell.row(), self.start_mouse_cell.column())
        return super().mousePressEvent(a0)

    def mouseReleaseEvent(self, a0: QtGui.QMouseEvent) -> None:
        self.start_mouse_cell = None
        return super().mouseReleaseEvent(a0)

#MARK: TableKeys
    def keyPressEvent(self, a0: QtGui.QKeyEvent | None) -> None:

        shift = QtWidgets.QApplication.keyboardModifiers() == QtCore.Qt.KeyboardModifier.ShiftModifier
        ctrl = QtWidgets.QApplication.keyboardModifiers() == QtCore.Qt.KeyboardModifier.ControlModifier
        ctrlShift = QtWidgets.QApplication.keyboardModifiers() == QtCore.Qt.KeyboardModifier.ControlModifier | QtCore.Qt.KeyboardModifier.ShiftModifier
        model = self.model()
        selModel = self.selectionModel()
        curIdx = selModel.currentIndex()
        clrs = QtCore.QItemSelectionModel.SelectionFlag.ClearAndSelect


        # print('keypress',a0.key(), shift, ctrl, ctrlShift, QtWidgets.QApplication.keyboardModifiers())

        if a0.key() == QtCore.Qt.Key.Key_Slash:
            self.p.search.setFocus()
            return

        if (ctrlShift or ctrl) and a0.key() == QtCore.Qt.Key.Key_Right:
            selModel.setCurrentIndex(curIdx.siblingAtColumn(self.model().columnCount() - 1), clrs)
            return
        
        if (ctrlShift or ctrl) and a0.key() == QtCore.Qt.Key.Key_Left:
            selModel.setCurrentIndex(curIdx.siblingAtColumn(0), clrs)
            return
        
        if (ctrlShift or ctrl) and a0.key() == QtCore.Qt.Key.Key_Up:
            selModel.setCurrentIndex(curIdx.siblingAtRow(0), clrs)
            return
        
        if (ctrlShift or ctrl) and a0.key() == QtCore.Qt.Key.Key_Down:
            selModel.setCurrentIndex(curIdx.siblingAtRow(self.model().rowCount() - 1), clrs)
            return

        return super().keyPressEvent(a0)



class TableWorker(QtCore.QObject):
    # calcWidths = QtCore.Signal(pl.DataFrame,QFontMetrics)
    provideDf = QtCore.Signal(pl.DataFrame)
    provideWidths = QtCore.Signal(list, list)

    def __init__(self, df: pl.DataFrame = None):
        super(TableWorker, self).__init__()
        self.widthsTimer = None
        self.org_df = df
        self.search_requets = []

    def set_up_timers(self):
        self.widthsTimer = QtCore.QTimer()
        self.widthsTimer.setInterval(100)
        self.widthsTimer.setSingleShot(True)
        self.widthsTimer.timeout.connect(self.calc_widths_2)
        self.searchTimer = QtCore.QTimer()
        self.searchTimer.setInterval(100)
        self.searchTimer.setSingleShot(True)
        self.searchTimer.timeout.connect(self.filter_df2)

    @QtCore.Slot(pl.DataFrame)
    def set_df(self, df: pl.DataFrame):
        self.org_df = df

    @QtCore.Slot(str)
    def filter_df(self, search: str):
        self.search_requets.append(search)
        if not self.searchTimer:
            self.set_up_timers()

        self.searchTimer.start()

    def filter_df2(self):
        search_str: str = self.search_requets.pop()
        self.search_requets = []
        terms = search_str.split()

        filters = []
        if not terms:
            self.provideDf.emit(self.org_df)
            return

        if len(terms) == 0:
            self.provideDf.emit(self.org_df)
            return

        df = self.org_df
        current_thread = QtCore.QThread.currentThread()

        str_cols = [
            c
            for i, c in enumerate(df.columns)
            # if not df.dtypes[i].is_numeric() or df.dtypes[i].is_temporal()
        ]
        for term in terms:
            filters.append(
                reduce(
                    lambda x, y: x | y,
                    (
                        pl.col(c).cast(pl.String).str.contains(rf"(?i){term}")
                        for c in str_cols
                    ),
                )
            )
        df = df.filter(*filters)
        self.provideDf.emit(df)

    @QtCore.Slot(pl.DataFrame, QFont)
    def calc_widths(self, df: pl.DataFrame, font: QFont):
        self.fm = QFontMetrics(font)
        self.df = df
        if df.shape[0] > 100_000:
            self.calc_widths_2(df[:3000], self.fm, clear=False)
        if not self.widthsTimer:
            self.set_up_timers()
        self.widthsTimer.start()

    def calc_widths_2(self, df=None, fm=None, clear=True):
        if df is None:
            df = self.df
        if fm is None:
            fm = self.fm
        if df.shape[0] == 0:
            return
        widths, fmts = get_col_fmts_and_widths(fm, df)
        self.provideWidths.emit(widths, fmts)
        if clear:
            self.fm = None
            self.df = None


class DfView(QtWidgets.QWidget):

    setWorkerDf = QtCore.Signal(pl.DataFrame)
    calcWidths = QtCore.Signal(pl.DataFrame, QFont)
    searchDf = QtCore.Signal(str)

    def __init__(self, parent=None):
        super(DfView, self).__init__(parent)
        self.setup_table()
        self.col_widths = []
        self.search = QtWidgets.QLineEdit()
        self.search.setPlaceholderText("Search...")
        self.search.textChanged.connect(self.search_df)
        self._layout = QtWidgets.QVBoxLayout()
        self._layout.addWidget(self.search)
        self._layout.addWidget(self.table)

        self.bottom_layout = QtWidgets.QHBoxLayout()
        self._layout.addLayout(self.bottom_layout)

        self.bottom_left_status = QtWidgets.QLabel('R')
        self.bottom_right_status = QtWidgets.QLabel('L')
        self.bottom_right_status.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight)

        self.bottom_layout.addWidget(self.bottom_left_status)
        self.bottom_layout.addWidget(self.bottom_right_status)


        self.setLayout(self._layout)
        self.set_shortcuts()
        self.set_font_size(self.font_size)
        self.set_up_worker()

    def set_up_worker(self):
        self.worker = TableWorker(pl.DataFrame())

        self.worker.provideWidths.connect(self.set_col_widths)
        self.worker.provideDf.connect(self.set_filter_sort_df)

        self.calcWidths.connect(self.worker.calc_widths)
        self.searchDf.connect(self.worker.filter_df)
        self.setWorkerDf.connect(self.worker.set_df)

        self.workerThread = QtCore.QThread()
        self.worker.moveToThread(self.workerThread)
        self.workerThread.start()

    def search_df(self):
        txt = self.search.text()
        self.searchDf.emit(txt)

    def set_df(self, df: pl.DataFrame):
        self._model.set_df(df)
        self.total_rows = df.shape[0]
        self.setWorkerDf.emit(df)
        self.update_col_widths()
        self.update_bottom_status()

    @QtCore.Slot(pl.DataFrame)
    def set_filter_sort_df(self, df: pl.DataFrame):
        self._model.df = df
        self._model.layoutChanged.emit()
        self.update_col_widths()
        self.update_bottom_status()

    def update_col_widths(self):
        print("update col widths")
        self.calcWidths.emit(self._model.df, self.table.font())

    def update_bottom_status(self):
        total_rows = self.total_rows
        rows_in_filter = self._model.df.shape[0]
        self.bottom_left_status.setText(f"showing {rows_in_filter} of {total_rows}")

        if self._model.start_row is None:
            self.bottom_right_status.setText("")
            return
        rows_selected = self._model.end_row - self._model.start_row + 1
        cols_selected = self._model.end_col - self._model.start_col + 1
        count = rows_selected * cols_selected
        
        sel_info = self.selection_calcs()
        sel_sum = sel_info["sel_sum"]
        sel_avg = sel_sum / count
        sel_nulls = sel_info["sel_nulls"]
        char_count = sel_info["char_count"]
        self.bottom_right_status.setText(f"char: {char_count}    {rows_selected:,}r x {cols_selected:,}c   count: {count:,}   null_count:{sel_nulls:,}   avg: {sel_avg:,.2f}   sum: {sel_sum:,.2f}")

    def selection_calcs(self):
        sel_sum = 0
        sel_nulls = 0
        char_count = 0
        for i in range(self._model.start_col, self._model.end_col + 1):
            colname = self._model.columns[i] 
            c = self._model.df[colname][self._model.start_row:self._model.end_row + 1]
            sel_nulls += c.is_null().sum()
            if c.dtype.is_numeric():
                sel_sum += c.sum()

            if c.dtype in [pl.String, pl.Categorical]:
                char_count += c.cast(pl.String).str.len_chars().sum()
        
        return {
            "sel_sum": sel_sum,
            "sel_nulls": sel_nulls,
            "char_count": char_count
        }


    

    def set_font_size(self, size_int: int):
        font = QtGui.QFont("Calibri", size_int)
        # font = QtGui.QFont('Arial', size_int)
        self.font_metrics = QtGui.QFontMetrics(font)
        self.font_width = self.font_metrics.width("A")
        self.table.setFont(font)
        self.search.setFont(font)
        self.bottom_left_status.setFont(font)
        self.bottom_right_status.setFont(font)
        self.table.verticalHeader().setDefaultSectionSize(
            self.font_metrics.height() + 2
        )

    @QtCore.Slot(list, list)
    def set_col_widths(self, widths, fmts):
        self._model.formats = fmts
        self.col_widths = widths
        print(widths)
        for i, w in enumerate(self.col_widths):
            self.table.setColumnWidth(i, int(w * 1.1 + 20))

    def increase_font_size(self):
        self.font_size += 1
        self.set_font_size(self.font_size)
        self.update_col_widths()

    def decrease_font_size(self):
        self.font_size -= 1
        self.set_font_size(self.font_size)
        self.update_col_widths()


    def set_shortcuts(self):
        self.shortcuts = [
            Shortcut("Ctrl+=", self, self.increase_font_size),
            Shortcut("Ctrl+-", self, self.decrease_font_size),
            Shortcut("Ctrl+c", self, lambda: print("copy")),
            Shortcut("Escape", self, self.search.clear),
        ]



    def setup_table(self):
        self._model = TableModel(pl.DataFrame(), self)
        # self.table = QtWidgets.QTableView()
        self.table = Table(self)
        self.style_delegate = CustomDelegate()
        self.table.setItemDelegate(self.style_delegate)
        self.table.setModel(self._model)
        self.font_size = 12
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(
            QtWidgets.QAbstractItemView.SelectionBehavior.SelectItems
        )
        self.table.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection
        )
        self.table.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self.table.verticalHeader().ResizeMode(QtWidgets.QHeaderView.ResizeMode.Fixed)
        self.table.setWordWrap(False)


        # self.table.selectionModel().selectionChanged.connect(self.selection_changed)
        self.table.selectionModel().currentChanged.connect(self.current_changed)
        self.table.clicked.connect(self.click_info)

    def click_info(self, index):
        shift = (
            QtWidgets.QApplication.keyboardModifiers() == QtCore.Qt.KeyboardModifier.ShiftModifier 
            or QtWidgets.QApplication.keyboardModifiers() == QtCore.Qt.KeyboardModifier.ControlModifier | QtCore.Qt.KeyboardModifier.ShiftModifier
        )

        print('click', index.row(), index.column(), shift)
        self._model.set_active_cell(index, shift)


        # self.table.setSelectionModel(None)
    def current_changed(self, index):
        print("current changed", index.row(), index.column())
        shift = (
            QtWidgets.QApplication.keyboardModifiers() == QtCore.Qt.KeyboardModifier.ShiftModifier 
            or QtWidgets.QApplication.keyboardModifiers() == QtCore.Qt.KeyboardModifier.ControlModifier | QtCore.Qt.KeyboardModifier.ShiftModifier
        )
        if self.table.start_mouse_cell and index != self.table.start_mouse_cell:
            shift = True

        self._model.set_active_cell(index, shift)
        self.update_bottom_status()
        # print("current changed", index.row(), index.column(), shift)





    def table_sel_changed(self):
        pass
        # sel = self.table.selectionModel().selectedIndexes()
        
        # print([(s.row(), s.column() ) for s in sel])
        # print(f'{sel[0].row()}, {sel[0].column()} - {sel[-1].row()}, {sel[-1].column()}')