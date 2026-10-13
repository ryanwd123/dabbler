import logging
import re
import sys
from dabbler.gui_qt import (
    QtWidgets,
    QtCore,
    QtGui,
    Qt,
    Signal,
    Slot,
    QFont,
    QFontMetrics,
    QKeySequence,
    QShortcut,
)


# from qtpy import QtWidgets, QtCore, QtGui
# from qtpy.QtCore import Qt, Signal, Slot     # type: ignore
# from qtpy.QtGui import QFont, QFontMetrics, QKeySequence, QShortcut
from typing import Union

# from dabbler.gui_compenents import Shortcut
import polars as pl


log = logging.getLogger("dabbler_gui")


def create_shortcut(key, function, parent):
    shortcut = QShortcut(QKeySequence(key), parent)
    shortcut.activated.connect(function)
    return shortcut

def get_col_fmts(c: str, df: pl.DataFrame, dtype: pl.DataType, fm: QFontMetrics):
    try:
        if dtype.is_numeric():
            q = df[c].quantile(0.9)
            b = df[c].quantile(0.1)
            m = df[c].mean()
            if q and q < 0.5:
                return ",.6f"
            if b and abs(b) > 10:
                return ",.0f"
            try:
                if m and abs(m) > 10:   # type: ignore
                    return ",.0f"
            except Exception:
                pass
            return ",.2f"

        if dtype.is_temporal():
            return "%Y-%m-%d"
        return ""
    except Exception:
        return ""

def get_str(val, fmt: str, limit = 200):
    if val is None:
        return ""

    if isinstance(val, pl.Series):
        val = val.to_list()

    if fmt is None:
        fmt = ""

    try:
        result = format(val, fmt)
    except Exception:
        result = str(val)
    if len(result) > limit:
        return result[:limit] + "..."
    return result


def get_col_width(
    fm: QFontMetrics, col: str, df: pl.DataFrame, dtype: pl.DataType, format: str, dtype2:str
):
    try:
        if dtype.is_numeric():
            vals = df[col].unique().top_k(10).to_list()
            return max(
                [fm.horizontalAdvance(str(dtype))]
                + [fm.horizontalAdvance(col)]
                + [fm.horizontalAdvance(dtype2)]
                + [fm.horizontalAdvance(get_str(val, format)) for val in vals]
            ) + 20
        if dtype.is_temporal():
            return max(
                [fm.horizontalAdvance(str(dtype))]
                + [fm.horizontalAdvance(dtype2)]
                + [fm.horizontalAdvance(col)]
                + [fm.horizontalAdvance(get_str(val, format))
                    for val in df[col].unique().top_k(10).to_list()
                ]
            ) + 20
        else:
            if dtype == pl.List:
                try:
                    c = df[col].list.join(", ").cast(pl.String).unique()
                except Exception:
                    return 200
            elif dtype == pl.Struct:
                return 200
            elif not dtype == pl.String:
                c = df[col].unique().cast(pl.String)
            elif dtype == pl.String:
                c = df[col].unique()
            else:
                c = df[col].cast(pl.String).unique()

            c = c.drop_nulls()

            if df.shape[0] > 100:
                length_chars = c.str.len_chars()
                q = length_chars.quantile(0.99)
                m = length_chars.median()
                s = length_chars.std()
                if (q and q > 100) or (m and isinstance(m,int) and m > 100):
                    return 600

                if not s is None and int(s) == 0:    # type: ignore
                    q = None
                if q:
                    if isinstance(m,int):
                        q = max(q,m)
                    c = c.filter(length_chars <= q)
            top = c.str.len_chars().arg_sort(descending=True)[:10].to_list()
            if any(len(c[val]) > 100 for val in top) or len(col) > 100:
                return 600
            w = max(
                [fm.horizontalAdvance(str(dtype.base_type()))]
                + [fm.horizontalAdvance(dtype2)]
                + [fm.horizontalAdvance(col)]
                + [fm.horizontalAdvance(c[val]) for val in top]
            )
            w = min(w, 600)
            return w * 1.15 + 5
    except Exception:
        return 200


def get_col_fmts_and_widths(font: QFontMetrics, df: pl.DataFrame, dtypes: list[str]):
    fm = font
    schema = df.schema
    fmts = [get_col_fmts(c, df, dt, fm) for c, dt in schema.items()]
    widths = [
        get_col_width(fm, c, df, dt, f, dt2)
        for c, dt, f, dt2 in zip(schema.keys(), schema.values(), fmts, dtypes)
    ]
    return widths, fmts


def sort_frame(df: pl.DataFrame, col: str, descending: bool) -> pl.DataFrame:
    if df.schema[col] == pl.Categorical:
        return df.sort(pl.col(col).cast(pl.String), descending=descending, nulls_last=True)
    return df.sort(col, descending=descending, nulls_last=True)


def get_alignment(dtype: pl.DataType):
    if dtype.is_numeric():
        return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
    if dtype.is_temporal():
        return Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
    return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter


def remove_regex_special_chars(term:str):
    return re.sub(r'(?<!\\)[\[\]{}()*+?.,\\^$|#\s]', '', term)


def build_filter(filters: list, term: str, columns: list[str], numeric_columns: Union[list[str], None] = None):
    """Adds the filter for one search term: text anywhere in a row (case-insensitive),
    "-text" for rows without it, ">5" / "<5" for rows with a number above / below."""
    try:
        if term[0] in ['>', '<']:
            try:
                value = float(term[1:])
            except ValueError:
                return filters
            if numeric_columns:
                cols = pl.col(numeric_columns)
                filters.append(pl.any_horizontal(cols > value if term[0] == ">" else cols < value))
            return filters

        if term[0] == "-":
            term = remove_regex_special_chars(term[1:])
            if columns and term:
                text = pl.col(columns).cast(pl.String).fill_null('')
                filters.append(pl.all_horizontal(~text.str.contains(rf"(?i){term}")))
            return filters

        term = remove_regex_special_chars(term)
        if columns and term:
            filters.append(pl.any_horizontal(pl.col(columns).cast(pl.String).str.contains(rf"(?i){term}")))
        return filters
    except Exception:
        return filters


def get_columns_for_filter(df: pl.DataFrame):
    """Columns that text search looks in: all but lists and structs."""
    return [c for c, dt in df.schema.items() if dt.base_type() not in (pl.List, pl.Struct, pl.Array)]


def get_numeric_columns(df: pl.DataFrame):
    return [c for c, dt in df.schema.items() if dt.is_numeric()]


# MARK: TableModel
class TableModel(QtCore.QAbstractTableModel):

    def __init__(self, df: pl.DataFrame, p: "DfView", dtypes:Union[list[str],None] = None):
        super(TableModel, self).__init__()
        # self.reset_selection()
        self.active_cell: Union[tuple[int, int],None] = None
        self.anchor_cell: Union[tuple[int, int],None] = None
        self.start_row: Union[int,None] = None
        self.start_col: Union[int,None] = None
        self.end_row: Union[int,None] = None
        self.end_col: Union[int,None] = None
        self.p = p
        self.set_df(df, dtypes)

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

    def update_start_end_row_col(self):
        if self.active_cell is None or self.anchor_cell is None:
            return
        self.start_row = min(self.active_cell[0], self.anchor_cell[0])
        self.start_col = min(self.active_cell[1], self.anchor_cell[1])
        self.end_row = max(self.active_cell[0], self.anchor_cell[0])
        self.end_col = max(self.active_cell[1], self.anchor_cell[1])
        self.layoutChanged.emit()

    def reset_selection(self):
        self.active_cell = None
        self.anchor_cell = None
        self.start_row = None
        self.start_col = None
        self.end_row = None
        self.end_col = None

    def set_df(self, df: pl.DataFrame, dtypes:Union[list[str],None] = None):
        self.beginResetModel()
        self.df = df
        self.reset_selection()
        self.formats = {i:"" for i,d in enumerate(df.dtypes)}
        self.columns = df.columns
        self.alignments = [get_alignment(d) for d in df.dtypes]

        if dtypes:
            self.dtypes = dtypes
        else:
            self.dtypes = [str(d.base_type()) for d in df.dtypes]

        self.endResetModel()
        self.layoutChanged.emit()

    def rowCount(self, parent=None):
        return self.df.shape[0]

    def columnCount(self, parent=None):
        return self.df.shape[1]

    def data(self, index, role=QtCore.Qt.ItemDataRole.DisplayRole):
        if role == QtCore.Qt.ItemDataRole.DisplayRole:
            row = index.row()
            if 0 <= row < self.rowCount():
                column = index.column()
                if 0 <= column < self.columnCount():
                    value = self.df.item(row, column)
                    fmt = self.formats.get(column, "")
                    # if ((self.df.dtypes[column].base_type() == pl.Struct
                    #     or self.df.dtypes[column].base_type() == pl.List)
                    #     and type(value) == pl.Series):
                    #         value = str(value.to_list())
                    #         if len(value) > 200:
                    #             value = value[:200] + "..."
                    #         return value

                    # if not fmt:
                    #     value = str(value)
                    #     if len(value) > 200:
                    #         value = value[:200] + "..."
                    #     return value
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
            if (
                self.active_cell
                and self.active_cell[0] == row
                and self.active_cell[1] == col
            ):
                return QtGui.QColor("gold")

            if self.start_row is None or self.end_row is None or self.start_col is None or self.end_col is None:
                return

            if row >= self.start_row and row <= self.end_row:
                if col >= self.start_col and col <= self.end_col:
                    return QtGui.QColor("lightblue")

    def headerData(self, section, orientation, role=QtCore.Qt.ItemDataRole.DisplayRole):
        if role == QtCore.Qt.ItemDataRole.DisplayRole:

            if orientation == QtCore.Qt.Orientation.Horizontal:
                col = self.columns[section]
                if self.p.show_dtypes:
                    dtype = self.dtypes[section]
                    return f"{col}\n{dtype}"
                return col
            if orientation == QtCore.Qt.Orientation.Vertical:
                # return str(section)
                return None


class Table(QtWidgets.QTableView):

    key_press_signal = Signal(QtGui.QKeyEvent, int, int)

    def __init__(self, parent: "DfView"):
        super(Table, self).__init__(parent)
        self.p = parent
        self.start_mouse_cell = None
        self.setAlternatingRowColors(True)
        self.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.NoSelection
        )
        self.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers
        )

        self.verticalHeader().setVisible(False)
        self.setWordWrap(False)

    # def eventFilter(self, object, event: QtCore.QEvent):
    #     print(f"eventFilter {event.type()}")
    #     if object == self and event.type() == QtCore.QEvent.Type.KeyPress:

    #         if event.matches(Qt.KeyboardModifier.ControlModifier | Qt.Key.Key_C):
    #             print("Ctrl+C")
    #             return True
    #     return super().eventFilter(object, event)



    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        self.start_mouse_cell = self.indexAt(event.pos())
        return super().mousePressEvent(event)

    def mouseReleaseEvent(self, event: QtGui.QMouseEvent) -> None:
        self.start_mouse_cell = None
        return super().mouseReleaseEvent(event)

    # MARK: TableKeys
    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:


        ctrl = (
            QtWidgets.QApplication.keyboardModifiers()
            == QtCore.Qt.KeyboardModifier.ControlModifier
        )
        ctrlShift = (
            QtWidgets.QApplication.keyboardModifiers()
            == QtCore.Qt.KeyboardModifier.ControlModifier
            | QtCore.Qt.KeyboardModifier.ShiftModifier
        )
        model:TableModel = self.model() # type: ignore
        selModel = self.selectionModel()
        curIdx = selModel.currentIndex()
        clrs = QtCore.QItemSelectionModel.SelectionFlag.ClearAndSelect

        key = event.key()

        if not self.p._model is None and not self.p._model.active_cell is None:
            self.key_press_signal.emit(event, self.p._model.active_cell[0], self.p._model.active_cell[1])


        if key == QtCore.Qt.Key.Key_Comma:
            if model.active_cell:
                col = model.active_cell[1]
                if model.df.dtypes[col].is_numeric():
                    fmt = model.formats.get(col, "")
                    if ',' in fmt:
                        fmt = fmt.replace(',', '')
                    else:
                        fmt = ',' + fmt

                    model.formats[col] = fmt
                    model.layoutChanged.emit()


        if key == QtCore.Qt.Key.Key_Slash:
            self.p.search.setFocus()
            return

        if ctrl and key == QtCore.Qt.Key.Key_C:
            self.p.copy_values2()
            event.accept()
            return

        if ctrlShift and key == QtCore.Qt.Key.Key_C:
            self.p.copy_values2(fmt=True)
            event.accept()
            return


        if (ctrlShift or ctrl) and key == QtCore.Qt.Key.Key_Right:
            selModel.setCurrentIndex(
                curIdx.siblingAtColumn(self.model().columnCount() - 1), clrs
            )
            selModel.clearSelection()
            return

        if (ctrlShift or ctrl) and key == QtCore.Qt.Key.Key_Left:
            selModel.setCurrentIndex(curIdx.siblingAtColumn(0), clrs)
            selModel.clearSelection()
            return

        if (ctrlShift or ctrl) and key == QtCore.Qt.Key.Key_Up:
            selModel.setCurrentIndex(curIdx.siblingAtRow(0), clrs)
            selModel.clearSelection()
            return

        if (ctrlShift or ctrl) and key == QtCore.Qt.Key.Key_Down:
            selModel.setCurrentIndex(
                curIdx.siblingAtRow(self.model().rowCount() - 1), clrs
            )
            selModel.clearSelection()
            return

        return super().keyPressEvent(event)

#MARK: TableWorker
class TableWorker(QtCore.QObject):

    provideDf = Signal(int, pl.DataFrame)
    provideWidths = Signal(int, list, list)

    def __init__(self, df: pl.DataFrame):
        super(TableWorker, self).__init__()
        # made on first use, so they belong to the worker's thread
        self.widthsTimer = None
        self.searchTimer = None
        self.org_df = df
        self.generation = 0
        self.search_requests = []
        self.df = None
        self.fm = None
        self.dtypes = None


    def set_up_timers(self):
        # children of the worker, so they're deleted with it, in the worker's thread
        self.widthsTimer = QtCore.QTimer(self)
        self.widthsTimer.setInterval(100)
        self.widthsTimer.setSingleShot(True)
        self.widthsTimer.timeout.connect(self.calc_widths_2)
        self.searchTimer = QtCore.QTimer(self)
        self.searchTimer.setInterval(100)
        self.searchTimer.setSingleShot(True)
        self.searchTimer.timeout.connect(self.filter_df2)

    @Slot(int, pl.DataFrame)
    def set_df(self, generation: int, df: pl.DataFrame):
        self.generation = generation
        self.org_df = df

    @Slot(str)
    def filter_df(self, search: str):
        self.search_requests.append(search)
        if self.searchTimer is None:
            self.set_up_timers()
        self.searchTimer.start()  # type: ignore

    def filter_df2(self):
        if not self.search_requests:
            return
        search_str: str = self.search_requests.pop()
        self.search_requests = []
        terms_split = search_str.split()
        terms = []
        for term in terms_split:
            if len(term) == 1 and term in ['-','>','<']:
                continue
            terms.append(term)

        filters = []
        if not terms:
            self.provideDf.emit(self.generation, self.org_df)
            return

        df = self.org_df


        col_to_filter = get_columns_for_filter(df)
        numeric_columns = get_numeric_columns(df)
        for term in terms:
            filters = build_filter(filters, term, col_to_filter, numeric_columns)

        if filters:
            try:
                df = df.filter(filters)
            except Exception:
                log.exception(f"search {search_str!r} failed")
                df = self.org_df
        self.provideDf.emit(self.generation, df)

    @Slot(int, pl.DataFrame, QFont, list)
    def calc_widths(self, generation: int, df: pl.DataFrame, font: QFont, dtypes: list[str]):
        self.widths_generation = generation
        self.fm = QFontMetrics(font)
        self.df = df
        self.dtypes = dtypes
        if df.shape[0] > 100_000:
            self.calc_widths_2(df[:3000], self.fm, clear=False, dtypes=dtypes)
        if not self.widthsTimer:
            self.set_up_timers()
        self.widthsTimer.start() # type: ignore

    def calc_widths_2(self, df=None, fm=None, clear=True, dtypes=None):
        if df is None:
            if self.df is None:
                return
            df = self.df
        if fm is None:
            if self.fm is None:
                return
            fm = self.fm
        if dtypes is None:
            if self.dtypes is None:
                return
            dtypes = self.dtypes

        if df.shape[0] == 0:
            return
        widths, fmts = get_col_fmts_and_widths(fm, df, dtypes)
        self.provideWidths.emit(self.widths_generation, widths, fmts)
        if clear:
            self.fm = None
            self.df = None

#MARK: DfView
class DfView(QtWidgets.QWidget):

    setWorkerDf = Signal(int, pl.DataFrame)
    calcWidths = Signal(int, pl.DataFrame, QFont, list)
    searchDf = Signal(str)

    def __init__(self, parent=None, app:Union[QtWidgets.QApplication,None] = None):
        super(DfView, self).__init__(parent)
        self.setup_table()
        self.col_widths = []
        self.sort = {}
        # (column, descending) of the last header click, kept when searching
        self.current_sort: Union[tuple[str, bool], None] = None
        # bumped for each new result, so the worker's answers for an older one are ignored
        self.generation = 0
        self.app = app
        self.show_dtypes = True

        # selection statistics (sum, median, ...) wait until the selection settles
        self.bottom_status_timer = QtCore.QTimer()
        self.bottom_status_timer.setInterval(150)
        self.bottom_status_timer.setSingleShot(True)
        self.bottom_status_timer.timeout.connect(self.update_bottom_status)

        self.search = QtWidgets.QLineEdit()
        self.search.setPlaceholderText("Search Result...")
        self.status_info = QtWidgets.QLabel()
        self.status_info.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight)
        self.status_info.setMinimumWidth(200)
        self.search.textChanged.connect(self.search_df)
        self._layout = QtWidgets.QVBoxLayout()
        self.top_layout = QtWidgets.QHBoxLayout()
        self.top_layout.addWidget(self.search)
        self.top_layout.addWidget(self.status_info)
        self._layout.addLayout(self.top_layout)
        self._layout.setSpacing(5)
        self._layout.setContentsMargins(0, 3, 0, 0)
        self._layout.addWidget(self.table)

        self.clear_status_info = QtCore.QTimer()
        self.clear_status_info.setInterval(5000)
        self.clear_status_info.setSingleShot(True)
        self.clear_status_info.timeout.connect(lambda: self.status_info.setText(""))

        self.bottom_layout = QtWidgets.QHBoxLayout()
        self.bottom_layout.setContentsMargins(4, 0, 4, 0)
        self._layout.addLayout(self.bottom_layout)

        self.bottom_left_status = QtWidgets.QLabel("")
        self.bottom_right_status = QtWidgets.QLabel("")
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

    def stop_worker(self):
        """Ends the worker thread; a QThread destroyed while running aborts the process.

        The worker and its timers belong to that thread and must be deleted there:
        destroyed later from another thread, Qt aborts the process on Linux. A
        deleteLater pending when the thread finishes runs in the thread.
        """
        if self.workerThread.isRunning():
            self.worker.deleteLater()
            self.workerThread.quit()
            self.workerThread.wait(2000)


    def search_df(self):
        txt = self.search.text()
        self.searchDf.emit(txt)

#MARK: set_df
    def set_df(self, df: pl.DataFrame, dtypes=None):
        self.generation += 1
        self.sort = {}
        self.current_sort = None
        new_model = TableModel(df, self, dtypes)
        self.table.setModel(new_model)
        self.table.selectionModel().currentChanged.connect(self.current_changed)
        self._model = new_model
        self.total_rows = df.shape[0]
        self.setWorkerDf.emit(self.generation, df)
        self.update_col_widths()
        self.bottom_status_timer.start()
        if self.search.text():
            self.search_df()

    @Slot(int, pl.DataFrame)
    def set_filter_sort_df(self, generation: int, df: pl.DataFrame):
        if generation != self.generation:
            return  # filtered from an earlier result
        if self.current_sort is not None:
            col, descending = self.current_sort
            try:
                df = sort_frame(df, col, descending)
            except Exception:
                pass
        self._model.df = df
        self._model.reset_selection()
        self._model.layoutChanged.emit()
        # self.update_col_widths()
        self.bottom_status_timer.start()

    def update_col_widths(self):
        self.calcWidths.emit(self.generation, self._model.df, self.table.font(), self._model.dtypes)


    def update_bottom_status(self):
        total_rows = self.total_rows
        rows_in_filter = self._model.df.shape[0]
        self.bottom_left_status.setText(f"showing {rows_in_filter:,} of {total_rows:,}")

        if (self._model.start_row is None
            or self._model.end_row is None
            or self._model.start_col is None
            or self._model.end_col is None):
            self.bottom_right_status.setText("")
            return
        rows_selected = self._model.end_row - self._model.start_row + 1
        cols_selected = self._model.end_col - self._model.start_col + 1
        count = rows_selected * cols_selected

        sel_info = self.selection_calcs()
        sel_nulls = sel_info["sel_nulls"]
        char_count = sel_info["char_count"]
        sep = " | "
        status_txt = f'r{rows_selected:,} x c{cols_selected:,} = {count:,}'
        if sel_info['char_count']:
            status_txt += f'{sep} char count: {char_count:,}'
        if sel_info['sel_nulls']:
            status_txt += f'{sep} nulls: {sel_nulls:,}'
        if sel_info['min_val']:
            status_txt += f'{sep} min: {sel_info["min_val"]:,.2f}'
        if sel_info['max_val']:
            status_txt += f'{sep} max: {sel_info["max_val"]:,.2f}'
        if sel_info['std_dev']:
            status_txt += f'{sep} std dev: {sel_info["std_dev"]:,.2f}'
        if sel_info['q10_pct']:
            status_txt += f'{sep} 10%: {sel_info["q10_pct"]:,.2f}'
        if sel_info['q50_pct']:
            status_txt += f'{sep} 50%: {sel_info["q50_pct"]:,.2f}'
        if sel_info['q90_pct']:
            status_txt += f'{sep} 90%: {sel_info["q90_pct"]:,.2f}'
        if sel_info['median']:
            status_txt += f'{sep} median: {sel_info["median"]:,.2f}'
        if sel_info['mean']:
            status_txt += f'{sep} mean: {sel_info["mean"]:,.2f}'
        if sel_info['sel_sum']:
            # if not sel_info['mean']:
            status_txt += f'{sep} avg: {sel_info["sel_sum"]/count :,.2f}'
            status_txt += f'{sep} sum: {sel_info["sel_sum"]:,.2f}'

        self.bottom_right_status.setText(status_txt)

    def selection_calcs(self):
        sel_sum = 0
        sel_nulls = 0
        char_count = 0
        mean = None
        median = None
        min_val = None
        max_val = None
        std_dev = None
        q50_pct = None
        q90_pct = None
        q10_pct = None

        if not (self._model.start_row is None
            or self._model.end_row is None
            or self._model.start_col is None
            or self._model.end_col is None):

            for i in range(self._model.start_col, self._model.end_col + 1):
                col_count = self._model.end_col - self._model.start_col + 1
                colname = self._model.columns[i]
                c = self._model.df[colname][self._model.start_row : self._model.end_row + 1]
                sel_nulls += c.is_null().sum()
                if c.dtype.is_numeric():
                    sel_sum += c.sum()
                    if col_count == 1:
                        mean = c.mean()
                        median = c.median()
                        min_val = c.min()
                        max_val = c.max()
                        std_dev = c.std()
                        # q50_pct = c.quantile(0.5)
                        # q90_pct = c.quantile(0.9)
                        # q10_pct = c.quantile(0.1)


                if c.dtype in [pl.String, pl.Categorical]:
                    char_count += c.cast(pl.String).str.len_chars().sum()

        return {
            "sel_sum": sel_sum,
            "sel_nulls": sel_nulls,
            "char_count": char_count,
            "mean": mean,
            "median": median,
            "min_val": min_val,
            "max_val": max_val,
            "std_dev": std_dev,
            "q50_pct": q50_pct,
            "q90_pct": q90_pct,
            "q10_pct": q10_pct,
            }

#MARK: set_font_size
    def set_font_size(self, size_int: int):
        font = QtGui.QFont()
        font.setPointSize(size_int)
        # font.setFamilies(["Calibri","Arial"])
        self.font_metrics = QtGui.QFontMetrics(font)
        self.font_width = self.font_metrics.horizontalAdvance("A")
        self.table.setFont(font)
        self.search.setFont(font)
        self.status_info.setFont(font)
        self.bottom_left_status.setFont(font)
        self.bottom_right_status.setFont(font)
        self.table.verticalHeader().setDefaultSectionSize(
            self.font_metrics.height()
        )

    @Slot(int, list, list)
    def set_col_widths(self, generation, widths, fmts):
        if generation != self.generation or len(widths) != self._model.columnCount():
            return  # measured for an earlier result
        self._model.formats = {i: fmts[i] for i in range(len(fmts))}

        self.col_widths = widths
        for i, w in enumerate(self.col_widths):
            self.table.setColumnWidth(i, int(w))
        self._model.layoutChanged.emit()

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
            # create_shortcut("Ctrl+=", self, self.increase_font_size),
            # create_shortcut("Ctrl+-", self, self.decrease_font_size),
            # create_shortcut("Ctrl+c", self.copy_values, self),
            # create_shortcut("Ctrl+Shift+c", lambda:self.copy_values(fmt=True), self),
            create_shortcut("Escape", self.search.clear, self),
        ]

#MARK: CopyValues
    def copy_values2(self, fmt=False):
        if (self._model.start_col is None
            or self._model.end_col is None
            or self._model.start_row is None
            or self._model.end_row is None):
            return
        col_names = [self._model.columns[i] for i in range(self._model.start_col, self._model.end_col + 1)]
        if fmt:
            col_fmts = [self._model.formats[i] for i in range(self._model.start_col, self._model.end_col + 1)]
        else:
            col_fmts = ['' for i in range(self._model.start_col, self._model.end_col + 1)]

        copy_str = ""

        data = self._model.df[col_names][self._model.start_row : self._model.end_row + 1].rows()
        if len(data) > 1 or len(data[0]) > 1:
            copy_str += "\t".join(col_names) + "\n"
        for row in data:
            if fmt:
                copy_str += "\t".join([get_str(c,col_fmts[i],limit= 1000) for i,c in enumerate(row)]) + "\n"
            else:
                copy_str += "\t".join([str(r) for r in row]) + "\n"

        self.set_clipboard(copy_str.rstrip('\n'))

    def set_clipboard(self, text):
        if sys.platform == 'win32':
            try:
                import win32clipboard
                import win32con

                win32clipboard.OpenClipboard()
                win32clipboard.EmptyClipboard()
                win32clipboard.SetClipboardData(win32con.CF_UNICODETEXT, text)
                win32clipboard.CloseClipboard()
                return True
            except ImportError:
                print("pywin32 is not installed. Falling back to Qt clipboard.")
            except Exception as e:
                print(f"Error using win32clipboard: {e}. Falling back to Qt clipboard.")

        clipboard = QtWidgets.QApplication.clipboard()
        clipboard.setText(text)



    def setup_table(self):
        self._model = TableModel(pl.DataFrame(), self)
        self.table = Table(self)
        self.table.setModel(self._model)
        self.font_size = 12


        self.table.horizontalHeader().sectionClicked.connect(self.sort_df)
#         self.table.setStyleSheet("""
# QHeaderView {
# 	font-family: Calibri, Arial;
# 	border:0px;
# 	font-size: 12pt;
# }

#                                  """)
        self.table.clicked.connect(self.click_info)


    def click_info(self, index):
        shift = (
            QtWidgets.QApplication.keyboardModifiers()
            == QtCore.Qt.KeyboardModifier.ShiftModifier
            or QtWidgets.QApplication.keyboardModifiers()
            == QtCore.Qt.KeyboardModifier.ControlModifier
            | QtCore.Qt.KeyboardModifier.ShiftModifier
        )

        self._model.set_active_cell(index, shift)

    def sort_df(self, index:int):
        if index in self.sort:
            self.sort[index] = not self.sort[index]
        else:
            self.sort[index] = False
        col = self._model.columns[index]
        dt = self._model.df.dtypes[index]
        if dt.base_type() == pl.List:
            self.status_info.setText('Cannot sort list columns')
            self.clear_status_info.start()
            return

        try:
            self._model.df = sort_frame(self._model.df, col, self.sort[index])
            self.current_sort = (col, self.sort[index])
        except Exception as e:
            self.status_info.setText(f'{type(e).__module__}.{type(e).__name__}:\n{e}')
            self.clear_status_info.start()

        self._model.layoutChanged.emit()

    def current_changed(self, index):
        shift = (
            QtWidgets.QApplication.keyboardModifiers()
            == QtCore.Qt.KeyboardModifier.ShiftModifier
            or QtWidgets.QApplication.keyboardModifiers()
            == QtCore.Qt.KeyboardModifier.ControlModifier
            | QtCore.Qt.KeyboardModifier.ShiftModifier
        )
        if self.table.start_mouse_cell and index != self.table.start_mouse_cell:
            shift = True

        self._model.set_active_cell(index, shift)
        self.bottom_status_timer.start()

