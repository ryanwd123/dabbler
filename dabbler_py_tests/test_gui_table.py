"""The result table: searching, sorting, and answers from the worker thread arriving late."""
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import time
from datetime import date

import polars as pl
import pytest

from dabbler.gui_qt import QtCore, QtWidgets
from dabbler.gui_table import DfView, TableWorker


@pytest.fixture(scope="module")
def qapp():
    return QtWidgets.QApplication.instance() or QtWidgets.QApplication([])


DF = pl.DataFrame({
    "n": [1, 2, 3, 4, 5],
    "fruit": ["apple", "banana", "cherry", "date", None],
    "color": pl.Series(["red", "yellow", "red", "brown", "purple"], dtype=pl.Categorical),
    "day": [date(2023, 1, d) for d in range(1, 6)],
    "tags": [[1], [2], [3], [4], [5]],
})


def search(term: str, df: pl.DataFrame = DF) -> pl.DataFrame:
    worker = TableWorker(df)
    found = []
    worker.provideDf.connect(lambda generation, result: found.append(result))
    worker.search_requests = [term]
    worker.filter_df2()
    return found[0]


@pytest.mark.parametrize(
    "term, expected",
    [
        ("an", [2]),  # banana
        ("RED", [1, 3]),  # case-insensitive, categorical column
        ("-red", [2, 4, 5]),  # exclude
        ("app ch", []),  # every term must match somewhere
        (">3", [4, 5]),
        ("<2", [1]),
        ("", [1, 2, 3, 4, 5]),
        ("a(b", []),  # regex characters are taken literally
    ],
)
def test_search(qapp, term, expected):
    assert search(term)["n"].to_list() == expected


def test_search_before_any_result_does_not_fail(qapp):
    worker = TableWorker(pl.DataFrame())
    worker.filter_df("x")  # used to raise AttributeError: no searchTimer yet
    assert worker.searchTimer is not None


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while not predicate() and time.time() < deadline:
        QtWidgets.QApplication.processEvents()
        time.sleep(0.01)
    return predicate()


@pytest.fixture
def view(qapp):
    v = DfView()
    yield v
    v.stop_worker()
    delete_now(v)


def delete_now(widget):
    """Deletes a widget on the GUI thread. Left to Python's garbage collector, it can be
    freed from whatever thread triggers a collection, which aborts the process on Linux."""
    widget.deleteLater()
    QtWidgets.QApplication.sendPostedEvents(None, QtCore.QEvent.Type.DeferredDelete)


def test_search_keeps_the_sort(view):
    view.set_df(DF, ["BIGINT", "VARCHAR", "ENUM", "DATE", "BIGINT[]"])
    view.sort_df(0)  # ascending
    view.sort_df(0)  # descending
    view.search.setText("r")  # rows 1, 3, 4 and 5 (red, cherry, brown, purple)
    assert wait_for(lambda: view._model.df.height < 5)
    numbers = view._model.df["n"].to_list()
    assert numbers == sorted(numbers, reverse=True)


def test_late_answers_for_an_earlier_result_are_ignored(view):
    view.set_df(DF, ["BIGINT", "VARCHAR", "ENUM", "DATE", "BIGINT[]"])
    old_generation = view.generation
    small = pl.DataFrame({"only": [1]})
    view.set_df(small, ["BIGINT"])
    view.set_filter_sort_df(old_generation, DF)  # a search on the earlier result
    view.set_col_widths(old_generation, [10, 20, 30, 40, 50], ["", "", "", "", ""])
    assert view._model.df.columns == ["only"]
    assert wait_for(lambda: view.table.columnWidth(0) > 0)
