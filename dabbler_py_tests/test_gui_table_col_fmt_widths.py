import pytest
import logging

from regex import P
from dabbler.gui_table import get_col_width, pl, QFont, QFontMetrics, QtWidgets

app = QtWidgets.QApplication([])

@pytest.fixture
def font_metrics():
    font = QFont("Arial", 10)
    return QFontMetrics(font)

def test_numeric_column(font_metrics):
    df = pl.DataFrame({"numbers": [1, 22, 333, 4444, 55555]})
    width = get_col_width(font_metrics, "numbers", df, pl.Int64, ",.0f")
    assert width > font_metrics.horizontalAdvance("numbers")
    assert width > font_metrics.horizontalAdvance("55555")

def test_temporal_column(font_metrics):
    df = pl.DataFrame({"dates": ["2023-01-01", "2023-12-31"]}).with_columns([pl.col("dates").cast(pl.Date)])
    width = get_col_width(font_metrics, "dates", df, pl.Date, "%Y-%m-%d")
    assert width > font_metrics.horizontalAdvance("dates")
    assert width > font_metrics.horizontalAdvance("2023-12-31")

def test_string_column(font_metrics):
    df = pl.DataFrame({"strings": ["short", "medium length", "very long string here"]})
    width = get_col_width(font_metrics, "strings", df, pl.String, "")
    assert width > font_metrics.horizontalAdvance("strings")
    assert width > font_metrics.horizontalAdvance("very long string here")

def test_list_column(font_metrics):
    df = pl.DataFrame({"lists": [[1, 2], [3, 4, 5], [6]]})
    width = get_col_width(font_metrics, "lists", df, pl.List, "")
    assert width == 200  # Default width for list columns

def test_struct_column(font_metrics):
    df = pl.DataFrame({"structs": [{"a": 1}, {"b": 2}]})
    width = get_col_width(font_metrics, "structs", df, pl.Struct, "")
    assert width == 200  # Default width for struct columns

def test_large_dataframe(font_metrics):
    df = pl.DataFrame({"long_strings": ["apple \n" * i for i in range(1, 10002)]})
    width = get_col_width(font_metrics, "long_strings", df, pl.String, "")
    assert width <= 600  # Max width for string columns

def test_constant_column(font_metrics):
    df = pl.DataFrame({"constant": ["same"] * 1000})
    width = get_col_width(font_metrics, "constant", df, pl.String, "")
    assert width > font_metrics.horizontalAdvance("constant")
    assert width > font_metrics.horizontalAdvance("same")

def test_empty_dataframe(font_metrics):
    df = pl.DataFrame({"empty": []})
    width = get_col_width(font_metrics, "empty", df, pl.Float64, ",.2f")
    assert width > font_metrics.horizontalAdvance("empty")

def test_single_value_dataframe(font_metrics):
    df = pl.DataFrame({"single": [42]})
    width = get_col_width(font_metrics, "single", df, pl.Int64, ",.0f")
    assert width > font_metrics.horizontalAdvance("single")
    assert width > font_metrics.horizontalAdvance("42")