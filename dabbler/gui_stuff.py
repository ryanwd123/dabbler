from typing import Union

from pygments import highlight as _highlight
from pygments.lexers import SqlLexer
from pygments.formatters import HtmlFormatter


# (top-level package, class name) -> kind; type paths move between versions
# (pandas 3 is pandas.DataFrame, DuckDB 1.5 is _duckdb.DuckDBPyRelation)
dataframe_types = {
    ("pandas", "DataFrame"): "pandas",
    ("polars", "DataFrame"): "polars",
    ("pyarrow", "Table"): "arrow",
    ("duckdb", "DuckDBPyRelation"): "duckdb_rel",
    ("_duckdb", "DuckDBPyRelation"): "duckdb_rel",
}


def dataframe_kind(value) -> Union[str, None]:
    """'pandas', 'polars', 'arrow' or 'duckdb_rel' for things a query can read, else None."""
    cls = type(value)
    return dataframe_types.get((cls.__module__.split(".")[0], cls.__name__))


def dataframes_in(ipython) -> dict[str, str]:
    """DataFrames, arrow tables and relations in the IPython namespace, with their kind."""
    if ipython is None:
        return {}
    found = {}
    for name, value in list(ipython.user_ns.items()):
        if name.startswith("_"):
            continue
        kind = dataframe_kind(value)
        if kind:
            found[name] = kind
    return found



gui_style = """
QLabel {
	font-family: Calibri, Arial, sans-serif;
	font-size: 12pt;
}

QLineEdit {
	font-family: Calibri, Arial, sans-serif;
	font-size: 12pt;
}


QTreeWidget {
	font-family: Calibri, Arial, sans-serif;
	font-size: 11pt;
}
QTextEdit {
	font-family: Consolas;
	font-size: 12pt;
}
QListView {
	font-family: Calibri, Arial, sans-serif;
	font-size: 11pt;
}


QHeaderView{
	font-family: Calibri, Arial, sans-serif;
	border:0px;
	font-size: 12pt;
    font-weight: bold;
}"""

def highlight(text):
    # Generated HTML contains unnecessary newline at the end
    # before </pre> closing tag.
    # We need to remove that newline because it's screwing up
    # QTextEdit formatting and is being displayed
    # as a non-editable whitespace.
    highlighted_text = _highlight(text, SqlLexer(), HtmlFormatter()).strip()

    # Split generated HTML by last newline in it
    # argument 1 indicates that we only want to split the string
    # by one specified delimiter from the right.
    parts = highlighted_text.rsplit("\n", 1)

    # Glue back 2 split parts to get the HTML without last
    # unnecessary newline
    highlighted_text_no_last_newline = "".join(parts)
    return highlighted_text_no_last_newline
