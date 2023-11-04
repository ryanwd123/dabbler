from pygments import highlight as _highlight
from pygments.lexers import SqlLexer
from pygments.formatters import HtmlFormatter


dataframe_types = {
    "pandas.core.frame.DataFrame":'pandas',
    "polars.dataframe.frame.DataFrame":'polars',
    "pyarrow.lib.Table":'arrow',
    "duckdb.DuckDBPyRelation":'duckdb_rel',
    # "duckdb.duckdb.DuckDBPyRelation":'duckdb_rel',
}

def check_dataframe_type(type_str:str):
    for k,v in dataframe_types.items():
        if k in type_str:
            return v
    


gui_style = """
QLabel {
	font-family: Consolas;
	font-size: 10pt;
}


QTreeWidget {
	font-family: Consolas;
	font-size: 10pt;
}
QTextEdit {
	font-family: Consolas;
	font-size: 10pt;
}
QListView {
	font-family: Consolas;
	font-size: 10pt;
}

QTableView {
	font-family: Consolas;
	font-size: 10pt;
}

QHeaderView{
	font-family: Consolas;
	border:0px;
	font-size: 10pt;
    font-weight: bold;
}

QTableView::item {
	padding: 0px;
}"""

format_map = {
        'STRING':'',
        'INT':',.0f',
        'NUMBER':',.2f',
        'DATE':'%x',
        'DATETIME':'%x',
        'TIME':'%X',
    }

fmt_types = {
    'BIGINT': 'INT',
    'BIT': 'STRING',
    'BOOLEAN': 'STRING',
    'BLOB': 'STRING',
    'DATE': 'DATE',
    'DOUBLE': 'NUMBER',
    'DECIMAL(s, p)': 'NUMBER',
    'HUGEINT': 'INT',
    'INTEGER': 'INT',
    'INTERVAL': 'DATE',
    'REAL': 'NUMBER',
    'SMALLINT': 'INT',
    'TIME': 'TIME',
    'TIMESTAMP': 'DATETIME',
    'TIMESTAMP WITH TIME ZONE': 'DATETIME',
    'TINYINT': 'INT',
    'UBIGINT': 'INT',
    'UINTEGER': 'INT',
    'USMALLINT': 'INT',
    'UTINYINT': 'INT',
    'UUID': 'STRING',
    'VARCHAR': 'STRING'
}


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
 

def apply_fmt(x,c,fmts):
    if x is None:
        return ''
    return f'{x:{fmts[c]}}'
