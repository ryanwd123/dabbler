"""Tests for formatting an SQL string in a python file."""
import pytest

from dabbler.lsp.formatting import FormatError, format_region
from dabbler.lsp.sql_regions import sql_regions


def formatted(source: str) -> str:
    (region,) = sql_regions(source)
    edit = format_region(source, region)
    return source[:edit.start] + edit.new_text + source[edit.end:]


def test_marker_block_at_column_zero_is_indented():
    source = 'q = """--sql\nselect a,b from t where a=1\n"""\n'
    assert formatted(source) == 'q = """--sql\n    SELECT\n        a,\n        b\n    FROM t\n    WHERE\n        a = 1\n"""\n'


def test_keeps_the_existing_indent_and_closing_quote_position():
    source = 'db.sql("""\n        select a from t\n    """)\n'
    assert formatted(source) == 'db.sql("""\n        SELECT\n            a\n        FROM t\n    """)\n'


def test_single_line_string_stays_on_one_line():
    source = 'db.sql("select a,b from t where a=1")\n'
    assert formatted(source) == 'db.sql("SELECT a, b FROM t WHERE a = 1")\n'


def test_duckdb_syntax_is_kept():
    source = 'db.sql("select * exclude (a) from t qualify row_number() over () = 1")\n'
    assert "EXCLUDE (a)" in formatted(source)
    assert "QUALIFY" in formatted(source)


def test_several_statements():
    source = 'q = """--sql\ncreate table x as select 1; select * from x\n"""\n'
    out = formatted(source)
    assert "CREATE TABLE x AS\n" in out
    assert ";\n\n    SELECT\n" in out


@pytest.mark.parametrize(
    "source",
    [
        pytest.param('db.sql(f"select * from {t}")\n', id="f_string_with_fields"),
        pytest.param('db.sql("select \\"a b\\" from t")\n', id="backslash_escapes"),
        pytest.param('db.sql("select from from")\n', id="unparseable"),
    ],
)
def test_refuses_what_it_cant_format_faithfully(source):
    (region,) = sql_regions(source)
    with pytest.raises(FormatError):
        format_region(source, region)
