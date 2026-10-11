"""Tests for finding the SQL string around the cursor in a python file (get_sql2)."""
import pytest

from conftest import split_cursor
from dabbler.lsp.sql_utils import get_sql2, line_col, strip_sql_whitespace


def extract(source_with_cursor: str):
    source, idx = split_cursor(source_with_cursor)
    before = source[:idx]
    line = before.count("\n")
    col = idx - (before.rfind("\n") + 1)
    rng = get_sql2(source, line, col)
    return source, idx, rng


@pytest.mark.parametrize(
    "source, expected_sql",
    [
        pytest.param('q = """--sql\nselect * from |orders\n"""\n', "select * from orders\n", id="triple_quote_block"),
        pytest.param('db.sql("""--sql\n    select |1\n    """)', "    select 1\n    ", id="triple_quote_in_call"),
        pytest.param('q = f"""--sql\nselect {x} from |t\n"""', "select {x} from t\n", id="f_string_block"),
        pytest.param('q = """--SQL\nselect |1\n"""', "select 1\n", id="uppercase_marker"),
        pytest.param('db.sql("select * from |orders")', "select * from orders", id="sql_call_double_quotes"),
        pytest.param("db.sql('select * from |orders')", "select * from orders", id="sql_call_single_quotes"),
        pytest.param("db.execute('select * from t where a = ?|', [1])", "select * from t where a = ?", id="execute_with_params"),
        pytest.param("cur.executemany(\"insert into t values (?|)\", rows)", "insert into t values (?)", id="executemany"),
        pytest.param(
            'a = """--sql\nselect 1\n"""\nb = """--sql\nselect * from |second\n"""',
            "select * from second\n", id="second_of_two_blocks",
        ),
        pytest.param('q = """--sql\r\nselect * from |orders\r\n"""\r\n', "select * from orders\r\n", id="crlf_line_endings"),
    ],
)
def test_finds_sql_around_cursor(source, expected_sql):
    source, idx, rng = extract(source)
    assert rng is not None
    assert rng.txt == expected_sql
    assert rng.cur_idx == idx
    assert source[rng.start:rng.end] == rng.txt
    assert rng.start <= idx <= rng.end


@pytest.mark.parametrize(
    "source",
    [
        pytest.param('x = 1|\nq = """--sql\nselect 1\n"""', id="before_block"),
        pytest.param('q = """--sql\nselect 1\n"""\nx = 1|', id="after_block"),
        pytest.param('q = """not sql |at all"""', id="plain_triple_quote_string"),
        pytest.param('print("select * from |orders")', id="string_not_in_sql_call"),
        pytest.param('q = """--sql|\nselect 1\n"""', id="on_marker_line"),
        pytest.param('q = """--sql\n|  \n"""', id="empty_block"),
    ],
)
def test_no_sql_outside_sql_strings(source):
    _, _, rng = extract(source)
    assert rng is None


def test_cursor_offset_within_range_maps_back_to_source():
    source, idx, rng = extract('import duckdb\n\nq = """--sql\nselect a,\n  b from |tbl\n"""\n')
    assert rng is not None
    pos_in_range = rng.cur_idx - rng.start
    assert rng.txt[pos_in_range:].startswith("tbl")
    assert line_col(source, idx) == (5, 10)


def test_strip_sql_whitespace_collapses_and_drops_marker():
    assert strip_sql_whitespace("--sql\nselect  *\n\tfrom   t") == "select * from t"
