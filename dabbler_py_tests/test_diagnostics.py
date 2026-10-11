"""Tests for syntax errors in SQL strings, found with DuckDB's own parser."""
import pytest

from dabbler.lsp.diagnostics import check_sql, sql_errors


@pytest.mark.parametrize(
    "sql",
    [
        "select list_transform([1, 2], x -> x + 1)",
        "delete from t where x = 1",
        "pivot t on year using sum(amount)",
        "select * from t window w as (order by a) qualify row_number() over w = 1",
        "select * from t where a = ? and b = ?",
        "-- only a comment",
        "select 1; select 2;",
    ],
)
def test_valid_sql(sql):
    assert check_sql(sql) is None


def test_error_points_at_the_token():
    sql = "select * fro t"
    start, end, message = check_sql(sql)  # type: ignore
    assert sql[start:end] == "t"
    assert "syntax error" in message


def test_error_at_end_of_input_marks_the_last_character():
    sql = "select * from t where  \n"
    start, end, _ = check_sql(sql)  # type: ignore
    assert sql[start:end] == "e"


def test_error_position_after_non_ascii_text():
    sql = "select 'é€😀' as x fro t"
    start, end, _ = check_sql(sql)  # type: ignore
    assert sql[start:end] == "fro"


def test_errors_map_to_file_offsets():
    source = 'a = 1\ndb.sql("select 1\\nfrom t wher x")\nq = f"select {x} fro t"\n'
    (error,) = sql_errors(source)  # the f-string with a field isn't checked
    assert source[error.start:error.end] == "x"
