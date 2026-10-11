"""Tests for signature help: the function call around the cursor and its parameters."""
import pytest

from conftest import split_cursor
from dabbler.lsp.signature_help import enclosing_call

KNOWN = {"date_trunc", "coalesce", "list_transform", "v.add", "read_csv", "sum"}


def call_at(sql_with_cursor: str):
    sql, pos = split_cursor(sql_with_cursor)
    return enclosing_call(sql, pos, lambda name: name.lower() in KNOWN)


@pytest.mark.parametrize(
    "sql, expected",
    [
        pytest.param("select date_trunc(|", ("date_trunc", 0), id="first_argument"),
        pytest.param("select date_trunc('day', |", ("date_trunc", 1), id="second_argument"),
        pytest.param("select date_trunc('a, b', |", ("date_trunc", 1), id="comma_in_string"),
        pytest.param("select coalesce(sum(x), |", ("coalesce", 1), id="after_nested_call"),
        pytest.param("select coalesce(sum(x|", ("sum", 0), id="inside_nested_call"),
        pytest.param("select coalesce((a + |", ("coalesce", 0), id="grouping_paren"),
        pytest.param("select list_transform([1, 2, 3], |", ("list_transform", 1), id="comma_in_list"),
        pytest.param("select v.add(1, |", ("v.add", 1), id="qualified_name"),
        pytest.param('select "date_trunc"(|', ("date_trunc", 0), id="quoted_name"),
        pytest.param("select date_trunc(\n -- a, b\n 'x', |", ("date_trunc", 1), id="comma_in_comment"),
        pytest.param("select * from read_csv('x.csv', header = true, |", ("read_csv", 2), id="table_function"),
    ],
)
def test_finds_the_call(sql, expected):
    call = call_at(sql)
    assert call is not None
    assert (call.name, call.arg_index) == expected


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("select date_trunc('day', x) |", id="after_the_call"),
        pytest.param("select * from t where a in (1, |", id="in_list"),
        pytest.param("select unknown_fn(|", id="unknown_function"),
        pytest.param("select (|", id="bare_paren"),
    ],
)
def test_no_call(sql):
    assert call_at(sql) is None


def test_inside_a_string_argument_still_has_the_call():
    call = call_at("select date_trunc('da|")
    assert call is not None and call.name == "date_trunc"


def help_at(completer, sql_with_cursor):
    sql, pos = split_cursor(sql_with_cursor)
    return completer.signature_help(pos, sql)


def test_builtin_signature_help(completer):
    help = help_at(completer, "select date_trunc('day', |) from orders")
    assert help is not None
    sig = help.signatures[help.active_signature]
    assert sig.label.startswith("date_trunc(part VARCHAR, timestamp ")
    start, end = sig.parameters[help.active_parameter].label
    assert sig.label[start:end].startswith("timestamp")


def test_user_macro_signature_help(completer):
    help = help_at(completer, "select addition(1, |")
    assert help is not None
    assert [s.label for s in help.signatures] == ["addition(a, b)"]
    assert help.active_parameter == 1


def test_varargs_stay_on_the_last_parameter(completer):
    help = help_at(completer, "select concat('a', 'b', 'c', |")
    sig = help.signatures[help.active_signature]
    assert help.active_parameter == len(sig.parameters) - 1


def test_no_help_outside_calls(completer):
    assert help_at(completer, "select a, | from orders") is None
