"""Tests for how completion items insert text: quoted names and function call snippets."""
import pytest
from lsprotocol.types import InsertTextFormat

from conftest import complete, split_cursor
from dabbler.lsp.completer import word_span


@pytest.mark.parametrize(
    "sql, expected",
    [
        pytest.param('select * from "Order Li|', ['"Order Lines"'], id="open_quote"),
        pytest.param('select * from "Order Li|"', ['"Order Lines"'], id="closed_quote"),
        pytest.param('select o."order d| from orders o', ['"order date"'], id="qualified"),
        pytest.param('select "am| from orders', ["amount"], id="unquoted_name_typed_with_quote"),
    ],
)
def test_half_typed_quoted_names(completer, sql, expected):
    assert complete(completer, sql) == expected


def test_column_names_are_quoted_when_duckdb_needs_it(completer):
    assert complete(completer, "select t.| from odd_names t") == [
        '"a.b"', '"1st"', '"x""y"', "\"a'b\"", '"a(b)"', '"$x"', "x$", "café", "Mixed",
    ]


def test_quoted_names_from_the_sql_are_requoted_once(completer):
    sql = 'with c as (select 1 as "x""y", 2 as "a.b", 3 as "Mixed") select c.| from c'
    assert complete(completer, sql) == ['"x""y"', '"a.b"', "Mixed"]


@pytest.mark.parametrize(
    "sql, span",
    [
        pytest.param("select amo|", "amo", id="word"),
        pytest.param("select amo|unt from t", "amount", id="inside_word"),
        pytest.param('select "Order Li|', '"Order Li', id="open_quote"),
        pytest.param('select "Order Li|" from t', '"Order Li"', id="closing_quote"),
        pytest.param('select "a" , "b|', '"b', id="after_other_quoted_name"),
    ],
)
def test_word_span(sql, span):
    text, pos = split_cursor(sql)
    start, end = word_span(text, pos)
    assert text[start:end] == span


def items_at(completer, sql_with_cursor, snippets=True):
    sql, pos = split_cursor(sql_with_cursor)
    prev = sql[pos - 1] if pos else ""
    result = completer.route_completion2(pos, sql, prev if prev in " ." else None, snippets=snippets)
    return {x.label: x for x in result.items} if result else {}


def test_functions_insert_a_call_snippet(completer):
    item = items_at(completer, "select date_tr|")["date_trunc"]
    assert (item.insert_text, item.insert_text_format) == ("date_trunc($0)", InsertTextFormat.Snippet)


def test_table_functions_insert_a_call_snippet(completer):
    assert items_at(completer, "select * from read_cs|")["read_csv"].insert_text == "read_csv($0)"


def test_no_snippet_when_a_paren_follows(completer):
    assert items_at(completer, "select date_tr|('day', x)")["date_trunc"].insert_text is None


def test_no_snippet_without_client_support(completer):
    assert items_at(completer, "select date_tr|", snippets=False)["date_trunc"].insert_text is None


def test_tables_and_columns_have_no_snippet(completer):
    items = items_at(completer, "select * from ord|")
    assert items["orders"].insert_text is None
