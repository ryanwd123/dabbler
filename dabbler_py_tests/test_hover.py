"""Tests for hover text over names in SQL."""
import pytest

from conftest import split_cursor


def hover_at(completer, sql_with_cursor):
    sql, pos = split_cursor(sql_with_cursor)
    found = completer.hover(pos, sql)
    return (found.markdown, sql[found.start:found.end]) if found else (None, None)


def test_function(completer):
    text, word = hover_at(completer, "select date_tr|unc('day', o.x) from orders o")
    assert word == "date_trunc"
    assert "Truncate to specified precision" in text


def test_user_macro(completer):
    text, _ = hover_at(completer, "select addit|ion(1, 2)")
    assert "addition(a, b)" in text
    assert "(a + b)" in text


def test_table_shows_columns(completer):
    text, word = hover_at(completer, "select * from ord|ers")
    assert word == "orders"
    assert "**table** `orders`" in text
    assert "| amount | DOUBLE |" in text


def test_alias_shows_its_table_columns(completer):
    text, word = hover_at(completer, "select o|.amount from orders o")
    assert word == "o"
    assert "| customer_id | INTEGER |" in text


def test_qualified_column(completer):
    text, word = hover_at(completer, "select o.amou|nt from orders o")
    assert word == "amount"
    assert "**column** `amount` `DOUBLE`" in text
    assert "from `o`" in text


def test_unqualified_column(completer):
    text, _ = hover_at(completer, "select regi|on from customers")
    assert "**column** `region` `VARCHAR`" in text


def test_cte(completer):
    text, _ = hover_at(completer, "with recent as (select order_id, amount from orders) select * from rec|ent")
    assert "**CTE** `recent`" in text
    assert "| order_id | INTEGER |" in text


def test_schema_qualified_table(completer):
    text, word = hover_at(completer, "select * from test_schema.schema_tab|le")
    assert word == "schema_table"
    assert "| column1 | VARCHAR |" in text


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("select date_trunc('da|y', x) from orders", id="string_literal"),
        pytest.param("select 1 -- da|y\n", id="comment"),
        pytest.param("sel|ect 1", id="keyword"),
        pytest.param("select * from orders where unknown_na|me = 1", id="unknown_name"),
    ],
)
def test_nothing(completer, sql):
    assert hover_at(completer, sql) == (None, None)


@pytest.mark.parametrize("sql", [
    "create seq|uence serial start 1",
    "cre|ate sequence serial start 1",
    "CREATE OR REPLACE TEMP SEQUEN|CE IF NOT EXISTS serial",
])
def test_create_sequence_statement(completer, sql):
    text, word = hover_at(completer, sql)
    assert word.lower().startswith("create") and word.lower().endswith("sequence")
    assert "**CREATE SEQUENCE**" in text
    assert "INCREMENT BY" in text


def test_sequence_name_is_not_the_statement(completer):
    text, _ = hover_at(completer, "create sequence seq|uence")
    assert text is None or "**CREATE SEQUENCE**" not in text


def test_create_table_has_no_statement_doc(completer):
    text, _ = hover_at(completer, "cre|ate table t (a int)")
    assert text is None or "**CREATE SEQUENCE**" not in text
