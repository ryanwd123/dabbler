"""Tests for hover text over names in SQL."""
import re

import duckdb
import pytest

from conftest import split_cursor
from dabbler.lsp.statement_docs import STATEMENTS


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
        pytest.param("select 1 from t whe|re a = 1", id="keyword"),
        pytest.param("select * from orders where unknown_na|me = 1", id="unknown_name"),
    ],
)
def test_nothing(completer, sql):
    assert hover_at(completer, sql) == (None, None)



@pytest.mark.parametrize("sql, title, word", [
    ("create seq|uence serial start 1", "CREATE SEQUENCE", "create sequence"),
    ("cre|ate sequence serial start 1", "CREATE SEQUENCE", "create sequence"),
    ("CREATE OR REPLACE TEMP SEQUEN|CE IF NOT EXISTS s", "CREATE SEQUENCE", "CREATE OR REPLACE TEMP SEQUENCE IF NOT EXISTS"),
    ("create or replace ta|ble t as select 1", "CREATE TABLE", "create or replace table"),
    ("create local temp tab|le t (a int)", "CREATE TABLE", "create local temp table"),
    ("create fun|ction f(a) as a + 1", "CREATE MACRO", "create function"),
    ("create unique ind|ex i on t (a)", "CREATE INDEX", "create unique index"),
    ("se|lect * from orders", "SELECT", "select"),
    ("insert into t sel|ect * from u", "SELECT", "select"),
    ("insert or replace in|to t values (1)", "INSERT", "insert or replace into"),
    ("upd|ate t set a = 1", "UPDATE", "update"),
    ("select 1;\n-- a note\nupd|ate t set a = 1", "UPDATE", "update"),
    ("update exten|sions", "UPDATE EXTENSIONS", "update extensions"),
    ("del|ete from t where a = 1", "DELETE", "delete from"),
    ("dr|op table if exists t", "DROP", "drop table if exists"),
    ("drop persistent sec|ret s", "DROP SECRET", "drop persistent secret"),
    ("se|t variable x = 1", "SET VARIABLE", "set variable"),
    ("se|t threads = 4", "SET / RESET", "set"),
    ("res|et global threads", "SET / RESET", "reset global"),
    ("cop|y t to 'out.csv'", "COPY", "copy"),
    ("attach 'f.db' as f; det|ach f", "DETACH", "detach"),
    ("explain anal|yze select 1", "EXPLAIN", "explain analyze"),
    ("select * from (summ|arize orders)", "SUMMARIZE", "summarize"),
    ("piv|ot cities on year using sum(population)", "PIVOT", "pivot"),
    ("from cities piv|ot (sum(population) for year in (2000))", "PIVOT", "pivot"),
    ("from t unpiv|ot include nulls (v for n in (a, b))", "UNPIVOT", "unpivot include nulls"),
    ("beg|in transaction", "Transactions", "begin transaction"),
    ("comm|it", "Transactions", "commit"),
])
def test_statement(completer, sql, title, word):
    text, found = hover_at(completer, sql)
    assert text.startswith(f"**{title}**")
    assert "[DuckDB docs](https://duckdb.org/docs/current/sql/statements/" in text
    assert found == word


@pytest.mark.parametrize("sql", [
    "create sequence seq|uence",
    "insert into t values (1, 2) on conflict do upd|ate set j = 1",
    "merge into t using s using (id) when matched then upd|ate",
    "merge into t using s using (id) when matched then del|ete",
    "update t s|et a = 1",
    "alter table t dr|op column k",
    "alter table t alter column i s|et default 10",
    "select case when a then 1 else 2 en|d from t",
])
def test_keyword_inside_another_statement(completer, sql):
    text, _ = hover_at(completer, sql)
    assert "[DuckDB docs]" not in (text or "")


@pytest.mark.parametrize("doc", STATEMENTS, ids=lambda d: d.markdown.split("**")[1])
def test_statement_examples_parse(doc):
    """Every example in the docs is valid DuckDB SQL."""
    examples = doc.markdown.partition("**Examples**")[2]
    for block in re.findall(r"```sql\n(.*?)```", examples, re.DOTALL):
        for sql in block.split(";\n"):
            duckdb.extract_statements(sql)
