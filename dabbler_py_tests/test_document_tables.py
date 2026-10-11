"""Tables a python file creates in its own SQL complete in the file's other SQL."""
import time

from conftest import split_cursor
from dabbler.lsp.document_tables import CreateStatement, create_statements, document_tables, resolve_with


def test_create_table_with_columns():
    assert create_statements("create or replace table s.sales (id int primary key, total decimal(10, 2))") == (
        CreateStatement(("s", "sales"), "table", columns=(("id", "INT"), ("total", "DECIMAL(10, 2)"))),
    )


def test_create_table_as_and_view():
    found = create_statements("create table t (a, b) as select 1, 2; create view v as select x from y")
    assert found == (
        CreateStatement(("t",), "table", query="select 1, 2", column_names=("a", "b")),
        CreateStatement(("v",), "view", query="select x from y"),
    )


def test_a_half_typed_statement_doesnt_hide_the_create():
    assert [s.parts for s in create_statements("create table t (a int); select * from wher")] == [("t",)]


def test_strings_without_create_arent_parsed():
    create_statements.cache_clear()
    assert create_statements("select * from orders") == ()
    assert create_statements("create index i on t (a)") == ()
    assert create_statements.cache_info().currsize == 2


SOURCE = '''
import duckdb
db = duckdb.connect()
db.sql("create table sales (id int, total double)")
db.sql("""--sql
create or replace table big_orders as
select o.order_id, o.amount * 2 as doubled, c.region
from orders o join customers c using (customer_id)
""")
db.sql("create table big_regions as select region, count(*) from big_orders group by region")
db.sql("create table orders (something_else int)")
'''


def tables_of(completer, source=SOURCE):
    return document_tables(source, resolve_with(completer))


def test_columns_of_created_tables(completer):
    tables = {t.name: t.columns for t in tables_of(completer).tables}
    assert tables["sales"] == [("id", "INT"), ("total", "DOUBLE")]
    # types are kept for plain column references, as for CTEs
    assert tables["big_orders"] == [("order_id", "INTEGER"), ("doubled", None), ("region", "VARCHAR")]
    # reads a table created earlier in the file; count(*) has no name
    assert tables["big_regions"] == [("region", "VARCHAR"), (None, None)]


def complete(completer, sql_with_cursor, tables, trigger=" "):
    sql, pos = split_cursor(sql_with_cursor)
    result = completer.route_completion2(pos, sql, trigger, document_tables=tables)
    return [x.label for x in result.items] if result else []


def test_created_tables_complete(completer):
    tables = tables_of(completer)
    assert {"sales", "big_orders", "big_regions"} <= set(complete(completer, "select * from |", tables))
    assert complete(completer, "select b.| from big_orders b", tables, ".") == ["order_id", "doubled", "region"]
    assert set(complete(completer, "select * from sales where |", tables, None)) >= {"id", "total"}


def test_the_schemas_own_table_wins(completer):
    labels = complete(completer, "select o.| from orders o", tables_of(completer), ".")
    assert "amount" in labels and "something_else" not in labels


def test_without_document_tables_nothing_leaks(completer):
    tables_of(completer)
    assert "sales" not in complete(completer, "select * from |", None)


def test_scanning_a_file_is_cheap(completer):
    # 200 SQL strings, a few of them CREATEs: only those are parsed, and only once
    queries = "".join(f'db.sql("select * from orders where order_id = {i}")\n' for i in range(200))
    source = SOURCE + queries
    tables_of(completer, source)
    start = time.perf_counter()
    tables_of(completer, source)
    assert time.perf_counter() - start < 0.2
