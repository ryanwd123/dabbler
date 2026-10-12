"""Table-driven completion tests.

Each case is SQL with a `|` marking the cursor, the labels that must be offered,
and optionally labels that must not be. To turn a bug report into a test, add a
case here. Known bugs are marked `bug(...)`: they are strict xfails, so when one
gets fixed the test run tells you to remove the marker.
"""
import pytest

from conftest import complete

ORDERS_COLS = ["order_id", "customer_id", "amount", '"order date"']
CUSTOMERS_COLS = ["customer_id", "name", "region"]


def case(sql, includes, excludes=(), id=None, marks=()):
    return pytest.param(sql, list(includes), list(excludes), id=id, marks=marks)


def bug(reason):
    return pytest.mark.xfail(strict=True, reason=reason)


COLUMN_CASES = [
    case("select o.| from orders o", ORDERS_COLS, CUSTOMERS_COLS[1:], id="alias_dot"),
    case(
        "select o.order_id, c.| from orders o join customers c on o.customer_id = c.customer_id",
        CUSTOMERS_COLS, ["amount"], id="second_join_alias",
    ),
    case("select * from orders o join customers c on o.|", ORDERS_COLS, id="join_on"),
    case("select * from orders o where o.|", ORDERS_COLS, id="where"),
    case("select * from orders o\nwhere o.customer_id = 1\n  and o.|", ORDERS_COLS, id="where_and_multiline"),
    case("select * from orders o group by o.|", ORDERS_COLS, id="group_by"),
    case("select * from orders o order by o.|", ORDERS_COLS, id="order_by"),
    case("select count(*) from orders o group by all having o.|", ORDERS_COLS, id="having"),
    case(
        "select * from orders o qualify row_number() over (partition by o.|) = 1",
        ORDERS_COLS, id="window_partition",
    ),
    case("select * from orders o, customers c where c.|", CUSTOMERS_COLS, id="comma_join"),
    case("create table x as select o.| from orders o", ORDERS_COLS, id="ctas"),
    case("select o.| from orders o; select c.name from customers c", ORDERS_COLS, id="first_of_two_statements"),
    case("select * from orders; select c.| from customers c", CUSTOMERS_COLS, id="second_of_two_statements"),
    case("select * from test_view t where t.|", ORDERS_COLS, id="view_alias"),
    case("select * from test_schema.schema_table s where s.|", ["column1", "column2"], id="schema_qualified_alias"),
    case(
        'select * from test_schema."org.category/dotted_view" v where v.|',
        CUSTOMERS_COLS, ["amount"], id="dot_inside_quoted_view_name",
    ),
    case('select reg| from test_schema."org.category/dotted_view"', ["region"], id="prefix_dot_inside_quoted_view_name"),
    case(
        'select test_schema."org.category/dotted_view".| from test_schema."org.category/dotted_view"',
        CUSTOMERS_COLS, id="qualified_dot_inside_quoted_view_name",
    ),
    case("select am| from orders", ["amount"], ["name"], id="prefix_in_select"),
    case("select * from orders where am|", ["amount"], id="prefix_in_where"),
    case(
        "select cu| from orders o join customers c on o.customer_id = c.customer_id",
        ["customer_id"], id="prefix_with_join",
    ),
    case("select cu| from orders o, customers c", ["customer_id"], id="prefix_comma_join"),
    case("select * from orders o where o.amount in (select am| from orders)", ["amount"], id="prefix_in_subquery"),
    # known bugs
    case("SELECT O.| FROM ORDERS O", ORDERS_COLS, id="uppercase_identifiers"),
    case('select * from "orders" o where o.|', ORDERS_COLS, id="quoted_table_name"),
    case("select o.amount, sum(o.| from orders o", ORDERS_COLS, id="unclosed_paren"),
    case(
        "select cu| from orders o join customers c using (customer_id)",
        ["customer_id"], id="join_using_prefix",
    ),
    case(
        "select * from orders o join customers c using (customer_id) where c.|",
        CUSTOMERS_COLS, id="join_using_dot",
    ),
    case(
        "select c.name, o.| from customers c, lateral (select order_id, amount from orders where customer_id = c.customer_id) o",
        ["order_id", "amount"], ["name"], id="lateral_subquery",
    ),
    case(
        "select c.name, o.| from customers c join lateral (select order_id as oid from orders) o on true",
        ["oid"], id="join_lateral_subquery",
    ),
    case(
        "select c.name, o.| from customers c, lateral (select * from orders where customer_id = c.customer_id) o",
        ORDERS_COLS, id="lateral_correlated_star",
    ),
    case(
        "select c.name, o.| from customers c, (select * from orders where customer_id = c.customer_id) o",
        ORDERS_COLS, id="implicit_lateral_correlated_star",
    ),
    case(
        "select o.| from customers c join lateral (select o2.*, c.name from orders o2 where o2.customer_id = c.customer_id) o on true",
        ORDERS_COLS + ["name"], id="join_lateral_correlated_star",
    ),
    case("select * from orders o qualify o.|", ORDERS_COLS, id="qualify"),
    case("select * from orders o qualify row_number() over (order by o.amount) = 1 order by o.|", ORDERS_COLS, id="order_by_after_qualify"),
    case("select * from orders o join customers c using (customer_id) where |", [], id="join_using_where_space"),
    case('select * from orders "O" where o.|', ORDERS_COLS, id="quoted_alias"),
    case("select o.| from main.orders o", ORDERS_COLS, id="schema_qualified_table_alias"),
    case("select o.| from MEMORY.MAIN.ORDERS o", ORDERS_COLS, id="uppercase_fully_qualified"),
    case("select sum(o.amount), count(o.| from orders o", ORDERS_COLS, id="second_unclosed_paren"),
]

DERIVED_TABLE_CASES = [
    case(
        "select s.| from (select order_id as oid, amount from orders) s",
        ["oid", "amount"], ["order_id", "customer_id"], id="subquery_alias",
    ),
    case(
        "with x as (select order_id, amount from orders) select x.| from x",
        ["order_id", "amount"], ["customer_id"], id="cte_columns",
    ),
    case(
        "with x as (select order_id, amount from orders) select * from |",
        ["x", "orders"], id="cte_in_from",
    ),
    case(
        'with cte_1 as materialized (select col1, col2 as c2, col3 as "Column 3" from bbb)\n'
        "select a.| from cte_1 a",
        ["col1", "c2", '"Column 3"'], id="cte_aliases_and_quoted",
    ),
    case(
        "with cte_a as (select order_id from orders), cte_b as (select * from cte_|)",
        ["cte_a"], id="earlier_cte_in_later_cte_prefix",
    ),
    case(
        "with a as (select order_id from orders), b as (select * from |)",
        ["a", "orders"], id="earlier_cte_in_later_cte",
    ),
]

TABLE_CASES = [
    case("select * from |", ["orders", "customers", "test_view", "test_schema", "main", "attached_db", "my_table_macro"], id="from"),
    case("select * from ord|", ["orders"], ["customers"], id="from_prefix"),
    case("select * from orders o join |", ["orders", "customers"], id="join"),
    case("select * from orders o left join |", ["customers"], id="left_join"),
    case("insert into |", ["orders"], id="insert_into"),
    case("delete from |", ["orders"], id="delete_from"),
    case("describe |", ["orders"], id="describe"),
    case("summarize |", ["orders"], id="summarize"),
    case("truncate |", ["orders"], id="truncate"),
    case("create index i on |", ["orders"], id="create_index_on"),
    case("exp|", ["EXPLAIN"], id="explain_keyword"),
    case("select * from test_schema.|", ["schema_table", "test_view2"], ["orders"], id="schema_dot"),
    case("select * from main.|", ["orders", "customers"], ["schema_table"], id="main_schema_dot"),
    case("select * from attached_db.|", ["attached_schema"], id="database_dot"),
    case("select * from attached_db.attached_schema.|", ["db_table"], id="database_schema_dot"),
    case("select * from v.|", ["my_table_macro_in_v", "test_view3"], id="schema_dot_macro"),
    case("select * from read_c|", ["read_csv", "read_csv_auto"], id="table_function_prefix"),
]

FUNCTION_CASES = [
    case("select su|(amount) from orders", ["sum", "substring"], id="builtin_prefix"),
    case("select add|", ["addition"], id="user_macro"),
    case("select v.|", ["add"], id="schema_qualified_macro"),
]

KEYWORD_CASES = [
    case("s|", ["SELECT", "SET"], id="statement_start"),
    case("w|", ["WITH"], id="with"),
    case("select * f|", ["FROM"], id="from"),
    case("select * from orders o j|", ["JOIN"], id="join"),
    case("select * from orders o l|", ["LEFT", "LIMIT"], id="left_limit"),
    case("select * from orders o join customers c o|", ["ON"], id="on"),
    case("select * from orders w|", ["WHERE"], id="where"),
    case("select * from orders o where o.amount > 1 a|", ["AND"], id="and"),
    case("select * from orders o where o.amount i|", ["IN", "IS", "ILIKE"], id="in_is"),
    case("select c1, sum(c2) from abc g|", ["GROUP BY", "GROUP BY ALL"], id="group_by"),
    case("select * from orders o order by o.amount d|", ["DESC"], id="desc"),
    case("select * from orders u|", ["UNION", "USING"], id="union_using"),
    case("select * from orders o where o.amount > 1 q|", ["QUALIFY"], id="qualify_keyword"),
]

SPECIAL_CONTEXT_CASES = [
    case("install |", ["'httpfs'", "'parquet'"], id="install"),
    case("load |", ["'httpfs'"], id="load"),
    case("pragma |", ["version", "database_size"], id="pragma"),
    case("set |", ["max_memory", "threads", "SESSION"], id="set"),
    case("set global |", ["max_memory", "threads"], ["SESSION", "GLOBAL"], id="set_global"),
    case("SET Session |", ["threads"], id="set_session"),
    case("reset local |", ["threads"], id="reset_local"),
    case("set thr|", ["threads"], id="set_partial"),
    case("select 1;\nset global |", ["threads"], id="set_after_statement"),
    case("set variable |", [], ["threads"], id="set_variable_isnt_a_setting"),
    case("select amount::| from orders", ["DOUBLE", "VARCHAR", "DATE"], id="cast_operator"),
    case(
        "select cast(amount as |) from orders", ["DOUBLE"], id="cast_function",
    ),
    case("select try_cast(sum(amount) as |) from orders", ["DOUBLE"], id="try_cast_nested_parens"),
    case("create table t (a |", ["INTEGER", "VARCHAR"], id="column_type_in_create_table"),
]

# Places where a text pattern guesses wrong and the parser's state gets it right.
PARSER_CONTEXT_CASES = [
    case("select extract(year from |) from orders o", [], ["orders", "customers"], id="extract_from_is_not_a_from_clause"),
    case("select extract(year from o.|) from orders o", ["amount"], id="extract_from_column"),
    case("select * from orders where note = 'shipped from |'", [], ["orders"], id="inside_string"),
    case("select * from orders o -- join |", [], ["customers"], id="inside_comment"),
    case('select "Order Lines".| from "Order Lines"', ["line_id", "qty"], id="quoted_table_dot"),
    case('select ol.| from "Order Lines" ol', ["line_id", "qty"], id="quoted_table_alias"),
    case("update |", ["orders", "customers"], id="update_table"),
    case("update orders set am|", ["amount"], id="update_set_column"),
    case("select * from orders o where o.amount = cu|", ["customer_id"], id="after_comparison"),
    case("select case when am| from orders", ["amount"], id="unfinished_case_when"),
    case("select case when amount > 1 then am| from orders", ["amount"], id="unfinished_case_then"),
    case("select * from orders where amount between 1 + 1 and am|", ["amount"], id="between_arithmetic"),
    case("select [x + 1 for x in [1, 2] if x > am|] from orders", ["amount"], id="list_comprehension"),
    case("select list_transform([1], lambda x: x + am|) from orders", ["amount"], id="lambda_body"),
    case("select o.amount::| from orders o", ["INTEGER", "VARCHAR"], ["orders"], id="cast_type"),
    case("select * from read_csv('x.csv', header := true) r where r.| ", [], ["orders"], id="named_arg_table_function"),
]

SCOPE_CASES = [
    case(
        "select * from orders o where exists (select 1 from customers c where c.customer_id = o.|)",
        ORDERS_COLS, id="outer_alias_in_exists",
    ),
    case(
        "select o.order_id, (select max(c.name) from customers c where c.customer_id = o.|) from orders o",
        ORDERS_COLS, id="outer_alias_in_scalar_subquery",
    ),
    case(
        "select * from orders o where exists (select 1 from customers c where c.customer_id = cu|)",
        ["customer_id"], id="outer_and_inner_columns_by_prefix",
    ),
    case(
        "select * from orders o where o.customer_id in (select c.| from customers c)",
        CUSTOMERS_COLS, ["amount"], id="inner_alias_in_in_subquery",
    ),
    case(
        "select * from orders o where exists (select 1 from customers o where o.|)",
        CUSTOMERS_COLS, ["amount"], id="inner_alias_shadows_outer",
    ),
    case(
        "select c.| from orders o where exists (select 1 from customers c)",
        [], ["name"], id="inner_alias_not_visible_outside",
    ),
    case(
        "with x as (select order_id from orders) select * from customers c where exists (select 1 from x where x.order_id = c.|)",
        CUSTOMERS_COLS, id="outer_alias_with_cte_in_inner_query",
    ),
    case(
        "with x as (select order_id from orders) select * from x where exists (select 1 from customers c where c.customer_id = x.|)",
        ["order_id"], id="outer_cte_alias_in_subquery",
    ),
]

COLUMN_ALIAS_LIST_CASES = [
    case("select t.| from (select 1, 2) t(a, b)", ["a", "b"], ["1", "2"], id="subquery_column_aliases"),
    case("select o.| from orders o(id, cust)", ["id", "cust", "amount"], ["order_id"], id="table_column_aliases"),
    case("with v(k1, k2) as (select 1, 2) select v.| from v", ["k1", "k2"], id="cte_column_aliases"),
    case("with v(k1) as (select 1), w as (select k1 from v) select w.| from w", ["k1"], id="aliased_cte_used_by_later_cte"),
]

# Columns of CTEs, subqueries and table functions are worked out from the SQL
# (no DuckDB describe), so these exercise the projection resolver.
RESOLVER_CASES = [
    case("with x as (select * exclude (amount) from orders) select x.| from x",
         ["order_id", "customer_id", '"order date"'], ["amount"], id="star_exclude"),
    case("with x as (select * replace (amount * 2 as amount) from orders) select x.| from x",
         ORDERS_COLS, id="star_replace"),
    case("with x as (select o.*, c.name from orders o join customers c using (customer_id)) select x.| from x",
         ORDERS_COLS + ["name"], ["region"], id="qualified_star_in_join"),
    case("with x as (select columns('id$') from orders) select x.| from x",
         ["order_id", "customer_id"], ["amount"], id="columns_regex"),
    case("with x as (select columns('^c') from orders) select x.| from x",
         ["customer_id"], ["order_id", "amount"], id="columns_regex_anchored"),
    case("with x as (select columns(* exclude (amount)) from orders) select x.| from x",
         ["order_id"], ["amount"], id="columns_star_exclude"),
    case("with x as (select * rename (amount as total) from orders) select x.| from x",
         ["order_id", "total"], ["amount"], id="star_rename"),
    case("with x as (select o.* exclude (o.amount) from orders o) select x.| from x",
         ["order_id"], ["amount"], id="star_exclude_qualified"),
    case("with x as (select * exclude amount from orders) select x.| from x",
         ["order_id"], ["amount"], id="star_exclude_no_parens"),
    case("with x as (select order_id as id from orders union all select customer_id from customers) select x.| from x",
         ["id"], ["customer_id"], id="union_takes_first_branch_names"),
    case("with recursive r(n) as (select 1 union all select n + 1 from r where n < 3) select r.| from r",
         ["n"], id="recursive_cte"),
    case("select v.| from (values (1, 2)) v(a, b)", ["a", "b"], id="values_with_column_aliases"),
    case("select v.| from (values (1, 2)) v", ["col0", "col1"], id="values_default_names"),
    case("select r.| from range(3) r", ["range"], id="table_function_alias"),
    case("with x as (select * from range(3)) select x.| from x", ["range"], id="unaliased_table_function_star"),
    case("select r.| from recent_orders(5) r", ["order_id", "total"], ["amount"], id="table_macro"),
    case("with x as (select * from recent_orders(5)) select x.| from x", ["order_id", "total"], id="table_macro_star"),
    case("with x as (select order_id, amount * 2 from orders) select x.| from x",
         ["order_id"], id="unaliased_expression_skipped"),
    case("select t.| from (select 1, amount * 2, order_id from orders) t(a, b)",
         ["a", "b", "order_id"], id="column_alias_list_counts_unnamed_columns"),
]


@pytest.mark.parametrize(
    "sql, includes, excludes",
    COLUMN_CASES + DERIVED_TABLE_CASES + TABLE_CASES + FUNCTION_CASES + KEYWORD_CASES + SPECIAL_CONTEXT_CASES
    + PARSER_CONTEXT_CASES + SCOPE_CASES + COLUMN_ALIAS_LIST_CASES + RESOLVER_CASES,
)
def test_completion(completer, sql, includes, excludes):
    labels = complete(completer, sql)
    missing = [x for x in includes if x not in labels]
    unexpected = [x for x in excludes if x in labels]
    assert not missing and not unexpected, f"missing={missing} unexpected={unexpected} got={labels[:40]}"


@pytest.mark.parametrize(
    "sql",
    [
        "select o.| from orders o",
        "select orders.| from orders",
        "SELECT ORDERS.| FROM ORDERS",
        "select * from |",
    ],
)
def test_no_duplicate_labels(completer, sql):
    labels = complete(completer, sql)
    dupes = sorted({x for x in labels if labels.count(x) > 1})
    assert not dupes


def test_space_trigger_outside_table_context_returns_nothing(completer):
    # A space should only pop the list where a table name is expected,
    # otherwise every space typed in a query would open a completion list.
    assert complete(completer, "select o.order_id, | from orders o") == []


@pytest.mark.parametrize(
    "sql, includes",
    [
        pytest.param("select | from orders o", ["amount", "o", "sum"], id="select_list"),
        pytest.param("select o.order_id, | from orders o", ["amount", "o"], id="after_comma"),
        pytest.param("select * from orders o where |", ["amount", "customer_id"], id="where"),
    ],
)
def test_invoked_completion_with_nothing_typed(completer, sql, includes):
    # ctrl+space sends no trigger character, even right after a space
    labels = complete(completer, sql, trigger=None)
    assert [x for x in includes if x not in labels] == []


@pytest.mark.parametrize(
    "sql, includes",
    [
        pytest.param("update orders o set amount = o.| where true", ORDERS_COLS, id="update_alias"),
        pytest.param("update orders set amount = 1 where cu|", ["customer_id"], id="update_where"),
        pytest.param("update orders set amount = c.| from customers c", CUSTOMERS_COLS, id="update_from"),
        pytest.param("delete from orders o where o.|", ORDERS_COLS, id="delete_alias"),
        pytest.param("delete from orders where cu|", ["customer_id"], id="delete_where"),
        pytest.param("delete from orders o using customers c where c.|", CUSTOMERS_COLS, id="delete_using"),
        pytest.param("insert into customers (select o.| from orders o)", ORDERS_COLS, id="insert_paren_select"),
        pytest.param("with s as (select customer_id as cid from customers) insert into orders select s.| from s",
                     ["cid"], id="with_insert"),
        pytest.param("from orders o where o.|", ORDERS_COLS, id="from_first_where"),
        pytest.param("select * from orders o, lateral range(o.|)", ORDERS_COLS, id="lateral_function_arg"),
        pytest.param("merge into orders o using customers c on o.| when matched then delete", ORDERS_COLS, id="merge_on_target"),
        pytest.param("merge into orders o using customers c on o.customer_id = c.| when matched then delete",
                     CUSTOMERS_COLS, id="merge_on_source"),
        pytest.param("merge into orders o using customers c on o.customer_id = c.customer_id "
                     "when matched then update set amount = c.|", CUSTOMERS_COLS, id="merge_update_set"),
        pytest.param("with s as (select customer_id as cid from customers) "
                     "merge into orders o using s on o.customer_id = s.| when matched then delete", ["cid"], id="merge_cte_source"),
        pytest.param("merge into orders using (select customer_id as cid from customers) s "
                     "on orders.customer_id = s.| when matched then delete", ["cid"], id="merge_subquery_source"),
        pytest.param("explain select o.| from orders o", ORDERS_COLS, id="explain"),
        pytest.param("describe select o.| from orders o", ORDERS_COLS, id="describe_query"),
    ],
)
def test_update_statement_scope(completer, sql, includes):
    labels = complete(completer, sql)
    assert [x for x in includes if x not in labels] == []


def test_types_kept_for_plain_column_references(completer):
    from conftest import split_cursor
    sql, pos = split_cursor("with x as (select o.amount, o.amount * 2 as doubled from orders o) select x.| from x")
    items = {i.label: i.label_details.description for i in completer.route_completion2(pos, sql, ".").items}
    assert items == {"amount": "DOUBLE", "doubled": None}


# DuckDB lets select-list aliases be used later in the query: earlier aliases
# within the select list, all aliases in WHERE / GROUP BY / HAVING / QUALIFY /
# ORDER BY, none in FROM / JOIN ... ON.
ALIAS_CASES = [
    pytest.param("select\n    1 + 1 as column_1,\n    1 + 1 as column_2,\n    col|", ["column_1", "column_2"], [], id="example_from_request"),
    pytest.param("select amount * 2 as doubled, dou| from orders", ["doubled"], [], id="earlier_alias_in_select_list"),
    pytest.param("select dou|, amount * 2 as doubled from orders", [], ["doubled"], id="later_alias_not_offered"),
    pytest.param("select amount * 2 as doubled from orders where dou|", ["doubled"], [], id="where"),
    pytest.param("select order_id % 2 as parity, count(*) as n from orders group by par|", ["parity"], [], id="group_by"),
    pytest.param("select order_id % 2 as parity, count(*) as n from orders group by parity having |", ["n", "parity"], [], id="having"),
    pytest.param("select order_id, row_number() over (order by amount) as rn from orders qualify |", ["rn"], [], id="qualify"),
    pytest.param("select amount + 1 as total from orders order by tot|", ["total"], [], id="order_by"),
    pytest.param("select o.amount + 1 as total from orders o join customers c on to|", [], ["total"], id="not_in_join_on"),
    pytest.param("select amount as x, (select |) from orders", ["x"], [], id="outer_alias_in_select_subquery"),
    pytest.param("select 1 as abc, ab| from orders", ["abc"], [], id="alias_and_prefix"),
]


@pytest.mark.parametrize("sql, includes, excludes", ALIAS_CASES)
def test_select_list_aliases(completer, sql, includes, excludes):
    # ctrl+space at an empty position, otherwise the typed prefix
    labels = complete(completer, sql, trigger=None)
    missing = [x for x in includes if x not in labels]
    unexpected = [x for x in excludes if x in labels]
    assert not missing and not unexpected, f"missing={missing} unexpected={unexpected} got={labels[:40]}"


def test_alias_items_are_marked_as_aliases(completer):
    from conftest import split_cursor
    sql, pos = split_cursor("select amount * 2 as doubled from orders where dou|")
    items = {i.label: i.label_details.description for i in completer.route_completion2(pos, sql, None).items}
    assert items["doubled"] == "alias"
