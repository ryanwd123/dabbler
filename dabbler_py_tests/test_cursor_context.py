"""What the parser says goes at the cursor (parser.cursor_context), which routes completions."""
import pytest

from conftest import split_cursor
from dabbler.lsp.parser import cursor_context, interactive_parse


def context_of(sql_with_cursor):
    sql, pos = split_cursor(sql_with_cursor)
    ctx = cursor_context(interactive_parse(sql, pos), sql, pos)
    return ctx.kind, ctx.qualifier


@pytest.mark.parametrize(
    "sql, kind",
    [
        # tables
        ("select * from |", "table"),
        ("select * from a join |", "table"),
        ("select * from a, |", "table"),
        ("select * from ord|", "table"),
        ("update |", "table"),
        ("insert into |", "table"),
        # columns and functions
        ("select |", "expression"),
        ("select am| from orders", "expression"),
        ("select * from a where |", "expression"),
        ("select * from a join b on |", "expression"),
        ("select * from a group by |", "expression"),
        ("select x from t order by |", "expression"),
        ("select sum(|", "expression"),
        ("select f(a, |", "expression"),
        ("select * from t where a = |", "expression"),
        ("select * from t where a=|", "expression"),
        ("select * from t where a in (|", "expression"),
        ("update orders set |", "expression"),
        ("select extract(year from |) from t", "expression"),  # "from" here isn't a FROM clause
        # types
        ("select a::|", "type"),
        ("select cast(a as |)", "type"),
        ("select cast(a as |", "type"),
        ("create table x (a |", "type"),
        # not SQL positions
        ("select * from t where note = 'shipped from |'", "string"),
        ("select * from t -- join |", "comment"),
        ("select * from t /* join | */", "comment"),
        # the parser can't tell (alias or keyword next): the regex router decides
        ("select * from orders o |", "unknown"),
        ("select * from t qualify |", "expression"),
        ("select * from t window w as (|", "unknown"),  # WINDOW isn't in the grammar yet
    ],
)
def test_context_kind(sql, kind):
    assert context_of(sql)[0] == kind


@pytest.mark.parametrize(
    "sql, kind, qualifier",
    [
        ("select o.| from orders o", "expression", ["o"]),
        ("select sum(o.| from orders o", "expression", ["o"]),
        ("select * from t where a = o.|", "expression", ["o"]),
        ('select "Order Lines".| from x', "expression", ['"Order Lines"']),
        ("select * from main.|", "table", ["main"]),
        ("select * from db.main.|", "table", ["db", "main"]),
        ("select o.am| from orders o", "expression", ["o"]),
    ],
)
def test_qualified_names(sql, kind, qualifier):
    assert context_of(sql) == (kind, qualifier)


def test_comment_closed_before_cursor_is_not_a_comment():
    assert context_of("select * from t /* c */ where |")[0] == "expression"
    assert context_of("select * -- c\nfrom |")[0] == "table"
