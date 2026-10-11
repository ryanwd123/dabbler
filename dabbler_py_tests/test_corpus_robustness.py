"""Runs the grammar and completer over the SQL corpus in test_files/sql_tst.

These catch grammar regressions and crashes or slowdowns on half-typed SQL,
which is what the completer sees most of the time while someone is typing.
"""
import re
import time

import pytest

from conftest import SQL_CORPUS_DIR
from dabbler.lsp.parser import sql_parser

CORPUS = sorted(SQL_CORPUS_DIR.glob("*.sql"))
GRAMMAR_GAPS: dict[str, str] = {}
# Generous: typical completions take ~5-10ms, the slowest seen is ~60ms.
MAX_SECONDS_PER_COMPLETION = 1.0

pytestmark = pytest.mark.skipif(not CORPUS, reason=f"no SQL corpus in {SQL_CORPUS_DIR}")


def corpus_params(files):
    params = []
    for f in files:
        marks = ()
        if f.name in GRAMMAR_GAPS:
            marks = pytest.mark.xfail(strict=True, reason=GRAMMAR_GAPS[f.name])
        params.append(pytest.param(f, id=f.name, marks=marks))
    return params


@pytest.mark.parametrize("sql_file", corpus_params(CORPUS))
def test_grammar_parses_corpus(sql_file):
    sql_parser.parse(sql_file.read_text())


def cursor_positions(sql: str, step: int):
    """Positions just before each run of whitespace, i.e. right after a token."""
    positions = [m.start() for m in re.finditer(r"\s+", sql)]
    return positions[::step] + [len(sql)]


def assert_completes(completer, sql, pos, label):
    start = time.perf_counter()
    try:
        completer.route_completion2(pos, sql, None)
    except Exception as e:
        pytest.fail(f"{label} pos={pos} raised {e!r}; text before cursor: {sql[max(0, pos - 60):pos]!r}")
    elapsed = time.perf_counter() - start
    assert elapsed < MAX_SECONDS_PER_COMPLETION, f"{label} pos={pos} took {elapsed:.2f}s"


# A sample keeps the run to a few seconds; widen it when working on the parser.
SAMPLE = CORPUS[::20]


@pytest.mark.parametrize("sql_file", SAMPLE, ids=[f.name for f in SAMPLE])
def test_completer_survives_cursor_anywhere(completer, sql_file):
    sql = sql_file.read_text()
    for pos in cursor_positions(sql, step=3):
        assert_completes(completer, sql, pos, sql_file.name)


@pytest.mark.parametrize("sql_file", SAMPLE, ids=[f.name for f in SAMPLE])
def test_completer_survives_partially_typed_sql(completer, sql_file):
    sql = sql_file.read_text()
    for pos in cursor_positions(sql, step=3):
        assert_completes(completer, sql[:pos], pos, sql_file.name)


# Statements the grammar must accept. Add a line here when a construct is added
# to duckdb.lark (plan.md, section 2).
GRAMMAR_CASES = [
    "select a % 2, a // 2, a ** 2, a ^ 2, a & 1, a | 1, a << 1, a >> 1, a || b from t",
    "select * from t where a % 2 = 0",
    "select * from t qualify row_number() over (partition by a order by b) = 1",
    "select * from orders o join customers c using (customer_id)",
    "select * from orders o using sample 10%",
    "select * from customers c, lateral (select * from orders where customer_id = c.customer_id) o",
    "update orders o set amount = 1 where o.order_id = 2",
    "select 1 + 1 as column_1, 1 + 1 as column_2, column_1 + column_2",
]


@pytest.mark.parametrize("sql", GRAMMAR_CASES)
def test_grammar_accepts(sql):
    sql_parser.parse(sql)
