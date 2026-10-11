import gc
import sys
import threading
from pathlib import Path
from typing import Union

import duckdb
import pytest

from dabbler.db_stuff import get_db_data_new
from dabbler.lsp.completer import SqlCompleter


@pytest.fixture(scope="session", autouse=True)
def delete_qt_objects_on_the_gui_thread():
    """After the tests, waits for query threads, stops table worker threads and deletes Qt widgets.

    Left to Python's garbage collector, a widget can be freed from whatever thread
    triggers a collection (a query or heartbeat thread), and on Linux Qt then aborts
    the process ("Timers cannot be stopped from another thread").
    """
    yield
    # Python 3.12 kills daemon threads still running at exit with pthread_exit, which
    # aborts the process when one is inside DuckDB's C++ ("terminate called ...")
    for thread in threading.enumerate():
        if thread.name in ("dabbler-query", "dabbler-cancel"):
            thread.join(10)
    qt = sys.modules.get("dabbler.gui_qt")
    app = qt.QtWidgets.QApplication.instance() if qt else None
    if app is None:
        return
    for widget in app.allWidgets():
        if hasattr(widget, "stop_worker"):
            widget.stop_worker()
    for widget in app.topLevelWidgets():
        widget.close()
        widget.deleteLater()
    app.sendPostedEvents(None, qt.QtCore.QEvent.Type.DeferredDelete)
    gc.collect()

# Must match the trigger_characters registered in dabbler/lsp/server.py.
# VS Code only sends a trigger character for these; when the user types an
# identifier character the request arrives with no trigger character.
TRIGGER_CHARACTERS = (":", ".", "/", " ", '"', "'")

CURSOR = "|"

SQL_CORPUS_DIR = Path(__file__).parent / "sql_corpus"


def make_completion_db():
    db = duckdb.connect(":memory:")
    db.sql(
        """--sql
        attach ':memory:' as attached_db;
        create schema test_schema;
        create schema attached_db.attached_schema;
        create schema v;
        create table orders(order_id int, customer_id int, amount double, "order date" date);
        create table customers(customer_id int, name varchar, region varchar);
        create table "Order Lines"(line_id int, qty int);
        create table test_schema.schema_table(column1 varchar, column2 integer);
        create table attached_db.attached_schema.db_table(column1 varchar, column2 integer);
        create function addition(a, b) as a + b;
        create function v.add(a, b) as a + b;
        create macro my_table_macro() as table select 1;
        create macro recent_orders(n) as table select order_id, amount as total from orders limit n;
        create macro v.my_table_macro_in_v() as table select 1;
        create view test_view as select * from orders;
        create view test_schema.test_view2 as select * from orders;
        create view v.test_view3 as select * from orders;
        """
    )
    return db


@pytest.fixture(scope="session")
def completer():
    db = make_completion_db()
    return SqlCompleter(get_db_data_new(db, skip_ipython=True))


def split_cursor(sql_with_cursor: str):
    """Returns (sql, cursor_position) for text containing a single `|` marker."""
    assert sql_with_cursor.count(CURSOR) == 1, "test SQL needs exactly one | cursor marker"
    pos = sql_with_cursor.index(CURSOR)
    return sql_with_cursor.replace(CURSOR, "", 1), pos


def complete(
    completer: SqlCompleter, sql_with_cursor: str, trigger: Union[str, None] = "auto"
) -> list[str]:
    """Runs the completer the way the language server does and returns the labels.

    By default the trigger is derived like VS Code does it: the character left of
    the cursor if it is a registered trigger character, otherwise None.
    """
    sql, pos = split_cursor(sql_with_cursor)
    if trigger == "auto":
        prev = sql[pos - 1] if pos else ""
        trigger = prev if prev in TRIGGER_CHARACTERS else None
    result = completer.route_completion2(pos, sql, trigger)
    if result is None:
        return []
    return [x.label for x in result.items]
