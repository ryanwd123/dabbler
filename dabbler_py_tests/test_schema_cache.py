import os
import sqlite3
import threading
import time

import pytest

from dabbler import schema_cache
from dabbler.schema_cache import IN_MEMORY, SchemaCache, normalize_path


@pytest.fixture
def cache(tmp_path):
    return SchemaCache(tmp_path / "cache.sqlite3")


DB_DATA = {
    "databases": ["memory", "lake"],
    "schemas": ["memory.main", "memory.Sales", "lake.main"],
    "data": [
        ["memory.main", "orders", "table", [["order_id", "INTEGER"], ["Amount", "DOUBLE"]]],
        ["memory.Sales", "Big Orders", "view", [["order_id", "INTEGER"]]],
        ["lake.main", "events", "table", [["ts", "TIMESTAMP"]]],
        ["memory.main", "seq", "CREATE SEQUENCE seq;", None],
    ],
    "functions": [
        ["memory.main", "add_tax", "macro", None],
        ["memory.main", "recent", "table_macro", "SELECT order_id FROM orders"],
    ],
    "current_schema": "memory.main",
    "cwd": "/work",
    "file_search_path": None,
    "loaded_extensions": ["json"],
    "paths": [["data_dir", "/work/data"]],
}


def names(rows):
    return {(r.kind, r.name) for r in rows}


def test_save_and_read_back(cache, tmp_path):
    nb = tmp_path / "nb.py"
    assert cache.save_snapshot(nb, DB_DATA) is True
    info = cache.source_info(nb)
    assert (info.current_schema, info.cwd, info.extensions, info.version) == ("memory.main", "/work", ["json"], 1)
    assert names(cache.objects(str(nb), kinds=["table", "view"])) == {
        ("table", "orders"), ("view", "Big Orders"), ("table", "events")
    }
    assert names(cache.objects(str(nb), catalog="memory", schema="main", kinds=["function", "table_macro"])) == {
        ("function", "add_tax"), ("table_macro", "recent")
    }
    assert cache.objects(str(nb), name="recent", kinds=["table_macro"])[0].definition == "SELECT order_id FROM orders"
    assert cache.columns(str(nb), "memory", "main", "orders") == [("order_id", "INTEGER"), ("Amount", "DOUBLE")]
    assert cache.paths(str(nb)) == {"data_dir": "/work/data"}


def test_lookups_ignore_case(cache, tmp_path):
    nb = str(tmp_path / "nb.py")
    cache.save_snapshot(nb, DB_DATA)
    assert cache.columns(nb, "MEMORY", "sales", "big orders") == [("order_id", "INTEGER")]
    assert names(cache.objects(nb, catalog="Memory", schema="SALES", kinds=["view"])) == {("view", "Big Orders")}


def test_unchanged_save_writes_nothing(cache, tmp_path):
    nb = tmp_path / "nb.py"
    cache.save_snapshot(nb, DB_DATA)
    before = cache.source_info(nb)
    assert cache.save_snapshot(nb, dict(DB_DATA)) is False
    assert cache.source_info(nb) == before


def rowids(cache, nb):
    with sqlite3.connect(cache.path) as con:
        return dict(con.execute(
            "select name, rowid from objects where source_file = ? and kind in ('table', 'view')", (normalize_path(nb),)
        ))


def test_only_changed_objects_are_rewritten(cache, tmp_path):
    nb = tmp_path / "nb.py"
    cache.save_snapshot(nb, DB_DATA)
    before = rowids(cache, nb)

    data = {**DB_DATA, "data": [list(r) for r in DB_DATA["data"]]}
    data["data"][0][3] = [["order_id", "INTEGER"], ["Amount", "DOUBLE"], ["note", "VARCHAR"]]  # orders gains a column
    del data["data"][2]  # lake.main.events is dropped
    assert cache.save_snapshot(nb, data) is True

    after = rowids(cache, nb)
    assert after["Big Orders"] == before["Big Orders"]  # untouched
    assert after["orders"] != before["orders"]  # rewritten
    assert "events" not in after
    assert cache.columns(str(nb), "memory", "main", "orders")[-1] == ("note", "VARCHAR")
    assert cache.columns(str(nb), "lake", "main", "events") == []
    assert cache.get_version(nb) == 2


def test_setting_changes_bump_the_version(cache, tmp_path):
    nb = tmp_path / "nb.py"
    cache.save_snapshot(nb, DB_DATA)
    assert cache.save_snapshot(nb, {**DB_DATA, "cwd": "/elsewhere"}) is True
    assert cache.get_version(nb) == 2
    assert cache.save_snapshot(nb, {**DB_DATA, "cwd": "/elsewhere", "paths": []}) is True
    assert cache.paths(str(nb)) == {}
    assert cache.get_version(nb) == 3


def test_missing_source(cache, tmp_path):
    assert cache.get_version(tmp_path / "nope.py") is None
    assert cache.source_info(tmp_path / "nope.py") is None


def test_paths_are_normalized(cache, tmp_path):
    nb = tmp_path / "Sub" / "NB.py"
    cache.save_snapshot(str(nb), DB_DATA)
    spelled_differently = str(tmp_path / "Sub" / "." / "NB.py")
    if os.name == "nt":
        spelled_differently = spelled_differently.upper().replace("\\", "/")
    assert cache.get_version(spelled_differently) == 1


def test_old_json_snapshot_file_is_upgraded(tmp_path):
    path = tmp_path / "old.sqlite3"
    with sqlite3.connect(path) as con:
        con.execute("create table snapshots(source_file text primary key, fingerprint text, updated_at real, db_data text)")
        con.execute("insert into snapshots values ('x', 'f', 0, '{}')")
    cache = SchemaCache(path)
    with sqlite3.connect(path) as con:
        tables = {r[0] for r in con.execute("select name from sqlite_master where type = 'table'")}
    assert "snapshots" not in tables and {"sources", "objects", "columns"} <= tables
    cache.save_snapshot(tmp_path / "nb.py", DB_DATA)


def test_in_memory_cache(tmp_path):
    cache = SchemaCache(IN_MEMORY)
    cache.save_snapshot("nb", DB_DATA)
    assert cache.columns("nb", "memory", "main", "orders")[0] == ("order_id", "INTEGER")
    assert SchemaCache(IN_MEMORY).get_version("nb") is None  # each in-memory cache is private


def test_resolve_prefers_own_snapshot_then_newest_in_workspace(cache, tmp_path):
    ws = tmp_path / "ws"
    nb1, nb2, helper = ws / "nb1.py", ws / "sub" / "nb2.py", ws / "lib" / "helper.py"
    cache.save_snapshot(nb1, DB_DATA)
    time.sleep(0.01)
    cache.save_snapshot(nb2, {**DB_DATA, "cwd": "2"})

    assert cache.resolve_source(nb1, ws) == normalize_path(nb1)
    assert cache.resolve_source(helper, ws) == normalize_path(nb2)  # newest in the workspace
    assert cache.resolve_source(tmp_path / "elsewhere" / "x.py", tmp_path / "elsewhere") is None


def test_resolve_without_workspace_uses_document_folder(cache, tmp_path):
    nb = tmp_path / "a" / "nb.py"
    cache.save_snapshot(nb, DB_DATA)
    assert cache.resolve_source(tmp_path / "a" / "other.py") == normalize_path(nb)
    assert cache.resolve_source(tmp_path / "b" / "other.py") is None


def test_resolve_does_not_match_sibling_folder_with_same_prefix(cache, tmp_path):
    cache.save_snapshot(tmp_path / "ws2" / "nb.py", DB_DATA)
    assert cache.resolve_source(tmp_path / "ws" / "x.py", tmp_path / "ws") is None


def test_commands_are_taken_once_in_order(cache, tmp_path):
    nb, other = tmp_path / "nb.py", tmp_path / "other.py"
    cache.push_command(nb, "run_sql", "select 1")
    cache.push_command(nb, "run_sql", "select 2")
    cache.push_command(other, "run_sql", "select 3")
    assert [c.data for c in cache.take_commands(nb)] == ["select 1", "select 2"]
    assert cache.take_commands(nb) == []
    assert [c.data for c in cache.take_commands(other)] == ["select 3"]


def test_stale_commands_are_dropped(cache, tmp_path, monkeypatch):
    nb = tmp_path / "nb.py"
    cache.push_command(nb, "run_sql", "old")
    later = time.time() + schema_cache.COMMAND_TIMEOUT_SECONDS + 1
    monkeypatch.setattr(schema_cache.time, "time", lambda: later)
    assert cache.take_commands(nb) == []


def test_session_heartbeat(cache, tmp_path, monkeypatch):
    nb = tmp_path / "nb.py"
    assert not cache.session_active(nb)
    cache.heartbeat(nb)
    assert cache.session_active(nb)

    later = time.time() + schema_cache.SESSION_TIMEOUT_SECONDS + 1
    monkeypatch.setattr(schema_cache.time, "time", lambda: later)
    assert not cache.session_active(nb)

    monkeypatch.undo()
    cache.heartbeat(nb)
    cache.end_session(nb)
    assert not cache.session_active(nb)


def test_two_handles_see_each_others_writes(tmp_path):
    # the IPython kernel and the language server each open the file themselves
    writer, reader = SchemaCache(tmp_path / "c.sqlite3"), SchemaCache(tmp_path / "c.sqlite3")
    writer.save_snapshot(tmp_path / "nb.py", DB_DATA)
    assert reader.columns(str(tmp_path / "nb.py"), "memory", "main", "orders")[0] == ("order_id", "INTEGER")


def test_concurrent_writers_and_readers(cache, tmp_path):
    nb = tmp_path / "nb.py"
    errors = []

    def write():
        try:
            for i in range(30):
                cache.save_snapshot(nb, {**DB_DATA, "cwd": str(i)})
                cache.push_command(nb, "run_sql", str(i))
        except Exception as e:
            errors.append(e)

    def read():
        try:
            taken = 0
            for _ in range(60):
                cache.source_info(nb)
                cache.columns(str(nb), "memory", "main", "orders")
                taken += len(cache.take_commands(nb))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=write), threading.Thread(target=read), threading.Thread(target=read)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


def test_env_var_overrides_location(tmp_path, monkeypatch):
    monkeypatch.setenv(schema_cache.CACHE_PATH_ENV, str(tmp_path / "custom.sqlite3"))
    assert SchemaCache().path == tmp_path / "custom.sqlite3"


def test_completer_reads_the_cache_live(tmp_path):
    from conftest import complete, make_completion_db
    from dabbler.db_stuff import get_db_data_new
    from dabbler.lsp.completer import SqlCompleter

    cache, nb = SchemaCache(tmp_path / "c.sqlite3"), str(tmp_path / "nb.py")
    db = make_completion_db()
    cache.save_snapshot(nb, get_db_data_new(db, skip_ipython=True))
    completer = SqlCompleter(cache=cache, source_file=nb)
    assert "added_later" not in complete(completer, "select * from |")

    db.execute("create table added_later(x int)")
    cache.save_snapshot(nb, get_db_data_new(db, skip_ipython=True))
    assert "added_later" in complete(completer, "select * from |")
    assert complete(completer, "select a.| from added_later a") == ["x"]
