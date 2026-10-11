import os
import threading
import time

import pytest

from dabbler import schema_cache
from dabbler.schema_cache import SchemaCache, normalize_path


@pytest.fixture
def cache(tmp_path):
    return SchemaCache(tmp_path / "cache.sqlite3")


DB_DATA = {"data": [["memory.main", "t", "sql", [["a", "INTEGER"]]]], "cwd": "/x", "paths": [["p", "/y"]]}


def test_snapshot_round_trip(cache, tmp_path):
    nb = tmp_path / "nb.py"
    assert cache.save_snapshot(nb, DB_DATA) is True
    snap = cache.get_snapshot(nb)
    assert snap.db_data == DB_DATA
    assert snap.source_file == normalize_path(nb)
    assert cache.get_fingerprint(nb) == snap.fingerprint


def test_unchanged_snapshot_is_not_rewritten(cache, tmp_path):
    nb = tmp_path / "nb.py"
    cache.save_snapshot(nb, DB_DATA)
    first = cache.get_snapshot(nb)
    assert cache.save_snapshot(nb, dict(DB_DATA)) is False
    assert cache.get_snapshot(nb).updated_at == first.updated_at

    assert cache.save_snapshot(nb, {**DB_DATA, "cwd": "/other"}) is True
    assert cache.get_fingerprint(nb) != first.fingerprint


def test_missing_snapshot(cache, tmp_path):
    assert cache.get_snapshot(tmp_path / "nope.py") is None
    assert cache.get_fingerprint(tmp_path / "nope.py") is None


def test_paths_are_normalized(cache, tmp_path):
    nb = tmp_path / "Sub" / "NB.py"
    cache.save_snapshot(str(nb), DB_DATA)
    spelled_differently = str(tmp_path / "Sub" / "." / "NB.py")
    if os.name == "nt":
        spelled_differently = spelled_differently.upper().replace("\\", "/")
    assert cache.get_snapshot(spelled_differently) is not None


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
    assert reader.get_snapshot(tmp_path / "nb.py").db_data == DB_DATA


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
                cache.get_snapshot(nb)
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
