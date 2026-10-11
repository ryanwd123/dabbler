"""`# dabbler use '<database file>'`: parsing the comment, completing it, and reading the file."""
import os
import subprocess
import sys
import textwrap

import duckdb
import pytest

from dabbler.db_stuff import database_file_db_data, file_databases, get_db_data_new
from dabbler.lsp.completer import SqlCompleter
from dabbler.lsp.database_files import DatabaseFiles
from dabbler.lsp.directives import directive_completions, find_use_directive, resolve_database_path
from dabbler.schema_cache import SchemaCache, database_mtime, normalize_path


# -- the comment --

@pytest.mark.parametrize("source, path", [
    ("# dabbler use '~/data/sales.duckdb'\n", "~/data/sales.duckdb"),
    ('import duckdb\n  #dabbler   use "x.db"  # trailing\n', "x.db"),
    ("# dabbler use 'C:\\data\\a b.duckdb'", "C:\\data\\a b.duckdb"),
    ("# dabbler use ''\n", ""),
])
def test_find_use_directive(source, path):
    found = find_use_directive(source)
    assert found is not None and found.path == path
    assert source[found.start:found.end] == path


@pytest.mark.parametrize("source", [
    "# dabbler used 'x.db'\n",
    "x = 1  # dabbler use 'x.db'\n",  # only whole-line comments
    "# dabbler use x.db\n",
    "# dabbler use 'x.db\n",
])
def test_not_a_use_directive(source):
    assert find_use_directive(source) is None


def test_first_directive_applies():
    assert find_use_directive("# dabbler use 'a.db'\n# dabbler use 'b.db'\n").path == "a.db"


def test_resolve_database_path(tmp_path):
    doc = str(tmp_path / "proj" / "nb.py")
    assert resolve_database_path("data/x.duckdb", doc) == os.path.normpath(tmp_path / "proj" / "data" / "x.duckdb")
    assert resolve_database_path("../x.duckdb", doc) == os.path.normpath(tmp_path / "x.duckdb")
    assert resolve_database_path("~/x.duckdb", doc) == os.path.normpath(os.path.expanduser("~/x.duckdb"))
    absolute = str(tmp_path / "abs.duckdb")
    assert resolve_database_path(absolute, None) == os.path.normpath(absolute)
    assert resolve_database_path("x.duckdb", None) is None  # untitled document
    assert resolve_database_path("  ", doc) is None


# -- completing the comment --

def test_use_is_offered_after_dabbler():
    for line, word in (("# dabbler ", ""), ("#dabbler u", "u"), ("    # dabbler us", "us")):
        found = directive_completions(line, len(line), None)
        assert found is not None and [i.label for i in found.items] == ["use"]
        assert found.items[0].insert_text == "use '"
        assert (found.start, found.end) == (len(line) - len(word), len(line))
    assert directive_completions("# dabbler x", 11, None) is None
    assert directive_completions("# dabble ", 9, None) is None


@pytest.fixture
def project(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "sales.duckdb").write_bytes(b"")
    (tmp_path / "local.duckdb").write_bytes(b"")
    (tmp_path / "nb.py").write_text("")
    return tmp_path


def test_files_relative_to_the_document(project):
    line = "# dabbler use '"
    found = directive_completions(line, len(line), str(project / "nb.py"))
    labels = [i.label for i in found.items]
    assert labels == ["data/", "local.duckdb"]  # folders first; .py files are left out
    assert (found.start, found.end) == (len(line), len(line))


def test_files_in_a_typed_folder_replace_the_last_segment(project):
    line = "# dabbler use 'data/sa' # note"
    column = line.index("sa'") + 2
    found = directive_completions(line, column, str(project / "nb.py"))
    assert [i.label for i in found.items] == ["sales.duckdb"]
    assert line[found.start:found.end] == "sa"


def test_double_quotes_and_absolute_paths(project):
    line = f'# dabbler use "{project.as_posix()}/da'
    found = directive_completions(line, len(line), None)
    assert [i.label for i in found.items] == ["data/"]


def test_recent_databases_come_first_until_a_folder_is_typed(project):
    recent = [("C:\\x\\recent.duckdb", 3), ("/y/one.duckdb", 1)]
    line = "# dabbler use '"
    found = directive_completions(line, len(line), str(project / "nb.py"), recent)
    assert [i.label for i in found.items] == ["C:\\x\\recent.duckdb", "/y/one.duckdb", "data/", "local.duckdb"]
    assert [i.detail for i in found.items[:2]] == ["3 tables", "1 table"]
    assert sorted(found.items, key=lambda i: i.sort_text) == found.items

    line = "# dabbler use 'data/"
    found = directive_completions(line, len(line), str(project / "nb.py"), recent)
    assert [i.label for i in found.items] == ["sales.duckdb"]


# -- reading database files --

def make_db_file(path, *tables):
    con = duckdb.connect(str(path))
    for table in tables:
        con.execute(f"create table {table}(id int, name varchar)")
    con.close()


@pytest.fixture
def cache(tmp_path):
    return SchemaCache(tmp_path / "cache.sqlite3")


def table_names(cache, source):
    return {r.name for r in cache.objects(source, kinds=["table", "view"])}


def test_database_file_is_read_and_completes(cache, tmp_path):
    db_path = tmp_path / "sales.duckdb"
    make_db_file(db_path, "orders", "customers")
    found = DatabaseFiles(cache).source(str(db_path), cwd=str(tmp_path))
    assert found.error is None and found.source == normalize_path(db_path)
    assert table_names(cache, found.source) == {"orders", "customers"}
    info = cache.source_info(found.source)
    assert info.current_schema == "sales.main" and info.cwd == str(tmp_path)

    completer = SqlCompleter(cache=cache, source_file=found.source)
    result = completer.route_completion2(len("select * from "), "select * from ", " ")
    assert {"orders", "customers"} <= {i.label for i in result.items}


def test_database_file_is_read_again_only_when_it_changes(cache, tmp_path, monkeypatch):
    db_path = tmp_path / "sales.duckdb"
    make_db_file(db_path, "orders")
    files = DatabaseFiles(cache)
    reads = []
    original = files._read
    monkeypatch.setattr(files, "_read", lambda *a: (reads.append(a[0]), original(*a)))

    files.source(str(db_path))
    files.source(str(db_path))
    assert len(reads) == 1

    make_db_file(db_path, "returns")
    os.utime(db_path, (database_mtime(db_path) + 5,) * 2)  # coarse file system clocks
    found = files.source(str(db_path))
    assert len(reads) == 2
    assert table_names(cache, found.source) == {"orders", "returns"}


def test_missing_database_file(cache, tmp_path):
    found = DatabaseFiles(cache).source(str(tmp_path / "nope.duckdb"))
    assert found.source is None and "not found" in found.error


def test_file_that_is_not_a_database(cache, tmp_path):
    path = tmp_path / "notes.duckdb"
    path.write_text("hello")
    found = DatabaseFiles(cache).source(str(path))
    assert found.source is None and found.error


HOLD_OPEN = textwrap.dedent("""
    import sys, duckdb
    con = duckdb.connect(sys.argv[1])
    print("open", flush=True)
    sys.stdin.read()
""")


def test_locked_file_uses_rows_saved_by_the_session(cache, tmp_path):
    db_path = tmp_path / "sales.duckdb"
    make_db_file(db_path, "orders")
    holder = subprocess.Popen([sys.executable, "-c", HOLD_OPEN, str(db_path)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "open"
        files = DatabaseFiles(cache)
        found = files.source(str(db_path))
        assert found.source is None and found.error  # locked, and nothing saved yet

        # what the IPython session with the file open saves (see SchemaSync.publish_database_files)
        db_data = {
            "databases": ["s"], "schemas": ["s.main"], "functions": [], "paths": [],
            "data": [("s.main", "orders", "table", [["id", "INTEGER"]])],
            "current_schema": "s.main", "cwd": str(tmp_path), "file_search_path": None, "loaded_extensions": [],
        }
        cache.save_snapshot(db_path, database_file_db_data(db_data, "s", str(db_path)), kind="database")
        found = files.source(str(db_path))
        assert found.error is None and found.source == normalize_path(db_path)
        assert table_names(cache, found.source) == {"orders"}
    finally:
        holder.communicate("")


def test_session_database_files_are_saved_as_if_opened_alone(tmp_path):
    db_path = tmp_path / "lake.duckdb"
    make_db_file(db_path, "events")
    con = duckdb.connect()
    con.execute(f"attach '{db_path.as_posix()}' as lk")
    con.execute("create table memory_table(a int)")
    con.execute("create macro lk.main.double_it(x) as x * 2")
    assert file_databases(con) == [("lk", str(db_path))]

    data = database_file_db_data(get_db_data_new(con, skip_ipython=True), "lk", str(db_path))
    assert data["databases"] == ["lake"] and data["current_schema"] == "lake.main"
    assert [(r[0], r[1]) for r in data["data"]] == [("lake.main", "events")]
    assert [(r[0], r[1]) for r in data["functions"]] == [("lake.main", "double_it")]
    assert data["schemas"] == ["lake.main"]
