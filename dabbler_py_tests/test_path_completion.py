"""File path completions: paths in SQL strings, and pathlib objects in python code."""
import logging

import duckdb
import pytest

from conftest import complete
from dabbler.db_stuff import get_db_data_new
from dabbler.lsp.completer import SqlCompleter
from conftest import split_cursor
from dabbler.lsp.completion_utils import apply_path_parents, path_at, pathlib_completetions

log = logging.getLogger("test")


@pytest.fixture
def data_dir(tmp_path):
    (tmp_path / "sales.csv").write_text("a,b\n1,2\n")
    (tmp_path / "notes.py").write_text("")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "orders.parquet").write_bytes(b"")
    return tmp_path


@pytest.fixture
def path_completer(data_dir, monkeypatch):
    # SqlCompleter changes into its cwd so relative paths resolve; undo that after the test
    monkeypatch.chdir(data_dir)
    db_data = get_db_data_new(duckdb.connect(), skip_ipython=True)
    db_data["cwd"] = str(data_dir)
    return SqlCompleter(db_data)


def test_sql_relative_path(path_completer):
    labels = complete(path_completer, "select * from './|")
    assert {"sales.csv", "sub/"} <= set(labels)
    assert "notes.py" not in labels


def test_sql_relative_path_subdir_and_prefix(path_completer):
    assert complete(path_completer, "select * from read_parquet('./sub/|") == ["orders.parquet"]
    assert complete(path_completer, "select * from './sa|") == ["sales.csv"]


def test_single_char_dir_is_not_a_relative_path(path_completer):
    # the old pattern treated any "'x/" like "'./"
    assert "sales.csv" not in complete(path_completer, "select * from 'a/|")


@pytest.mark.parametrize(
    "text, expected",
    [
        pytest.param("df = read(data_dir / '", {"sales.csv", "sub/"}, id="slash"),
        pytest.param("x = data_dir.joinpath('", {"sales.csv", "sub/"}, id="joinpath"),
        pytest.param("x = data_dir.joinpath('sub') / '", {"orders.parquet"}, id="joinpath_then_slash"),
        pytest.param("x = data_dir.joinpath('sub').parent / 'sa", {"sales.csv"}, id="parent_chain_with_prefix"),
    ],
)
def test_pathlib_completions(data_dir, text, expected):
    items = pathlib_completetions(text, {"data_dir": str(data_dir)}, log)
    assert {x.label for x in items} == expected


def test_pathlib_completions_ignore_unknown_variables(data_dir):
    assert pathlib_completetions("x = other / '", {"data_dir": str(data_dir)}, log) is None


def test_apply_path_parents_does_not_evaluate_code(tmp_path):
    # only .parent and .joinpath('literal') steps are applied; nothing else is run
    assert apply_path_parents(tmp_path, ".joinpath('a').parent.joinpath(\"b\")") == tmp_path / "b"
    assert apply_path_parents(tmp_path, ".__class__") == tmp_path


@pytest.fixture
def awkward_dir(data_dir):
    for name in ["my file (1).csv", "a&b #2.csv", "données.csv", "x" * 120 + ".csv"]:
        (data_dir / name).write_text("")
    (data_dir / "sub" / "deeper").mkdir()
    return data_dir


@pytest.mark.parametrize(
    "sql, expected",
    [
        pytest.param("select * from 'sa|", ["sales.csv"], id="from_string_without_dot_slash"),
        pytest.param("select * from read_csv('sa|", ["sales.csv"], id="read_function_argument"),
        pytest.param("copy orders to 'sa|", ["sales.csv"], id="copy_to"),
        pytest.param("select * from 'sub/|", ["deeper/", "orders.parquet"], id="folders_first"),
        pytest.param("select * from 'sub\\|", ["deeper/", "orders.parquet"], id="backslash"),
        pytest.param("select * from './sub/../sa|", ["sales.csv"], id="dot_dot"),
        pytest.param("select * from 'my f|", ["my file (1).csv"], id="space_and_parens"),
        pytest.param("select * from 'a&|", ["a&b #2.csv"], id="ampersand_and_hash"),
        pytest.param("select * from 'don|", ["données.csv"], id="unicode"),
        pytest.param("select * from 'xxxxx|", ["x" * 120 + ".csv"], id="long_name"),
        pytest.param("select * from 'missing/|", [], id="missing_folder"),
    ],
)
def test_sql_string_paths(awkward_dir, path_completer, sql, expected):
    assert complete(path_completer, sql) == expected


def test_absolute_path(awkward_dir, path_completer):
    folder = str(awkward_dir / "sub").replace("\\", "/")
    assert complete(path_completer, f"select * from '{folder}/or|") == ["orders.parquet"]


def test_home_directory(awkward_dir, path_completer, monkeypatch):
    monkeypatch.setenv("HOME", str(awkward_dir))
    monkeypatch.setenv("USERPROFILE", str(awkward_dir))
    assert complete(path_completer, "select * from '~/sa|") == ["sales.csv"]


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("select * from t where note = 'sa|'", id="value_string"),
        pytest.param("select * from t where a in ('x', 'sa|", id="list_of_values"),
        pytest.param("select 'it''s sa|", id="escaped_quote"),
    ],
)
def test_other_strings_are_not_paths(sql):
    text, pos = split_cursor(sql)
    assert path_at(text, pos) is None


def test_replace_span_covers_the_file_name_segment(path_completer):
    text, pos = split_cursor("select * from './sub/my f|ile.csv' x")
    start, end = path_completer.replace_span(pos, text)
    assert text[start:end] == "my file.csv"
