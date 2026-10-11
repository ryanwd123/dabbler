"""File path completions: './' inside SQL, and pathlib objects in python code."""
import logging

import duckdb
import pytest

from conftest import complete
from dabbler.db_stuff import get_db_data_new
from dabbler.lsp.completer import SqlCompleter
from dabbler.lsp.completion_utils import apply_path_parents, pathlib_completetions

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
    assert {"sales.csv", "sub"} <= set(labels)
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
        pytest.param("df = read(data_dir / '", {"sales.csv", "sub"}, id="slash"),
        pytest.param("x = data_dir.joinpath('", {"sales.csv", "sub"}, id="joinpath"),
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
