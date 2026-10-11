"""Tests for function signatures and docs from duckdb_functions()."""
import duckdb

from dabbler.lsp.functions import builtin_functions, functions_from, user_function_info


def test_builtin_function_has_overloads_description_and_examples():
    info = builtin_functions()["date_trunc"]
    assert info.kind == "scalar"
    assert info.description
    assert info.examples
    assert any(s.params == ("part", "timestamp") for s in info.signatures)
    assert "date_trunc(part VARCHAR, timestamp DATE) -> TIMESTAMP" in info.markdown()


def test_functions_missing_from_the_old_docs_file_have_docs():
    functions = builtin_functions()
    for name in ("acosh", "countif", "dense_rank"):
        assert functions[name].markdown()


def test_varargs_and_table_function_named_parameters():
    functions = builtin_functions()
    assert functions["concat"].detail == "concat(value, ...)"
    read_csv = functions["read_csv"]
    assert read_csv.signatures[0].params == ("col0",)
    assert "header" in dict(read_csv.signatures[0].named)


def test_user_macros_from_a_connection():
    con = duckdb.connect()
    con.execute("create macro add_one(x) as x + 1")
    info = functions_from(con)["add_one"]
    assert info.signatures[0].params == ("x",)


def test_user_function_info_shows_the_body():
    info = user_function_info("add_one", "macro", ["x"], "(x + 1)")
    assert info.detail == "add_one(x)"
    assert "**Definition**" in info.markdown()
    assert "(x + 1)" in info.markdown()
