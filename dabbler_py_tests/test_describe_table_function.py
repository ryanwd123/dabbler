"""Tests for describing table function calls like read_csv('x.csv') for their columns."""
import time

import duckdb

from dabbler.lsp import parser
from dabbler.lsp.parser import SqlParserNew


def test_describes_a_local_file(tmp_path):
    (tmp_path / "data.csv").write_text("a,b\n1,x\n")
    p = SqlParserNew(duckdb.connect(), file_search_path=str(tmp_path))
    assert p.describe_table_function("read_csv('data.csv')") == [("a", "BIGINT"), ("b", "VARCHAR")]


def test_remote_urls_are_skipped_without_httpfs():
    p = SqlParserNew(duckdb.connect())
    start = time.perf_counter()
    assert p.describe_table_function("read_parquet('https://example.invalid/x.parquet')") is None
    assert time.perf_counter() - start < 0.5
    assert p.describe_cache == {}


def test_cache_keeps_the_most_recent_calls(monkeypatch):
    monkeypatch.setattr(parser, "DESCRIBE_CACHE_SIZE", 3)
    p = SqlParserNew(duckdb.connect())
    calls = [f"range({n})" for n in range(5)]
    for sql in calls:
        assert p.describe_table_function(sql) == [("range", "BIGINT")]
    assert list(p.describe_cache) == calls[-3:]


def test_failed_describe_gives_none_and_is_cached():
    p = SqlParserNew(duckdb.connect())
    assert p.describe_table_function("read_csv('does_not_exist.csv')") is None
    assert "read_csv('does_not_exist.csv')" in p.describe_cache
