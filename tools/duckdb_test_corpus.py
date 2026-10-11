"""How much of DuckDB's own test SQL dabbler's completion grammar parses.

Reads the sqllogictest files of a DuckDB source checkout (test/sql/**/*.test),
keeps the statements the installed DuckDB parses, and reports the ones
duckdb.lark rejects, grouped by the token the parse stopped at.

Get the files with a sparse clone of the tag matching the installed DuckDB:

    git clone --depth 1 --branch v1.5.6 --filter=blob:none --sparse https://github.com/duckdb/duckdb.git duckdb-src
    git -C duckdb-src sparse-checkout set test/sql

    python tools/duckdb_test_corpus.py duckdb-src/test/sql [--show 5] [--failures failures.sql]
"""
import argparse
import collections
import re
import sys
from pathlib import Path

import duckdb
from lark.exceptions import UnexpectedCharacters, UnexpectedEOF, UnexpectedToken

sys.path.insert(0, str(Path(__file__).parent.parent))
from dabbler.lsp.parser import sql_parser  # noqa: E402

RECORD_START = re.compile(r"^(statement\s+(ok|maybe)|query\s+\S+)", re.IGNORECASE)
SKIP_START = re.compile(r"^(statement\s+error|require|load|loop|endloop|foreach|endfor|mode|onlyif|skipif|halt|concurrentloop)\b")
# sqllogictest substitutions and test-only syntax
UNSUPPORTED = re.compile(r"\$\{|__TEST_DIR__|__WORKING_DIRECTORY__|\{\w+\}")


def records(path: Path):
    """SQL of the `statement ok` and `query` records in one test file."""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    i = 0
    while i < len(lines):
        if RECORD_START.match(lines[i].strip()):
            sql = []
            i += 1
            while i < len(lines) and lines[i].strip() and lines[i].strip() != "----":
                sql.append(lines[i])
                i += 1
            text = "\n".join(sql).strip()
            if text and not UNSUPPORTED.search(text):
                yield text
        i += 1


def statements(test_dir: Path):
    con = duckdb.connect()
    seen = set()
    for path in sorted(test_dir.rglob("*.test")):
        for text in records(path):
            try:
                parts = con.extract_statements(text)
            except duckdb.Error:
                continue  # syntax DuckDB rejects too (or needs an extension)
            if len(parts) != 1:
                continue
            key = re.sub(r"\s+", " ", text).rstrip(";").lower()
            if key not in seen:
                seen.add(key)
                yield path, text.rstrip().rstrip(";")


def failure_key(sql: str, error: Exception) -> str:
    """What to group a failure by: the statement's first word and the unexpected token."""
    first = (re.match(r"\s*(\w+)", sql) or re.match(r"(.?)", sql)).group(1).upper()
    if isinstance(error, UnexpectedToken):
        token = error.token
        what = token.type if token.type in ("IDENT", "STRING", "NUMBER", "$END") else str(token.value).upper()
    elif isinstance(error, UnexpectedCharacters):
        what = f"char {sql[error.pos_in_stream]!r}"
    elif isinstance(error, UnexpectedEOF):
        what = "$END"
    else:
        what = type(error).__name__
    return f"{first:10} at {what}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("test_dir", type=Path)
    ap.add_argument("--show", type=int, default=3, help="examples per group")
    ap.add_argument("--top", type=int, default=40, help="groups to list")
    ap.add_argument("--failures", type=Path, help="write every failing statement here")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # examples aren't all ASCII

    total = 0
    groups: dict[str, list[tuple[Path, str]]] = collections.defaultdict(list)
    for path, sql in statements(args.test_dir):
        total += 1
        try:
            sql_parser.parse(sql)
        except Exception as e:
            groups[failure_key(sql, e)].append((path, sql))

    failed = sum(len(v) for v in groups.values())
    print(f"{total} distinct statements DuckDB parses; the grammar parses {total - failed} ({(total - failed) / max(total, 1):.1%})\n")
    for key, items in sorted(groups.items(), key=lambda kv: -len(kv[1]))[: args.top]:
        print(f"{len(items):6}  {key}")
        for path, sql in items[: args.show]:
            one_line = re.sub(r"\s+", " ", sql)
            print(f"          {one_line[:150]}")
    if args.failures:
        with args.failures.open("w", encoding="utf-8") as f:
            for key, items in sorted(groups.items(), key=lambda kv: -len(kv[1])):
                for path, sql in items:
                    f.write(f"-- {key} ({path.name})\n{sql};\n\n")


if __name__ == "__main__":
    main()
