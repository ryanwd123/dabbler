# Changelog

## dabbler 0.9.0 and VS Code extension 0.0.9

A large update to completions, a new way to get them without an IPython
session, and a more reliable IPython extension and window.

### Breaking changes
- **Python 3.12 or newer** is required.
- **The window's dependencies are now an extra.** `pip install dabbler` installs
  only what the language server needs (duckdb, lark, pygls, sqlglot). For
  `%load_ext dabbler.ext` and the window, install `pip install "dabbler[gui]"`.
  Excel export is `pip install "dabbler[excel]"`.
- **The IPython extension and the language server talk through a SQLite file**
  instead of ZMQ. Upgrade dabbler in both the notebook environment and the
  language server's environment (usually the same one). The schema cache
  rebuilds itself on first use.
- **The VS Code extension needs VS Code 1.82 or newer.**

### New
- **`# dabbler use '<database file>'`**: name a DuckDB database file in a comment
  and the file's SQL completes from that database, without an IPython session.
  `# dabbler ` offers `use`, and after `use '` come database files used before,
  then the files in the folder you type. Relative paths are relative to the
  python file, and `~` works. The file is re-read when it changes. While a
  notebook with dabbler has it open, the notebook's view of it is used.
- **Tables created in the file complete elsewhere in it.** `CREATE TABLE t (...)`,
  `CREATE OR REPLACE TABLE t AS SELECT ...` and `CREATE VIEW` complete with their
  columns in the file's other SQL before they've been run.
- **Signature help** for function calls, and **hover** docs for functions,
  tables, CTEs, aliases and columns, from DuckDB's own function catalog.
- **Syntax errors** from DuckDB's own parser, for every SQL string in the file.
- **Format** the SQL string under the cursor (sqlglot, DuckDB dialect).
- **File path completion** in any SQL string that holds a path (`FROM '...'`,
  `read_csv('...')`, `COPY ... TO '...'`, `ATTACH`), including names with spaces.
- **Setting completion** after `SET`, `SET GLOBAL`/`SESSION`/`LOCAL` and `RESET`.
- The window lists pandas, polars and arrow DataFrames and DuckDB relations from
  the session, and queries can read them. Queries run in the background and can
  be canceled (Ctrl+.); results export to CSV, Parquet or Excel.

### Better completions
- Completions follow where the cursor is in the parsed SQL instead of regular
  expressions, and work out the columns of CTEs, subqueries, `VALUES` lists, table
  functions and table macros from the SQL itself, including `* EXCLUDE`,
  `REPLACE`, `RENAME`, `COLUMNS(...)` and column alias lists.
- The grammar now parses about 98% of the statements in DuckDB's own test suite
  (up from about 92%), including MERGE, standard PIVOT/UNPIVOT, CREATE TABLE
  constraints, PREPARE/EXECUTE, WITH before INSERT/UPDATE/DELETE, `COLLATE`,
  `ARRAY[...]`, lambdas, window clauses, GROUPING SETS and more.
- Half-typed quoted names (`"Order Li`) complete; functions insert `name()` with
  the cursor inside.
- SQL strings are found with python's tokenizer: f-strings, raw and
  triple-quoted strings, and the `query=` keyword all work.
- Much faster with large schemas: building completions for 1,500 tables went
  from about 0.56 s to 0.04 s.

### IPython extension and window
- `%load_ext dabbler.ext` finds your DuckDB connection (the only one, or one named
  `db`, `con` or `conn`); `%dabbler <variable>` picks another, `%dabbler refresh`
  re-reads the schema, and `%unload_ext` / `%reload_ext` clean up properly.
- The schema is checked after every cell. `USE` and `SET search_path` /
  `file_search_path` carry over to dabbler's queries, and long-running cells no
  longer end the session.
- Fixes to the result table's search, sorting and column widths.
- Canceling a query (Ctrl+. or the cancel button) right after starting it, or
  starting another one, now stops it; before, it could keep running in the
  background.
- Closing a window cancels its query and frees the window right away.
- No more crash ("Aborted", "terminate called") when the IPython kernel exits on
  Linux after the window was used.

### VS Code extension 0.0.9
- Starts reliably when VS Code opens, and offers to install dabbler when it's
  missing from the selected Python environment.
- The output channel shows which dabbler, Python and schema cache the server uses.
- SQL syntax highlighting matches the strings the server treats as SQL.
- Licensed MIT, like the python package.

### For developers
- Test suite of about 1,150 tests, with CI on Ubuntu and Windows (Python 3.12 and
  3.13), ruff, and a workflow that builds the extension.
- `tools/duckdb_test_corpus.py` measures grammar coverage against DuckDB's test
  SQL; `tools/show_schema_cache.py` shows what's in the schema cache.
