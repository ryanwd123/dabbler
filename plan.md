# dabbler plan

Status as of 2026-10-11. Done so far: test suite (`dabbler_py_tests`, ~470 tests),
SQLite schema cache instead of ZMQ, pygls 2, completions routed on the parser's
state at the cursor (regexes only as a fallback), and no more rebuilt copy of the
session's database in the language server (below).

## Done: language server no longer rebuilds the session's database

The language server used to recreate every table of the IPython session in an
in-memory DuckDB and `describe` CTEs and subqueries against it. That failed
silently for table macros, user types (ENUMs were faked as `ENUM('DUMMY')`) and
anything else that can't be recreated from names and types, and cost ~0.5s per
schema change at 1,500 tables. Now:

- The completion list is built from the snapshot plus DuckDB's built-in catalog
  (functions, table functions, system views), cached per set of loaded extensions.
- Columns of CTEs, subqueries, LATERAL subqueries, `VALUES` lists and table macros
  are worked out from the parse tree (`ProjectionResolver` in `lsp/parser.py`):
  select-list names and aliases, `*` with `EXCLUDE`/`REPLACE`, `t.*`,
  `COLUMNS('regex')` and `COLUMNS(* EXCLUDE ...)`, column alias lists, the first
  branch of set operations. Types are kept for plain column references; computed
  columns have no type, and unaliased expressions get no completion (by choice).
- Table macro bodies come along in the snapshot and are parsed like any query.
- DuckDB still runs in the language server for one thing: describing table
  functions such as `read_csv('x.csv')`, on a plain connection with the session's
  extensions and file search path.

Result: building a completer for 1,500 tables went from ~0.56s to 0.04s; a
completion averages ~2.6ms (was ~6.9ms).

## Done: the language server queries the schema cache directly

The IPython extension used to save one JSON snapshot per file, which the
language server loaded and turned into an in-memory completion map on every
change. Now the schema is stored as rows in `schema_cache.sqlite3`:

| Table | Rows |
|---|---|
| `sources` | one per source file: version (bumped on any change), current schema, cwd, file search path, extensions |
| `objects` | databases, schemas, tables, views, functions, table macros (with their body), with lower-cased names for matching and a hash per object |
| `columns` | name, type and position per table or view |
| `paths` | the session's `pathlib.Path` variables |

- Saving rewrites only objects whose hash changed, in one transaction: at 1,500
  tables, 14ms when nothing changed and 17ms when a table is added (164ms for the
  first save).
- The language server's `Catalog` (`lsp/catalog.py`) queries these rows per
  request, inside one read transaction, with lookups memoized for the request.
  DuckDB's built-in catalog stays in memory (built once per set of extensions).
- Completers are recreated when a source's version changes (~5ms), which only
  refreshes settings and cached table-function columns; tables and columns are
  read live.
- Old version-1 cache files (JSON snapshots) are upgraded in place on first open.

## Done: IPython extension and GUI (2026-10-11)

- `%load_ext dabbler.ext` picks the only connection or one named db/con/conn,
  and otherwise asks for `%dabbler <name>`. `%dabbler refresh` re-reads the
  schema. `%unload_ext` / `%reload_ext` clean up: windows, worker threads, cell
  events and the session. An existing QApplication (matplotlib) is reused.
- The schema is checked after every cell (not only VS Code cells). `USE` /
  `SET search_path` / `file_search_path` in a cell are copied to dabbler's
  cursors. The heartbeat is a thread, so long cells don't end the session.
- GUI queries run on a worker thread with Cancel (Ctrl+.). The row limit is
  applied with `rel.limit()`, DDL is reported, and export writes
  CSV/Parquet/Excel.
- Result table: search before the first query, `>5`/`<5`, `-text` on
  categoricals, sort kept when searching, and stale worker answers are all
  fixed. String column widths are measured again.
- Known limitation: dabbler's cursors can't see the user's TEMP tables (DuckDB
  keeps those per connection). Reading the user's connection directly would
  drop an unfetched `db.execute()` result, so it's only done after cells that
  change the session state.

## Done: completion features

- Function docs come from `duckdb_functions()` (`lsp/functions.py`), not
  scraped JSON: overloads with types, descriptions, examples, and a macro's
  parameters and body. The schema cache stores function parameters (cache
  version 3).
- Signature help (`lsp/signature_help.py`) and hover (`lsp/hover.py`).
- Function items insert `name($0)`. Half-typed quoted names (`"Order Li`)
  complete and replace the quote.

## Done: finding SQL strings, syntax errors, formatting

- SQL strings are found with python's tokenizer (`lsp/sql_regions.py`), not
  regexes. It handles f-strings (fields become same-length `___` names),
  `'''`/raw strings, the `query=` keyword, and half-typed code. Unterminated
  strings end at their line, and indentation errors restart the scan. Python
  escapes in non-raw strings are blanked to same-length whitespace. Positions
  are UTF-16 as LSP requires.
- Syntax errors come from DuckDB's parser (`lsp/diagnostics.py`:
  `extract_statements`, with the position from `json_serialize_sql`). Every SQL
  string in the file is checked 0.3s after typing stops. f-strings with
  fields are skipped. Never bind python parameters in DuckDB calls from the
  language server's threads: DuckDB may import pandas under its lock and
  deadlock (seen with `json_serialize_sql(?)`).
- Table function describes (`read_csv(...)`) are interrupted after 1s, skip
  URLs without httpfs, and keep an LRU of 256.
- The format command uses sqlglot's DuckDB dialect on the whole SQL string,
  keeps its indentation, and refuses f-strings and escaped strings.

## 1. DataFrames

Done: completions don't look for DataFrames (the old detection broke on Python
3.13, PEP 667). The GUI lists pandas/polars/arrow DataFrames and DuckDB relations
from `user_ns` (`gui_stuff.dataframe_kind`, matched by package and class name,
since pandas 3 and DuckDB 1.5 moved their classes), and `gui_query` registers
the ones a query names on its cursor (relations become temp views).

## 2. Grammar coverage

Done (2026-10-11):
- Expressions are a precedence ladder (OR, AND, NOT, predicates, operators, + -,
  * /, ^, unary, postfix, primary) instead of one flat ambiguous rule. LALR
  conflicts dropped from about 1,300 to about 330, and `a BETWEEN 2/3 AND 3/2`
  (corpus 21.sql) parses.
- Added:
  - statements: DELETE, TRUNCATE, EXPLAIN, DESCRIBE/SHOW/SUMMARIZE,
    transactions, CREATE INDEX/SECRET, EXPORT/IMPORT, VACUUM/ANALYZE, COMMENT
    ON, SET VARIABLE, VALUES
  - query clauses: WINDOW + OVER w, GROUPING SETS, SEMI/ANTI joins, set
    operations with ALL / BY NAME, LIMIT n%, DISTINCT ON, `* LIKE`
  - expressions: lambdas (arrow and `lambda x:`), named args, rows, list
    comprehensions, TRIM/SUBSTRING forms, TRY_CAST, typed literals, factorial
  - types: any name with params and arrays (INT, VARCHAR(10), INT[], user
    types)
  - strings: `''`, `E'...'`, `$$...$$`
- `test_grammar_coverage.py` has 117 statements (all parse; new gaps go in
  `KNOWN_GAPS` as strict xfails). It also checks all 492 of DuckDB's function
  examples that are statements against the installed DuckDB, so a new DuckDB
  version's syntax shows up as a test failure.
- Diagnostics no longer depend on the grammar (they use DuckDB's parser), so
  remaining gaps only affect completions, which fall back to the regex router.

Still open:
- Port further constructs from DuckDB's PEG grammar (`src/parser/peg/grammar/`
  in the DuckDB repo, MIT): MERGE INTO, PIVOT's full SQL-standard form, CREATE
  TABLE constraints and options, `RENAME` in star expressions, ALL as a
  subquery quantifier.
- A wider corpus from DuckDB's `test/sql/**/*.test` files would find gaps the
  hand-picked cases miss.

## 3. `# dabbler use '<database file>'`

Point a file at a DuckDB database without an IPython session:

```python
# dabbler use '~/data/sales.duckdb'
```

Editing:
- Typing `# dabbler ` offers `use`; after `use '` the path completer takes over
  (`~`, absolute and relative paths, relative to the file's folder).
- Previously used database files (section 5) are offered first, as full paths.
- A diagnostic on the comment when the file doesn't exist.

Resolution, for each document: its `# dabbler use` directive, else the IPython
session's schema for that file, else the newest one in the workspace (today's rule).

Reading the schema:
- The language server opens the file `read_only`, extracts the schema with
  `get_db_data_new(..., skip_ipython=True)`, and saves it to the schema cache as a
  source of its own (`sources.kind = 'database'`, keyed by the database path),
  using the same `save_snapshot` as IPython sessions. Re-read it when the file's modification time changes.
- On Windows a DuckDB file can't be opened by a second process at all while
  another process has it open read-write (checked: "File is already open in ...
  python.exe"). So when IPython has the file open, use the rows already saved for
  that database path instead. To keep it current, the IPython extension also
  saves its schema under the database file's path when its connection is a
  file database (`duckdb_databases().path`).
- `~` is expanded with `os.path.expanduser` for reading; the comment keeps what
  the user wrote. DuckDB itself also accepts `~` on Windows (checked).

Tests: directive parsing, completion after `# dabbler ` and `use '`, resolution
order, locked-file fallback, end-to-end through the server.

## 4. Path completion

Done (2026-10-11): any SQL string holding a path completes files, either when
it looks like a path (`./`, `../`, `~/`, `C:\`, `/`, `a/b`) or when SQL reads a
file there (`FROM '...'`, `read_csv('...')`, `COPY ... TO '...'`, `ATTACH`).
Names with any characters work, and items replace the whole last segment
(text_edit). Folders come first and reopen the list. Still open: `Path('...')`
literals in python code (only `Path` variables complete today).

## 5. Remember database files

- Database files are already sources in the schema cache (`kind = 'database'`, see
  section 3); add a table of which documents referenced which database file and
  when (from `# dabbler use` directives and from IPython sessions
  with a file database).
- `# dabbler use '` completions list these first, most recent first, with the
  table count as detail.
- Prune entries whose file no longer exists.

## Other open items

- `Pipfile` still lists the empty `zmq` placeholder package (the pyproject
  dependency was removed); re-lock when convenient.
