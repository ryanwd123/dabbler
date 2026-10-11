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

## 1. Remove DataFrame completions

Completions for pandas/polars DataFrames and DuckDB relations in the IPython
namespace stopped working after a DuckDB upgrade, and tables/views/temp tables
cover the same need. Remove the feature rather than fix it.

- `db_stuff.get_dataframe_data`: drop the DataFrame part (the `locals().__setitem__`
  trick and `describe` per DataFrame). Keep the `pathlib.Path` variable scan, which
  feeds path completions in python code.
- `db_data["dataframes"]`: stop producing it. (`make_db`, which turned DataFrames
  into tables, is gone, so the language server already ignores them; what's left
  is the IPython side.)
- Tests: remove the `dataframes` assertions; add one that a DataFrame in the
  namespace doesn't show up in completions.
- Decide separately: the GUI's table list also shows DataFrames, and
  `gui_main.query_db` makes them queryable with the same `locals()` trick, which
  breaks on Python 3.13+. Either remove that too, or switch it to
  `cursor.register(name, df)`.

## 2. Make the grammar cover DuckDB's statements

Goal: every statement DuckDB accepts parses, so completions work everywhere and
syntax-error diagnostics stop flagging valid SQL. Where the grammar has a gap the
completer falls back to the regex router, which is less accurate.

Current coverage (a sample of 76 common statements): 41 parse. Missing, roughly
in order of how often they show up in notebooks:

| Area | Missing |
|---|---|
| Statements | `DELETE`, `EXPLAIN [ANALYZE]`, `DESCRIBE`/`SHOW`/`SUMMARIZE`, `BEGIN`/`COMMIT`/`ROLLBACK`, `CREATE [UNIQUE] INDEX`, `CREATE SECRET`, `EXPORT`/`IMPORT DATABASE`, `VACUUM`, `ANALYZE` |
| SELECT clauses | `QUALIFY`, `WINDOW w AS (...)` + `OVER w`, `LIMIT n%` |
| Set operations | `EXCEPT`, `INTERSECT`, `UNION [ALL] BY NAME` |
| Joins | `SEMI`, `ANTI` |
| Grouping | `GROUPING SETS`, (`CUBE`/`ROLLUP` parse) |
| Special function syntax | `TRIM(BOTH 'x' FROM a)`, `SUBSTRING(a FROM 2 FOR 3)`, `POSITION('a' IN b)` |
| Expressions | lambdas `x -> x + 1`, named args `f(a := 1)`, array types `::INT[]`, `E'...'` and `$$...$$` strings |
| DDL details | `ALTER TABLE ... ADD COLUMN b INT`, macro defaults `m(a, b := 2)`, `ATTACH ... (READ_ONLY)` |
| Known corpus failure | `test_files/sql_tst/21.sql`: `(CASE ... END) BETWEEN 2.000/3.000 AND 3.000/2.000` |

### Use DuckDB's PEG grammar as the reference

DuckDB has a new PEG parser (experimental in 1.5 with `CALL enable_peg_parser();`,
the default in v2.0; it has driven the CLI's autocomplete since v1.2). Its grammar
is in the DuckDB repo (MIT license) under `src/parser/peg/grammar/statements/`:
42 `.gram` files, one per statement (`select.gram`, `expression.gram`,
`delete.gram`, `pivot.gram`, `merge_into.gram`, ...). That's the most complete and
current description of DuckDB's syntax there is.

dabbler can't load those files as they are: completions depend on Lark's LALR
mode, which is what reports the rules expected at the cursor, and PEG grammars
(ordered choice, unlimited lookahead) don't convert to LALR mechanically. Options:

1. Use the `.gram` files as the spec and port constructs into `duckdb.lark` by
   hand, in the priority order of the table above. Lowest risk; the corpus tests
   catch conflicts and regressions. Start here.
2. Write a converter from `.gram` to Lark syntax, then resolve the LALR conflicts
   it produces by hand. More work up front, but makes following DuckDB's grammar
   changes much easier. Worth evaluating once option 1 shows how many conflicts
   typical rules cause.
3. Run the PEG grammar directly with a small Python PEG interpreter that reports
   what it expects at the cursor. Most faithful, but replaces the parser the
   completer is built on; only if 1 and 2 stall.

Keep a note of the DuckDB commit each ported rule came from, so later grammar
changes upstream can be diffed.

Approach:
- Build a statement corpus from DuckDB's docs and its `test/sql/**/*.test` files
  (thousands of real statements), and add a coverage test that reports the parse
  rate, with known gaps listed as strict xfails like `21.sql` is now.
- Add constructs in the order above. Every new keyword also becomes a keyword
  completion automatically (they come from the grammar).
- Watch for LALR conflicts: adding `USING` joins needed `USING SAMPLE` to become a
  single token. Run the full suite after each grammar change; the corpus tests
  catch regressions.

Done in this round: `QUALIFY`, and columns for correlated `LATERAL` subqueries.

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
snapshot for that file, else the newest snapshot in the workspace (today's rule).

Reading the schema:
- The language server opens the file `read_only`, extracts the schema with
  `get_db_data_new(..., skip_ipython=True)`, and stores it in the schema cache
  under the database path. Re-read it when the file's modification time changes.
- On Windows a DuckDB file can't be opened by a second process at all while
  another process has it open read-write (checked: "File is already open in ...
  python.exe"). So when IPython has the file open, use the cached snapshot for
  that database path instead. To keep it current, the IPython extension also
  saves its snapshot under the database file's path when its connection is a
  file database (`duckdb_databases().path`).
- `~` is expanded with `os.path.expanduser` for reading; the comment keeps what
  the user wrote. DuckDB itself also accepts `~` on Windows (checked).

Tests: directive parsing, completion after `# dabbler ` and `use '`, resolution
order, locked-file fallback, end-to-end through the server.

## 4. Make the path completer reliable

Problems today:
- Only `'./` triggers path completion in SQL. `'~/`, `'../`, `'C:/...`, `'/abs`
  and paths with `\` don't.
- The allowed characters are a fixed list (`[\w./ ,:-]`), so names with `(`, `)`,
  `&`, `#`, `+`, `=`, `[`, `@`, `'` stop completion.
- Labels with spaces or punctuation: VS Code replaces only the "word" before the
  cursor, so completing `my file.csv` after `'./my f` can duplicate text. Items
  need a `text_edit` that replaces from the last `/` to the cursor.
- In python code it only works for `Path` variables, not `Path('...')` literals.

Approach:
- In SQL, use the parser's tokens: when the cursor is inside a string literal
  (`cursor_context(...).kind == "string"`), the path is the string's text up to
  the cursor. No regex over the line.
- One path-resolution function shared by SQL strings, `# dabbler use`, and python
  `Path` expressions: expands `~`, accepts `/` and `\`, resolves relative paths
  against the session's working directory or the document's folder.
- Return `text_edit`s covering the typed segment.
- Tests with awkward names: spaces, `()`, `&`, `#`, unicode, very long names,
  directories vs files, and a missing parent directory.

## 5. Remember database files

- A `databases` table in the schema cache: path, last used, and the documents
  that referenced it (from `# dabbler use` directives and from IPython sessions
  with a file database).
- `# dabbler use '` completions list these first, most recent first, with the
  table count as detail.
- Prune entries whose file no longer exists.

## Other open items

- `Pipfile` still lists the empty `zmq` placeholder package (the pyproject
  dependency was removed); re-lock when convenient.
- The schema only refreshes after cells that have a `cell_id` (VS Code sets one,
  terminal IPython doesn't). Decide whether terminal sessions should refresh too.
