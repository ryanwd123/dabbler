# dabbler plan

Open work as of 2026-10-11. Completed work is in the git history.

## 1. `# dabbler use '<database file>'`

Point a file at a DuckDB database without an IPython session:

```python
# dabbler use '~/data/sales.duckdb'
```

Editing:
- Typing `# dabbler ` offers `use`; after `use '` the path completer takes over
  (`~`, absolute and relative paths, relative to the file's folder).
- Previously used database files (section 2) are offered first, as full paths.
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

## 2. Remember database files

- Database files are already sources in the schema cache (`kind = 'database'`, see
  section 1); add a table of which documents referenced which database file and
  when (from `# dabbler use` directives and from IPython sessions
  with a file database).
- `# dabbler use '` completions list these first, most recent first, with the
  table count as detail.
- Prune entries whose file no longer exists.

## 3. Grammar coverage

- Port further constructs from DuckDB's PEG grammar (`src/parser/peg/grammar/`
  in the DuckDB repo, MIT): MERGE INTO, PIVOT's full SQL-standard form, CREATE
  TABLE constraints and options, `RENAME` in star expressions, ALL as a
  subquery quantifier.
- A wider corpus from DuckDB's `test/sql/**/*.test` files would find gaps the
  hand-picked cases miss.

## 4. Path completion for `Path('...')` literals

Paths complete inside SQL strings, and `Path` variables complete, but
`Path('...')` literals in python code don't.

## Other open items

- VS Code extension: run `npm ci` and `npm run compile` to type-check the new
  extension.ts. It wasn't compiled when it changed, because node_modules wasn't
  installed. Then:
  - upgrade vscode-languageclient 8 to 9, TypeScript and eslint
  - turn on `strict` in tsconfig
  - add a packaging workflow
- License: LICENSE.txt in dabbler_vsc_ext is Apache-2.0 (from the pygls
  template), and package.json says MIT. Decide which applies.
- `Pipfile`: drop sqlparse and the zmq placeholder, move pytest and friends to
  [dev-packages], and re-lock (`pipenv lock`).
- The CI workflow hasn't run yet; push and check it, especially Qt offscreen on
  Ubuntu.
- Widen the ruff rules (pyflakes F, then pycodestyle) and fix what they find.
- Shrinking the repo needs a history rewrite (git filter-repo) to drop the
  large files; ask before doing it.

## Known limitations

- dabbler's cursors can't see the user's TEMP tables (DuckDB keeps those per
  connection). Reading the user's connection directly would drop an unfetched
  `db.execute()` result, so it's only done after cells that change the session
  state.
