# dabbler plan

Open work as of 2026-10-11. Completed work is in the git history.

## 1. Grammar coverage

- Port further constructs from DuckDB's PEG grammar (`src/parser/peg/grammar/`
  in the DuckDB repo, MIT): MERGE INTO, PIVOT's full SQL-standard form, CREATE
  TABLE constraints and options, `RENAME` in star expressions, ALL as a
  subquery quantifier.
- A wider corpus from DuckDB's `test/sql/**/*.test` files would find gaps the
  hand-picked cases miss.

## 2. Path completion for `Path('...')` literals

Paths complete inside SQL strings, and `Path` variables complete, but
`Path('...')` literals in python code don't.

## Other open items

- License: LICENSE.txt in dabbler_vsc_ext is Apache-2.0 (from the pygls
  template), and package.json says MIT. Decide which applies.
- The CI workflows haven't run yet (tests, and the new VS Code extension
  packaging); push and check them, especially Qt offscreen on Ubuntu. Publishing
  the extension from a `vsc-v*` tag needs a `VSCE_PAT` secret.
- Widen the ruff rules further (pycodestyle) and fix what they find.
- The extension's packager (vsce 3) has a dev-only audit finding; vsce 4
  fixes it but needs Node 22.
- Shrinking the repo needs a history rewrite (git filter-repo) to drop the
  large files; ask before doing it.

## Known limitations

- dabbler's cursors can't see the user's TEMP tables (DuckDB keeps those per
  connection). Reading the user's connection directly would drop an unfetched
  `db.execute()` result, so it's only done after cells that change the session
  state.
- While the language server reads a `# dabbler use` database file (only when
  it changed, and briefly), another process can't open it for writing.
