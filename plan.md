# dabbler plan

Open work as of 2026-10-11. Completed work is in the git history.

## 1. Grammar coverage

The grammar parses 98.1% of the 25,041 distinct statements in DuckDB 1.5.6's
test suite that DuckDB itself parses (`tools/duckdb_test_corpus.py`; the
docstring says how to get the files). The largest remaining groups:
- names with more than two parts: `db.schema.table.col`, `db.schema.f()`,
  `CREATE MACRO db.schema.m`, `ALTER TABLE t ADD COLUMN s.a.b` (about 80)
- a VALUES branch in a set operation: `select ... union all values (...)` (16)
- prefix aliases in FROM: `FROM t : alias` (19)
- casts and subscripts on `COLUMNS(*)` and quoted names: `COLUMNS(*)::VARCHAR`,
  `"t"."col"['a']` (20)
- reserved words as column names after a dot: `t.when`, `AS end` (16)
- `IN ((subquery), (subquery))`, `GROUP BY *` / `ORDER BY *`, a string as an
  alias (`t('k')`), trailing commas in VALUES rows
- test-only syntax (`EXPORT_STATE`, `check_peg_parser`) isn't worth adding.

Adding rules raises the LALR conflict count (403 now, 327 before this round);
each new conflict was checked to resolve the way SQL means.

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
