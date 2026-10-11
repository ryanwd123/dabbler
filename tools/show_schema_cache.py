"""Prints what's in dabbler's schema cache, and which source a python file's SQL completes from.

    python tools/show_schema_cache.py [path/to/file.py ...]
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dabbler.schema_cache import SchemaCache  # noqa: E402


def main():
    cache = SchemaCache()
    print(f"schema cache: {cache.path}\n")
    with cache.read_transaction() as con:
        rows = con.execute(
            "select source_file, kind, version, updated_at, current_schema, "
            "  (select count(*) from objects o where o.source_file = s.source_file and o.kind in ('table', 'view')) "
            "from sources s order by updated_at desc"
        ).fetchall()
        sessions = dict(con.execute("select source_file, last_seen from sessions").fetchall())
    for source, kind, version, updated, schema, tables in rows:
        alive = " (IPython session running)" if time.time() - sessions.get(source, 0) < 15 else ""
        print(f"{kind:9} v{version:<4} {time.strftime('%H:%M:%S', time.localtime(updated))}  "
              f"{tables:4} tables  {schema}  {source}{alive}")
    for document in sys.argv[1:]:
        print(f"\n{document}: IPython session schema = {cache.resolve_source(Path(document).resolve())}")


if __name__ == "__main__":
    main()
