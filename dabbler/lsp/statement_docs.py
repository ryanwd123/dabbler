"""Hover docs for SQL statements, condensed from DuckDB's documentation.

The text is adapted from the DuckDB docs (github.com/duckdb/duckdb-web,
docs/current/sql/statements), Copyright Stichting DuckDB Foundation, MIT License.
"""
import re
from dataclasses import dataclass
from typing import Union

DOCS_URL = "https://duckdb.org/docs/stable/sql/statements/"


@dataclass(frozen=True)
class StatementDoc:
    head: re.Pattern
    """The statement's leading keywords; hovering any of them shows the doc."""
    markdown: str


def _doc(head: str, page: str, markdown: str) -> StatementDoc:
    pattern = re.compile(r"\b" + head.replace(" ", r"\s+") + r"\b", re.IGNORECASE)
    return StatementDoc(pattern, markdown.strip() + f"\n\n[DuckDB docs]({DOCS_URL}{page})")


STATEMENTS = [
    _doc(
        "CREATE (?:OR REPLACE )?(?:TEMP(?:ORARY)? )?SEQUENCE",
        "create_sequence",
        """
**CREATE SEQUENCE**: creates a new sequence number generator.

```sql
CREATE [OR REPLACE] [TEMP | TEMPORARY] SEQUENCE [IF NOT EXISTS] name
    [INCREMENT [BY] increment]
    [MINVALUE minvalue | NO MINVALUE]
    [MAXVALUE maxvalue | NO MAXVALUE]
    [START [WITH] start]
    [[NO] CYCLE]
```

| option | meaning |
|---|---|
| `INCREMENT BY` | added to the current value for each new value; negative makes a descending sequence. Default 1. |
| `MINVALUE` | lowest value. Default 1 ascending, -(2^63 - 1) descending. |
| `MAXVALUE` | highest value. Default 2^63 - 1 ascending, -1 descending. |
| `START WITH` | first value. Default `minvalue` ascending, `maxvalue` descending. |
| `CYCLE` | wrap around at the limit instead of raising an error. Default `NO CYCLE`. |
| `TEMP` | dropped at the end of the session. |

Get values with `nextval('name')` and `currval('name')`.

**Examples**
```sql
CREATE SEQUENCE serial START WITH 1 INCREMENT BY 2;
CREATE SEQUENCE id_sequence START 1;
CREATE TABLE tbl (id INTEGER PRIMARY KEY DEFAULT nextval('id_sequence'), s VARCHAR);
```
""",
    ),
]


def statement_doc(sql: str, start: int, end: int) -> Union[tuple[int, int, str], None]:
    """The doc of the statement whose leading keywords include sql[start:end],
    with the span of those keywords."""
    for doc in STATEMENTS:
        for m in doc.head.finditer(sql):
            if m.start() <= start and end <= m.end():
                return m.start(), m.end(), doc.markdown
            if m.start() > end:
                break
    return None
