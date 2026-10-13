"""Hover docs for SQL statements, condensed from DuckDB's documentation.

The text is adapted from the DuckDB docs (github.com/duckdb/duckdb-web,
docs/current/sql/statements), Copyright Stichting DuckDB Foundation, MIT License.

Each doc has a head: a regex of the statement's leading keywords, where a space
stands for any whitespace. Hovering any word of the head shows the doc. The first
doc whose head covers the word wins, so specific heads come before general ones
(SET VARIABLE before SET).
"""
import re
from dataclasses import dataclass
from typing import Union

DOCS_URL = "https://duckdb.org/docs/current/sql/statements/"
TEMP = "(?:TEMP |TEMPORARY |LOCAL TEMP |LOCAL TEMPORARY )?"
re_line_comment = re.compile(r"--[^\n]*")
re_statement_start = re.compile(r"(?:^|[;(]|\bEXPLAIN|\bEXPLAIN\s+ANALYZE)$", re.IGNORECASE)


@dataclass(frozen=True)
class StatementDoc:
    head: re.Pattern
    markdown: str
    start: bool
    """The head only counts at the start of a statement, since its first word
    also appears inside other statements (UPDATE in ON CONFLICT DO UPDATE)."""


def _doc(head: str, page: str, markdown: str, start: bool = False) -> StatementDoc:
    pattern = re.compile(r"\b(?:" + head.replace(" ", r"\s+") + r")\b", re.IGNORECASE)
    return StatementDoc(pattern, markdown.strip() + f"\n\n[DuckDB docs]({DOCS_URL}{page})", start)


def at_statement_start(sql: str, pos: int) -> bool:
    before = re_line_comment.sub("", sql[:pos]).rstrip()
    return re_statement_start.search(before) is not None


PIVOT_DOC = """
**PIVOT**: turns the distinct values of columns into new columns, aggregating the rows of each.

```sql
PIVOT dataset
ON columns [IN (values)]
USING aggregates
[GROUP BY rows]
[ORDER BY ...] [LIMIT n];

-- SQL standard form, inside FROM:
FROM dataset PIVOT (aggregates FOR column IN (values) ... [GROUP BY rows])
```

`ON` without `IN` creates a column for every distinct value. Columns not in `ON` or `USING` are grouped by unless `GROUP BY` says otherwise. `PIVOT_WIDER` is an alias.

**Examples**
```sql
PIVOT cities ON year USING sum(population);
PIVOT cities ON year IN (2000, 2010) USING sum(population) GROUP BY country;
FROM cities PIVOT (sum(population) FOR year IN (2000, 2010) GROUP BY country);
```
"""

UNPIVOT_DOC = """
**UNPIVOT**: turns columns into rows: one name column and one or more value columns.

```sql
UNPIVOT dataset
ON columns
INTO NAME name_column VALUE value_columns
[ORDER BY ...] [LIMIT n];

-- SQL standard form, inside FROM:
FROM dataset UNPIVOT [INCLUDE NULLS] (value_column FOR name_column IN (columns))
```

`ON COLUMNS(* EXCLUDE (id))` unpivots every other column. Rows with `NULL` values are dropped unless `INCLUDE NULLS`. `PIVOT_LONGER` is an alias.

**Examples**
```sql
UNPIVOT monthly_sales ON jan, feb, mar INTO NAME month VALUE sales;
UNPIVOT monthly_sales ON COLUMNS(* EXCLUDE (empid, dept)) INTO NAME month VALUE sales;
FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb, mar));
```
"""

STATEMENTS = [
    _doc(
        "SELECT",
        "select",
        """
**SELECT**: retrieves rows from the database.

```sql
[WITH ...]
SELECT [DISTINCT] select_list
FROM tables
    [USING SAMPLE sample]
[WHERE condition]
[GROUP BY groups | GROUP BY ALL]
[HAVING group_filter]
    [WINDOW window]
    [QUALIFY qualify_filter]
[ORDER BY order_expression | ORDER BY ALL]
[LIMIT n] [OFFSET n]
```

`FROM tbl` alone is short for `SELECT * FROM tbl`.

**Examples**
```sql
SELECT i, sum(j) FROM tbl GROUP BY i;
SELECT * FROM tbl ORDER BY i DESC LIMIT 3;
SELECT * FROM t1 JOIN t2 USING (a, b);
SELECT DISTINCT city FROM addresses;
```
""",
    ),
    _doc(
        "INSERT (?:OR (?:REPLACE|IGNORE) )?INTO",
        "insert",
        """
**INSERT**: inserts new rows into a table.

```sql
INSERT [OR REPLACE | OR IGNORE] INTO table [(columns)] [BY POSITION | BY NAME]
    { VALUES (...), ... | query | DEFAULT VALUES }
    [ON CONFLICT [(columns)] DO NOTHING
        | ON CONFLICT [(columns)] DO UPDATE SET column = expr, ... [WHERE condition]]
    [RETURNING * | expressions]
```

| clause | meaning |
|---|---|
| `BY NAME` | match the query's column names to the table's, not their order. Missing columns get their default. |
| `ON CONFLICT DO NOTHING` | skip rows that break a `PRIMARY KEY` or `UNIQUE` constraint. Same as `INSERT OR IGNORE`. |
| `ON CONFLICT DO UPDATE` | update the existing row instead (upsert); `EXCLUDED.col` is the new value. `INSERT OR REPLACE` updates every column. |
| `RETURNING` | return the inserted rows. |

**Examples**
```sql
INSERT INTO tbl VALUES (1), (2), (3);
INSERT INTO tbl (i) SELECT * FROM other_tbl;
INSERT INTO tbl BY NAME (SELECT 42 AS b, 32 AS a);
INSERT INTO tbl VALUES (1, 84) ON CONFLICT DO UPDATE SET j = EXCLUDED.j;
```
""",
    ),
    _doc(
        "UPDATE EXTENSIONS",
        "update_extensions",
        """
**UPDATE EXTENSIONS**: updates installed extensions from the repositories they came from.

```sql
UPDATE EXTENSIONS [(name, ...)]
```

Updated extensions are not reloaded: restart the process to use them.

**Examples**
```sql
UPDATE EXTENSIONS;
UPDATE EXTENSIONS (httpfs, spatial);
```
""",
        start=True,
    ),
    _doc(
        "UPDATE",
        "update",
        """
**UPDATE**: changes the values of rows in a table.

```sql
UPDATE table [AS alias]
SET column = expr, ...
[FROM other_tables]
[WHERE condition]
[RETURNING * | expressions]
```

Only rows matching `WHERE` change; without it, every row changes. `FROM` joins other tables, matched up in `WHERE`.

**Examples**
```sql
UPDATE tbl SET i = 0 WHERE i IS NULL;
UPDATE original SET value = new.value FROM new WHERE original.key = new.key;
UPDATE tbl SET j = j + 10 WHERE i = 1 RETURNING *;
```
""",
        start=True,
    ),
    _doc(
        "DELETE FROM",
        "delete",
        """
**DELETE**: removes rows from a table.

```sql
DELETE FROM table [AS alias]
[USING other_tables]
[WHERE condition]
[RETURNING * | expressions]
```

Without `WHERE`, every row is deleted. Deleted space is reclaimed at the next `CHECKPOINT`.

**Examples**
```sql
DELETE FROM tbl WHERE i = 2;
DELETE FROM tbl;
DELETE FROM employees RETURNING name;
```
""",
    ),
    _doc(
        "TRUNCATE(?: TABLE)?",
        "delete#the-truncate-statement",
        """
**TRUNCATE**: removes all rows from a table, like `DELETE FROM` without `WHERE`.

```sql
TRUNCATE [TABLE] table
```

**Examples**
```sql
TRUNCATE tbl;
```
""",
        start=True,
    ),
    _doc(
        "MERGE INTO",
        "merge_into",
        """
**MERGE INTO**: inserts, updates or deletes rows of a table by matching them against a source. Unlike `INSERT ... ON CONFLICT` it needs no primary key.

```sql
MERGE INTO target [AS alias]
USING source [AS alias]
{ ON condition | USING (columns) }
WHEN MATCHED [AND condition] THEN { UPDATE [SET column = expr, ...] | DELETE | DO NOTHING }
WHEN NOT MATCHED [BY TARGET] [AND condition] THEN { INSERT [BY NAME] [VALUES (...)] | DO NOTHING }
WHEN NOT MATCHED BY SOURCE [AND condition] THEN { UPDATE SET ... | DELETE }
[RETURNING merge_action, *]
```

`UPDATE` and `INSERT` without a list copy the whole source row.

**Examples**
```sql
MERGE INTO people
    USING (SELECT 3 AS id, 'Sarah' AS name, 95000.0 AS salary) AS upserts
    ON (upserts.id = people.id)
    WHEN MATCHED THEN UPDATE
    WHEN NOT MATCHED THEN INSERT;
MERGE INTO people
    USING (SELECT 1 AS id, 98000.0 AS salary) AS salary_updates
    USING (id)
    WHEN MATCHED THEN UPDATE SET salary = salary_updates.salary;
```
""",
    ),
    _doc(
        f"CREATE (?:OR REPLACE )?{TEMP}TABLE(?: IF NOT EXISTS)?",
        "create_table",
        """
**CREATE TABLE**: creates a table.

```sql
CREATE [OR REPLACE] [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] name (
    column type [NOT NULL] [DEFAULT value] [PRIMARY KEY | UNIQUE]
        [CHECK (expr)] [REFERENCES other (column)],
    column [type] [GENERATED ALWAYS] AS (expr) [VIRTUAL],
    [PRIMARY KEY (columns)],
    [UNIQUE (columns)],
    [FOREIGN KEY (columns) REFERENCES other (columns)],
    [CONSTRAINT name CHECK (expr)]
);
CREATE [OR REPLACE] [TEMP] TABLE [IF NOT EXISTS] name AS query [WITH NO DATA];
```

| option | meaning |
|---|---|
| `OR REPLACE` | drop the table first if it exists. |
| `IF NOT EXISTS` | do nothing if the table exists. |
| `TEMP` | visible only to this connection, dropped when it closes; lives in `temp.main`. |
| `AS query` | create the table from a query's result (CTAS). Constraints are not allowed. |
| `WITH NO DATA` | copy only the column names and types. |

**Examples**
```sql
CREATE TABLE t1 (id INTEGER PRIMARY KEY, j VARCHAR);
CREATE OR REPLACE TABLE t1 AS FROM read_csv('path/file.csv');
CREATE TABLE t1 (x FLOAT, two_x AS (2 * x));
CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, FOREIGN KEY (t1_id) REFERENCES t1 (id));
```
""",
    ),
    _doc(
        f"CREATE (?:OR REPLACE )?{TEMP}VIEW(?: IF NOT EXISTS)?",
        "create_view",
        """
**CREATE VIEW**: defines a view: a named query that runs each time the view is referenced.

```sql
CREATE [OR REPLACE] [TEMP | TEMPORARY] VIEW [IF NOT EXISTS] name [(column_names)]
    AS query
```

The query behind a view is in `duckdb_views()`. Dropping a table a view uses leaves the view broken.

**Examples**
```sql
CREATE VIEW view1 AS SELECT * FROM tbl;
CREATE OR REPLACE VIEW view1 AS SELECT 42;
CREATE VIEW view1(a) AS SELECT 42;
```
""",
    ),
    _doc(
        f"CREATE (?:OR REPLACE )?{TEMP}SEQUENCE(?: IF NOT EXISTS)?",
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
    _doc(
        f"CREATE (?:OR REPLACE )?{TEMP}(?:MACRO|FUNCTION)(?: IF NOT EXISTS)?",
        "create_macro",
        """
**CREATE MACRO**: defines a scalar or table macro. `CREATE FUNCTION` is an alias.

```sql
CREATE [OR REPLACE] [TEMP | TEMPORARY] MACRO [IF NOT EXISTS] name
    (param [type] [:= default], ...) AS expression
  | (param, ...) AS TABLE query
  [, (other params) AS ...]
```

| part | meaning |
|---|---|
| `AS expression` | a scalar macro, usable wherever an expression is. |
| `AS TABLE query` | a table macro, used in `FROM` like a table function. |
| `:= default` | makes the parameter optional; such parameters come last. |
| more `(params) AS ...` | overloads, picked by the number and types of arguments. |

Call with positional arguments or `name := value`.

**Examples**
```sql
CREATE MACRO add(a, b) AS a + b;
CREATE MACRO add_default(a, b := 5) AS a + b;
CREATE MACRO dynamic_table(col1_value, col2_value) AS TABLE
    SELECT col1_value AS column1, col2_value AS column2;
CREATE MACRO add_x
    (a, b) AS a + b,
    (a, b, c) AS a + b + c;
```
""",
    ),
    _doc(
        "CREATE (?:UNIQUE )?INDEX(?: IF NOT EXISTS)?",
        "create_index",
        """
**CREATE INDEX**: creates an index on columns or expressions of a table.

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] name
    ON table [USING index_type] (column | (expression), ...)
    [WITH (option = value, ...)]
```

`UNIQUE` rejects duplicate values. `CREATE INDEX` has no `OR REPLACE`.

**Examples**
```sql
CREATE UNIQUE INDEX films_id_idx ON films (id);
CREATE INDEX gy_idx ON films (genre, year);
CREATE INDEX i_index ON integers ((j + k));
```
""",
    ),
    _doc(
        "CREATE (?:OR REPLACE )?SCHEMA(?: IF NOT EXISTS)?",
        "create_schema",
        """
**CREATE SCHEMA**: creates a schema. The default schema is `main`.

```sql
CREATE [OR REPLACE] SCHEMA [IF NOT EXISTS] [database.]name
```

**Examples**
```sql
CREATE SCHEMA s1;
CREATE SCHEMA IF NOT EXISTS s2;
CREATE TABLE s1.t (id INTEGER PRIMARY KEY, other_id INTEGER);
```
""",
    ),
    _doc(
        "CREATE (?:OR REPLACE )?(?:PERSISTENT |TEMP |TEMPORARY )?SECRET(?: IF NOT EXISTS)?",
        "create_secret",
        """
**CREATE SECRET**: stores credentials, such as for S3, Azure, GCS or HTTP, in the Secrets Manager.

```sql
CREATE [OR REPLACE] [PERSISTENT | TEMPORARY] SECRET [IF NOT EXISTS] [name] (
    TYPE type,
    [PROVIDER provider,]
    [SCOPE 'prefix',]
    key value, ...
)
```

| option | meaning |
|---|---|
| `TYPE` | the service: `s3`, `gcs`, `r2`, `azure`, `http`, `huggingface`, ... |
| `PROVIDER` | where the credentials come from: `config` (the default, given inline) or `credential_chain` (environment, config files). |
| `SCOPE` | the path prefix the secret applies to, such as `'s3://my-bucket'`. |
| `PERSISTENT` | saved to disk (`~/.duckdb/stored_secrets`) unencrypted; the default is temporary. |

List secrets with `FROM duckdb_secrets()`.

**Examples**
```sql
CREATE SECRET (TYPE s3, KEY_ID 'my_key', SECRET 'my_secret', REGION 'us-east-1');
CREATE PERSISTENT SECRET my_s3 (TYPE s3, PROVIDER credential_chain, SCOPE 's3://my-bucket');
```
""",
    ),
    _doc(
        "DROP (?:PERSISTENT |TEMP |TEMPORARY )?SECRET(?: IF EXISTS)?",
        "create_secret#syntax-for-drop-secret",
        """
**DROP SECRET**: removes a secret from the Secrets Manager.

```sql
DROP [PERSISTENT | TEMPORARY] SECRET [IF EXISTS] name
```

**Examples**
```sql
DROP SECRET my_s3;
DROP PERSISTENT SECRET IF EXISTS my_s3;
```
""",
        start=True,
    ),
    _doc(
        "CREATE (?:OR REPLACE )?TYPE(?: IF NOT EXISTS)?",
        "create_type",
        """
**CREATE TYPE**: defines a new named type: an `ENUM`, `STRUCT`, `UNION` or an alias of another type.

```sql
CREATE [OR REPLACE] TYPE [IF NOT EXISTS] name AS
    ENUM ('value', ...) | STRUCT(field type, ...) | UNION(tag type, ...) | type
```

Types are listed in `duckdb_types()`.

**Examples**
```sql
CREATE TYPE mood AS ENUM ('happy', 'sad', 'curious');
CREATE TYPE many_things AS STRUCT(k INTEGER, l VARCHAR);
CREATE TYPE x_index AS INTEGER;
```
""",
    ),
    _doc(
        "ALTER TABLE(?: IF EXISTS)?",
        "alter_table",
        """
**ALTER TABLE**: changes the schema of an existing table.

```sql
ALTER TABLE [IF EXISTS] name
    ADD [COLUMN] [IF NOT EXISTS] column type [DEFAULT value]
  | DROP [COLUMN] [IF EXISTS] column
  | ALTER [COLUMN] column [SET [DATA]] TYPE type [USING expr]
  | ALTER [COLUMN] column SET DEFAULT value | DROP DEFAULT
  | ALTER [COLUMN] column SET NOT NULL | DROP NOT NULL
  | RENAME [COLUMN] column TO new_column
  | RENAME TO new_name
  | ADD PRIMARY KEY (columns)
```

Changes are transactional. Views using the table are not updated by renames. Columns with an index on them can't be dropped or retyped.

**Examples**
```sql
ALTER TABLE integers ADD COLUMN k INTEGER DEFAULT 10;
ALTER TABLE integers ALTER i SET DATA TYPE VARCHAR USING concat(i, '_', j);
ALTER TABLE integers RENAME i TO ii;
ALTER TABLE integers RENAME TO integers_old;
```
""",
    ),
    _doc(
        "ALTER VIEW(?: IF EXISTS)?",
        "alter_view",
        """
**ALTER VIEW**: changes an existing view.

```sql
ALTER VIEW [IF EXISTS] name RENAME TO new_name
```

Other views that use the view are not updated.

**Examples**
```sql
ALTER VIEW view1 RENAME TO view2;
```
""",
    ),
    _doc(
        "DROP (?:TABLE|VIEW|SEQUENCE|SCHEMA|INDEX|MACRO TABLE|MACRO|FUNCTION|TYPE)(?: IF EXISTS)?",
        "drop",
        """
**DROP**: removes a catalog entry created with `CREATE`.

```sql
DROP TABLE | VIEW | SEQUENCE | SCHEMA | INDEX | MACRO | MACRO TABLE | FUNCTION | TYPE
    [IF EXISTS] name [CASCADE | RESTRICT]
```

| option | meaning |
|---|---|
| `IF EXISTS` | no error if the entry doesn't exist. |
| `CASCADE` | also drop entries that depend on it, like a schema's tables. |
| `RESTRICT` | refuse if anything depends on it (the default). |

Views are not tracked as dependents: dropping their table leaves them broken.

**Examples**
```sql
DROP TABLE tbl;
DROP VIEW IF EXISTS view1;
DROP SCHEMA myschema CASCADE;
```
""",
        start=True,
    ),
    _doc(
        "COPY",
        "copy",
        """
**COPY**: moves data between tables and CSV, Parquet or JSON files, or between databases.

```sql
COPY table [(columns)] FROM 'file' [(option value, ...)];
COPY table [(columns)] | (query) TO 'file' [(option value, ...)];
COPY FROM DATABASE source TO target [(SCHEMA)];
```

| option | meaning |
|---|---|
| `FORMAT` | `csv`, `parquet`, `json` or `blob`; by default from the file extension. |
| `HEADER`, `DELIMITER`, `QUOTE`, `NULLSTR` | CSV layout. |
| `COMPRESSION` | such as `zstd`, `snappy` or `gzip`. |
| `PARTITION_BY (columns)` | write a Hive-partitioned directory of files. |
| `OVERWRITE_OR_IGNORE`, `OVERWRITE`, `APPEND` | what to do with existing files when partitioning. |
| `ROW_GROUP_SIZE` | rows per Parquet row group. |

`COPY ... FROM` appends to an existing table. `COPY ... TO` overwrites the file.

**Examples**
```sql
COPY lineitem FROM 'lineitem.csv' (DELIMITER '|');
COPY lineitem TO 'lineitem.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM tbl) TO 'out.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY orders TO 'orders' (FORMAT parquet, PARTITION_BY (year, month));
COPY FROM DATABASE db1 TO db2;
```
""",
        start=True,
    ),
    _doc(
        "ATTACH(?: DATABASE)?(?: OR REPLACE| IF NOT EXISTS)?",
        "attach",
        """
**ATTACH**: opens another database file so its tables can be read and written.

```sql
ATTACH [DATABASE] [OR REPLACE | IF NOT EXISTS] 'path' [AS alias] [(option, ...)]
```

| option | meaning |
|---|---|
| `READ_ONLY` | open read-only; the default for `http(s)://` and `s3://` paths. |
| `TYPE` | another engine's file, such as `sqlite` or `postgres`. |
| `ENCRYPTION_KEY` | open or create an encrypted database. |
| `STORAGE_VERSION` | the oldest DuckDB version that must read the file, such as `'v1.2.0'` or `'latest'`. |

The alias defaults to the file name. Attachments last for the session only. Refer to tables as `alias.schema.table`.

**Examples**
```sql
ATTACH 'file.db';
ATTACH 'file.db' AS file_db (READ_ONLY);
ATTACH 'sqlite_file.db' AS sqlite_db (TYPE sqlite);
ATTACH ':memory:' AS memory_db;
```
""",
        start=True,
    ),
    _doc(
        "DETACH(?: DATABASE)?(?: IF EXISTS)?",
        "attach#detach",
        """
**DETACH**: closes an attached database and releases its file locks.

```sql
DETACH [DATABASE] [IF EXISTS] alias
```

The default database can't be detached: `USE` another one first.

**Examples**
```sql
DETACH file_db;
```
""",
        start=True,
    ),
    _doc(
        "USE",
        "use",
        """
**USE**: sets the default database and schema for names that aren't fully qualified.

```sql
USE database | database.schema | schema
```

**Examples**
```sql
USE memory;
USE duck.main;
```
""",
        start=True,
    ),
    _doc(
        "(?:SET|RESET) VARIABLE",
        "set_variable",
        """
**SET VARIABLE**: assigns a SQL-level variable, read with `getvariable('name')`.

```sql
SET VARIABLE name = expression;
RESET VARIABLE name;
```

The value can be any type, or a query's result. `getvariable` returns `NULL` for an unset variable.

**Examples**
```sql
SET VARIABLE my_var = 30;
SELECT 20 + getvariable('my_var') AS total;
SET VARIABLE list_of_files = (SELECT list(file) FROM csv_files);
RESET VARIABLE my_var;
```
""",
        start=True,
    ),
    _doc(
        "(?:SET|RESET)(?: GLOBAL| SESSION)?",
        "set",
        """
**SET / RESET**: changes a configuration option, or restores its default.

```sql
SET [GLOBAL | SESSION] option { = | TO } value;
RESET [GLOBAL | SESSION] option;
```

`GLOBAL` applies to the whole DuckDB instance, `SESSION` only to this connection. Without either, the option's own default scope applies (usually `GLOBAL`). Settings are not saved in the database file. Read one with `current_setting('option')`.

**Examples**
```sql
SET memory_limit = '10GB';
SET threads TO 1;
SET SESSION default_collation = 'nocase';
RESET threads;
```
""",
        start=True,
    ),
    _doc(
        "(?:FORCE )?INSTALL",
        "load_and_install",
        """
**INSTALL**: downloads an extension so it can be loaded.

```sql
[FORCE] INSTALL name [FROM repository]
```

`FORCE` reinstalls it. `FROM community` gets a community extension. `LOAD` installs known extensions automatically.

**Examples**
```sql
INSTALL httpfs;
INSTALL h3 FROM community;
```
""",
        start=True,
    ),
    _doc(
        "LOAD",
        "load_and_install#load",
        """
**LOAD**: loads an installed extension into the session, installing a known extension first if needed.

```sql
LOAD name
```

**Examples**
```sql
LOAD httpfs;
LOAD spatial;
```
""",
        start=True,
    ),
    _doc(
        "PREPARE",
        "prepare",
        """
**PREPARE**: creates a prepared statement, run later with `EXECUTE`.

```sql
PREPARE name AS statement
```

Parameters are `?`, `$1`, `$2`, ... or named, like `$min_age`.

**Examples**
```sql
PREPARE query_person AS
    SELECT * FROM person WHERE starts_with(name, $name_start) AND age >= $minimum_age;
PREPARE add_values AS SELECT $1 + $2;
```
""",
        start=True,
    ),
    _doc(
        "EXECUTE",
        "prepare#execute",
        """
**EXECUTE**: runs a prepared statement.

```sql
EXECUTE name [(value, ... | param := value, ...)]
```

**Examples**
```sql
EXECUTE add_values(20, 22);
EXECUTE query_person(name_start := 'B', minimum_age := 40);
```
""",
        start=True,
    ),
    _doc(
        "DEALLOCATE(?: PREPARE)?",
        "prepare#deallocate",
        """
**DEALLOCATE**: removes a prepared statement.

```sql
DEALLOCATE [PREPARE] name
```

**Examples**
```sql
DEALLOCATE query_person;
```
""",
        start=True,
    ),
    _doc(
        "EXPLAIN(?: ANALYZE)?",
        "profiling",
        """
**EXPLAIN**: shows a query's plan.

```sql
EXPLAIN query;
EXPLAIN ANALYZE query;
```

`EXPLAIN` shows the plan with estimated row counts, without running the query. `EXPLAIN ANALYZE` runs it and shows the actual row counts and time spent in each operator.

**Examples**
```sql
EXPLAIN SELECT * FROM tbl WHERE i > 3;
EXPLAIN ANALYZE SELECT count(*) FROM tbl;
```
""",
        start=True,
    ),
    _doc(
        "DESCRIBE",
        "describe",
        """
**DESCRIBE**: shows the columns and types of a table, view or query.

```sql
DESCRIBE table | query
```

**Examples**
```sql
DESCRIBE tbl;
DESCRIBE SELECT * FROM tbl;
```
""",
        start=True,
    ),
    _doc(
        "SHOW(?: ALL)?(?: TABLES| DATABASES| SCHEMAS)?",
        "show",
        """
**SHOW**: lists tables, databases or schemas, or describes a table like `DESCRIBE`.

```sql
SHOW TABLES;
SHOW ALL TABLES;
SHOW DATABASES;
SHOW SCHEMAS;
SHOW table;
```

`SHOW TABLES` lists the current schema's tables; `SHOW ALL TABLES` lists every attached database's.

**Examples**
```sql
SHOW ALL TABLES;
SHOW tbl;
```
""",
        start=True,
    ),
    _doc(
        "SUMMARIZE",
        "summarize",
        """
**SUMMARIZE**: returns summary statistics for each column of a table, view or query: min, max, approximate unique count, average, standard deviation, quartiles, row count and null percentage.

```sql
SUMMARIZE table | query
```

**Examples**
```sql
SUMMARIZE tbl;
SUMMARIZE SELECT * FROM tbl WHERE i > 3;
```
""",
        start=True,
    ),
    _doc(
        "CALL",
        "call",
        """
**CALL**: runs a table function and returns its result. `FROM function()` does the same.

```sql
CALL table_function(arguments)
```

**Examples**
```sql
CALL duckdb_functions();
CALL pragma_table_info('tbl');
```
""",
        start=True,
    ),
    _doc(
        "(?:FORCE )?CHECKPOINT",
        "checkpoint",
        """
**CHECKPOINT**: writes the write-ahead log (WAL) into the database file.

```sql
[FORCE] CHECKPOINT [database]
```

Checkpoints happen automatically as the WAL grows. A plain `CHECKPOINT` fails while other transactions run; `FORCE` waits for them. Checkpointing reclaims some of the space of deleted rows.

**Examples**
```sql
CHECKPOINT;
CHECKPOINT file_db;
FORCE CHECKPOINT;
```
""",
        start=True,
    ),
    _doc(
        "VACUUM(?: ANALYZE)?",
        "vacuum",
        """
**VACUUM**: recomputes a column's distinct-value statistics; otherwise kept for PostgreSQL compatibility. It does not reclaim space: use `CHECKPOINT`.

```sql
VACUUM [ANALYZE] [table[(columns)]]
```

**Examples**
```sql
VACUUM my_table(my_column);
```
""",
        start=True,
    ),
    _doc(
        "ANALYZE",
        "analyze",
        """
**ANALYZE**: recomputes table statistics, which the optimizer uses to order joins. Worth running after large changes to the data.

```sql
ANALYZE [table]
```

**Examples**
```sql
ANALYZE;
```
""",
        start=True,
    ),
    _doc(
        "COMMENT ON",
        "comment_on",
        """
**COMMENT ON**: attaches a comment to a catalog entry.

```sql
COMMENT ON TABLE | COLUMN | VIEW | INDEX | SEQUENCE | TYPE | MACRO | MACRO TABLE name
    IS 'text' | NULL
```

`NULL` removes the comment. Read comments from the `comment` column of `duckdb_tables()`, `duckdb_columns()` and the like. Schemas, databases and entries with dependents can't have comments.

**Examples**
```sql
COMMENT ON TABLE test_table IS 'very nice table';
COMMENT ON COLUMN test_table.test_table_column IS 'very nice column';
COMMENT ON TABLE test_table IS NULL;
```
""",
        start=True,
    ),
    _doc(
        "EXPORT DATABASE",
        "export",
        """
**EXPORT DATABASE**: writes the whole database (schema and data) to a directory, to be read back with `IMPORT DATABASE`.

```sql
EXPORT DATABASE [database TO] 'directory' [(FORMAT csv | parquet, option value, ...)]
```

The directory gets `schema.sql`, `load.sql` and one data file per table. Options are those of `COPY ... TO`.

**Examples**
```sql
EXPORT DATABASE 'target_directory';
EXPORT DATABASE 'target_directory' (FORMAT parquet, COMPRESSION zstd);
```
""",
        start=True,
    ),
    _doc(
        "IMPORT DATABASE",
        "export#import-database",
        """
**IMPORT DATABASE**: loads a directory written by `EXPORT DATABASE` by running its `schema.sql` and `load.sql`.

```sql
IMPORT DATABASE 'directory'
```

Import into a fresh database: the exported `CREATE` statements fail if the objects exist.
""",
        start=True,
    ),
    _doc("(?:PIVOT|PIVOT_WIDER)", "pivot", PIVOT_DOC, start=True),
    _doc(r"PIVOT(?=\s*[(])", "pivot", PIVOT_DOC),  # FROM t PIVOT (...)
    _doc("(?:UNPIVOT|PIVOT_LONGER)(?: INCLUDE NULLS)?", "unpivot", UNPIVOT_DOC, start=True),
    _doc(r"UNPIVOT(?: INCLUDE NULLS)?(?=\s*[(])", "unpivot", UNPIVOT_DOC),  # FROM t UNPIVOT (...)
    _doc(
        "BEGIN(?: TRANSACTION)?|START TRANSACTION|COMMIT|ROLLBACK|ABORT",
        "transactions",
        """
**Transactions**: group statements so they take effect together or not at all.

```sql
BEGIN TRANSACTION;
...
COMMIT;      -- make the changes permanent and visible to others
ROLLBACK;    -- or discard them (ABORT does the same)
```

DuckDB gives snapshot isolation. Statements sent together in one batch already run in a single implicit transaction.

**Examples**
```sql
BEGIN TRANSACTION;
INSERT INTO person VALUES ('Ada', 52);
COMMIT;
```
""",
        start=True,
    ),
]


def statement_doc(sql: str, start: int, end: int) -> Union[tuple[int, int, str], None]:
    """The doc of the statement whose leading keywords include sql[start:end],
    with the span of those keywords."""
    for doc in STATEMENTS:
        for m in doc.head.finditer(sql):
            if m.start() > end:
                break
            if m.start() <= start and end <= m.end() and (not doc.start or at_statement_start(sql, m.start())):
                return m.start(), m.end(), doc.markdown
    return None
