#%%
import pytest
import logging
from dabbler.db_stuff import get_db_data_new
from dabbler.lsp.db_data import make_completion_map, make_db
from dabbler.lsp.completer import SqlCompleter
from pathlib import Path
import duckdb
#set logging to print debug messages

# logging.basicConfig(level=logging.DEBUG)
# log = logging.getLogger(__name__)

def make_test_db():
    db = duckdb.connect(":memory:")
    db.install_extension('autocomplete')
    db.load_extension('autocomplete')
    db.sql(
    f"""--sql,
    attach ':memory:' as attached_db;
    create SCHEMA test_schema;
    create SCHEMA attached_db.attached_schema;
    CREATE OR REPLACE TABLE test_table(
        column1 VARCHAR,
        column2 INTEGER,
    );
    CREATE OR REPLACE TABLE test_schema.schema_table(
        column1 VARCHAR,
        column2 INTEGER,
    );
    CREATE OR REPLACE TABLE attached_db.attached_schema.db_table(
        column1 VARCHAR,
        column2 INTEGER,
    );
    CREATE SCHEMA v;
    CREATE OR REPLACE FUNCTION addition(a, b) as a + b;
    CREATE OR REPLACE FUNCTION v.add(a, b) as a + b;
    CREATE OR REPLACE MACRO v.my_table_macro_in_v() as table select 1;
    CREATE OR REPLACE MACRO my_table_macro() as table select 1;
    create or replace view test_view as select * from duckdb_columns;
    create or replace view test_schema.test_view2 as select * from duckdb_columns;
    create or replace view v.test_view3 as select * from duckdb_columns;


    """
    )

    return db

@pytest.fixture
def test_db():
    return make_test_db()

@pytest.fixture
def sql_completer(test_db):
    db_data = get_db_data_new(test_db)
    return SqlCompleter(db_data)

def test_completion_db_items(sql_completer):
    
    sql = "SELECT * FROM "
    postion_word = "FROM "
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)
    assert "test_table" in [x.label for x in completions.items]
    assert "my_table_macro" in [x.label for x in completions.items]
    assert "test_view" in [x.label for x in completions.items]
    assert "test_schema" in [x.label for x in completions.items]
    assert "attached_db" in [x.label for x in completions.items]
    assert "read_csv" in [x.label for x in completions.items]
    assert "duckdb_functions" in [x.label for x in completions.items]
    assert "glob" in [x.label for x in completions.items]
    assert "read_parquet" in [x.label for x in completions.items]
    assert "v" in [x.label for x in completions.items]
    assert "sql_auto_complete" in [x.label for x in completions.items]

    sql = "SELECT * FROM v."
    postion_word = "FROM v."
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "my_table_macro_in_v" in [x.label for x in completions.items]
    assert "test_view3" in [x.label for x in completions.items]


    sql = "SELECT * FROM test_schema."
    postion_word = "FROM test_schema."
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "schema_table" in [x.label for x in completions.items]
    assert "test_view2" in [x.label for x in completions.items]


    sql = "SELECT * FROM attached_db.attached_schema."
    postion_word = "FROM attached_db.attached_schema."
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "db_table" in [x.label for x in completions.items]


    sql = "SELECT v."
    postion_word = "SELECT v."
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "add" in [x.label for x in completions.items]


    sql = """--sql
    SELECT
        a, b, c, a
    from abc
    """
    postion_word = "a, b, c, a"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "addition" in [x.label for x in completions.items]
    assert "attached_db" in [x.label for x in completions.items]
    assert "avg" in [x.label for x in completions.items]
    assert "apply" in [x.label for x in completions.items]
    assert "array_has_all" in [x.label for x in completions.items]

    sql = """--sql
    select c
    from test_table
    """
    postion_word = "select c"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "column1" in [x.label for x in completions.items]
    assert "column2" in [x.label for x in completions.items]


def test_completion_cte_items(sql_completer):

    sql = """--sql
    with cte_1 as MATERIALIZED (
        SELECT col1, col2 as c2, col3 as "Column 3" from bbb
    ), cte_2 as (
        SELECT * from cte_1
    )
    SELECT abc FROM 
    """
    postion_word = "SELECT abc FROM "
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "cte_1" in [x.label for x in completions.items]
    assert "cte_2" in [x.label for x in completions.items]

    sql = """--sql
    with cte_1 as MATERIALIZED (
        SELECT col1, col2 as c2, col3 as "Column 3" from bbb
    ), cte_2 as (
        SELECT * from cte_1
    )
    SELECT cte1_alias. 
    FROM cte_1 cte1_alias
    """
    postion_word = "cte1_alias."
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "col1" in [x.label for x in completions.items]
    assert "c2" in [x.label for x in completions.items]
    assert '"Column 3"' in [x.label for x in completions.items]

    sql = """--sql
    SELECT subq_alias.  
    FROM cte_1 cte1_alias
    join (select subq_col1, subq_col2 as col2, "sub col3" as "col 3" from subq) subq_alias
    """
    postion_word = "subq_alias."
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "subq_col1" in [x.label for x in completions.items]
    assert "col2" in [x.label for x in completions.items]
    assert '"col 3"' in [x.label for x in completions.items]

    sql = """--sql
    with cte_1 as MATERIALIZED (
        SELECT col1, col2 as c2, col3 as "Column 3" from bbb
    ), cte_2 as (
        SELECT aaa from c
    )
    SELECT cte1_alias. 
    FROM cte_1 cte1_alias
    """
    postion_word = "SELECT aaa from c"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "cte_1" in [x.label for x in completions.items]


def test_keyword_completions(sql_completer):

    sql = """--sql
    s
    """
    postion_word = "    s"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "SELECT" in [x.label for x in completions.items]
    assert "SET" in [x.label for x in completions.items]

    sql = """--sql
    w
    """
    postion_word = "    w"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "WITH" in [x.label for x in completions.items]

    sql = """--sql
    SELECT * F
    """
    postion_word = "* F"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "FROM" in [x.label for x in completions.items]

    sql = """--sql
    SELECT * FROM abc j
    """
    postion_word = "abc j"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "JOIN" in [x.label for x in completions.items]

    sql = """--sql
    SELECT * FROM abc JOIN def j o
    """
    postion_word = "def j o"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "ON" in [x.label for x in completions.items]

    sql = """--sql
    SELECT * FROM abc JOIN def j ON abc.a = def.b w
    """
    postion_word = "def.b w"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "WHERE" in [x.label for x in completions.items]


    sql = """--sql
    SELECT c1, sum(c2) 
    FROM abc g
    """
    postion_word = "FROM abc g"
    position = sql.find(postion_word) + len(postion_word)
    trigger_char = sql[position - 1]
    print(f'text_before_cursor "{sql[max(position-7,0):position]}"| trigger: "{trigger_char}"')

    completions = sql_completer.route_completion2(position, sql, trigger_char)

    assert "GROUP BY" in [x.label for x in completions.items]
    assert "GROUP BY ALL" in [x.label for x in completions.items]

