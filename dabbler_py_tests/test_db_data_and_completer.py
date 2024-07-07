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

    test_file = Path(__file__).parent / "test_data.parquet"

    db.sql(
        f"""--sql,
    CREATE OR REPLACE TABLE test_table AS
    select
        *
    from '{test_file}'
    """
    )
    db.sql(
        """--sql,
    CREATE OR REPLACE VIEW test_view as
    select
        *
    from test_table
    """
    )

    db.sql(
        """--sql,
    CREATE schema test_schema;
    CREATE macro test_schema.test_macro(a, b) as a + b;
    CREATE function test_schema.test_function(a, b) as a + b;
    create macro test_schema.test_table_macro() as TABLE select [1,2,3,4] as num;
    """
    )
    
    return db

@pytest.fixture
def test_db():
    return make_test_db()


#%%
# db = make_test_db()
# #!%load_ext dabbler.ext
# db_data = get_db_data_new(db)
# db2 = make_db(db_data)
# comp_map = make_completion_map(db2, db_data)
# db2.execute("SELECT function_name, function_type FROM duckdb_functions() WHERE schema_name = 'test_schema'").fetchall()
# db_data['schemas']
# db_data['databases']

#%%


# db2.sql("SELECT database_name, schema_name, table_name FROM duckdb_tables")

# db2.sql('use memory.test_schema')

# completer = SqlCompleter(db_data)

# sql = """select
#     t.APPENDIX_F_REMOVED
# from test_schema. 
# """
# pos = len(sql)-2
# sql[pos]
# comps = completer.route_completion2(pos,sql,'.')
# for c in comps.items:
#     print(c.label)


# completer.completion_map['test_schema']
# completer.completion_map['memory."test schema2"']

# db.sql(
# """--sql,
# select
#     *
# from memory."test schema2".
# """
# )

#%%

columns_in_test_table = [
    "PERMIT_NUMBER",
    "PROJECT_ID",
    "PERMIT_CLASS",
    "PROPERTY_ID",
    "ISSUED_DATE",
    "EXPIRES_DATE",
    "PERMIT_STATUS",
    "APPLICATION_TYPE",
    "FEE_REQUIRED",
    "HERITAGE_TREE",
    "PUBLIC_TREE",
    "REMOVAL_OF_REGULATED_TREE",
    "ENCROACHMENT_OF_ROOT_ZONE",
    "CROWN_REMOVAL",
    "SPECIES",
    "TRUNK_DIAMETER",
    "TREE_CONDITION",
    "REASON_FOR_REQUEST",
    "PROPX",
    "PROPY",
    "LATITUDE",
    "LONGITUDE",
    "COUNCIL_DISTRICT",
    "JURISDICTION",
    "LINK_TO_DETAILS",
    "APPENDIX_F_REMOVED",
    "NON_APPENDIX_F_REMOVED",
    "MITIGATION",
    "Combined_Geo",
    "list_col",
    "struct_col",
]


def test_get_db_data_new_basic_structure(test_db):
    result = get_db_data_new(test_db)
    
    assert isinstance(result, dict)
    assert 'data' in result
    assert 'dataframes' in result
    assert 'databases' in result
    assert 'functions' in result
    assert 'paths' in result
    assert 'schemas' in result
    assert 'current_schema' in result
    assert 'cwd' in result
    assert 'file_search_path' in result

def test_get_db_data_new_table_info(test_db):
    result = get_db_data_new(test_db)
    
    tables = [item for item in result['data'] if item[1] == 'test_table']
    assert len(tables) == 1
    table_info = tables[0]
    
    assert table_info[0] == 'memory.main'  # db_scm
    assert table_info[1] == 'test_table'  # table_name
    assert 'CREATE TABLE' in table_info[2]  # sql
    col = [x for x in table_info[3] if x[0] == 'ISSUED_DATE'][0]
    assert col[0] == 'ISSUED_DATE'  # col_name
    assert col[1] == 'DATE'  # col_type

def test_get_db_data_new_view_info(test_db):
    result = get_db_data_new(test_db)
    
    views = [item for item in result['data'] if item[1] == 'test_view']
    assert len(views) == 1
    view_info = views[0]
    
    assert view_info[0] == 'memory.main'  # db_scm
    assert view_info[1] == 'test_view'  # table_name
    assert 'CREATE VIEW' in view_info[2]  # sql
    col = [x for x in view_info[3] if x[0] == 'SPECIES'][0]
    assert col[0] == 'SPECIES'  # col_name
    assert col[1] == 'VARCHAR'  # col_type

def test_get_db_data_new_function_info(test_db):
    result = get_db_data_new(test_db)
    
    functions = [item for item in result['functions'] if item[1] == 'test_function']
    assert len(functions) == 1
    function_info = functions[0]
    
    assert function_info[0] == 'memory.test_schema'  # db_scm
    assert function_info[1] == 'test_function'  # function_name
    print(function_info[2])
    assert function_info[2] == 'macro'  # function_type

def test_get_db_data_new_macro_info(test_db):
    result = get_db_data_new(test_db)
    
    macros = [item for item in result['functions'] if item[1] == 'test_macro']
    assert len(macros) == 1
    macro_info = macros[0]
    
    assert macro_info[0] == 'memory.test_schema'  # db_scm
    assert macro_info[1] == 'test_macro'  # function_name
    assert macro_info[2] == 'macro'  # function_type

def test_get_db_data_new_table_macro_info(test_db):
    result = get_db_data_new(test_db)
    
    macros = [item for item in result['functions'] if item[1] == 'test_table_macro']
    assert len(macros) == 1
    macro_info = macros[0]
    
    assert macro_info[0] == 'memory.test_schema'  # db_scm
    assert macro_info[1] == 'test_table_macro'  # function_name
    assert macro_info[2] == 'table_macro'  # function_type

def test_get_db_data_new_schema_info(test_db):
    result = get_db_data_new(test_db)
    
    assert 'memory.main' in result['schemas']
    assert 'memory.test_schema' in result['schemas']

def test_get_db_data_new_current_schema(test_db):
    result = get_db_data_new(test_db)
    
    assert result['current_schema'] == 'memory.main'

def test_get_db_data_new_cwd(test_db):
    result = get_db_data_new(test_db)
    
    assert isinstance(result['cwd'], str)
    assert Path(result['cwd']).exists()

def test_get_db_data_new_file_search_path(test_db):
    custom_path = '/custom/search/path'
    result = get_db_data_new(test_db, file_search_path=custom_path)
    
    assert result['file_search_path'] == custom_path


def test_make_db(test_db):
    db_data = get_db_data_new(test_db)
    new_db = make_db(db_data)

    assert isinstance(new_db, duckdb.DuckDBPyConnection)

    # Test databases
    databases = new_db.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    assert ('memory',) in databases

    # Test schemas
    schemas = new_db.execute("SELECT database_name || '.' || schema_name FROM duckdb_schemas()").fetchall()
    assert ('memory.main',) in schemas
    assert ('memory.test_schema',) in schemas

    # Test current schema
    current_schema = new_db.execute("SELECT current_schema()").fetchone()[0]
    assert current_schema == 'main'

    # Test tables
    tables = new_db.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    assert ('test_table',) in tables

    # Test views
    # views = [new_db.execute("SELECT view_name FROM duckdb_views()").fetchall()]
    assert ('test_view',) in tables

    # Test table columns
    columns = new_db.execute("SELECT column_name, data_type FROM duckdb_columns WHERE table_name = 'test_table'").fetchall()
    assert ('PERMIT_NUMBER', 'VARCHAR') in columns
    assert ('ISSUED_DATE', 'DATE') in columns
    assert ('SPECIES', 'VARCHAR') in columns

    # Test view columns
    view_columns = new_db.execute("SELECT column_name, data_type FROM duckdb_columns WHERE table_name = 'test_view'").fetchall()
    assert ('PERMIT_NUMBER', 'VARCHAR') in view_columns
    assert ('ISSUED_DATE', 'DATE') in view_columns
    assert ('SPECIES', 'VARCHAR') in view_columns

    # Test dataframes (if any were in the original db_data)
    if db_data['dataframes']:
        for df in db_data['dataframes']:
            df_name = df[0]
            assert (df_name,) in tables

    # Test file_search_path
    if db_data['file_search_path']:
        file_search_path = new_db.execute("SELECT current_setting('file_search_path')").fetchone()[0]
        assert file_search_path == db_data['file_search_path']


@pytest.fixture
def sql_completer(test_db):
    db_data = get_db_data_new(test_db)
    db2 = make_db(db_data)
    completion_map = make_completion_map(db2, db_data)
    return SqlCompleter(completion_map)

# def test_route_completions2(sql_completer: SqlCompleter):
#     # Test root namespace completions
#     completions = sql_completer.route_completion2("SELECT ")
#     completions = completions.items
#     assert any(item.label == 'test_table' for item in completions)
#     assert any(item.label == 'test_view' for item in completions)
