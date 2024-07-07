import duckdb
from dabbler.gui_main import MyApp

def get_duck_db_columns_df():
    db= duckdb.connect()
    df = db.sql(
    """--sql,
    create table test_table as
    select
        * REPLACE (tags::VARCHAR AS tags)
    from duckdb_types;
    create TABLE test_table2 as
    select
        MAP {'key1': 10, 'key2': 20, 'key3': 30} as map_column,
        [1,2,3] as list_column,
        [[1,2,3],[4,5,6]] as list_list_column,
        {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'} as struct_col1,
        {'x': 1, 'y': 2, 'z': 3} as struct_col2,
        {'key1': 'string', 'key2': 1, 'key3': 12.345} as struct_col3,
        {'birds':
        {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'},
    'aliens':
        NULL,
    'amphibians':
        {'yes':'frog', 'maybe': 'salamander', 'huh': 'dragon', 'no':'toad'}
    } as struct_col4,
    """
    )
    return db

db = get_duck_db_columns_df()


app = MyApp([],db)

app.exec_()

