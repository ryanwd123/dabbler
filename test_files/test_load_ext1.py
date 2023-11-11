#%%
import duckdb
db = duckdb.connect()
#!%load_ext dabbler.ext_debug
#%%

db.execute(
    """--sql
    CREATE or REPLACE TABLE t1 as
    from (VALUES
        ('a',1),
        ('b',2),
        ('c',3),
        ('d',4),
    )
    """
)


#%%
import random
from faker import Faker
fake = Faker()
#%%
from itertools import cycle, count

fake_types = {
    'name':lambda:fake.name(),
    'address':lambda:fake.address(),
    'value':lambda:random.random() * random.randint(1,10000),
    'amount':lambda:random.randint(1,10000),
    'date':lambda:fake.date(),
    'date2':lambda:fake.date_time(),
    'phone_number':lambda:fake.phone_number(),
    'company':lambda:fake.company(),
    'month_name':lambda:fake.month_name(),
    'ship':lambda:fake.military_ship(),
    'mac_address':lambda:fake.mac_address(),
    'free_email':lambda:fake.free_email(),
    'email':lambda:fake.email(),
    'file_ext':lambda:fake.file_extension(),
}

col_keys = cycle(list(fake_types.keys()))

next(col_keys)

cc = count()
next(cc)
#%%
import datetime
def fmt_type(val):
    if isinstance(val,str):
        return f"'{val}'"
    if isinstance(val,datetime.date):
        return f"DATE '{val}'"
    if isinstance(val,datetime.datetime):
        return f"DATE '{val}'"
    return str(val)

from time import time

for ci in range(20):
    start = time()
    col_count = random.randint(3,100)
    row_count = random.randint(3,200)
    
    
    col_def = []
    
    
    for i in range(col_count):
        col = next(col_keys)
        func = fake_types[col]
        col_def.append([f'{col}_{i}',func])

    alias = f"a({', '.join([f'{col[0]}' for col in col_def])})"
    
    values = []
    
    for row in range(row_count):
        row_data = []
        for col in range(col_count):
            txt = f"{fmt_type(col_def[col][1]())}"
            row_data.append(txt)
        row_txt = ', '.join(row_data)
        values.append(f'({row_txt})')
        
    values_txt = ',\n'.join(values)

    sql_txt = (
    f"""CREATE or REPLACE TABLE ajj{next(cc)} as
from (VALUES
{values_txt}
) {alias}    
    """)
    make_fake_table = time() - start
    start = time()
    # print(sql_txt)
    db.execute(sql_txt)   
    exe_time = time() - start
    print(f'{ci: >3}: data: {make_fake_table: >5.1f}s      insert:{exe_time: >5.1f}s')

# print(values_txt)
# %%
db.execute("set file_search_path to 'C:/scripts';")
#%%

#%%

next(cc)
db.sql(
    """--sql
    with gg as (from ajj10 a
    SELECT a.email_1 as j, a.file_ext_2 as k)
    from gg g
    SELECT g.j, g.k
    ;
    FROM ajj16 a
    SELECT COLUMNS(c -> c LIKE 'a%');
    from ajj16 a
    SELECT a.address_31, a.address_31;
    """
)

