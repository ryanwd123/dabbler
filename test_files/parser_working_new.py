#%%
from dabbler.lsp.parser import interactive_parse, sql_parser, Token, get_parser
import duckdb
db = duckdb.connect(':memory:')
#!%load_ext dabbler.ext_debug

sql = """
with num as (
select
    l."Draw Date", unnest(split(l."Winning Numbers",' ')) as numbers
from './../../sample_data/Lottery.csv' l)
select
    numbers,
    count(*) as freq
FROM 
"""

p = sql_parser.parse_interactive(sql)
tokens = p.iter_parse()
token_history = []
# tk = next(lex)
choices_pos = []
#%%
while True:
    try:
        token = next(tokens)
        token_history.append(token)
        print(token)
        print(', '.join(p.accepts()))
    except StopIteration:
        break
#%%
p.accepts()
p.choices()
if (token_history[-1].lower() == 'from'
    and 'IDENT' in p.accepts()
    and 'table_ref' in p.choices()):
    p.feed_token(Token('IDENT', 'placeholder'))
p.feed_eof()

#%%
sql_parser = get_parser()

sql = """--sql
create table abc (
    wbs VARCHAR,
    name VARCHAR,
);
create table def (
    wbs VARCHAR,
    name VARCHAR,
);
attach 'abc.db'; attach 'def.db';
from information_schema i SELECT i.character_sets;
"""
sql = """--sql
CREATE TABLE idf(
    my_name VARCHAR,
    my_age INTEGER
)
"""



t = sql_parser.parser.parse(sql)

print(t.pretty())
# %%
p = sql_parser.parse_interactive(sql)
tokens = p.iter_parse()
token_history = []
# tk = next(lex)
choices_pos = []
#%%
while True:
    try:
        token = next(tokens)
        token_history.append(token)
        print(token)
        print(', '.join(p.accepts()))
    except StopIteration:
        break
#%%
p.accepts()
p.choices()

#%%
import logging
log = logging.getLogger('dabbler')
interactive_parse(sql, len(sql),log)
from dabbler.lsp.parser import SqlParserNew
p = SqlParserNew(db,None,log)
p.parse_sql(sql,1)
# %%
