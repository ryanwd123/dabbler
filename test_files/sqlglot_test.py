#%%
from sqlglot import tokenize, Token, tokens, TokenType, Tokenizer
import duckdb
from duckdb import token_type

sql = """--sql
CREATE OR REPLACE MACRO custom_summarize() AS TABLE (
    WITH metrics AS (
        FROM any_cte 
        SELECT 
            {
                name: first(alias(COLUMNS(*))),
                type: first(typeof(COLUMNS(*))),
                max: max(COLUMNS(*))::VARCHAR,
                min: min(COLUMNS(*))::VARCHAR,
                approx_unique: approx_count_distinct(COLUMNS(*)),
                nulls: count(*) - count(COLUMNS(*)),
            }
    ), stacked_metrics AS (
        UNPIVOT metrics 
        ON COLUMNS(*)
    )
    SELECT value.* FROM stacked_metrics
);
"""
toks = tokenize(sql)
Tokenizer.KEYWORDS

for t in toks:
    if t.token_type.name in Tokenizer.KEYWORDS:
        print(f'{t} is a keyword')
    # if t.token_type.name in Tokenizer:
        # print(f'{t} is a keyword')
# toks

#%%


import re
dt = duckdb.tokenize(sql)
dt[0][1] == token_type.numeric_const







#%%
from sqlglot import parse_one, tokenize, parse, exp, tokens, ParseError
import re

def get_idx(txt,cur_line,cur_col):
    x = sum(len(x)+1 for i,x in enumerate(txt.split('\n'),start=0) if i < cur_line)
    return x + cur_col

sql =     """--sql
    with aaa as (
        SELECT
            i.CROWN_REMOVAL,
            split_part(i.ENCROACHMENT_OF_ROOT_ZONE::VARCHAR)[0] as peachy,
            i.JURISDICTION,
            i.SPECIES
        from Issued_Tree_Permits i
        WHERE
            i.ENCROACHMENT_OF_ROOT_ZONE = true
    ),
    ggg as (SELECT
        z.* EXCLUDE (CROWN_REMOVAL)
    from aaa z
    ),
    xyz as (select 
        *
    from Issued_Tree_Permits i
        join ggg g on g.SPECIES = i.SPECIES
    ), t123 as (
    select 
        CASE 
            when j.PERMIT_ADDRESS ILIKE '%grover%' then 'grover'
            when j.PERMIT_ADDRESS ILIKE '%gor%' then 'grover'
            when j.PERMIT_ADDRESS ILIKE '%oak%' then 'grover'
            else 'not grover'
        END as j7,
        j.*
    from xyz j
    WHERE j.ISSUED_DATE > '2020-01-01'
    ),
    t1234 as (
    SELECT
        t.j7,
        t.JURISDICTION,
        t.Combined_Geo,
        t.TRUNK_DIAMETER,
        t.PERMIT_STATUS,
        t.APPENDIX_F_REMOVED,
        t.PERMIT_CLASS,
        t.APPENDIX_F_REMOVED,
        t.PROJECT_ID,
        t.PERMIT_NUMBER,
    from t123 t
    ), g0a9s8d as (pivot t1234 on j7 using max(TRUNK_DIAMETER))
    SELECT 
        g.APPENDIX_F_REMOVED,
        g.JURISDICTION,
        asd.a,
        g.APPENDIX_F_REMOVED,
        g.PROJECT_ID
    from t1234 g 
    """

p = parse_one(sql, read='duckdb')

t = tokenize(sql)
t
print(p.to_s())
# %%
j = list(p.find_all(exp.Select))[2]
j: exp.Select
j.arg_types
list(j.find_all(exp.From))
#%%
sql2 = """--sql
SELECT 
    a.abc,
    sum(b)::VARCHAR,
    sum(a11) as f
from apple

"""

p = parse(sql2, read='duckdb')
x = p[0].expressions[0]
t = tokenize(sql2)
1+1
t
sql2[64:68]

#%%
def insert_into_str(loc:int,txt:str,insert:str):
    new_sql = txt[:loc] + f'{insert}' + txt[loc:]
    print(f'new sql: {new_sql}')
    return new_sql





def try_parse(sql, try_count=0) -> exp.Select:
    sql = re.sub(r'[^\S\n]+', ' ', sql)
    try:
        result = parse_one(sql, read='duckdb')
        return result
    except ParseError as e:
        if try_count > 3:
            return e
        # return e
        print(e)
        print(e.__dict__)
        line = e.errors[0]['line']
        col = e.errors[0]['col']
        desc = e.errors[0]['description']
        pos = get_idx(sql,line-1,col)
        print(f'line: {line}, col: {col}, desc: {desc}, char: {sql[pos]}')
        if 'Expected table name but got' in desc:
            sql = insert_into_str(pos,sql,' table_placeholder')
            print('try_count is:', try_count)
            return try_parse(sql, try_count+1)
        if re.match(r"""Required keyword: 'this' missing for.*Column""",desc):
            if sql[pos-1] == ',':
                pos = pos - 1
                pos_char = sql[pos]
                print(f'pos_car: {pos_char}')
                sql = insert_into_str(pos,sql,'col_placeholder')
                return try_parse(sql, try_count+1)
    except Exception as e:
        print(e)
        return e



p = try_parse(sql2)
p.this


x = list(p.expressions)[1]
x: exp.Alias
print(x.to_s())
list(p.find_all(exp.Select))
p.arg_types

#%%
p: ParseError
p.errors[0]['description']

# re.match(r"""Required keyword: 'this' missing for.*Column""","Required keyword: 'this' missing for <class 'sqlglot.expressions.Column'>.")
print(re.sub(r'[^\S\n]+', ' ', sql2))
re.match(r's', sql2)

re.sub('dog', 'cat', 'I like dogs dogs')