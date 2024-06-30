#%%
from sqlglot import parse_one, tokenize, Parser, exp

sql = (
"""--sql
select 
    apple as a, 
    banana as b, 
    sum(candy)
from fruites f
    join jars j on f.id = j.id
group by all

"""
)
p = parse_one(sql, read='duckdb')
start = p.find(exp.Select).meta['start']
end = p.find(exp.Select).meta['end']
print(sql[start:end+1])
start

#%%
toks = tokenize(sql)

toks