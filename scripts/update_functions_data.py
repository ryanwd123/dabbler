#%%
import requests
import json
import duckdb
from pathlib import Path

url = 'https://github.com/duckdb/duckdb-web/blob/main/docs/functions.json'

data = requests.get(url).json()
data = json.loads('\n'.join(data['payload']['blob']['rawLines']))


# %%

functions = {d['name']:d for d in data}

all_functions = {}
all_functions.update(functions)
#%%

for fn in functions:
    if 'aliases' in functions[fn]:
        name = functions[fn]['name']
        aliases = functions[fn]['aliases']
        for a in aliases:
            new_fn = functions[fn].copy()
            for k in new_fn:
                if type(new_fn[k]) == str:
                    new_fn[k] = new_fn[k].replace(name,a)
                
            all_functions.update({new_fn['name']:new_fn})

# %%
db_fn = set([x[0] for x in duckdb.execute("select distinct function_name from duckdb_functions()").fetchall()])
json_fn = set(all_functions.keys())

#%%
db_fn.difference(json_fn)
#%%
new_fn

#%%
all_functions['arg_max']
#%%
all_functions['argmax']

#%%

def function_doc(fn):
    name:str = fn.get('name',None)
    param:str = fn.get('parameters',None)
    param_types:str = fn.get('parameter_types',None)
    desc:str = fn.get('description',None)
    example:str = fn.get('example',None)
    category:str = fn.get('category',None)
    result:str = fn.get('result',None)
    
    
        
    #markdown
    doc = ''
    # doc += f'```\n'
    # doc += f'<code>\n'
    param_str = ', '.join([p for p in param])
    if category:
        doc += f'**{category.capitalize()} function**: '
    # doc += f'```{name}({param_str}```\n\n'
    # doc += f'{name}({param_str})\n'
    # doc += f'`{name}({param_str})`\n'
    # doc += f'<mark>{name}({param_str})</mark>\n'
    # doc += f'</code>\n\n'
    # doc += f'```\n\n'
    if desc:
        doc += f'{desc}\n\n'
    # doc += f'**Sytnax**\n\n'
    if example:
        doc += f'**Example**\n\n'
        doc += f'```sql\n'
        doc += f'{example}\n'
        doc += f'```\n\n'
        if result:
            doc += f'**Returns**\n\n'
            doc += f'{result}\n'
    
    if param_types:
        # doc += f'**Parameters**\n\n'
        # param_len = max(len(p) for p in param)
        doc += '| Parameter | Type |\n'
        doc += '| ------------- | ------------ |\n'
        for p,t in zip(param,param_types):
            doc += f'| {p} | {t} |\n'
            # doc += f'- {p: <{param_len}}  {t}\n'
    

    detail = f'{name}({param_str})'
    
    
    result = {
        'documentation':doc,
        'detail':detail
    }
    
    return result


function_lookup = {
    k:{
        'documentation':function_doc(v),
        'docstring':function_doc(v),
    }
    for k,v in all_functions.items()}

# import pandas as pd
# pd.da

db_fn = duckdb.execute("select function_name, first(parameters), first(parameter_types), first(description), first(function_type), first(example) from duckdb_functions() where function_name not similar to '.*\W.*' group by all ").fetchall()
db_fn = {x[0]:{
    'name':x[0],
    'parameters':x[1],
    'parameter_types':x[2],
    'description':x[3],
    'function_type':x[4],
    'example':x[5],
    } for x in db_fn}

items_to_add = list(set(db_fn.keys()).difference(function_lookup.keys()))

items_to_add = {k:{
                   'documentation':function_doc(db_fn[k]),
                   'docstring':function_doc(db_fn[k]),
                   } for k in items_to_add}

function_lookup.update(items_to_add)


output = Path(__file__).parent.parent.joinpath('dabbler').joinpath('lsp').joinpath('functions.json')

output.write_text(json.dumps(function_lookup,indent=4))
txt = function_lookup['read_csv']['documentation']['documentation']
Path(__file__).parent.joinpath('tst.md').write_text(txt)

#%%


#%%


#%%
print(function_lookup['strftime']['documentation']['documentation'])

# %%
fn_lu = json.loads(output.read_text())

#%%



