#%%
import imp
from os import path
import sqlparse as sp
import paths_z as MK
import IPython
from pathlib import Path

ipython = IPython.get_ipython()

f = Path(MK.__file__).parent

#%% 
paths = {}
for item in ipython.ev('dir()'):
    # print(item)
    i_type = str(type(ipython.ev(item)))
    if i_type == "<class 'module'>":
        if '__file__' not in ipython.ev(f'dir({item})'):
            continue
        if 'site-packages' in ipython.ev(item).__file__:
            continue
        if Path(ipython.ev(item).__file__).parent.name == 'Lib':
            continue
        if Path(ipython.ev(item).__file__).parent.parent.name == 'Lib':
            continue
        
        for item2 in ipython.ev(f'dir({item})'):
            item_item2 = f'{item}.{item2}'
            i_type2 = str(type(ipython.ev(item_item2)))
            print(item_item2, i_type2)

            if 'WindowsPath' in i_type2 or 'PosixPath' in i_type2:
                paths[item_item2] = str(ipython.ev(item_item2))
paths
#%%
from pathlib import Path


f = Path(__file__).parent
f/'a'

#%%
import re

txt = "abc = MK.main2 / '"
txt2 = "abc = MK.main2.parent.joinpath('abc') / '"
txt3 = "abc = f.parent/'"
txt4 = 'MK.main.parent.parent/"'


".*(^| )(join |from |pivot |unpivot |alter table |insert into )(\w+( \w+)?, )*\w?$"

# pathlib_slash_pattern = re.compile(r""".*(^|\s+)(?P<path_ojb>\w+)(?P<parent>(\.parent)+)?(\s+)?/(\s+)?['"](?P<search>[A-Za-z0-9./_,]+)?$""")
path_strings = '|'.join([x.replace('.',r'\.') for x in paths.keys()])
pathlib_slash_pattern = r""".*(^|\s+)(?P<path_ojb>(""" + path_strings + r"""))(?P<parent>(\.parent|\.joinpath\(['"][A-Za-z0-9./_,]+['"]\))+)?(\s+)?/(\s+)?['"](?P<search>[A-Za-z0-9./_,]+)?$"""

re.compile(r""".*(^|\s+)(MK\.main|abc).*""").search(txt4)


for t in [txt, txt2, txt3, txt4]:
    m = pathlib_slash_pattern.search(t)
    if m:
        print(m.groupdict())

'.parent.parent'.strip('.').split('.')

#%%


txt = "abc = f.joinpath('"
txt2 = "abc = f.parent.parent.joinpath('asbc"


pathlib_joinpath_pattern = re.compile(r""".*(^|\s+)(?P<path_ojb>f)(?P<parent>(\.parent)+)?\.joinpath\(["'](?P<search>[A-Za-z0-9./_,]+)?$""")

for t in [txt, txt2]:
    m = pathlib_joinpath_pattern.search(t)
    if m:
        print(m.groupdict())

#%%
import duckdb
db = duckdb.connect(':memory:')
#!%load_ext dabbler.ext


db.sql(
"""--sql,
select 1

"""
)

'.parent.parent'.count('parent')

#%%
from dabbler.lsp.completion import PathCompleter=
import logging
logger = logging.getLogger('test')

#%%
#%%
#%%
c = Path('/').resolve()
path_dict = {
    'f':str(f),
    'root':c
    }

txt = "root.joinpath('Users/ryanw/Application Data/"

def pathlib_completetions(text:str, path_dict:dict[str,str],logger):
    path_strings = '|'.join([x.replace('.',r'\.') for x in path_dict.keys()])
    slash_pattern = r""".*(^|\s+)(?P<path_ojb>(""" + path_strings + r"""))(?P<parent>(\.parent|\.joinpath\(['"][A-Za-z0-9./_, ]+['"]\))+)?(\s+)?/(\s+)?['"](?P<search>[A-Za-z0-9./_, ]+)?$"""
    join__pattern = r""".*(^|\s+)(?P<path_ojb>(""" + path_strings + r"""))(?P<parent>(\.parent|\.joinpath\(['"][A-Za-z0-9./_, ]+['"]\))+)?\.joinpath\(["'](?P<search>[A-Za-z0-9./_, ]+)?$"""

    m = re.search(slash_pattern,text)
    if not m:
        m = re.search(join__pattern,text)
    if not m:
        return
    
    groups = m.groupdict()
    if not groups['path_ojb']:
        return
    path_ojb = groups['path_ojb']
    if path_ojb not in path_dict:
        return
    _path = Path(path_dict[path_ojb])
    if groups['parent']:
        _path = eval(f'_path{groups["parent"]}')
    if not _path.is_dir():
        return
    
    if groups['search']:
        search = groups['search']
    else:
        search = ''
    
    completer = PathCompleter(_path,None,logger)
    return completer.get_items(search)

pathlib_completetions(txt, path_dict,logger)

# %%
