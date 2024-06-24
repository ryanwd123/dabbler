import logging
from pathlib import Path
import duckdb
import re
# from dabbler.lsp.server_classes import InlineSqlLangServer
from lsprotocol.types import (
    CompletionItem,
    CompletionItemKind,
    CompletionItemLabelDetails
)

duckdb_types = [CompletionItem(label=f'{x[0]}') for x in 
                duckdb.execute("select distinct logical_type from duckdb_types()").fetchall()]


duckdb_extensions = [CompletionItem(label=f"'{x[0]}'") for x in 
                duckdb.execute("select distinct extension_name from duckdb_extensions()").fetchall()]


duckdb_settings = [CompletionItem(label=f'{x[0]}') for x in 
                duckdb.execute("select distinct name from duckdb_settings()").fetchall()]

duckdb_kw_comp = [CompletionItem(label=f'{x[0]}',sort_text="99") for x in 
                duckdb.execute("select upper(keyword_name) from duckdb_keywords() where keyword_category = 'reserved'").fetchall()]

duckdb_pragmas = [CompletionItem(label=f'{x[0]}',sort_text="99") for x in 
                duckdb.execute("select distinct function_name from duckdb_functions() where function_type ='pragma'").fetchall()]

types_to_exclude = set(['.py','.ipynb'])
file_path_completion_pattern = r'''.*('./)([A-Za-z0-9./-_, ]+)?$'''
file_path_completion_regex = re.compile(file_path_completion_pattern)


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


class PathCompleter:
    def __init__(self,cwd:str,search_path:str,logger:logging.Logger,types_to_exclude=types_to_exclude) -> None:
        self.cwd = None
        self.search_path = None
        self.log = logger
        if cwd and Path(cwd).is_dir():
            self.cwd = Path(cwd)
        if search_path and Path(search_path).is_dir():
            self.search_path = Path(search_path)
            
    def get_items(self, search:str):
        path = None
        file = None
        if search is None:
            search = ''
        
        if '/' in search:
            path,file = search.rsplit('/',1)
        
        if path is None:
            path = ''
        
        if file is None:
            file = ''
        
        items:list[Path] = []

        if self.cwd:
            items.extend([f for f in self.cwd.joinpath(path).glob(f'{file}*') if f.suffix.lower() not in types_to_exclude])
        
        file_names = {x.name for x in items}
        
        if self.search_path:
            glob = self.search_path.joinpath(path).glob(f'{file}*')
            files = [f for f in glob if f.name not in file_names and f.suffix.lower() not in types_to_exclude]
            items.extend(files)
        
        
        comp_items = [CompletionItem(
            label=f.name,
            sort_text="1",
            kind=CompletionItemKind.Folder if f.is_dir() else CompletionItemKind.File,
            label_details=CompletionItemLabelDetails(
                detail=None,
                description=sizeof_fmt(f.stat().st_size) if f.is_file() else None,
            )
            
            )
                      
                      for f in items]
        
        self.log.debug([search,path,file,items])
        return comp_items
    

def pathlib_completetions(text:str, path_dict:dict[str,str],logger):
    if len(path_dict) == 0:
        return
    path_strings = '|'.join([x.replace('.',r'\.') for x in path_dict.keys()])
    slash_pattern = r""".*(^|\s+|\(|=)(?P<path_ojb>(""" + path_strings + r"""))(?P<parent>(\.parent|\.joinpath\(['"][A-Za-z0-9./-_, ]+['"]\))+)?(\s+)?/(\s+)?['"](?P<search>[A-Za-z0-9./-_, ]+)?$"""
    join__pattern = r""".*(^|\s+|\(|=)(?P<path_ojb>(""" + path_strings + r"""))(?P<parent>(\.parent|\.joinpath\(['"][A-Za-z0-9./-_, ]+['"]\))+)?\.joinpath\(["'](?P<search>[A-Za-z0-9./-_, ]+)?$"""

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
