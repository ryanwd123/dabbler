from typing import Union
import logging
import os
from pathlib import Path
import duckdb
import re
from lsprotocol.types import (
    Command,
    CompletionItem,
    CompletionItemKind,
    CompletionItemLabelDetails
)

def _static_lists():
    # a private connection: duckdb.execute() would use the process's default connection
    con = duckdb.connect()
    try:
        def rows(sql):
            return [x[0] for x in con.execute(sql).fetchall()]
        return (
            [CompletionItem(label=x) for x in rows("select distinct logical_type from duckdb_types()")],
            [CompletionItem(label=f"'{x}'") for x in rows("select distinct extension_name from duckdb_extensions()")],
            [CompletionItem(label=x) for x in rows("select distinct name from duckdb_settings()")],
            [CompletionItem(label=x, sort_text="99")
             for x in rows("select distinct function_name from duckdb_functions() where function_type = 'pragma'")],
        )
    finally:
        con.close()


duckdb_types, duckdb_extensions, duckdb_settings, duckdb_pragmas = _static_lists()

types_to_exclude = set(['.py','.ipynb'])
# path characters in python code (pathlib completions); a bare "/-_" in a character
# class is a range that also matches ;<=>?@ etc.
path_chars = r"[\w./ ,:\\~()&#+=@\[\]-]"
MAX_PATH_ITEMS = 500
# SQL that reads or writes the file named by the string that follows
re_file_context = re.compile(
    r"(?:\b(?:read_\w+|parquet_\w+|sniff_csv|glob|from|join|attach|copy\s+\w+\s+(?:to|from)|to|from)\s*\(?\s*)$",
    re.IGNORECASE,
)
re_path_like = re.compile(r"^(?:\.{1,2}[/\\]|~|[/\\]|[a-zA-Z]:)|[/\\]")


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def open_string_start(sql: str, pos: int) -> Union[int, None]:
    """Where the single-quoted SQL string around pos starts (after its quote), if pos is in one."""
    i, start = 0, None
    while i < pos:
        c = sql[i]
        if start is not None:
            if c == "'":
                if i + 1 < pos and sql[i + 1] == "'":
                    i += 2  # '' is a quote inside the string
                    continue
                start = None
        elif c == "'":
            start = i + 1
        elif c == '"':
            end = sql.find('"', i + 1)
            if end < 0 or end >= pos:
                return None
            i = end
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            if end < 0 or end >= pos:
                return None
            i = end
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            if end < 0 or end + 2 > pos:
                return None
            i = end + 1
        i += 1
    return start


def path_at(sql: str, pos: int) -> Union[tuple[str, int, int], None]:
    """(typed path, start, end of its last segment) when the cursor is in a string
    holding a file path: one that looks like a path ('./', '~/', 'C:\\', 'a/b'),
    or any string where SQL reads a file (FROM '...', read_csv('...'), COPY ... TO '...')."""
    start = open_string_start(sql, pos)
    if start is None:
        return None
    typed = sql[start:pos]
    if "\n" in typed:
        return None
    if not (re_path_like.search(typed) or re_file_context.search(sql[:start - 1])):
        return None
    segment_start = start + max(typed.rfind("/"), typed.rfind("\\")) + 1
    end = pos
    while end < len(sql) and sql[end] not in "'/\\\n":
        end += 1
    return typed, segment_start, end


class PathCompleter:
    """Files and folders for a path typed so far, relative to the session's working
    directory and file_search_path, or absolute, or starting with ~."""

    def __init__(self, cwd: Union[str, None], search_path: Union[str, None], types_to_exclude=types_to_exclude) -> None:
        self.log = logging.getLogger('PathCompleter')
        self.bases: list[Path] = []
        for base in [cwd, *(search_path or '').split(',')]:
            if base and Path(base).is_dir() and Path(base) not in self.bases:
                self.bases.append(Path(base))
        self.types_to_exclude = types_to_exclude

    def get_items(self, search: Union[str, None]) -> list[CompletionItem]:
        search = os.path.expanduser(search or '')
        cut = max(search.rfind('/'), search.rfind('\\')) + 1
        folder, prefix = search[:cut], search[cut:].lower()
        if os.path.isabs(folder) or re.match(r'^[a-zA-Z]:', folder):
            dirs = [Path(folder)]
        else:
            dirs = [base / folder for base in self.bases]

        found: dict[str, Path] = {}
        for d in dirs:
            try:
                entries = list(os.scandir(d))
            except OSError:
                continue
            for entry in entries:
                if entry.name in found or not entry.name.lower().startswith(prefix):
                    continue
                if Path(entry.name).suffix.lower() in self.types_to_exclude:
                    continue
                found[entry.name] = Path(entry.path)
                if len(found) >= MAX_PATH_ITEMS:
                    break

        items = []
        for name, path in sorted(found.items(), key=lambda x: (not x[1].is_dir(), x[0].lower())):
            is_dir = path.is_dir()
            try:
                size = None if is_dir else sizeof_fmt(path.stat().st_size)
            except OSError:
                size = None
            items.append(CompletionItem(
                label=name + ('/' if is_dir else ''),
                sort_text=('0' if is_dir else '1') + name.lower(),
                filter_text=name,
                kind=CompletionItemKind.Folder if is_dir else CompletionItemKind.File,
                label_details=CompletionItemLabelDetails(description=size),
                # a folder's contents come next
                command=Command(title='', command='editor.action.triggerSuggest') if is_dir else None,
            ))
        self.log.debug([search, folder, prefix, len(items)])
        return items


re_path_parent_step = re.compile(r"""\.parent|\.joinpath\(['"]([^'"]+)['"]\)""")


def apply_path_parents(path: Path, chain: str) -> Path:
    """Applies a chain like `.parent.joinpath('data')` without evaluating editor text."""
    for step in re_path_parent_step.finditer(chain):
        path = path.joinpath(step.group(1)) if step.group(1) else path.parent
    return path


def pathlib_completetions(text:str, path_dict:dict[str,str],logger):
    if len(path_dict) == 0:
        return
    path_strings = '|'.join([re.escape(x) for x in path_dict.keys()])
    parent_pattern = r"""(?P<parent>(\.parent|\.joinpath\(['"]""" + path_chars + r"""+['"]\))+)?"""
    slash_pattern = r""".*(^|\s+|\(|=)(?P<path_ojb>(""" + path_strings + r"""))""" + parent_pattern + r"""(\s+)?/(\s+)?['"](?P<search>""" + path_chars + r"""+)?$"""
    join__pattern = r""".*(^|\s+|\(|=)(?P<path_ojb>(""" + path_strings + r"""))""" + parent_pattern + r"""\.joinpath\(["'](?P<search>""" + path_chars + r"""+)?$"""

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
        _path = apply_path_parents(_path, groups['parent'])
    if not _path.is_dir():
        return
    
    if groups['search']:
        search = groups['search']
    else:
        search = ''
    
    completer = PathCompleter(str(_path),None)
    return completer.get_items(search)
