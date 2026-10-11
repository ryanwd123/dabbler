"""`# dabbler use '<database file>'` comments, which point a python file at a DuckDB
database file so its SQL completes without an IPython session.

    # dabbler use '~/data/sales.duckdb'

Relative paths are relative to the python file's folder, and `~` is the home
folder. The first such comment in a file applies.
"""
import os
import re
from dataclasses import dataclass
from typing import Union

from lsprotocol.types import Command, CompletionItem, CompletionItemKind

from dabbler.lsp.completion_utils import PathCompleter

re_use_directive = re.compile(
    r"""^[ \t]*\#[ \t]*dabbler[ \t]+use[ \t]+(?P<quote>['"])(?P<path>[^'"\n]*)(?P=quote)""", re.MULTILINE
)
# a comment being typed, up to the cursor
re_typing_keyword = re.compile(r"^[ \t]*#[ \t]*dabbler[ \t]+(?P<word>\w*)$")
re_typing_path = re.compile(r"""^[ \t]*#[ \t]*dabbler[ \t]+use[ \t]+(?P<quote>['"])(?P<path>[^'"]*)$""")

DIRECTIVES = {"use": "use a DuckDB database file for this file's SQL"}


@dataclass
class UseDirective:
    path: str  # as written
    start: int  # offsets of the path in the document
    end: int


def find_use_directive(source: str) -> Union[UseDirective, None]:
    m = re_use_directive.search(source)
    if m is None:
        return None
    return UseDirective(m.group("path"), m.start("path"), m.end("path"))


def resolve_database_path(path: str, document_path: Union[str, None]) -> Union[str, None]:
    """The absolute path a directive names; None for a relative path in a document that isn't a file."""
    path = os.path.expanduser(path.strip())
    if not path:
        return None
    if not os.path.isabs(path):
        if not document_path:
            return None
        path = os.path.join(os.path.dirname(document_path), path)
    return os.path.normpath(path)


def is_typing_database_path(before_cursor: str) -> bool:
    """True when the cursor is in the path of a `# dabbler use '...` comment."""
    return re_typing_path.match(before_cursor) is not None


@dataclass
class DirectiveCompletion:
    items: list[CompletionItem]
    start: int  # columns on the line that the items replace
    end: int


def directive_completions(
    line: str, column: int, document_path: Union[str, None], recent_databases: list[tuple[str, int]] = ()
) -> Union[DirectiveCompletion, None]:
    """Completions in a `# dabbler ...` comment: the directive name after `# dabbler `,
    and after `use '` the database files used before (recent_databases: (path, table
    count), most recent first) and the files in the folder being typed."""
    before = line[:column]
    m = re_typing_keyword.match(before)
    if m:
        items = [
            CompletionItem(
                label=name,
                kind=CompletionItemKind.Keyword,
                detail=detail,
                insert_text=f"{name} '",
                # the path list comes next
                command=Command(title="", command="editor.action.triggerSuggest"),
            )
            for name, detail in DIRECTIVES.items()
            if name.startswith(m.group("word").lower())
        ]
        return DirectiveCompletion(items, m.start("word"), column) if items else None

    m = re_typing_path.match(before)
    if not m:
        return None
    typed, quote = m.group("path"), m.group("quote")
    path_start = m.start("path")
    end = column
    while end < len(line) and line[end] not in (quote, "\n"):
        end += 1

    # files of the folder typed so far replace the last segment
    segment_start = path_start + max(typed.rfind("/"), typed.rfind("\\")) + 1
    folder = os.path.dirname(document_path) if document_path else None
    items = PathCompleter(folder, None).get_items(typed)
    if segment_start == path_start:
        # until a folder is typed, files used before come first; they replace the whole
        # path, which here is the same range
        items[:0] = [
            CompletionItem(
                label=path,
                kind=CompletionItemKind.File,
                detail=f"{tables} table{'' if tables == 1 else 's'}",
                sort_text=f"0{i:04d}",
                # matched by file name while typing; the full path is what's inserted
                filter_text=re.split(r"[/\\]", path)[-1],
            )
            for i, (path, tables) in enumerate(recent_databases)
        ]
        for item in items[len(recent_databases):]:
            item.sort_text = "1" + (item.sort_text or "")
    return DirectiveCompletion(items, segment_start, end)
