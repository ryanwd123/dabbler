from dataclasses import dataclass
import re as regex
from dabbler.common import check_name, unquote_name
from dabbler.lsp.sql_regions import position_to_offset, region_at
from typing import Union

from lsprotocol.types import (
    CompletionItem,
    CompletionItemKind,
    CompletionItemLabelDetails,
    MarkupContent
)


@dataclass
class SqlTxtRange:
    txt:str
    length:int
    start:int
    end:int
    cur_idx:int


@dataclass
class CmpItem:
    label:str
    kind:int
    detail:Union[str,None]
    typ:Union[str,None]
    sort:str
    obj_type:str
    doc:Union[str,MarkupContent,None] = None
    
    def __eq__(self, other) -> bool:
        return self.label == other
    
    def __hash__(self) -> int:
        return hash(self.label)
    
    def __repr__(self) -> str:
        return self.label
    
    @property    
    def comp(self):
        if self.kind == CompletionItemKind.Keyword:
            label = self.label
        else:
            label = check_name(unquote_name(self.label))

        if self.typ and self.typ.startswith('ENUM('):
            description = 'ENUM'
        elif self.typ and self.typ.startswith('STRUCT('):
            description = 'STRUCT'
        else:
            description = self.typ
        
        return CompletionItem(
            label=label,
            kind=self.kind,
            sort_text=self.sort,
            filter_text=self.label.lower(),
            documentation=self.doc,
            label_details=CompletionItemLabelDetails(
                description=description),
                detail=self.detail,
            )


def line_col(str, idx):
    return str.count('\n', 0, idx) + 1, idx - str.rfind('\n', 0, idx)


def strip_sql_whitespace(txt):
    txt = regex.sub(r'(.*)--sql.*\n',r'\g<1>',txt,flags=regex.IGNORECASE)
    txt = regex.sub(r'\s+',' ',txt,flags=regex.IGNORECASE)
    return txt


def sql_range_at(txt: str, idx: int) -> Union[SqlTxtRange, None]:
    """The SQL string around offset idx of a python file (see sql_regions)."""
    region = region_at(txt, idx)
    if region is None:
        return None
    return SqlTxtRange(
        txt=region.text,
        length=region.end - region.start,
        start=region.start,
        end=region.end,
        cur_idx=idx,
    )


def get_sql2(txt, cur_line, cur_col):
    """The SQL string around an LSP position (UTF-16 columns) of a python file."""
    return sql_range_at(txt, position_to_offset(txt, cur_line, cur_col))

