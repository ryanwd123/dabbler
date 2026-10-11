#%%
from dataclasses import dataclass
import duckdb
import sqlparse
import re as regex
from dabbler.common import check_name
from dabbler.lsp.sql_regions import position_to_offset, region_at
from typing import Union

from lsprotocol.types import (
    CompletionItem,
    CompletionItemKind,
    CompletionItemLabelDetails,
    MarkupContent
)


@dataclass
class SelectNode:
    stmt:sqlparse.sql.Parenthesis
    txt:str
    length:int
    start:int
    end:int
    cur_idx:int
    

@dataclass
class SqlStatement:
    stmt:sqlparse.sql.Statement
    length:int
    start:int
    end:int
    txt:str
    cur_idx:int
    

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
            label = label=check_name(self.label.strip('"'))

        if self.typ and self.typ.startswith('ENUM('):
            description = 'ENUM'
        elif self.typ and self.typ.startswith('STRUCT('):
            description = 'STRUCT'
        else:
            description = self.typ
        
        return CompletionItem(
            label=label,
            kind=self.kind, # type: ignore
            sort_text=self.sort,
            filter_text=self.label.lower(),
            documentation=self.doc, # type: ignore
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


def get_statement(rng:SqlTxtRange,txt:str):
    
    pos = rng.cur_idx
    result = None
    length = len(rng.txt)
    stmt = None
    r_s = None
    r_e = None
    r_l = None

    
    for stmt in sqlparse.parse(rng.txt):
        
        skipped = 0
        pattern = r"(create\s+)(or\s+replace\s+)?(?P<type>view|table)\s+(?P<name>\w+)(\s+as\s+)(?P<select>.*)"
        match = regex.search(pattern,stmt.value,regex.DOTALL | regex.IGNORECASE)
        if match:
            # print(match.group('select'))
            stmt = sqlparse.parse(match.group('select'))[0]
            skipped = match.span('select')[0]
            # print(stmt.value)
        
        s = rng.txt.find(stmt.value)+rng.start
        e = s + len(stmt.value)
        l = e - s
        # print(txt[s:e])
        # print(f'pos:{pos} se:{s},{e}')
                
        # print(pos,s,e)
        if pos >= s and pos <= e and l <= length:
            result = stmt
            length = l
            r_s, r_e, r_l = s,e,l
            
            
    if result and stmt and r_l and r_s and r_e:
        return SqlStatement(
            stmt=stmt,
            length=r_l,
            start=r_s,
            end=r_e,
            txt=stmt.value,
            cur_idx=rng.cur_idx
        )



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


get_range = get_sql2
