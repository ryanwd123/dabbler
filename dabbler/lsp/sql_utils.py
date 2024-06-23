#%%
from dataclasses import dataclass
from itertools import chain
import duckdb
import sqlparse
import re as regex
from dabbler.common import check_name

from lsprotocol.types import (
    CompletionItem,
    CompletionItemKind,
    CompletionItemLabelDetails,
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
    length:str
    start:str
    end:str
    cur_idx:int


@dataclass
class CmpItem:
    label:str
    kind:int
    detail:str
    typ:str
    sort:str
    obj_type:str
    doc:str = None
    
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
    txt = regex.sub('(.*)--sql.*\n','\g<1>',txt,flags=regex.IGNORECASE)
    txt = regex.sub('\s+',' ',txt,flags=regex.IGNORECASE)
    return txt


select_node_words = [
    'select',
    'from',
    'pivot',
    'unpivot',
]

whitespace_tokens = [
    sqlparse.tokens.Newline,
    sqlparse.tokens.Whitespace,
]


def get_stmts(sql_txt,pos_in_doc):
    stmts:list[SqlStatement] = []
    
    for x in sqlparse.parse(sql_txt):
        
        start=sql_txt.find(x.value)+pos_in_doc
        length = len(x.value)
        
        stmt = SqlStatement(
            stmt=x,
            length=length,
            start=start,
            end=start+length,
            txt=x.value,
            cur_idx=pos_in_doc
        )
        
        stmts.append(stmt)
    return stmts


def get_statement(rng:SqlTxtRange,txt:str) -> SqlStatement:
    
    pos = rng.cur_idx
    result = None
    length = len(rng.txt)
    
    for stmt in sqlparse.parse(rng.txt):
        
        skipped = 0
        pattern = "(create\s+)(or\s+replace\s+)?(?P<type>view|table)\s+(?P<name>\w+)(\s+as\s+)(?P<select>.*)"
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
            
            
    if result:
        return SqlStatement(
            stmt=stmt,
            length=r_l,
            start=r_s,
            end=r_e,
            txt=stmt.value,
            cur_idx=rng.cur_idx
        )


def get_selects(stmt):
    tokens = [x for x in stmt.tokens]
    for t in tokens:
        if t.is_group:
            tokens.extend(t.tokens)
        if (t.value == '(' and 
            [x for x in t.parent.tokens if x.ttype not in whitespace_tokens][1].value.lower().split(' ')[0] in select_node_words):
            yield t.parent



def get_sel_node(stmt:SqlStatement,txt:str) -> SelectNode:
        
    pos = stmt.cur_idx
    result = None
    length = len(stmt.txt)
    
    for sel in get_selects(stmt.stmt):
        s = stmt.txt.find(sel.value) + stmt.start
        e = s + len(sel.value)
        l = e-s
        
        # print(sel.value)
        # print(txt[s:e])
        # print(txt[pos-5:pos])
        if pos >= s and pos <= e and l <= length:
            result = sel
            length = l
            r_s, r_e, r_l = s,e,l
            
    if result:
        return SelectNode(
            stmt=sel,
            length=r_l,
            start=r_s,
            end=r_e,
            txt=result.value,
            cur_idx=stmt.cur_idx
        )
    else:
        return stmt


re_patterns = [
    # '(?:"""--sql[^\n]*\n)(?P<sql>.*?)(?:""")',
    # '([^\n]*"""--sql[^\n]*\n)(?P<sql>.*?)([\n]*?""")',
    '([^\n]*?"""--sql[^\n]*?\n)(?P<sql>.*?)(""")',
    # '([^\n]*?"""--sql[^\n]*?\n)(?P<sql>.*?)([\n]*?\s*?""")',
    '(.(sql|execute|executemany)\(")(?P<sql>.*?)("\s*(\)|,))',
    "(.(sql|execute|executemany)\(')(?P<sql>.*?)('\s*(\)|,))",
    ]


def get_idx(txt,cur_line,cur_col):
    x = sum(len(x)+1 for i,x in enumerate(txt.split('\n'),start=0) if i < cur_line)
    return x + cur_col

def get_range(txt,cur_line,cur_col):
    # print(len(txt))
    idx = get_idx(txt,cur_line,cur_col)
    # print(idx)
    re_matches = chain.from_iterable(
        (regex.finditer(
            pattern=p,
            string=txt,
            flags=regex.DOTALL | regex.IGNORECASE) for p in re_patterns)
    )
    
    for m in re_matches:
        s,e = m.span('sql')
        if idx >= s and idx <= e:
            # print(f'{s},{e}\n',m.groups()[0])
            txt = m.group('sql')
            if not txt or len(txt.strip()) == 0:
                return
            # if txt.strip()[-1] != ';':
            #     txt += ';'
            
            return SqlTxtRange(
                txt=txt,
                length=e-s,
                start=s,
                end=e,
                cur_idx=idx
            )


def get_sql2(txt,cur_line,cur_col) -> SelectNode:
    
    result = get_range(txt,cur_line,cur_col)
    return result

def get_sql(txt,cur_line,cur_col) -> SelectNode:
    
    result = get_range(txt,cur_line,cur_col)
    if not result:
        return
    
    result = get_statement(result,txt)
    if not result:
        return
    
    sel_node = get_sel_node(result,txt)
    
    if not sel_node:
        return result
    
    return sel_node


def left_of_cur_matches(line_txt:str,search_txt_list:list[str]):
    return any([line_txt.lower()[-len(search_txt):] == search_txt for search_txt in search_txt_list])


# table_predicates = ['from ','join ','pivot ','unpivot ']


# %%



def clean_partial_sql(txt):
    
    placeholder = (f'placeholder{x}' for x in range(2000))
    
    patterns = [
        ('(\w+[.])([\n\s)])',   f'\g<1>{next(placeholder)}\g<2>'),
        ('(\(\s*)(\))',         f'\g<1>{next(placeholder)}\g<2>'),
        ('(=[^\n\s]*$)',         f'\g<1> {next(placeholder)}'),
        ]
    
    for pat, rep in patterns:
        txt = regex.sub(pat,rep,txt,flags=regex.IGNORECASE)
    return txt


