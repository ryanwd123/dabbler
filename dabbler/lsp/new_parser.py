#%%
from ast import In
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from enum import Enum
from lark import Lark, Transformer, v_args, Discard, Visitor, Tree, UnexpectedToken, Token, UnexpectedCharacters, UnexpectedEOF, UnexpectedInput
from lark.parsers.lalr_interactive_parser import InteractiveParser
import duckdb
import time

lark_cache = Path(__file__).parent.joinpath('lark_cache')
lark_file = Path(__file__).parent.joinpath('sql3b.lark')

def get_parser():
    sql_grammer = lark_file.read_text()
    sql_parser = Lark(
        sql_grammer,
        parser="lalr",
        cache=str(lark_cache),
        propagate_positions=True,
        maybe_placeholders=True,
        debug=False,
    )
    return sql_parser

sql_parser = get_parser()



check_choices = (
    ('RPAREN', ')'),
    ('NAME', 'placeholder'),
)


def find_end(p:InteractiveParser,cur_token:Token=None):
    choices = list(p.choices().keys())
    print(cur_token,choices)
    if 'table_ref' in choices and cur_token.upper() in ['FROM','JOIN']:
        try:
            p.feed_token(Token('IDENT', 'placeholder'))
            choices = list(p.choices().keys())
        except:
            pass


    if '$END' in choices:
        try:
            return p.feed_eof()
        except Exception as e:
            print(e)
    # for typ, value in check_choices:
    #     if typ in choices:
    #         t = Token(typ, value)
    #         print(f'feeding {t}')
    #         p.feed_token(t)
    #         return find_end(p)

@dataclass
class TokenHistory:
    token:Token
    choices:list
    accept:list

@dataclass
class ParseResult:
    parser:InteractiveParser
    tree:Tree
    choices:list[str]
    token_history:list[TokenHistory]
    tokens_to_pos:list[TokenHistory]
    duration:float


no_space_tokens = set([".", ",", ";", "(", ")", "[", "]","{","}"])


def interactive_parse_new(sql:str,pos:int,logger:logging.Logger = None):
    start = time.time()
    p = sql_parser.parse_interactive(sql)
    tokens = p.iter_parse()
    token_history:list[TokenHistory] = []

    token_history.append(
        TokenHistory(
            token=None,
            choices=list(p.choices().keys()),
            accept=list(p.accepts())
        )
    )
    # tk = next(lex)
    choices_pos = []
    if logger:
        logger.debug(['interactive_parse',sql,pos])
    
    token = None
        
    while True:
        prev_token = token
        try:
            token = next(tokens)
        except StopIteration:
            break
        except UnexpectedCharacters as e:
            print(e)
            break
        except UnexpectedToken as e:
            print(e)
            break
        
        token_history.append(
            TokenHistory(
                token=prev_token,
                choices=list(p.choices().keys()),
                accept=list([])
            )
        )

    token_history.append(
        TokenHistory(
            token=prev_token,
            choices=list(p.choices().keys()),
            accept=list([])
        )
    )

    result_history:list[TokenHistory] = []

    for i, t in enumerate(token_history):
        if t.token is None:
            result_history.append(t)
            continue
        if t.token.end_pos < pos or (t.token.end_pos <= pos and t.token in no_space_tokens):
            result_history.append(t)
        else:
            break
    t = result_history[-1]
    tree=find_end(p,cur_token=token)
    choices_pos = t.choices
    # print(t)
    return ParseResult(
        parser=p,
        tree=tree,
        choices=choices_pos,
        token_history=[f'{x.token} {x.token.start_pos}:{x.token.end_pos}' for x in token_history if x.token],
        tokens_to_pos=[f'{x.token} {x.token.start_pos}:{x.token.end_pos}' for x in result_history if x.token],
        duration=time.time()-start,
    )


    

    # if (len(token_history) > 0
    #     and token_history[-1]
    #     and token_history[-1].lower() == 'from'
    #     and 'IDENT' in p.accepts()
    #     and 'table_ref' in p.choices()):
    #     p.feed_token(Token('IDENT', 'placeholder'))

    # if not choices_pos:
    #     choices_pos = list(p.choices().keys())
    # tree = find_end(p,cur_token=token)
    # if logger:
    #     logger.debug({'choices_pos':choices_pos})
    #     logger.debug({'tree':tree})
    # return tree, choices_pos, token_history

#%%

