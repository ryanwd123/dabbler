#%%
from pathlib import Path
import pprint
import time
from dabbler.lsp.parser import get_parser, SqlParserNew
import duckdb
import re
from lark import Lark, Token, UnexpectedToken, exceptions as lark_exceptions
db = duckdb.connect()



db.read_csv(
    "./../../sample_data/austin/Issued_Tree_Permits.csv", header=True, normalize_names=True
).create("tree_permits")


db.sql("create or replace view tp as select * from tree_permits")



sql_parser = get_parser()
parse2 = sql_parser.parse

pass_test = 0
fail_test = 0

start = time.time()

tst_files = list(Path("./sql_tst").glob("*.sql"))

for f in tst_files[:]:
    txt = f.read_text()
    # print(f.name, duckdb_parse(txt)["error"])
    try:
        parse2(txt)
        pass_test += 1
        # print(f'{time.time() - start:.4f} seconds')
    except Exception as e:
        print(f.name,e)
        fail_test += 1

print(f"pass: {pass_test}, fail: {fail_test}, duration: {time.time() - start:.2f} seconds")


#%%
def parser_error_handler(e:UnexpectedToken):
    assert isinstance(e, UnexpectedToken)
    print(e.token,e.token.type)
    print(e.accepts)   


    
    if '_AS' in e.accepts and e.token.type == 'RPAREN':
        e.interactive_parser.feed_token(Token('_AS', 'AS'))
        e.interactive_parser.feed_token(Token('NAME', 'placeholder'))
        e.interactive_parser.feed_token(e.token)
        print('added as name')    
        return True
    
    if e.token == Token('$END', '') and 'NAME' in e.accepts:
        e.interactive_parser.feed_token(Token('NAME', 'xyz'))
        # e.interactive_parser.feed_token(e.token)
        print('end add name')
        return True

    if 'NAME' in e.accepts:
        e.interactive_parser.feed_token(Token('NAME', 'QQ'))
        e.interactive_parser.feed_token(e.token)
        print('added name')
        return True
    

    # print(e.token)
    # e.interactive_parser.feed_eof() 
    # e.interactive_parser.feed_token(Token('NAME', 'xyz'))
    # e.interactive_parser.feed_token(e.token)
    return False



sql_grammer = Path("./../dabbler/lsp/sql3b.lark").read_text()

test_parser = Lark(
    sql_grammer,
    parser="lalr",
    # cache=str(lark_cache),
    propagate_positions=True,
    maybe_placeholders=True,
    debug=False,
)





sql2 = """
    from tree_permits t
    select
        sum(t.abc),"""

try:
    t = test_parser.parse(sql2, on_error=parser_error_handler)
    print(t.pretty())
except Exception as e:
    ee = e
    print(e)

 
 


#%%

check_choices = (
    ('RPAREN', ')'),
    ('NAME', 'placeholder'),
)


def find_end(p):
    choices = list(p.choices().keys())
    if '$END' in choices:
        try:
            return p.feed_eof()
        except:
            pass
    # for typ, value in check_choices:
    #     if typ in choices:
    #         t = Token(typ, value)
    #         print(f'feeding {t}')
    #         p.feed_token(t)
    #         return find_end(p)


def interactive_parse(sql:str,pos:int):

    p = test_parser.parse_interactive(sql)
    tokens = p.iter_parse()
    token_history = []
    # tk = next(lex)
    choices_pos = None
    
        
    while True:
        try:
            token = next(tokens)
        except StopIteration:
            break
        except UnexpectedToken as e:
            print('unexpected token', e.token, e.token.type)
            if e.token == '$END':
                print('end')
                break
            choices = p.choices().keys()
            if 'col_replace' in choices and e.token == ')':
                p.feed_token(Token('NAME', 'placeholder'))
                p.feed_token(Token('_AS', 'as'))
                p.feed_token(Token('NAME', 'placeholder'))
                p.feed_token(e.token)
                continue
            
            if 'col_exclude' in choices and e.token == ')':
                p.feed_token(Token('NAME', 'placeholder'))
                p.feed_token(e.token)
                continue
            
            raise e
        except Exception as e:
            print(e)
            raise e
        
        if not choices_pos and token.end_pos > pos:
            choices_pos = list(p.choices().keys())
            print(f'choices, {token}')
        token_history.append(token)
        
    tree = find_end(p)
    if not tree and not choices_pos:
        choices_pos = list(p.choices().keys())
    return tree, choices_pos
#%%

Token('RPAREN', ')') == ')'


sql2 = """--sql
    with qq as (
    select
        t.date, from tree_permits t
    where t.a in (from jj j select j.))
    select q.*,  from qq q
    """
# sql2 = 'set '

# txt = 't.date, '
pos = sql2.find(txt) + len(txt)
sql2[0:pos]


p,c = interactive_parse(sql2,pos)
# if p:
    # print(p.pretty())


print(p.pretty())
c
#%%

from dabbler.common import check_name
check_name('date')


#%%
l = test_parser.lex(sql2)
dir(test_parser)

t = next(l)
print(t)

t.type
#%%
p = test_parser.parse_interactive(sql2)
i = p.iter_parse()
#%%
t = next(i)
print(t)
print(p.choices().keys())
#%%
t
dir(p.pretty())
p.__dict__
p.parser.parse_table.end_states
p.parser.parse_table.start_states
p.parser.parse_table.states
#%%
p.parser.parse_table.states[192]

#%%
dir(p.parser.debug)
dir(p.parser_state)
p.parser_state.state_stack
p.parser_state.value_stack[0]
#%%
p.feed_token(Token('NAME', 'placeholder'))
p.feed_token(Token('_AS', 'as'))
p.feed_token(Token('NAME', 'placeholder'))
p.feed_token(Token('RPAREN', ')'))
p.feed_eof()

#%%
print(p.pretty())
list(p.choices())


#%%
p.feed_token(Token('NAME', 'placeholder'))
print(p.pretty())
#%%
c = p.copy()
tree = c.feed_eof()
print(tree.pretty())

#%%

pass_test = 0
fail_test = 0

start = time.time()

tst_files = list(Path("./sql_tst").glob("*.sql"))

for f in tst_files[:]:
    txt = f.read_text()
    # print(f.name, duckdb_parse(txt)["error"])
    try:
        interactive_parser(txt)
        pass_test += 1
        # print(f'{time.time() - start:.4f} seconds')
    except Exception as e:
        print(f.name,e)
        fail_test += 1

print(f"pass: {pass_test}, fail: {fail_test}, duration: {time.time() - start:.2f} seconds")


#%%
from dabbler.lsp.parser import lark_file
#%%
from pathlib import Path
import re
grammer_txt = (Path(__file__).parent.parent / 'dabbler' / 'lsp' / 'sql3b.lark').read_text()
reg = re.compile(r'''([A-Z_]+)\s*:\s*"[A-Z_]+"''')
defined_kw = set(reg.findall(grammer_txt))

used_kw_reg = reg = re.compile(r'''([A-Z_]+)''')
used_kw = set(used_kw_reg.findall(grammer_txt))

ignore = ['FACTORIAL',
 'DESCRIBE',
 'KEY',
 'ESCAPED_STRING',
 'BITWISE_OR',
 'AS',
 'LESS_THAN_OR_EQUAL',
 'S',
 'ORDER',
 'BITWISE_SHIFT_LEFT',
 'WS',
 'VIRTUAL',
 'COMMA',
 'EQUAL',
 '_',
 'PLUS',
 'BITWISE_SHIFT_RIGHT',
 'AND_OP',
 'EXPONENT',
 'STAR',
 'LESS_THAN',
 'NUMBER',
 'A',
 'IDENT',
 'DOT',
 'CNAME',
 'SIGNED_NUMBER',
 'COMMENT',
 'BITWISE_AND',
 'DIVIDE',
 'BITWISE_NOT',
 'NOT_EQUALS',
 'STORED',
 'GREATER_THAN',
 'INTEGER_DIVIDE',
 'STRING',
 'ALWAYS',
 'MODULO',
 'MINUS',
 'Z_',
 'CONCAT',
 'LPAREN',
 'RPAREN',
 'GREATER_THAN_OR_EQUAL',
 'CAST_OP']


[x for x in used_kw if x not in defined_kw and x not in ignore]
# %%
