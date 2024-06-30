#%%
from pathlib import Path
import pprint
import time
from dabbler.lsp.parser import get_parser, SqlParserNew
import duckdb
from sqlglot import parse_one
import re
from lark import Lark, Token, UnexpectedToken, exceptions as lark_exceptions
db2 = duckdb.connect()



# db2.read_csv(
#     "./../../sample_data/austin/Issued_Tree_Permits.csv", header=True, normalize_names=True
# ).create("tree_permits")


# db2.sql("create or replace view tp as select * from tree_permits")



sql_parser = get_parser()
parse2 = sql_parser.parse

tst_files = list(Path("./sql_tst").glob("*.sql"))



pass_test = 0
fail_test = 0

start = time.time()

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

print(f"pass: {pass_test}, fail: {fail_test}, duration: {time.time() - start:.2f} seconds (lark)")

pass_test = 0
fail_test = 0

start = time.time()

for f in tst_files[:]:
    txt = f.read_text()
    # print(f.name, duckdb_parse(txt)["error"])
    try:
        z = parse_one(txt)
        pass_test += 1
        # print(f'{time.time() - start:.4f} seconds')
    except Exception as e:
        print(f.name,e)
        fail_test += 1

print(f"pass: {pass_test}, fail: {fail_test}, duration: {time.time() - start:.2f} seconds (sqlglot)")


#%%

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


update = [x for x in used_kw if x not in defined_kw and x not in ignore]
update = [f'{x}: "{x}"i' for x in update]
print('\n'.join(update))
# %%
