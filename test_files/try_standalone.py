#%%
# 
from pathlib import Path
from lark import Lark, Token, Tree, Visitor, Transformer, v_args, ParseTree
from lark.parsers.lalr_parser import LALR_Parser
import sys
sys.path.append(str(Path(__file__).parent.parent))
from test_standalone_duckdb import Lark_StandAlone
# %%
sql = """--sql
select a from b

"""
parser:Lark = Lark_StandAlone()


def parse(sql:str) -> ParseTree:
    result = parser.parse(sql)
    return result
# %%
parser.parse_interactive(sql)


t = parse(sql)
print(t.pretty())