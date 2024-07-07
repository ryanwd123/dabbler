#%%
import sys
from typing import List
from dataclasses import dataclass
from pprint import pprint

from lark import Lark, ast_utils, Transformer, v_args
from lark.tree import Meta

this_module = sys.modules[__name__]


#
#   Define AST
#
class _Ast(ast_utils.Ast):
    # This will be skipped by create_transformer(), because it starts with an underscore
    pass

class _Statement(_Ast):
    # This will be skipped by create_transformer(), because it starts with an underscore
    pass

@dataclass
class Value(_Ast, ast_utils.WithMeta):
    "Uses WithMeta to include line-number metadata in the meta attribute"
    meta: Meta
    value: object

@dataclass
class Name(_Ast):
    name: str

@dataclass
class CodeBlock(_Ast, ast_utils.AsList):
    # Corresponds to code_block in the grammar
    statements: List[_Statement]

@dataclass
class If(_Statement):
    cond: Value
    then: CodeBlock

@dataclass
class SetVar(_Statement):
    # Corresponds to set_var in the grammar
    name: str
    value: Value

@dataclass
class Print(_Statement):
    value: Value


class ToAst(Transformer):
    # Define extra transformation functions, for rules that don't correspond to an AST class.

    def STRING(self, s):
        # Remove quotation marks
        return s[1:-1]

    def DEC_NUMBER(self, n):
        return int(n)

    @v_args(inline=True)
    def start(self, x):
        return x

#
#   Define Parser
#

parser = Lark("""
    start: code_block

    code_block: statement+

    ?statement: if | set_var | print

    if: "if" value "{" code_block "}"
    set_var: NAME "=" value ";"
    print: "print" value ";"

    value: name | STRING | DEC_NUMBER
    name: NAME

    %import python (NAME, STRING, DEC_NUMBER)
    %import common.WS
    %ignore WS
    """,
    parser="lalr",
)

transformer = ast_utils.create_transformer(this_module, ToAst())

def parse(text):
    tree = parser.parse(text)
    return transformer.transform(tree)

#
#   Test
#

j:CodeBlock =(parse("""
    a = 1;
    if a {
        print "a is 1";
        a = 2;
    }
"""))
# %%
from sqlglot import parse_one, exp

sql = f"""--sql
CREATE TABLE t1 (
    a VARCHAR,
    b INTEGER
);
"""

sql2 = f"""--sql
CREATE TABLE t1 as (
    select a, b, c from g
);
"""

tree = parse_one(sql, read='duckdb')
tree2 = parse_one(sql2, read='duckdb')
for c in tree.find_all(exp.ColumnDef):
    print(c.name)

print(tree)
tree
tree2
tree.expression
tree2.expression

#%%

sql3 = f"""--sql
"""

tree3 = parse_one(sql3, read='duckdb')

for c in tree3.find_all(exp.Select):

    try:
        print(sql3[c.meta['start']:c.meta['end']])
    except:
        pass
c.meta
