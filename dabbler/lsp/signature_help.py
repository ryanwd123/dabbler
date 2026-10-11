"""Finds the function call around the cursor, for signature help.

Scans the SQL left of the cursor rather than parsing it: while an argument
list is being typed the SQL usually doesn't parse yet.
"""
import re
from dataclasses import dataclass
from typing import Callable, Union

from dabbler.lsp.functions import FunctionInfo, Signature

re_word = re.compile(r"\w+")


@dataclass
class _Open:
    name: Union[str, None]
    """The name before "(" (dotted parts joined), None for brackets and bare parens."""
    args: int = 0
    """Top-level commas seen so far, i.e. the index of the argument being typed."""


@dataclass(frozen=True)
class Call:
    name: str
    arg_index: int


def open_calls(sql: str, pos: int) -> list[_Open]:
    """The parens and brackets still open at pos, outermost first."""
    stack: list[_Open] = []
    name_parts: list[str] = []  # identifier chain just before the current position, like schema.fn
    expect_part = False  # just saw a dot after an identifier
    i = 0
    while i < pos:
        c = sql[i]
        if c == "'":
            end = i + 1
            while True:
                end = sql.find("'", end)
                if end < 0 or end + 1 >= len(sql) or sql[end + 1] != "'":
                    break
                end += 2
            if end < 0 or end >= pos:
                return stack  # the cursor is inside a string argument
            i = end + 1
            name_parts, expect_part = [], False
            continue
        if c == '"':
            end = sql.find('"', i + 1)
            if end < 0 or end >= pos:
                return stack
            word = sql[i + 1:end]
            name_parts = name_parts + [word] if expect_part else [word]
            expect_part = False
            i = end + 1
            continue
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            if end < 0 or end >= pos:
                return stack
            i = end + 1
            continue
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            if end < 0 or end + 2 > pos:
                return stack
            i = end + 2
            continue
        m = re_word.match(sql, i)
        if m:
            word = m.group(0)
            name_parts = name_parts + [word] if expect_part else [word]
            expect_part = False
            i = m.end()
            continue
        if c == "." and name_parts:
            expect_part = True
        elif c == "(":
            stack.append(_Open(".".join(name_parts) if name_parts and not expect_part else None))
            name_parts, expect_part = [], False
        elif c in "[{":
            stack.append(_Open(None))
            name_parts, expect_part = [], False
        elif c in ")]}":
            if stack:
                stack.pop()
            name_parts, expect_part = [], False
        elif c == ",":
            if stack:
                stack[-1].args += 1
            name_parts, expect_part = [], False
        elif not c.isspace():
            name_parts, expect_part = [], False
        i += 1
    return stack


def enclosing_call(sql: str, pos: int, is_function: Callable[[str], bool]) -> Union[Call, None]:
    """The innermost call of a known function around pos, and the argument the cursor is in."""
    for open_ in reversed(open_calls(sql, pos)):
        if open_.name and is_function(open_.name):
            return Call(open_.name, open_.args)
    return None


def signature_parts(sig: Signature) -> tuple[str, list[tuple[int, int]]]:
    """The signature's label and each parameter's [start, end) offsets in it."""
    returns = f" -> {sig.return_type}" if sig.return_type else ""
    label = f"{sig.name}("
    spans = []
    for i, param in enumerate(sig.param_labels):
        if i:
            label += ", "
        spans.append((len(label), len(label) + len(param)))
        label += param
    return label + ")" + returns, spans


def active_signature(info: FunctionInfo, arg_index: int) -> int:
    """The first overload that has a parameter at arg_index (or takes varargs)."""
    for i, sig in enumerate(info.signatures):
        if arg_index < len(sig.params) or sig.varargs:
            return i
    return 0
