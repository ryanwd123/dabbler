"""Finds the SQL strings in a python file with python's tokenizer.

A string is SQL when it is the first argument of `.sql(`, `.execute(` or
`.executemany(` (or their `query=` keyword), or when its text starts with a
`--sql` line; the SQL then starts on the line after the marker.

Regions keep the string's exact offsets in the file, so positions map back
and forth by adding the region's start. In f-strings, each replacement field
like `{table}` becomes underscores of the same length, which parse as a name.

Python code is often half-typed in an editor, so tokenizer errors don't end
the scan: an unterminated string runs to the end of its line (or the file, for
triple quotes), and an indentation error restarts tokenizing at that line.
"""
import functools
import io
import re
import tokenize
from collections import deque
from dataclasses import dataclass
from typing import Union

SQL_CALLS = frozenset(["sql", "execute", "executemany"])
SQL_KEYWORD_ARG = "query"
re_marker = re.compile(r"--sql[^\n]*\n", re.IGNORECASE)
re_prefix = re.compile(r"[a-zA-Z]*")
SKIP = frozenset([tokenize.NL, tokenize.NEWLINE, tokenize.COMMENT, tokenize.INDENT, tokenize.DEDENT])
# tokens whose offsets are needed; the rest only matter by their text
POSITIONED = frozenset([tokenize.STRING, tokenize.FSTRING_START, tokenize.FSTRING_END, tokenize.OP])
# files without any of these have no SQL, and skip tokenizing
re_maybe_sql = re.compile(r"\.(?:sql|execute|executemany)\s*\(|--sql", re.IGNORECASE)
RECOVERED = -1
"""Token type of an unterminated string, cut off at the end of its line or the file."""


@dataclass(frozen=True)
class SqlRegion:
    start: int
    """Offset in the file of the SQL's first character."""
    end: int
    text: str
    """The SQL; as long as source[start:end], with f-string fields replaced."""
    fields: tuple[tuple[int, int], ...] = ()
    """File offsets of an f-string's replacement fields, which are python code."""

    @property
    def has_fields(self) -> bool:
        """An f-string with replacement fields, so the SQL isn't exactly what will run."""
        return bool(self.fields)


def line_starts(source: str) -> list[int]:
    starts = [0]
    starts.extend(m.end() for m in re.finditer("\n", source))
    return starts


def position_to_offset(source: str, line: int, character: int) -> int:
    """Offset of an LSP position, whose character counts UTF-16 code units."""
    starts = line_starts(source)
    if line >= len(starts):
        return len(source)
    start = starts[line]
    end = starts[line + 1] if line + 1 < len(starts) else len(source)
    units = 0
    for i in range(start, end):
        if units >= character:
            return i
        units += 2 if ord(source[i]) > 0xFFFF else 1
    return end if units >= character or end == len(source) else end - 1


def _tokens(source: str):
    """(type, string, start, end) tuples, carrying on after tokenizer errors.

    Offsets are only worked out for strings and operators (else -1).

    Unterminated strings come out as STRING tokens running to the end of the
    line (single quotes) or of the file (triple quotes).
    """
    starts = line_starts(source)
    first_line = 0  # 0-based line the current tokenize run started at
    while first_line < len(starts):
        base = starts[first_line]
        chunk = source[base:]

        def offset(row_col):
            row, col = row_col
            return starts[first_line + row - 1] + col

        try:
            for t in tokenize.generate_tokens(io.StringIO(chunk).readline):
                if t.type in POSITIONED:
                    yield t.type, t.string, offset(t.start), offset(t.end)
                elif t.type == tokenize.ENDMARKER:
                    return
                else:
                    yield t.type, t.string, -1, -1
            return
        except tokenize.TokenError as e:
            msg, (row, col) = e.args
            if "string" not in msg:
                return  # EOF inside brackets: everything has been yielded
            # col is 1-based and points at the string (its prefix letters or its quote)
            string_start = offset((row, col)) - 1
            quote_start = string_start
            while quote_start < len(source) and source[quote_start].isalpha():
                quote_start += 1
            if quote_start >= len(source) or source[quote_start] not in "'\"":
                return
            triple = source.startswith(source[quote_start] * 3, quote_start)
            quote_end = quote_start + (3 if triple else 1)
            line_end = source.find("\n", quote_end)
            end = len(source) if triple or line_end < 0 else line_end
            if end > quote_end and source[end - 1] == "\r":
                end -= 1
            yield RECOVERED, source[string_start:end], string_start, end
            if triple:
                return
            next_line = first_line + row  # the line after the string
        except (IndentationError, SyntaxError) as e:
            row = e.lineno or 1
            # restart at the offending line, or past it if it's where this run started
            next_line = first_line + row - 1 if row > 1 else first_line + 1
        first_line = next_line


def _string_content(s: str, tok_start: int, tok_end: int) -> Union[tuple[int, int, bool], None]:
    """(start, end, raw) of a plain string's text, or None for bytes."""
    prefix = re_prefix.match(s).group(0).lower()  # type: ignore
    if "b" in prefix:
        return None
    body = s[len(prefix):]
    quote = body[:3] if body[:3] in ('"""', "'''") else body[:1]
    start = tok_start + len(prefix) + len(quote)
    end = tok_end - len(quote) if s.endswith(quote) and len(body) >= 2 * len(quote) else tok_end
    return start, max(start, end), "r" in prefix


def _blank_escapes(chars: list[str]) -> None:
    """Turns python escapes into what SQL sees, without moving anything: `\\n` into
    two spaces, `\\"` into a space and a quote."""
    i = 0
    while i < len(chars) - 1:
        if chars[i] != "\\":
            i += 1
            continue
        nxt = chars[i + 1]
        if nxt in "ntr":
            chars[i] = chars[i + 1] = " "
        elif nxt in "'\"\\\n":
            chars[i] = " "
        i += 2


def _is_sql_call(previous: deque) -> bool:
    """True if the significant tokens before a string are `.name(` or `.name(query=`."""
    names = list(previous)
    if names[-2:] == [SQL_KEYWORD_ARG, "="]:
        names = names[:-2]
    return len(names) >= 3 and names[-1] == "(" and names[-2] in SQL_CALLS and names[-3] == "."


def _make_region(
    source: str, start: int, end: int, fields: list[tuple[int, int]], is_call: bool, fstring=False, raw=False
):
    text = source[start:end]
    if not raw and "\\" in text:
        chars = list(text)
        _blank_escapes(chars)
        text = "".join(chars)
    if fstring:
        chars = list(text)
        field_set = set()
        for fs, fe in fields:
            for i in range(fs - start, fe - start):
                chars[i] = "_"
                field_set.add(i)
        # what's left of the braces are the escapes {{ and }}; keep offsets with a space
        i = 0
        while i < len(chars) - 1:
            if i not in field_set and chars[i] in "{}" and chars[i + 1] == chars[i]:
                chars[i + 1] = " "
                i += 2
            else:
                i += 1
        text = "".join(chars)
    marker = re_marker.match(text)
    if marker:
        start += marker.end()
        text = text[marker.end():]
    elif not is_call:
        return None
    if not text.strip():
        return None
    return SqlRegion(start, start + len(text), text, tuple(f for f in fields if f[1] > start))


@functools.lru_cache(maxsize=32)
def sql_regions(source: str) -> tuple[SqlRegion, ...]:
    """Every SQL string in a python file, in order."""
    if not re_maybe_sql.search(source):
        return ()
    regions: list[SqlRegion] = []
    previous: deque[str] = deque(maxlen=5)  # text of the last significant tokens
    # f-strings being read: [content start, brace depth, fields, is_call, raw]
    fstrings: list[list] = []

    def close_fstring(ctx, end: int):
        fields = [(s, e) for s, e in ctx[2] if e is not None]
        region = _make_region(source, ctx[0], end, fields, ctx[3], fstring=True, raw=ctx[4])
        if region:
            regions.append(region)
        previous.append("")

    for typ, string, start, end in _tokens(source):
        if fstrings:
            ctx = fstrings[-1]
            if typ == RECOVERED:
                # the outermost f-string never closed; the tokenizer gave up on it
                close_fstring(fstrings[0], end)
                fstrings.clear()
            elif typ == tokenize.FSTRING_START:
                fstrings.append([end, 0, [], False, False])
            elif typ == tokenize.OP and string == "{":
                if ctx[1] == 0:
                    ctx[2].append([start, None])
                ctx[1] += 1
            elif typ == tokenize.OP and string == "}":
                ctx[1] -= 1
                if ctx[1] == 0 and ctx[2]:
                    ctx[2][-1][1] = end
            elif typ == tokenize.FSTRING_END:
                fstrings.pop()
                if not fstrings:
                    close_fstring(ctx, start)
            continue

        if typ == tokenize.FSTRING_START:
            fstrings.append([end, 0, [], _is_sql_call(previous), "r" in string.lower()])
            continue
        if typ == tokenize.STRING or typ == RECOVERED:
            content = _string_content(string, start, end)
            if content:
                region = _make_region(source, content[0], content[1], [], _is_sql_call(previous), raw=content[2])
                if region:
                    regions.append(region)
        if typ not in SKIP:
            previous.append(string)

    if fstrings:
        close_fstring(fstrings[0], len(source))
    return tuple(regions)


def region_at(source: str, offset: int) -> Union[SqlRegion, None]:
    """The SQL string containing offset (its ends included), if any."""
    for region in sql_regions(source):
        if region.start <= offset <= region.end:
            if any(fs < offset < fe for fs, fe in region.fields):
                return None  # python code inside an f-string
            return region
        if region.start > offset:
            break
    return None
