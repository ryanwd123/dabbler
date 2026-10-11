"""Function signatures and docs from DuckDB's own catalog (`duckdb_functions()`).

DuckDB describes every built-in function: overloads with parameter names and
types, the return type, a description and examples. This replaces the
functions.json that used to be scraped from the DuckDB docs site.
"""
import functools
import logging
import re
from dataclasses import dataclass, field
from typing import Union

import duckdb

logger = logging.getLogger(__name__)

# overloads listed in docs; functions like strftime have dozens
MAX_SIGNATURES_IN_DOCS = 8
re_positional = re.compile(r"col\d+$")

FUNCTIONS_SQL = """
    select function_name, function_type, description, return_type, parameters, parameter_types, varargs, examples
    from duckdb_functions()
    where function_type in ('scalar', 'aggregate', 'macro', 'table', 'table_macro')
        and not starts_with(function_name, '__')
    order by function_name, function_oid
"""


@dataclass(frozen=True)
class Signature:
    name: str
    params: tuple[str, ...]
    types: tuple[Union[str, None], ...] = ()
    return_type: Union[str, None] = None
    varargs: Union[str, None] = None
    named: tuple[tuple[str, Union[str, None]], ...] = ()
    """A table function's named parameters (`name := value`), in name order."""

    @property
    def param_labels(self) -> list[str]:
        labels = [f"{p} {t}" if t and t != "ANY" else p for p, t in zip(self.params, self.types or [None] * len(self.params))]
        if self.varargs:
            labels.append(f"{self.varargs}..." if self.varargs != "ANY" else "...")
        return labels

    @property
    def label(self) -> str:
        returns = f" -> {self.return_type}" if self.return_type else ""
        return f"{self.name}({', '.join(self.param_labels)}){returns}"


@dataclass
class FunctionInfo:
    name: str
    kind: str
    """scalar, aggregate, macro, table or table_macro"""
    description: Union[str, None] = None
    examples: list[str] = field(default_factory=list)
    signatures: list[Signature] = field(default_factory=list)
    user: bool = False
    """Created by the session; its examples are a macro's body."""

    @property
    def detail(self) -> str:
        """Short form for completion lists: the parameter names of the first overload."""
        if not self.signatures:
            return f"{self.name}()"
        sig = self.signatures[0]
        params = list(sig.params) + (["..."] if sig.varargs else [])
        return f"{self.name}({', '.join(params)})"

    def markdown(self) -> str:
        parts = [f"**{self.kind.replace('_', ' ')} function**" + (f": {self.description}" if self.description else "")]
        if self.signatures:
            shown = [s.label for s in self.signatures[:MAX_SIGNATURES_IN_DOCS]]
            more = len(self.signatures) - len(shown)
            if more > 0:
                shown.append(f"-- and {more} more")
            parts.append("```sql\n" + "\n".join(shown) + "\n```")
            named = self.signatures[0].named
            if named:
                parts.append("**Named parameters**: " + ", ".join(f"`{n}`" for n, _ in named))
        if self.examples:
            heading = "**Definition**" if self.user else "**Example**"
            parts.append(heading + "\n```sql\n" + "\n".join(self.examples[:3]) + "\n```")
        return "\n\n".join(parts)


def _add_row(functions: dict[str, FunctionInfo], row) -> None:
    name, kind, description, return_type, params, types, varargs, examples = row
    params, types = list(params or []), list(types or [])
    if len(types) < len(params):
        types += [None] * (len(params) - len(types))
    named: tuple = ()
    if kind == "table":
        # positional parameters are named col0, col1, ...; the rest are named parameters
        positional = [(p, t) for p, t in zip(params, types) if re_positional.match(p)]
        named = tuple(sorted((p, t) for p, t in zip(params, types) if not re_positional.match(p)))
        params, types = [p for p, _ in positional], [t for _, t in positional]
    sig = Signature(name, tuple(params), tuple(types), return_type, varargs, named)

    info = functions.get(name.lower())
    if info is None:
        info = functions[name.lower()] = FunctionInfo(name, kind)
    if sig not in info.signatures:
        info.signatures.append(sig)
    if not info.description and description:
        info.description = description
    for example in examples or []:
        if example not in info.examples:
            info.examples.append(example)


def functions_from(con: duckdb.DuckDBPyConnection) -> dict[str, FunctionInfo]:
    """Functions known to a connection, by lower-cased name."""
    functions: dict[str, FunctionInfo] = {}
    for row in con.execute(FUNCTIONS_SQL).fetchall():
        _add_row(functions, row)
    return functions


@functools.lru_cache(maxsize=8)
def builtin_functions(extensions: tuple[str, ...] = ()) -> dict[str, FunctionInfo]:
    """DuckDB's own functions with the given extensions loaded, by lower-cased name."""
    con = duckdb.connect()
    try:
        for ext in extensions:
            try:
                con.load_extension(ext)
            except Exception as e:
                logger.info(f'could not load extension "{ext}": {e}')
        return functions_from(con)
    finally:
        con.close()


def user_function_info(name: str, kind: str, params: list[str], definition: Union[str, None] = None) -> FunctionInfo:
    """A function or macro the session created, from the schema cache; a macro's body shows as its example."""
    return FunctionInfo(
        name, kind, examples=[definition] if definition else [], signatures=[Signature(name, tuple(params))], user=True
    )
