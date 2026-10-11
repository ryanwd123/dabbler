from typing import Union

from dabbler.ext_classes import DbDabbler

_magics: Union[DbDabbler, None] = None


def load_ipython_extension(ipython, debug=None):
    """%load_ext dabbler.ext: opens dabbler's window for the session's DuckDB connection."""
    global _magics
    unload_ipython_extension(ipython)
    _magics = DbDabbler(ipython, debug=debug)
    ipython.register_magics(_magics)


def unload_ipython_extension(ipython):
    """%unload_ext dabbler.ext (and %reload_ext): closes the windows and stops sharing the schema."""
    global _magics
    if _magics is not None:
        _magics.close()
        _magics = None
