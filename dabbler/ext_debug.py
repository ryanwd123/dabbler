"""%load_ext dabbler.ext_debug: dabbler with debug logging (same as setting DABBLER_DEBUG=1)."""
from dabbler import ext


def load_ipython_extension(ipython):
    ext.load_ipython_extension(ipython, debug=True)


def unload_ipython_extension(ipython):
    ext.unload_ipython_extension(ipython)
