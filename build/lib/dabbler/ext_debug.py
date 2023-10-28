from dabbler.ext_classes import DbDabbler


def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.

    # app = .instance()
    # if app is None:

    magics = DbDabbler(ipython,debug=True)
    ipython.register_magics(magics)
