from IPython.core.magic import cell_magic, magics_class, Magics
from queue import Queue
from dabbler.gui_main import MyApp


@magics_class
class DbDabbler(Magics):
    def __init__(self, ipython, local_ns: dict = None):
        super(DbDabbler, self).__init__(ipython)
        for item in ipython.ev("dir()"):
            if "duckdb.DuckDBPyConnection" in str(type(ipython.ev(item))):
                db = ipython.ev(item).cursor()
                break
        if db:
            self.q = Queue()
            self.app = MyApp([], db, q=self.q)
            ipython.run_line_magic("gui", "qt")
            from IPython.lib.guisupport import start_event_loop_qt4

            start_event_loop_qt4(self.app)
        else:
            print("db not found")

    @cell_magic
    def runq(self, line, cell):
        start_txt = '"""--sql\n'
        start = cell.find(start_txt) + len(start_txt)
        end_txt = '"""'
        end = cell[start:].find(end_txt) + start
        sql = cell[start:end]
        print(sql)
        self.q.put(sql)


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

    magics = DbDabbler(ipython)
    ipython.register_magics(magics)
