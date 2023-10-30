from IPython.core.magic import cell_magic, magics_class, Magics
from queue import Queue
from dabbler.gui_main import MyApp


@magics_class
class DbDabbler(Magics):
    def __init__(self, ipython, debug=False):
        super(DbDabbler, self).__init__(ipython)
        db = None
        file = ipython.ev('__file__')
        for item in ipython.ev("dir()"):
            if "duckdb.DuckDBPyConnection" in str(type(ipython.ev(item))):
                db = ipython.ev(item)
                file_search_path = db.execute(
                    "select current_setting('file_search_path')"
                ).fetchone()[0]
                db = db.cursor()
                break
        if db:
            self.q = None
            self.app = MyApp([], db, debug=debug, file_search_path=file_search_path, file=file)
            ipython.run_line_magic("gui", "qt")
            from IPython.lib.guisupport import start_event_loop_qt4

            start_event_loop_qt4(self.app)
        else:
            print("db not found, create duckdb db before loading extension 'db = duckdb.connect()'")

    @cell_magic
    def runq(self, line, cell):
        if self.q:
            start_txt = '"""--sql\n'
            start = cell.find(start_txt) + len(start_txt)
            end_txt = '"""'
            end = cell[start:].find(end_txt) + start
            sql = cell[start:end]
            print(sql)
            self.q.put(sql)