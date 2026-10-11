import os
from typing import Union

import duckdb
from IPython.core.magic import Magics, line_magic, magics_class

# names checked first when the namespace has several connections
CONNECTION_NAMES = ("db", "con", "conn", "connection", "duck")

USAGE = """dabbler: %dabbler            show dabbler's window (finds the DuckDB connection)
         %dabbler <name>     use the DuckDB connection in variable <name>
         %dabbler refresh    re-read the schema for completions now"""


def find_connections(user_ns: dict) -> dict[str, duckdb.DuckDBPyConnection]:
    return {
        name: value
        for name, value in user_ns.items()
        if isinstance(value, duckdb.DuckDBPyConnection) and not name.startswith("_")
    }


def pick_connection(connections: dict) -> Union[str, None]:
    """The connection to use: the only one, or one with a usual name."""
    if len(connections) == 1:
        return next(iter(connections))
    return next((name for name in CONNECTION_NAMES if name in connections), None)


def source_file(user_ns: dict) -> Union[str, None]:
    """The file this session runs: a script's __file__, or VS Code's notebook/interactive file."""
    return user_ns.get("__file__") or user_ns.get("__vsc_ipynb_file__")


@magics_class
class DbDabbler(Magics):
    def __init__(self, ipython, debug: Union[bool, None] = None):
        super().__init__(ipython)
        self.debug = bool(os.environ.get("DABBLER_DEBUG")) if debug is None else debug
        self.app = None
        self.attach()

    def attach(self, name: Union[str, None] = None) -> None:
        user_ns = self.shell.user_ns  # type: ignore
        connections = find_connections(user_ns)
        if name is None:
            name = pick_connection(connections)
            if name is None:
                if connections:
                    print(f"dabbler: found several DuckDB connections ({', '.join(sorted(connections))}); "
                          "choose one with %dabbler <name>")
                else:
                    print("dabbler: no DuckDB connection yet. Create one (db = duckdb.connect()) and run %dabbler")
                return
        elif name not in connections:
            print(f"dabbler: {name!r} isn't a DuckDB connection")
            return
        self.start(name, connections[name])

    def start(self, name: str, db: duckdb.DuckDBPyConnection) -> None:
        from dabbler.gui_main import MyApp

        self.close()
        # reading the connection's setting here is safe: %load_ext / %dabbler is the cell being run
        file_search_path = db.execute("select current_setting('file_search_path')").fetchone()[0]  # type: ignore
        self.app = MyApp(
            db,
            debug=self.debug,
            file_search_path=file_search_path or None,
            file=source_file(self.shell.user_ns),  # type: ignore
            db_name=name,
        )
        try:
            self.shell.run_line_magic("gui", "qt")  # type: ignore
        except NotImplementedError:
            pass  # a shell without GUI event loop support, e.g. in tests

    def close(self) -> None:
        if self.app is not None:
            self.app.close()
            self.app = None

    @line_magic
    def dabbler(self, line: str):
        """%dabbler [<connection variable> | refresh]: show dabbler's window, switch connection, or re-read the schema."""
        arg = line.strip()
        if arg in ("help", "-h", "--help"):
            print(USAGE)
        elif arg == "refresh":
            if self.app is None:
                print("dabbler: not attached to a connection yet; run %dabbler")
            else:
                self.app.refresh_schema()
        elif arg:
            self.attach(arg)
        elif self.app is None:
            self.attach()
        else:
            self.app.show()
