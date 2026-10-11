# dabbler
An IPython extension and sql language server (for sql text with python files) that works with DuckDB.  This project includes a vscode extension to work with the language server.  This project is a work in progress and there still some bugs.

Table/Query browser:
![screenshot of browser](images/gui_screenshot.png)


Intellisnse / autocomplete:

![screenshot of intellisense](images/auto_complete.png)

### Features:
- A window to view tables and queries
- Auto completion for:
    - ✔️tables 
    - ✔️columns 
        - should work for tables, views, CTEs, table functions, and subqueries
    - ✔️functions names
    - ✔️file paths (triggered by characters **`'./`**  )
    - ✔️keywords
    - 🔲function signature / parameters
    - 🔲re-use of named columns in current query
- Send/Execute SQL query from vscode editor to view in dabbler window

### Current Limitations
- The parser does not yet support every SQL feature in the duckdb dialect and it will fail to provide autocompletion if it cannot parse the SQL, this problem can be avoided in some cases by seperating statements into different blocks of text
- This currently only works with the "Python Interactive Window" in vscode - here is an article that explains how the interactive window works: https://code.visualstudio.com/docs/python/jupyter-support-py
- does not work well with python f-strings


### Install / Setup
#### Prerequisites
- Install VSCode
- Install Python VScode Extension
- Install Jupyter VScode Extension
#### Install dabbler
- `pip install dabbler`
- install PySide or PyQt e.g. `pip install PySide6`  (note: pyqt5 seems to work better on Linux)
- install "dabbler" vscode extension

#### Setup
- For the language server to work in VScode you need to have dabbler installed in the python enviornment that is active in VScode.  
    - Alternatively you can set the default enviornment for the language server in VScode settings
- In vscode python settings you can enable line magics for python files:
    - open settings by pressing: `ctr+,`
    - search for "line magic" and check the box:
       >![screenshot of text box](https://raw.githubusercontent.com/ryanwd123/dabbler/master/images/line_magic_checkbox.png)
- Use the line magic to load dabbler after making the duckdb connection as shown below.  Run the cell to start the "Interactive Window" in vscode. *note: If the file is run as a normal script the line-magic does not get executed*
    ```py
    #%%
    import duckdb
    db = duckdb.connect()
    #!%load_ext dabbler.ext
    ```

- In vscode you can set a keyboard shortcut to the dabbler command run the select node that your cursor is on in the GUI windown
  - press: `ctrl-shit-p` to bring up the command pallette
  - type in "keyboard" to find the open keyboard shortcuts command
  - with the keyboard shortcuts window search for "dabbler" and you can then assign a keybinding


### Overview
- The dabbler python package inlcudes a pygls language server.
    - The vscode extension starts the language server by running: `py -m dabbler.lsp`
- The language server gets table and column names from a SQLite schema cache that the IPython extension writes to
    - the cache is `schema_cache.sqlite3` in the dabbler data folder (`%APPDATA%\dabbler` on Windows, `~/.local/share/dabbler` on Linux, `~/Library/Application Support/dabbler` on macOS), or the path in the `DABBLER_SCHEMA_CACHE` environment variable
    - the IPython extension checks for updates to the database each time cells are executed (the ipython pre_run_cell and post_run_cell events) and saves the schema for the file it is running as rows (`sources`, `objects`, `columns`, `paths` tables); only tables, views and functions that changed are rewritten
    - the language server queries those rows on each completion request instead of keeping a copy; DuckDB's built-in functions and system views are the only catalog it keeps in memory
    - the rows persist, so completions keep working after the IPython session ends; files without their own schema (e.g. helper modules) use the most recently updated one in the same workspace
    - "send SQL to GUI" goes the other way through the same cache: the language server queues the query and the IPython extension picks it up
    - set `DABBLER_LSP_LOG` to a file path to get a debug log from the language server
- The language server includes an SQL parser created from using lark package
    - SQL strings in the python document are found with python's tokenizer (`lsp/sql_regions.py`). A string is SQL when:
        - it is the first argument of `.sql(...)`, `.execute(...)` or `.executemany(...)` (or their `query=` keyword), with any quotes and `r`/`f` prefixes, or
        - its text starts with a `--sql` line, e.g. `q = """--sql` (the SQL starts on the next line)
        - in f-strings, replacement fields like `{table}` are treated as a name; completions inside the braces are left to python
        - example:
        
            ![syntax_highlighting_example](images/syntax_highlighting_example.png)
    - If changes are made to the document within an sql text range the only that range of text is parsed and if there is an error that is published to the client.  Since the parse is not 100% complete at this point the parser could return errors that due parse in DuckDb.


### Next steps
- known bugs
    - autocompletion will not work with for database / schema names with spaces
    - need to update completions to use 'search_path' setting for multiple databases
- Updates to vs_code extension
    - better handling of server failing to start when dabbler not installed in python env
- SQL grammer features missing:
    - SQL standard syntax for pivot/unpivot (simplified is supported)
    - delete statement
    - update statement
    - create type
    - qualify clause
    - grouping sets, cube, rollup
- Ipython extension updates
    - Make it so you can Load dabbler without first creating DB
    - Support multiple cursors





### Future Improvement Ideas:
- GUI
    - Query history
    - Query manipulation - pivot filter
    - Column selector - hide columns
    - page history - press page up or page down
    - Configuration - retain information / setting on per file basis (file launched from)
        - query history
        - use full path or minimal path for tables
        - linting settings (if implemented)
- Language Server
    - Function signature helpers
    - Ability to connect to database file directly
        - could have the language server read a special comment line in python file that tells it what database to use for that file.
    - SQL Validation / Linting
    - Autocomplete for files
    - Add Notebook support
    - Tailor the syntax highlighting to DuckDB, I think this can be done by modifying a copy of this file: https://github.com/microsoft/vscode/blob/fa99dace5ee3b35a070ca4970422621af07c2781/extensions/sql/syntaxes/sql.tmLanguage.json


![dabblers in flight](images/dabblers.jpg)