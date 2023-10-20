# dabbler
An IPython extension that works with DuckDB.  This project is a work in progress and there could be many bugs.

Table/Query browser:
![screenshot of browser](images/gui_screenshot.png)


Intellisnse / autocomplete:

![screenshot of intellisense](images/auto_complete.png)

### Features:
- A window to view tables and queries
- Auto completion for:
    - ✔️tables 
    - ✔️columns 
    - ✔️functions 
    - 🔲keywords
    - 🔲file paths
- Send/Execute SQL query from vscode editor window to dabbler window to and page through outputs

### Install / Setup
- `pip install dabbler`
- install "dabbler" vscode extension
- install PySide or PyQt e.g. `pip install PySide6`
- For the language server to work in VScode you need to have dabbler installed in the python enviornment that is active in VScode.  
- In vscode python settings enable line magics for python files:
    - open settings by pressing: `ctr+,`
    - search for "line magic" and check the box:
       >![screenshot of text box](images/line_magic_checkbox.png)
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
- The language server communicates with ipython/jupyter to get table and column names using zmq






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