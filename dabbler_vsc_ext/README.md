# dabbler

The VS Code client for dabbler's language server, which helps with DuckDB SQL inside python files: completions (tables, columns, CTEs, functions, keywords, file paths), function signature help, hover docs, and syntax errors from DuckDB's own parser.

The server runs in your python environment, so install dabbler there (Python 3.12+):

```
pip install "dabbler[gui]"
```

If it isn't installed in the selected interpreter, the extension offers to install it. More: https://github.com/ryanwd123/dabbler

![screenshot](https://github.com/ryanwd123/dabbler/blob/master/images/auto_complete.png?raw=true)

### Development
- install the build tools: `npm ci`
- build: `npm run esbuild` (then F5 in VS Code to try it)
- type check: `npm run compile`
- publish: `./publish_build.bat` (bumps the version with `python version.py`, builds, packages and publishes with vsce)
