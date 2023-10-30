# dabber

This extension is the client portion of a language server that provides auto-completion for sql text within python files.  To use this extension the server must be installed in your python enviornment.  

Instructions on installing the server: https://github.com/ryanwd123/dabbler


![screenshot](https://github.com/ryanwd123/dabbler/blob/master/images/auto_complete.png?raw=true)

### Developement
- to test changes to the extension:
    ```
    npm run esbuild
    ```
- to publish updates:
    ```
    ./publish_build.bat
    ```