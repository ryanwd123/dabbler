use this python: C:\Users\ryanw\.virtualenvs\dabbler-F30Abi41\Scripts\python.exe

Dabbler is a Python tool to help with writing SQL within Python files. Dabbler has a GUI that is launched with an IPython extension, and it has a language server that is launched by a VS Code extension. 

A goal is for the auto-completion to be very full-featured, and that is still a work in progress.


## Git workflow

- After completing each change or task, commit it automatically. Don't ask first.
- Stage only the files related to that change. Never use `git add -A` or `git add .` if there are unrelated modifications.
- Write commit messages in imperative mood ("Add parser for X", not "Added..."), with a short summary line under 72 characters and a body explaining why if the change isn't obvious.
- Make small, focused commits: one logical change per commit.
- Work on `main`. The public repo is the `github` remote (github.com/ryanwd123/datadiver). `origin` is an old local copy and `master` is a private archive of the full history; never push either.
- Push to `github` only when I ask. Do not add remotes or create branches unless I ask.
- Never amend, rebase, reset, or force anything without asking.
- Never commit secrets, .env files, or large data files.
