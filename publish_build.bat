py scripts\version.py
py -m build
py -m twine upload dist/*
rmdir /s /q build