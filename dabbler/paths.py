import os
import sys
from pathlib import Path


def get_datadir() -> Path:
    """dabbler's data folder (%APPDATA%\\dabbler, ~/.local/share/dabbler, ~/Library/Application Support/dabbler)."""
    home = Path.home()
    if sys.platform == "win32":
        data_dir = Path(os.environ.get("APPDATA") or home / "AppData/Roaming") / "dabbler"
    elif sys.platform == "darwin":
        data_dir = home / "Library/Application Support/dabbler"
    else:
        data_dir = Path(os.environ.get("XDG_DATA_HOME") or home / ".local/share") / "dabbler"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir
