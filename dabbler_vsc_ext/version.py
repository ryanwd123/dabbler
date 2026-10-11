"""Bumps the extension's version in package.json and package-lock.json: the patch part by default, or minor / major.

    python version.py          0.0.8 -> 0.0.9
    python version.py minor    0.0.8 -> 0.1.0

Commit the bump, then publish that version with publish_build.bat.
"""
import json
import sys
from pathlib import Path

here = Path(__file__).parent
pkg, lock = here / "package.json", here / "package-lock.json"
data = json.loads(pkg.read_text())
major, minor, patch = (int(x) for x in data["version"].split("."))
part = sys.argv[1] if len(sys.argv) > 1 else "patch"
new = {"major": f"{major + 1}.0.0", "minor": f"{major}.{minor + 1}.0"}.get(part, f"{major}.{minor}.{patch + 1}")
print(f"{data['version']} -> {new}")
data["version"] = new
pkg.write_text(json.dumps(data, indent=2) + "\n")

locked = json.loads(lock.read_text())
locked["version"] = new
locked["packages"][""]["version"] = new
lock.write_text(json.dumps(locked, indent=2) + "\n")
