"""Bumps dabbler.__version__: the last part by default, or the part named on the command line.

    python scripts/version.py          0.8.1.2 -> 0.8.1.3
    python scripts/version.py minor    0.8.1.2 -> 0.9.0
    python scripts/version.py major    0.8.1.2 -> 1.0.0
    python scripts/version.py 0.9.0    sets it
"""
import re
import sys
from pathlib import Path

FILE = Path(__file__).parent.parent / "dabbler" / "__init__.py"
PATTERN = re.compile(r'(__version__\s*(?::\s*str\s*)?=\s*)"([^"]+)"')


def bump(version: str, part: str) -> str:
    if re.fullmatch(r"\d+(\.\d+)*", part):
        return part
    numbers = [int(x) for x in version.split(".")]
    while len(numbers) < 3:
        numbers.append(0)
    if part == "major":
        return f"{numbers[0] + 1}.0.0"
    if part == "minor":
        return f"{numbers[0]}.{numbers[1] + 1}.0"
    numbers[-1] += 1
    return ".".join(str(n) for n in numbers)


def main(argv: list[str]) -> None:
    text = FILE.read_text()
    match = PATTERN.search(text)
    if not match:
        sys.exit(f"no __version__ in {FILE}")
    old = match.group(2)
    new = bump(old, argv[0] if argv else "last")
    FILE.write_text(PATTERN.sub(lambda m: f'{m.group(1)}"{new}"', text, count=1))
    print(f"{old} -> {new}")


if __name__ == "__main__":
    main(sys.argv[1:])
