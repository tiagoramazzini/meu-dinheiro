from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "VERSION"


def read_version() -> tuple[int, int]:
    if not VERSION_FILE.exists():
        return 0, 0
    raw = VERSION_FILE.read_text(encoding="utf-8").strip() or "0.0"
    major_str, *rest = raw.split(".", 1)
    minor_str = rest[0] if rest else "0"
    try:
        major = int(major_str)
        minor = int(minor_str)
    except ValueError:
        return 0, 0
    return major, minor


def write_version(major: int, minor: int) -> str:
    value = f"{major}.{minor}"
    VERSION_FILE.write_text(value, encoding="utf-8")
    return value


def bump_minor() -> str:
    major, minor = read_version()
    return write_version(major, minor + 1)


def main() -> None:
    new_version = bump_minor()
    print(f"Version bumped to {new_version}")


if __name__ == "__main__":
    main()
