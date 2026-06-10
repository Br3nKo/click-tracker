"""Loads standalone .lua files (Redis scripts) read once at import time."""

from pathlib import Path

_LUA_DIR = Path(__file__).parent


def load_script(name: str) -> str:
    """Return the contents of ``<name>.lua`` from this directory."""
    return (_LUA_DIR / f"{name}.lua").read_text()
