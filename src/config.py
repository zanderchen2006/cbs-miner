"""Shared env loading."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def sqlite_path() -> Path:
    load_dotenv()
    return Path(env("SQLITE_PATH", "data/mqtt.db"))
