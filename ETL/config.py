"""
Lightweight configuration helpers.

We eagerly load values from a local `.env` file (if present) so environment
variables can be managed without exporting them in the shell each run.
"""

import os
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and key not in os.environ:
            os.environ[key] = value


_HERE = Path(__file__).resolve().parent

# Load local .env files, preferring repo root overrides.
for candidate in (
    _HERE.parent / ".env",
    _HERE / ".env",
):
    _load_env_file(candidate)


def require_env(name: str) -> str:
    """Return an environment variable or raise a clear error."""
    value = os.environ.get(name)
    if value:
        return value
    raise RuntimeError(
        f"Missing environment variable {name}. "
        "Set it in your shell, or add it to a local .env file."
    )
