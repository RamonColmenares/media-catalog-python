#!/usr/bin/env sh
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_dir"

if ! command -v uv >/dev/null 2>&1; then
  echo "Install uv to run the locked verification environment: https://docs.astral.sh/uv/" >&2
  exit 2
fi

uv sync --locked --extra dev
uv run --locked python - <<'PY'
import sys

if sys.version_info < (3, 11):
    raise SystemExit("Python 3.11 or newer is required")
print(f"Using Python {sys.version.split()[0]}")
PY
uv run --locked pytest -q
