#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"

if [[ -d /opt/homebrew/opt/rustup/bin ]]; then
  export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
fi

unset CONDA_PREFIX || true
export VIRTUAL_ENV="$project_root/.venv"
export UV_CACHE_DIR="$project_root/.uv-cache"
export PATH="$VIRTUAL_ENV/bin:$PATH"

mkdir -p .rust-dist
maturin build --manifest-path rust-core/Cargo.toml --release --out .rust-dist

wheels=(.rust-dist/*.whl)
if [[ ${#wheels[@]} -ne 1 ]]; then
  echo "Expected exactly one Rust wheel, found ${#wheels[@]}" >&2
  exit 1
fi

uv pip install --python .venv/bin/python --no-deps "${wheels[0]}"
