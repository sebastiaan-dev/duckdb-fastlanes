#!/usr/bin/env bash
set -euo pipefail

TPCH_SCALE_FACTORS="${TPCH_SCALE_FACTORS:-1}"
VENV_PATH="$(pwd)/venv"

if [[ ! -d "$VENV_PATH" ]]; then
  python3 -m venv "$VENV_PATH"
fi

source "$VENV_PATH/bin/activate"

pip install --upgrade pip >/dev/null
pip install "duckdb==1.4.0"

python scripts/benchmark/generate_tpch_datav2.py \
  --scale-factors "$TPCH_SCALE_FACTORS" \
  --file-formats parquet fls vortex duckdb

deactivate || true