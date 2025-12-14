#!/usr/bin/env bash
set -euo pipefail

DATA_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir)
      DATA_DIR="$2"
      shift 2
      ;;
    *)
      echo "Usage: $0 --data-dir <path>" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$DATA_DIR" ]]; then
  echo "Usage: $0 --data-dir <path>" >&2
  exit 1
fi

mkdir -p processed_data


if [[ -f "venv/bin/activate" ]]; then
  source venv/bin/activate
fi

python3 -m caspianpetro.cli --data-dir "$DATA_DIR" --out processed_data/sgx_traces.parquet

