#!/usr/bin/env bash
set -euo pipefail

DATA_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir) DATA_DIR="${2:-}"; shift 2 ;;
    *) echo "Usage: $0 --data-dir /path/to/data" >&2; exit 2 ;;
  esac
done

if [[ -z "$DATA_DIR" ]]; then
  echo "Missing --data-dir" >&2
  echo "Usage: $0 --data-dir /path/to/data" >&2
  exit 2
fi

OUT_DIR="processed_data/flag_parquet"
mkdir -p "$OUT_DIR"

# TODO: replace with your real command/script
echo "TODO: implement flag extraction. DATA_DIR=$DATA_DIR OUT_DIR=$OUT_DIR"
