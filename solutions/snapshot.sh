#!/usr/bin/env bash
set -euo pipefail

TS=$(date -u +"%Y%m%d_%H%M%S")
OUT="snapshots/$TS"
mkdir -p "$OUT"

cp -r processed_data/dv "$OUT/dv"
cp -r processed_data/star "$OUT/star"
cp -r processed_data/marts "$OUT/marts"

echo "✅ Snapshot created: $OUT"
