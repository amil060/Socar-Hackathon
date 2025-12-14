#!/usr/bin/env bash
set -euo pipefail

if [[ -f "venv/bin/activate" ]]; then
  source venv/bin/activate
fi

# 1) Hubs
python3 src/dv/build_hubs.py --processed-dir processed_data --out-dir processed_data/dv

# 2) Links
python3 src/dv/build_links.py --processed-dir processed_data --out-dir processed_data/dv

# 3) Satellites
python3 src/dv/build_sats.py --processed-dir processed_data --out-dir processed_data/dv

# 4) Provenance (checksums)
python3 src/dv/build_provenance.py --processed-dir processed_data --out-dir processed_data/dv
