import argparse
from pathlib import Path
import hashlib
import pandas as pd
from datetime import datetime, timezone

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed-dir", default="processed_data")
    ap.add_argument("--out-dir", default="processed_data/dv")
    args = ap.parse_args()

    processed = Path(args.processed_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    files = [
        processed / "archive_batch_seismic_readings.parquet",
        processed / "archive_batch_seismic_readings_2.parquet",
        processed / "sgx_traces.parquet",
    ]

    rows = []
    ingest_dts = now_utc_iso()
    for fp in files:
        if not fp.exists():
            continue
        rows.append({
            "file_name": fp.name,
            "file_path": str(fp.resolve()),
            "size_bytes": fp.stat().st_size,
            "sha256": sha256_file(fp),
            "ingest_dts": ingest_dts,
            "record_source": "processed_data"
        })

    df = pd.DataFrame(rows).sort_values("file_name")
    out_path = out / "batch_provenance.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Wrote {out_path} rows={len(df)}")

if __name__ == "__main__":
    main()
