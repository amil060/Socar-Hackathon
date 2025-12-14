import argparse
import glob
import os
import struct

import pandas as pd

MAGIC = b"CPETRO01"
HEADER_SIZE = 16
RECORD_SIZE = 13  # well_id(4) + depth_ft(4) + amplitude(4) + quality_flag(1)

def parse_sgx_file(path: str) -> pd.DataFrame:
    with open(path, "rb") as f:
        data = f.read()

    if len(data) < HEADER_SIZE:
        raise ValueError(f"{path}: too small for SGX header")

    magic = data[0:8]
    if magic != MAGIC:
        raise ValueError(f"{path}: bad magic {magic!r}, expected {MAGIC!r}")

    survey_type_id = struct.unpack_from("<I", data, 8)[0]
    trace_count = struct.unpack_from("<I", data, 12)[0]

    expected = HEADER_SIZE + trace_count * RECORD_SIZE
    if len(data) < expected:
        raise ValueError(f"{path}: truncated: expected >= {expected}, got {len(data)}")

    rows = []
    off = HEADER_SIZE
    base = os.path.basename(path)

    for i in range(trace_count):
        well_id = struct.unpack_from("<I", data, off)[0]; off += 4
        depth_ft = struct.unpack_from("<f", data, off)[0]; off += 4
        amplitude = struct.unpack_from("<f", data, off)[0]; off += 4
        quality_flag = data[off]; off += 1

        rows.append({
            "survey_type_id": int(survey_type_id),
            "trace_index": int(i),
            "well_id": int(well_id),
            "depth_ft": float(depth_ft),
            "amplitude": float(amplitude),
            "quality_flag": int(quality_flag),
            "source_file": base,
        })

    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="processed_data")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    files = glob.glob(os.path.join(args.data_dir, "**", "*.sgx"), recursive=True)
    if not files:
        raise SystemExit(f"No .sgx files found under: {args.data_dir}")

    dfs = []
    for fp in sorted(files):
        dfs.append(parse_sgx_file(fp))

    out_df = pd.concat(dfs, ignore_index=True)
    out_path = os.path.join(args.out_dir, "sgx_traces.parquet")
    out_df.to_parquet(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(f"files={len(files)} rows={len(out_df)}")

if __name__ == "__main__":
    main()
