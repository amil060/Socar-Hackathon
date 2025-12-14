import argparse
from pathlib import Path
import hashlib
import pandas as pd
from datetime import datetime, timezone

def hk(prefix: str, bk: str) -> str:
    return hashlib.sha256(f"{prefix}|{bk}".encode("utf-8")).hexdigest()

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def safe_read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed-dir", default="processed_data")
    ap.add_argument("--out-dir", default="processed_data/dv")
    args = ap.parse_args()

    processed = Path(args.processed_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    p1 = processed / "archive_batch_seismic_readings.parquet"
    p2 = processed / "archive_batch_seismic_readings_2.parquet"

    df1 = safe_read_parquet(p1)
    if not df1.empty:
        df1 = df1.copy()
        df1["source_file"] = p1.name

    df2 = safe_read_parquet(p2)
    if not df2.empty:
        df2 = df2.copy()
        df2["source_file"] = p2.name

    readings = pd.concat([d for d in [df1, df2] if not d.empty], ignore_index=True) if (not df1.empty or not df2.empty) else pd.DataFrame()
    if readings.empty:
        raise SystemExit("No recovered parquet readings found; cannot build sat_readings")

    needed = {"well_id","sensor_id","survey_type_id","depth_ft","amplitude","timestamp","quality_flag","source_file"}
    missing = needed - set(readings.columns)
    if missing:
        raise SystemExit(f"Missing columns in readings: {missing}")

    df = readings[list(needed)].dropna().copy()

    df["well_id"] = df["well_id"].astype(int)
    df["sensor_id"] = df["sensor_id"].astype(int)
    df["survey_type_id"] = df["survey_type_id"].astype(int)
    df["quality_flag"] = df["quality_flag"].astype(int)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df[df["timestamp"].notna()].copy()

    df["hk_well"] = df["well_id"].astype(str).map(lambda x: hk("WELL", x))
    df["hk_sensor"] = df["sensor_id"].astype(str).map(lambda x: hk("SENSOR", x))
    df["hk_survey"] = df["survey_type_id"].astype(str).map(lambda x: hk("SURVEY", x))

    df["hk_link_well_sensor_survey"] = df.apply(
        lambda r: hk("LINK_WSS", f"{r['hk_well']}|{r['hk_sensor']}|{r['hk_survey']}"),
        axis=1
    )

    load_dts = now_utc_iso()
    df["load_dts"] = load_dts
    df["record_source"] = "recovered_parquet"

    sat = df[[
        "hk_link_well_sensor_survey",
        "timestamp",
        "depth_ft",
        "amplitude",
        "quality_flag",
        "source_file",
        "load_dts",
        "record_source",
    ]].copy()

    out_path = out / "sat_readings.parquet"
    sat.to_parquet(out_path, index=False)
    print(f"Wrote {out_path} rows={len(sat)}")

if __name__ == "__main__":
    main()
