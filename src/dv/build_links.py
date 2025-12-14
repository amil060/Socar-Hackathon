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

    # recovered readings contain well_id, sensor_id, survey_type_id
    p1 = processed / "archive_batch_seismic_readings.parquet"
    p2 = processed / "archive_batch_seismic_readings_2.parquet"
    df1 = safe_read_parquet(p1)
    df2 = safe_read_parquet(p2)
    readings = pd.concat([d for d in [df1, df2] if not d.empty], ignore_index=True) if (not df1.empty or not df2.empty) else pd.DataFrame()

    if readings.empty:
        raise SystemExit("No recovered parquet readings found; cannot build link_well_sensor_survey")

    needed = {"well_id","sensor_id","survey_type_id"}
    missing = needed - set(readings.columns)
    if missing:
        raise SystemExit(f"Missing columns in readings: {missing}")

    rel = readings[["well_id","sensor_id","survey_type_id"]].dropna().copy()
    rel["well_id"] = rel["well_id"].astype(int)
    rel["sensor_id"] = rel["sensor_id"].astype(int)
    rel["survey_type_id"] = rel["survey_type_id"].astype(int)
    rel = rel.drop_duplicates().sort_values(["well_id","sensor_id","survey_type_id"])

    load_dts = now_utc_iso()
    rel["load_dts"] = load_dts
    # Hash keys (must match hub hashing scheme)
    rel["hk_well"] = rel["well_id"].astype(str).map(lambda x: hk("WELL", x))
    rel["hk_sensor"] = rel["sensor_id"].astype(str).map(lambda x: hk("SENSOR", x))
    rel["hk_survey"] = rel["survey_type_id"].astype(str).map(lambda x: hk("SURVEY", x))

    # Link HK: deterministic from the three hub keys
    rel["hk_link_well_sensor_survey"] = rel.apply(
        lambda r: hk("LINK_WSS", f"{r['hk_well']}|{r['hk_sensor']}|{r['hk_survey']}"),
        axis=1
    )

    link = rel[["hk_link_well_sensor_survey","hk_well","hk_sensor","hk_survey","load_dts"]].copy()
    link["record_source"] = "recovered_parquet"

    out_path = out / "link_well_sensor_survey.parquet"
    link.to_parquet(out_path, index=False)
    print(f"Wrote {out_path} rows={len(link)}")

if __name__ == "__main__":
    main()
