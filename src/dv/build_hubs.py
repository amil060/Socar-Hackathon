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
    sgx = processed / "sgx_traces.parquet"

    df1 = safe_read_parquet(p1)
    df2 = safe_read_parquet(p2)
    readings = pd.concat([d for d in [df1, df2] if not d.empty], ignore_index=True) if (not df1.empty or not df2.empty) else pd.DataFrame()
    sgx_df = safe_read_parquet(sgx)

    load_dts = now_utc_iso()

    # HUB: WELL
    well_parts = []
    if not readings.empty and "well_id" in readings.columns:
        well_parts.append(readings[["well_id"]])
    if not sgx_df.empty and "well_id" in sgx_df.columns:
        well_parts.append(sgx_df[["well_id"]])

    if well_parts:
        wells = pd.concat(well_parts, ignore_index=True).dropna()
        wells["well_id"] = wells["well_id"].astype(int)
        wells = wells.drop_duplicates().sort_values("well_id")
        hub_well = pd.DataFrame({
            "hk_well": wells["well_id"].astype(str).map(lambda x: hk("WELL", x)),
            "well_id": wells["well_id"],
            "load_dts": load_dts,
            "record_source": "processed_data"
        })
        hub_well.to_parquet(out / "hub_well.parquet", index=False)
        print(f"Wrote {out/'hub_well.parquet'} rows={len(hub_well)}")
    else:
        print("No well_id found; hub_well not written.")

    # HUB: SENSOR (only recovered parquet has sensor_id)
    if not readings.empty and "sensor_id" in readings.columns:
        sensors = readings[["sensor_id"]].dropna()
        sensors["sensor_id"] = sensors["sensor_id"].astype(int)
        sensors = sensors.drop_duplicates().sort_values("sensor_id")
        hub_sensor = pd.DataFrame({
            "hk_sensor": sensors["sensor_id"].astype(str).map(lambda x: hk("SENSOR", x)),
            "sensor_id": sensors["sensor_id"],
            "load_dts": load_dts,
            "record_source": "recovered_parquet"
        })
        hub_sensor.to_parquet(out / "hub_sensor.parquet", index=False)
        print(f"Wrote {out/'hub_sensor.parquet'} rows={len(hub_sensor)}")
    else:
        print("No sensor_id found; hub_sensor not written.")

    # HUB: SURVEY TYPE
    survey_parts = []
    if not readings.empty and "survey_type_id" in readings.columns:
        survey_parts.append(readings[["survey_type_id"]])
    if not sgx_df.empty and "survey_type_id" in sgx_df.columns:
        survey_parts.append(sgx_df[["survey_type_id"]])

    if survey_parts:
        surveys = pd.concat(survey_parts, ignore_index=True).dropna()
        surveys["survey_type_id"] = surveys["survey_type_id"].astype(int)
        surveys = surveys.drop_duplicates().sort_values("survey_type_id")
        hub_survey = pd.DataFrame({
            "hk_survey": surveys["survey_type_id"].astype(str).map(lambda x: hk("SURVEY", x)),
            "survey_type_id": surveys["survey_type_id"],
            "load_dts": load_dts,
            "record_source": "processed_data"
        })
        hub_survey.to_parquet(out / "hub_survey.parquet", index=False)
        print(f"Wrote {out/'hub_survey.parquet'} rows={len(hub_survey)}")
    else:
        print("No survey_type_id found; hub_survey not written.")

if __name__ == "__main__":
    main()
