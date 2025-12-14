import sys
import pandas as pd

DV = "processed_data/dv"

def die(msg: str):
    print("❌ TEST FAILED:", msg)
    sys.exit(1)

def ok(msg: str):
    print("✅", msg)

def assert_unique(df, col, name):
    if df[col].isna().any():
        die(f"{name}: {col} has NULLs")
    dups = df[col].duplicated().sum()
    if dups > 0:
        die(f"{name}: {col} has {dups} duplicate(s)")
    ok(f"{name}: {col} unique ({len(df)} rows)")

def assert_no_nulls(df, cols, name):
    for c in cols:
        n = int(df[c].isna().sum())
        if n > 0:
            die(f"{name}: {c} has {n} NULL(s)")
    ok(f"{name}: no NULLs in {cols}")

def main():
    hub_well = pd.read_parquet(f"{DV}/hub_well.parquet")
    hub_sensor = pd.read_parquet(f"{DV}/hub_sensor.parquet")
    hub_survey = pd.read_parquet(f"{DV}/hub_survey.parquet")
    link = pd.read_parquet(f"{DV}/link_well_sensor_survey.parquet")
    sat = pd.read_parquet(f"{DV}/sat_readings.parquet")
    prov = pd.read_parquet(f"{DV}/batch_provenance.parquet")

    # HUB uniqueness
    assert_unique(hub_well, "hk_well", "hub_well")
    assert_unique(hub_well, "well_id", "hub_well")

    assert_unique(hub_sensor, "hk_sensor", "hub_sensor")
    assert_unique(hub_sensor, "sensor_id", "hub_sensor")

    assert_unique(hub_survey, "hk_survey", "hub_survey")
    assert_unique(hub_survey, "survey_type_id", "hub_survey")

    # LINK
    assert_unique(link, "hk_link_well_sensor_survey", "link_well_sensor_survey")
    assert_no_nulls(link, ["hk_well","hk_sensor","hk_survey","load_dts","record_source"], "link_well_sensor_survey")

    # SAT
    assert_no_nulls(
        sat,
        ["hk_link_well_sensor_survey","timestamp","depth_ft","amplitude","quality_flag","source_file","load_dts","record_source"],
        "sat_readings"
    )

    # Referential integrity
    missing = sat.loc[~sat["hk_link_well_sensor_survey"].isin(link["hk_link_well_sensor_survey"])]
    if len(missing) > 0:
        die(f"sat_readings: {len(missing)} rows reference missing link keys")
    ok("sat_readings: all link keys exist in link_well_sensor_survey")

    # Provenance checks
    assert_no_nulls(prov, ["file_name","file_path","size_bytes","sha256","ingest_dts","record_source"], "batch_provenance")
    if (prov["sha256"].astype(str).str.len() != 64).any():
        die("batch_provenance: some sha256 values are not length 64")
    ok("batch_provenance: sha256 looks valid (64 hex chars)")

    print("\n🎉 All Data Vault + Provenance tests passed.")

if __name__ == "__main__":
    main()
