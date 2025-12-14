import pandas as pd
from pathlib import Path

MASTER_DIR = Path("data/caspian_hackathon_assets")
DV_DIR = Path("processed_data/dv")
OUT = Path("processed_data/star")
OUT.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load master dimensions (CSV)
# -----------------------------
dim_well = pd.read_csv(MASTER_DIR / "master_wells.csv")
dim_well["spud_date"] = pd.to_datetime(dim_well["spud_date"], errors="coerce")
dim_well.to_parquet(OUT / "dim_well.parquet", index=False)

dim_sensor = pd.read_csv(MASTER_DIR / "master_sensors.csv")
dim_sensor["calibration_date"] = pd.to_datetime(dim_sensor["calibration_date"], errors="coerce")
dim_sensor.to_parquet(OUT / "dim_sensor.parquet", index=False)

dim_survey = pd.read_csv(MASTER_DIR / "master_surveys.csv")
dim_survey.to_parquet(OUT / "dim_survey.parquet", index=False)

# -----------------------------
# Build fact from DV (recovered)
# -----------------------------
link = pd.read_parquet(DV_DIR / "link_well_sensor_survey.parquet")
sat  = pd.read_parquet(DV_DIR / "sat_readings.parquet")
hub_well = pd.read_parquet(DV_DIR / "hub_well.parquet")
hub_sensor = pd.read_parquet(DV_DIR / "hub_sensor.parquet")
hub_survey = pd.read_parquet(DV_DIR / "hub_survey.parquet")

base = sat.merge(link[["hk_link_well_sensor_survey","hk_well","hk_sensor","hk_survey"]],
                 on="hk_link_well_sensor_survey", how="left")
base = base.merge(hub_well[["hk_well","well_id"]], on="hk_well", how="left")
base = base.merge(hub_sensor[["hk_sensor","sensor_id"]], on="hk_sensor", how="left")
base = base.merge(hub_survey[["hk_survey","survey_type_id"]], on="hk_survey", how="left")

base["timestamp"] = pd.to_datetime(base["timestamp"], errors="coerce")
base = base[base["timestamp"].notna()].copy()
base["source_format"] = "recovered_parquet"

fact_sensor_readings = base[[
    "timestamp","well_id","sensor_id","survey_type_id",
    "depth_ft","amplitude","quality_flag","source_file","record_source","source_format"
]].copy()
fact_sensor_readings.to_parquet(OUT / "fact_sensor_readings.parquet", index=False)

# -----------------------------
# Fact from SGX traces
# (note: no sensor_id, no timestamp)
# -----------------------------
sgx_fp = Path("processed_data/sgx_traces.parquet")
if sgx_fp.exists():
    sgx = pd.read_parquet(sgx_fp)
    sgx["source_format"] = "sgx"
    sgx_fact = sgx[["survey_type_id","well_id","depth_ft","amplitude","quality_flag","source_file","source_format"]].copy()
    sgx_fact.to_parquet(OUT / "fact_sgx_traces.parquet", index=False)
else:
    sgx_fact = pd.DataFrame()

# -----------------------------
# dim_time from recovered timestamps
# -----------------------------
dim_time = pd.DataFrame({"day_ts": base["timestamp"].dt.floor("D").drop_duplicates().sort_values()})
dim_time["date"] = dim_time["day_ts"].dt.date
dim_time["year"] = dim_time["day_ts"].dt.year
dim_time["month"] = dim_time["day_ts"].dt.month
dim_time["day"] = dim_time["day_ts"].dt.day
dim_time.to_parquet(OUT / "dim_time.parquet", index=False)

# -----------------------------
# Anomalies fact (z-score per well_id, recovered only)
# -----------------------------
g = base.groupby("well_id")["amplitude"]
mu = g.transform("mean")
sd = g.transform("std").replace(0, pd.NA)
base["z"] = (base["amplitude"] - mu) / sd
anoms = base[base["z"].abs() >= 2.5].copy()
fact_anomalies = anoms[[
    "timestamp","well_id","sensor_id","survey_type_id",
    "depth_ft","amplitude","quality_flag","z","source_file","source_format"
]].copy()
fact_anomalies.to_parquet(OUT / "fact_anomalies.parquet", index=False)

print("✅ Wrote Star Schema to processed_data/star/")
print("dim_well:", len(dim_well), "dim_sensor:", len(dim_sensor), "dim_survey:", len(dim_survey), "dim_time:", len(dim_time))
print("fact_sensor_readings:", len(fact_sensor_readings), "fact_sgx_traces:", len(sgx_fact), "fact_anomalies:", len(fact_anomalies))
