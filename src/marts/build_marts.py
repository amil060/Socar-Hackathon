import pandas as pd
from pathlib import Path

STAR = Path("processed_data/star")
OUT = Path("processed_data/marts")
OUT.mkdir(parents=True, exist_ok=True)

fact = pd.read_parquet(STAR / "fact_sensor_readings.parquet")  # recovered
fact["source_format"] = "recovered_parquet"

sgx_fp = STAR / "fact_sgx_traces.parquet"
if sgx_fp.exists():
    sgx = pd.read_parquet(sgx_fp)
else:
    sgx = pd.DataFrame(columns=["survey_type_id","well_id","depth_ft","amplitude","quality_flag","source_file","source_format"])

# ------------------------------------------------
# mart_well_performance (by well_id + source_format)
# ------------------------------------------------
well_fact = pd.concat([
    fact[["well_id","amplitude","quality_flag","source_format"]],
    sgx[["well_id","amplitude","quality_flag","source_format"]],
], ignore_index=True)

mart_well = (well_fact.groupby(["well_id","source_format"], as_index=False)
    .agg(
        total_readings=("amplitude","count"),
        avg_amplitude=("amplitude","mean"),
        data_quality_rate=("quality_flag", lambda s: (s==1).mean())
    )
    .sort_values(["source_format","total_readings"], ascending=[True,False])
)
mart_well.to_parquet(OUT / "mart_well_performance.parquet", index=False)
print("Wrote mart_well_performance rows=", len(mart_well))

# ------------------------------------------------
# mart_sensor_analysis (recovered only; sensor_id var)
# ------------------------------------------------
mart_sensor = (fact.groupby(["sensor_id"], as_index=False)
    .agg(
        total_readings=("amplitude","count"),
        avg_amplitude=("amplitude","mean"),
        data_quality_rate=("quality_flag", lambda s: (s==1).mean())
    )
    .sort_values("total_readings", ascending=False)
)
mart_sensor.to_parquet(OUT / "mart_sensor_analysis.parquet", index=False)
print("Wrote mart_sensor_analysis rows=", len(mart_sensor))

# ------------------------------------------------
# mart_survey_summary (by survey_type_id + source_format)
# ------------------------------------------------
survey_fact = pd.concat([
    fact[["survey_type_id","well_id","amplitude","quality_flag","timestamp","source_format"]],
    sgx.assign(timestamp=pd.NaT)[["survey_type_id","well_id","amplitude","quality_flag","timestamp","source_format"]],
], ignore_index=True)

mart_survey = (survey_fact.groupby(["survey_type_id","source_format"], as_index=False)
    .agg(
        wells_surveyed=("well_id","nunique"),
        total_readings=("amplitude","count"),
        avg_amplitude=("amplitude","mean"),
        data_quality_rate=("quality_flag", lambda s: (s==1).mean()),
        first_timestamp=("timestamp","min"),
        last_timestamp=("timestamp","max"),
    )
    .sort_values(["source_format","total_readings"], ascending=[True,False])
)
mart_survey.to_parquet(OUT / "mart_survey_summary.parquet", index=False)
print("Wrote mart_survey_summary rows=", len(mart_survey))
