import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path
import duckdb
from datetime import datetime



st.set_page_config(page_title="SOCAR Hackathon Dashboard", layout="wide")

SNAPSHOT_ROOT = Path("snapshots")

versions = []
if SNAPSHOT_ROOT.exists():
    versions = sorted([p.name for p in SNAPSHOT_ROOT.iterdir() if p.is_dir()])

VERSION = st.sidebar.selectbox(
    "Data version (Time Travel)",
    options=["latest"] + versions,
    index=0
)
if VERSION == "latest":
    BASE = Path("processed_data")
else:
    BASE = SNAPSHOT_ROOT / VERSION

st.title("SOCAR Hackathon – Seismic Data Dashboard")
st.caption(f"Source: {'processed_data/' if VERSION=='latest' else str(SNAPSHOT_ROOT / VERSION) + '/'} (recovery + Data Vault + marts)")

p = BASE
dv = p / "dv"
marts = p / "marts"

readings_fp = p / "archive_batch_seismic_readings.parquet"
readings2_fp = p / "archive_batch_seismic_readings_2.parquet"
sgx_fp = p / "sgx_traces.parquet"

ICEBERG_ENDPOINT = "http://localhost:8181"
ICEBERG_CATALOG = "iceberg_catalog"
ICEBERG_NAMESPACE = "sumba101"


def iceberg_connect() -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB and attach the Iceberg REST catalog."""
    con = duckdb.connect()

    # Enable Iceberg and allow reading latest snapshot without explicit version
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET unsafe_enable_version_guessing = true;")

    con.execute(
        """
        ATTACH 'warehouse' AS iceberg_catalog (
          TYPE iceberg,
          ENDPOINT 'http://localhost:8181',
          AUTHORIZATION_TYPE 'none'
        );
        """
    )
    return con



def iceberg_list_snapshots(con: duckdb.DuckDBPyConnection, fq_table: str):
    """Return snapshot_id + timestamp_ms for a given Iceberg table."""
    return con.execute(
        f"""
        SELECT snapshot_id, timestamp_ms
        FROM iceberg_snapshots('{fq_table}')
        ORDER BY timestamp_ms DESC
        """
    ).fetchall()


def iceberg_count_rows(con: duckdb.DuckDBPyConnection, fq_table: str, snapshot_id: int | None = None) -> int:
    """Count rows either at current table head or at a specific snapshot."""
    if snapshot_id is None:
        return con.execute(f"SELECT count(*) FROM {fq_table};").fetchone()[0]
    return con.execute(
        f"SELECT count(*) FROM {fq_table} AT (VERSION => {snapshot_id});"
    ).fetchone()[0]

@st.cache_data
def load_raw(base_path_str: str):
    p = Path(base_path_str)

    readings_fp = p / "archive_batch_seismic_readings.parquet"
    readings2_fp = p / "archive_batch_seismic_readings_2.parquet"
    sgx_fp = p / "sgx_traces.parquet"

    dfs = []
    for fp in [readings_fp, readings2_fp]:
        if fp.exists():
            dfs.append(pd.read_parquet(fp))

    readings = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    sgx = pd.read_parquet(sgx_fp) if sgx_fp.exists() else pd.DataFrame()

    if "timestamp" in readings.columns:
        readings["timestamp"] = pd.to_datetime(readings["timestamp"], errors="coerce")

    return readings, sgx

@st.cache_data
def load_marts(base_path_str: str):
    marts = Path(base_path_str) / "marts"

    mw_fp = marts / "mart_well_performance.parquet"
    ms_fp = marts / "mart_sensor_analysis.parquet"
    msv_fp = marts / "mart_survey_summary.parquet"

    mw = pd.read_parquet(mw_fp) if mw_fp.exists() else pd.DataFrame()
    ms = pd.read_parquet(ms_fp) if ms_fp.exists() else pd.DataFrame()
    msv = pd.read_parquet(msv_fp) if msv_fp.exists() else pd.DataFrame()

    return mw, ms, msv


def pick_col(df, candidates):
    """Returns the first existing column in a DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

tab1, tab2, tab3 = st.tabs(["Raw Data (Recovered)", "Marts (Analytics)", "Star Schema (Map & Anomalies)"])


# =========================
# TAB 1: Raw data
# =========================
with tab1:
    readings, sgx = load_raw(str(BASE))


    st.sidebar.header("Filters (Raw)")
    dataset_choice = st.sidebar.selectbox("Dataset", ["Recovered Parquet Readings", "SGX Traces"])

    if dataset_choice == "Recovered Parquet Readings":
        df = readings.copy()
        if df.empty:
            st.error("No readings parquet found in processed_data.")
            st.stop()

        well_ids = sorted(df["well_id"].dropna().unique().tolist()) if "well_id" in df.columns else []
        survey_ids = sorted(df["survey_type_id"].dropna().unique().tolist()) if "survey_type_id" in df.columns else []
        sensor_ids = sorted(df["sensor_id"].dropna().unique().tolist()) if "sensor_id" in df.columns else []

        sel_wells = st.sidebar.multiselect("Well ID", well_ids, default=well_ids[:10] if len(well_ids) > 10 else well_ids)
        sel_surveys = st.sidebar.multiselect("Survey Type", survey_ids, default=survey_ids)
        sel_sensors = st.sidebar.multiselect("Sensor ID", sensor_ids, default=sensor_ids[:10] if len(sensor_ids) > 10 else sensor_ids)

        if sel_wells and "well_id" in df.columns:
            df = df[df["well_id"].isin(sel_wells)]
        if sel_surveys and "survey_type_id" in df.columns:
            df = df[df["survey_type_id"].isin(sel_surveys)]
        if sel_sensors and "sensor_id" in df.columns:
            df = df[df["sensor_id"].isin(sel_sensors)]

        st.subheader("Recovered Parquet Readings – Summary")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Row count", f"{len(df):,}")
        if "amplitude" in df.columns and len(df) > 0:
            c2.metric("Amplitude orta", f"{df['amplitude'].mean():.2f}")
            c3.metric("Amplitude min", f"{df['amplitude'].min():.2f}")
            c4.metric("Amplitude max", f"{df['amplitude'].max():.2f}")

        colA, colB = st.columns(2)
        if "amplitude" in df.columns:
            colA.plotly_chart(px.histogram(df, x="amplitude", nbins=60, title="Amplitude distribution (Histogram)"),
                              use_container_width=True)
        if "quality_flag" in df.columns:
            q = df["quality_flag"].value_counts().reset_index()
            q.columns = ["quality_flag", "count"]
            colB.plotly_chart(px.pie(q, names="quality_flag", values="count", title="Quality flag distribution"),
                              use_container_width=True)

        colC, colD = st.columns(2)
        if "timestamp" in df.columns and df["timestamp"].notna().any() and "amplitude" in df.columns:
            ts = df.dropna(subset=["timestamp"]).copy()
            ts["date"] = ts["timestamp"].dt.date
            daily = ts.groupby("date", as_index=False)["amplitude"].mean()
            colC.plotly_chart(px.line(daily, x="date", y="amplitude", title="Daily average amplitude (trend)"),
                              use_container_width=True)

        if "survey_type_id" in df.columns and "amplitude" in df.columns:
            colD.plotly_chart(px.box(df, x="survey_type_id", y="amplitude", points="outliers",
                                     title="Amplitude comparison by Survey Type (Boxplot)"),
                              use_container_width=True)

        st.subheader("Sample data")
        st.dataframe(df.head(50), use_container_width=True)

    else:
        df = sgx.copy()
        if df.empty:
            st.error("sgx_traces.parquet not found in processed_data.")
            st.stop()

        st.subheader("SGX Traces – Summary")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Row count", f"{len(df):,}")
        if "amplitude" in df.columns:
            c2.metric("Amplitude orta", f"{df['amplitude'].mean():.2f}")
        if "depth_ft" in df.columns:
            c3.metric("Depth orta (ft)", f"{df['depth_ft'].mean():.1f}")
        if "source_file" in df.columns:
            c4.metric("File count", f"{df['source_file'].nunique()}")

        colA, colB = st.columns(2)
        if "depth_ft" in df.columns and "amplitude" in df.columns:
            colA.plotly_chart(px.scatter(df, x="depth_ft", y="amplitude", title="Depth vs Amplitude (scatter)", opacity=0.5),
                              use_container_width=True)
        if "amplitude" in df.columns:
            colB.plotly_chart(px.histogram(df, x="amplitude", nbins=60, title="Amplitude distribution (SGX)"),
                              use_container_width=True)

        st.subheader("Sample data")
        st.dataframe(df.head(50), use_container_width=True)

# =========================
# TAB 2: Marts
# =========================
with tab2:

    st.header("Iceberg Time Travel")

    table_name = st.selectbox(
        "Select an Iceberg table",
        options=[
            "mart_sensor_analysis",
            "mart_well_performance",
            "mart_well_summary",   # change to your real third table name if different
        ],
        index=0,
    )

    fq = f"{ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}.{table_name}"

    con = None
    try:
        con = iceberg_connect()
        snaps = iceberg_list_snapshots(con, fq)

        if len(snaps) < 2:
            st.warning("Not enough snapshots. Run the publish job at least twice to generate history.")
        else:
            # Build dropdown options as readable timestamps
            snapshot_options = []
            for sid, ts_ms in snaps[:20]:
                if isinstance(ts_ms, (int, float)):
                    dt = datetime.utcfromtimestamp(ts_ms / 1000.0)
                else:
                    dt = ts_ms
                ts_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            snapshot_options.append((sid, ts_str))

            selected = st.selectbox(
                "As of snapshot",
                options=snapshot_options,
                format_func=lambda x: f"{x[1]}  |  snapshot_id={x[0]}",
            )

            # Use explicit snapshots for both current and past versions (Iceberg best practice)
            latest_snapshot_id = int(snaps[0][0])  # Most recent snapshot

            current_rows = iceberg_count_rows(con, fq, snapshot_id=latest_snapshot_id)
            past_rows = iceberg_count_rows(con, fq, snapshot_id=int(selected[0]))


            c1, c2, c3 = st.columns(3)
            c1.metric("Current rows", f"{current_rows:,}")
            c2.metric("Selected snapshot rows", f"{past_rows:,}")
            c3.metric("Delta", f"{(current_rows - past_rows):,}")

            st.caption("This view is powered by Iceberg metadata snapshots and DuckDB time travel queries.")

    except Exception as e:
        st.error(f"Iceberg time travel is not available: {e}")
    finally:
        if con is not None:
            con.close()
        
    mw, ms, msv = load_marts(str(BASE))

    if mw.empty or ms.empty or msv.empty:
        st.warning("Mart files not found. First run `python3 src/marts/build_marts.py`.")
        st.stop()

    st.subheader("Analytics Marts – Summary")

    MW_READ = pick_col(mw, ["total_readings", "readings", "n_readings", "count"])
    MW_AVG  = pick_col(mw, ["avg_amplitude", "amplitude_avg", "mean_amplitude"])
    MW_QR   = pick_col(mw, ["data_quality_rate", "good_quality_rate", "quality_rate"])

    MS_READ = pick_col(ms, ["total_readings", "readings", "n_readings", "count"])
    MS_AVG  = pick_col(ms, ["avg_amplitude", "amplitude_avg", "mean_amplitude"])
    MS_QR   = pick_col(ms, ["data_quality_rate", "good_quality_rate", "quality_rate"])

    MSV_READ = pick_col(msv, ["total_readings", "readings", "n_readings", "count"])

    # TOP KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("Well count", f"{mw['well_id'].nunique():,}" if "well_id" in mw.columns else f"{len(mw):,}")
    k2.metric("Sensor count", f"{ms['sensor_id'].nunique():,}" if "sensor_id" in ms.columns else f"{len(ms):,}")
    k3.metric("Survey type count", f"{msv['survey_type_id'].nunique():,}" if "survey_type_id" in msv.columns else f"{len(msv):,}")

    st.markdown("### Top 10 Wells (most measurements)")
    if MW_READ is None:
        st.error(f"mart_well_performance columns mismatch: {mw.columns.tolist()}")
        st.stop()

    topw = mw.sort_values(MW_READ, ascending=False).head(10)

    color_col = "source_format" if "source_format" in mw.columns else None

    st.plotly_chart(
        px.bar(topw, x="well_id" if "well_id" in topw.columns else topw.index, y=MW_READ, color=color_col,
               title="Top 10 wells by readings"),
        use_container_width=True
    )

    st.markdown("### Sensor quality and dispersion")

    cA, cB = st.columns(2)
    
    if MS_READ is None:
        cA.warning(f"readings column not found in mart_sensor_analysis: {ms.columns.tolist()}")
    else:
        tops = ms.sort_values(MS_READ, ascending=False).head(15)
        cA.plotly_chart(
            px.bar(tops, x="sensor_id" if "sensor_id" in tops.columns else tops.index, y=MS_READ,
                   title="Top sensors by readings"),
            use_container_width=True
        )

    if (MS_AVG is not None) and (MS_QR is not None):
        cB.plotly_chart(
            px.scatter(ms, x=MS_AVG, y=MS_QR,
                       title=f"Sensor: {MS_AVG} vs {MS_QR}", opacity=0.6),
            use_container_width=True
        )
    else:
        cB.info(f"Required columns for scatter not found. Available: {ms.columns.tolist()}")

    # Survey Summary
    st.markdown("### Survey Summary")
    if MSV_READ is None:
        st.dataframe(msv, use_container_width=True)
    else:
        st.dataframe(msv.sort_values(MSV_READ, ascending=False), use_container_width=True)
# =========================
# TAB 3: Star Schema (Map & Anomalies)
# =========================
with tab3:
    star = BASE / "star"

    dim_well = pd.read_parquet(star / "dim_well.parquet")
    fact = pd.read_parquet(star / "fact_sensor_readings.parquet")
    anom = pd.read_parquet(star / "fact_anomalies.parquet")

    # =========================
    # WELLS MAP
    # =========================
    st.subheader("Wells Map (interaktiv)")

    if {"location_lat", "location_long"}.issubset(dim_well.columns):
        m = dim_well.rename(
            columns={
                "location_lat": "lat",
                "location_long": "lon"
            }
        ).copy()

        m["spud_date"] = pd.to_datetime(m["spud_date"], errors="coerce")

        fig = px.scatter_mapbox(
            m,
            lat="lat",
            lon="lon",
            hover_name="well_name",
            hover_data={
                "well_id": True,
                "operator": True,
                "spud_date": True,
                "lat": False,
                "lon": False,
            },
            zoom=6,
            height=520,
            title="Wells map (hover for details)"
        )

        # MAP STYLE + RED MARKERS
        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"l": 0, "r": 0, "t": 40, "b": 0}
        )
        fig.update_traces(marker=dict(color="red", size=10))

        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "Note: Hover over a point to see well_name, well_id, operator, and spud_date."
        )
    else:
        st.warning("location_lat / location_long not found in master_wells.csv.")

    # =========================
    # WELL DETAIL (WITH SELECTION)
    # =========================
    st.subheader("Well details (with selection)")

    sel = st.selectbox(
        "Select a well",
        sorted(dim_well["well_id"].unique())
    )

    st.dataframe(
        dim_well[dim_well["well_id"] == sel][
            ["well_id", "well_name", "operator", "spud_date", "location_lat", "location_long"]
        ],
        use_container_width=True
    )

    # =========================
    # WELL STATE
    # =========================
    st.subheader("Well State (quality & measurement count)")

    ws = (
        fact.groupby("well_id", as_index=False)
        .agg(
            total_readings=("amplitude", "count"),
            avg_amplitude=("amplitude", "mean"),
            quality_rate=("quality_flag", lambda s: (s == 1).mean()),
        )
        .sort_values("total_readings", ascending=False)
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Well count", ws["well_id"].nunique())
    c2.metric("Total readings", int(ws["total_readings"].sum()))
    c3.metric("Orta quality rate", f"{ws['quality_rate'].mean():.2%}")

    st.plotly_chart(
        px.bar(
            ws.head(15),
            x="well_id",
            y="total_readings",
            title="Top 15 wells by total readings",
        ),
        use_container_width=True,
    )

    # =========================
    # AMPLITUDE + QUALITY
    # =========================
    col1, col2 = st.columns(2)

    col1.plotly_chart(
        px.histogram(
            fact,
            x="amplitude",
            nbins=70,
            title="Amplitude distribution (Recovered)",
        ),
        use_container_width=True,
    )

    q = fact["quality_flag"].value_counts().reset_index()
    q.columns = ["quality_flag", "count"]

    col2.plotly_chart(
        px.pie(
            q,
            names="quality_flag",
            values="count",
            title="Quality flag distribution",
        ),
        use_container_width=True,
    )

    # =========================
    # ANOMALY HEATMAP
    # =========================
    st.subheader("Anomaly Heatmap (well_id x survey_type_id)")

    if len(anom) == 0:
        st.info("No anomalies found (with current threshold).")
    else:
        h = anom.groupby(
            ["well_id", "survey_type_id"], as_index=False
        ).size()
        h = h.rename(columns={"size": "anomaly_count"})

        pivot = (
            h.pivot(
                index="well_id",
                columns="survey_type_id",
                values="anomaly_count",
            )
            .fillna(0)
        )

        st.plotly_chart(
            px.imshow(
                pivot,
                title="Anomaly heatmap",
                aspect="auto",
            ),
            use_container_width=True,
        )

        st.caption(f"Anomalies found: {len(anom)}")
