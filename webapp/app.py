import time
import uuid
import subprocess
from pathlib import Path

import streamlit as st

# English comments only:
# Streamlit UI that accepts a data directory path (no upload), triggers the pipeline,
# and shows logs + useful links.

APP_TITLE = "SumBa 101 - Data Pipeline Launcher"
RUNS_ROOT = Path("/home/hackathon/webapp/runs")
RUN_SCRIPT = "/home/hackathon/webapp/scripts/run_pipeline.sh"
VALIDATE_SCRIPT = "/home/hackathon/webapp/scripts/validate_inputs.py"

AIRFLOW_URL = "http://sumba101de-vm-ip.polandcentral.cloudapp.azure.com:80"
DASHBOARD_URL = "http://sumba101de-vm-ip.polandcentral.cloudapp.azure.com:8080"

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.header("Links")
    st.link_button("Open Airflow", AIRFLOW_URL)
    st.link_button("Open Dashboard", DASHBOARD_URL)

st.subheader("1) Select data directory (no upload)")
data_dir = st.text_input(
    "Enter mounted dataset directory path",
    value="/home/hackathon/data",
    help="Example: /mnt/data or /home/hackathon/data (must exist on the VM).",
)

col1, col2 = st.columns(2)
with col1:
    run_btn = st.button("Run pipeline", type="primary")
with col2:
    refresh_btn = st.button("Refresh latest run")

def list_runs():
    if not RUNS_ROOT.exists():
        return []
    return sorted([p.name for p in RUNS_ROOT.iterdir() if p.is_dir()], reverse=True)

def read_log(run_id: str) -> str:
    log_path = RUNS_ROOT / run_id / "pipeline.log"
    if not log_path.exists():
        return ""
    return log_path.read_text(errors="ignore")

def run_pipeline(data_dir_path: str) -> str:
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    v = subprocess.run(
        ["python", VALIDATE_SCRIPT, data_dir_path],
        capture_output=True,
        text=True,
    )
    if v.returncode != 0:
        st.error(v.stdout.strip() or v.stderr.strip() or "Validation failed.")
        return ""

    out_dir = RUNS_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    log_file = (out_dir / "pipeline.log").open("a")

    subprocess.Popen(
        [RUN_SCRIPT, data_dir_path, run_id],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )

    return run_id

if run_btn:
    new_run = run_pipeline(data_dir)
    if new_run:
        st.success(f"Started: {new_run}")
        st.rerun()

if refresh_btn:
    st.rerun()

st.subheader("2) Runs")
runs = list_runs()
selected = st.selectbox("Select a run to view logs", runs if runs else ["(no runs yet)"])

if runs and selected != "(no runs yet)":
    st.subheader(f"Logs: {selected}")
    log_text = read_log(selected)
    st.code(log_text if log_text else "(no logs yet)", language="text")

    done_path = RUNS_ROOT / selected / "DONE"
    if done_path.exists():
        st.success("Pipeline finished ✅")
    else:
        st.info("Pipeline running (or not started) ⏳")
