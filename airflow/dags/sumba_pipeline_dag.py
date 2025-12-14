from __future__ import annotations

from datetime import datetime
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

# English comments only:
# This DAG calls the same runner script used by the webapp.

RUN_SCRIPT = "/home/hackathon/webapp/scripts/run_pipeline.sh"

def run_pipeline(**context) -> None:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    data_dir = conf.get("data_dir", "/home/hackathon/data")
    run_id = conf.get("run_id", (context.get("run_id") or "airflow_run").replace(":", "_").replace("+", "_"))
    subprocess.run([RUN_SCRIPT, data_dir, run_id], check=True)

with DAG(
    dag_id="sumba_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sumba101", "pipeline"],
) as dag:
    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
