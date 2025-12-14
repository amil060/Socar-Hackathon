from __future__ import annotations

from datetime import datetime
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

# English comments only:
# This DAG triggers the same pipeline runner used by the webapp.
# You can pass data_dir and run_id via dagrun.conf.

RUN_SCRIPT = "/home/hackathon/webapp/scripts/run_pipeline.sh"

def run_pipeline(**context) -> None:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    data_dir = conf.get("data_dir", "/home/hackathon/data")

    # Use Airflow run_id if not provided
    airflow_run_id = context.get("run_id", "airflow_run")
    run_id = conf.get("run_id", airflow_run_id.replace(":", "_").replace("+", "_"))

    env = os.environ.copy()
    # If you want, you can pass extra env vars here
    # env["PIPELINE_RUN_ID"] = run_id

    subprocess.run(
        [RUN_SCRIPT, data_dir, run_id],
        check=True,
        env=env,
    )

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
