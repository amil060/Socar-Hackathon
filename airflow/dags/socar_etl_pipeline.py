from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

PROJECT = "/opt/project"

default_args = {"owner": "hackathon", "retries": 0}

def run_cmd(cmd: str):
    subprocess.run(cmd, shell=True, check=True, executable="/bin/bash")

with DAG(
    dag_id="socar_etl_pipeline",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["socar", "etl", "data-platform"],
) as dag:

    build_vault = PythonOperator(
        task_id="build_vault",
        python_callable=run_cmd,
        op_args=[f"""
            set -e
            cd {PROJECT}
            mkdir -p processed_data processed_data/dv
            chmod -R ug+rwX processed_data || true
            ./solutions/build_vault.sh
        """],
    )

    run_tests = PythonOperator(
        task_id="run_dv_tests",
        python_callable=run_cmd,
        op_args=[f"set -e; cd {PROJECT} && python3 src/dv/run_tests.py"],
    )

    build_star = PythonOperator(
        task_id="build_star",
        python_callable=run_cmd,
        op_args=[f"set -e; cd {PROJECT} && python3 src/star/build_star.py"],
    )

    build_marts = PythonOperator(
        task_id="build_marts",
        python_callable=run_cmd,
        op_args=[f"set -e; cd {PROJECT} && python3 src/marts/build_marts.py"],
    )

    make_snapshot = PythonOperator(
        task_id="make_snapshot",
        python_callable=run_cmd,
        op_args=[f"""
            set -e
            cd {PROJECT}
            mkdir -p snapshots
            ./solutions/snapshot.sh
        """],
    )

    build_vault >> run_tests >> build_star >> build_marts >> make_snapshot
