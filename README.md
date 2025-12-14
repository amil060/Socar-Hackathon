SOCAR Hackathon 2025 – SumBa 101
Data Engineering Track

This project presents a production-style end-to-end data platform developed for SOCAR Hackathon 2025 by Team SumBa 101.
The solution demonstrates how raw and legacy datasets can be processed through a full ETL lifecycle — from ingestion and recovery to analytics — using modern data engineering tools.

The entire system runs on the provided hackathon virtual machine and strictly follows the competition guidelines.

🎯 Project Objective

The main objective is to build a single web-based entry point from which:

A dataset directory is selected (no file upload)

The complete ETL pipeline is executed

Data flows through recovery, parsing, Data Vault, and marts

Orchestration is handled via Airflow

Analytical results are exposed through a dashboard

Links to Airflow and the dashboard are provided to the user

🏗️ System Architecture
User (Browser)
      |
      v
Flask Web Application (HTML + CSS)
      |
      v
ETL Pipeline (Bash + Python)
      |
      +--> Parquet Recovery
      +--> SGX → Parquet Conversion
      +--> Data Vault Modeling
      +--> Dimensional Marts
      |
      v
Airflow Orchestration
      |
      v
Analytics Dashboard (Streamlit)

🧰 Technologies Used

Python 3 – core data processing

Bash – ETL orchestration scripts

Flask – lightweight pipeline web application

HTML & CSS – web UI design

Streamlit – analytics dashboard

Apache Airflow – pipeline orchestration

Docker & Docker Compose – Airflow deployment

Parquet – columnar data storage

DuckDB / Iceberg – analytical data processing

Data Vault 2.0 – data modeling methodology

🌐 Web Application (Pipeline Launcher)

A lightweight Flask-based web application serves as the main user interface.

Key Features

No dataset upload (directory-based processing for large datasets)

One-click pipeline execution

Clean and modern UI (HTML + CSS)

Provides direct links to:

Airflow UI

Analytics Dashboard

Port: 5000

Path:

/home/hackathon/flask_app/

🔁 ETL Pipeline Flow

The pipeline is executed via a single runner script and consists of the following stages:

1. Parquet Recovery

Repairs corrupted parquet files and recovers hidden metadata.

solutions/corrupted_parquet.sh --data-dir <DATA_DIR>

2. SGX → Parquet Conversion

Parses legacy SGX binary seismic files into parquet format.

python src/sgx_to_parquet.py --data-dir <DATA_DIR> --out-dir <OUT_DIR>

3. Data Vault Modeling

Transforms data into Raw Data Vault structures (Hubs, Links, Satellites).

solutions/build_vault.sh --data-dir <DATA_DIR>


Outputs:

processed_data/dv/
  hub_*.parquet
  link_*.parquet
  sat_*.parquet

4. Analytical Marts

Creates analytical, query-optimized marts.

python scripts/iceberg_publish.py

⏱️ Airflow Orchestration

Airflow is deployed using Docker Compose

A custom DAG executes the same pipeline used by the web app

Supports monitoring, retries, and logging

DAG Name: sumba_pipeline
Port: 80

Trigger example:

docker-compose exec airflow-webserver airflow dags trigger sumba_pipeline \
  --conf '{"data_dir":"/home/hackathon/data"}'

📊 Analytics Dashboard

Built with Streamlit

Reads processed marts and vault outputs

Automatically reflects newly processed datasets

Port: 8080

🔌 Ports Summary
Component	Port
Airflow UI	80
Dashboard	8080
Web App (Flask)	5000
▶️ How to Run
Activate environment
source /home/hackathon/venv/bin/activate

Start Web Application
cd /home/hackathon/flask_app
nohup python app.py > flask.log 2>&1 &

Start Airflow
cd /home/hackathon/airflow
docker-compose restart

Start Dashboard
streamlit run /home/hackathon/dashboard.py --server.port 8080

📁 Repository Structure
solutions/
  corrupted_parquet.sh
  flag_parquet.sh
  load_sgx.sh
  build_vault.sh

src/
  sgx_to_parquet.py
  dv/

flask_app/
  app.py
  templates/
  static/

airflow/
  dags/sumba_pipeline_dag.py

README.md

✅ Hackathon Compliance

✔ Uses provided VM

✔ No large file uploads

✔ Scripts accept --data-dir

✔ Outputs parquet files

✔ Handles corrupted parquet data

✔ Parses legacy SGX format

✔ Implements Data Vault modeling

✔ Uses Airflow orchestration

✔ Provides analytics dashboard

✔ End-to-end reproducible pipeline

👥 Team Information

Team Name: SumBa 101

Hackathon: SOCAR Hackathon 2025

Track: Data Engineering

🎤 Demo Summary (For Jury)

“From a single web interface, we trigger the full ETL pipeline.
Data flows from raw sources through recovery, parsing, Data Vault modeling, and marts.
Airflow handles orchestration, and results are instantly visible in the dashboard.”