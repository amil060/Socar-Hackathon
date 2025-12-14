# SOCAR Hackathon 2025 – SumBa 101
## Data Engineering Track

This project is a complete end-to-end data engineering solution developed for SOCAR Hackathon 2025 by Team SumBa 101.
The goal of the project is to ingest raw and legacy datasets, recover corrupted files, transform them into structured analytical models, orchestrate the pipeline with Airflow, and expose results through dashboards.

The solution strictly follows the hackathon guidelines and runs entirely on the provided virtual machine.

---

## Project Architecture

Raw Data Sources (.parquet, .sgx)
→ Recovery & Parsing (parquet repair, SGX parsing)
→ Raw Data Vault (Hubs, Links, Satellites)
→ Dimensional Models / Data Marts
→ Analytics Dashboard
→ Airflow Orchestration & Web Pipeline Launcher

---

## Web Application – Pipeline Launcher

- Built with Streamlit
- No file upload (directory-based processing for large datasets)
- Executes full pipeline and displays logs
- Port: 8090

Path:
/home/hackathon/webapp/

---

## Data Processing Pipeline

1. Parquet Recovery  
solutions/corrupted_parquet.sh --data-dir <DATA_DIR>

2. SGX → Parquet  
python src/sgx_to_parquet.py --data-dir <DATA_DIR> --out-dir <OUT_DIR>

3. Data Vault Build  
solutions/build_vault.sh --data-dir <DATA_DIR>

Outputs written to:
processed_data/dv/

4. Mart / Iceberg Publish  
python scripts/iceberg_publish.py

---

## Airflow Orchestration

- Runs via Docker Compose
- DAG: sumba_pipeline
- Port: 80

Trigger example:
docker-compose exec airflow-webserver airflow dags trigger sumba_pipeline \
--conf '{"data_dir":"/home/hackathon/data"}'

---

## Dashboard

- Built with Streamlit
- Port: 8080

---

## Ports

Airflow: 80  
Dashboard: 8080  
Web App: 8090  

---

## How to Run

Activate environment:
source /home/hackathon/venv/bin/activate

Start Web App:
streamlit run /home/hackathon/webapp/app.py --server.port 8090 --server.address 0.0.0.0

Start Airflow:
cd /home/hackathon/airflow
docker-compose restart

Run pipeline manually:
bash /home/hackathon/webapp/scripts/run_pipeline.sh /home/hackathon/data run_test

---

## Hackathon Compliance

- Uses provided VM
- Bash scripts accept --data-dir
- Outputs parquet files
- Handles corrupted parquet files
- Converts SGX legacy format
- Implements Data Vault modeling
- Uses Airflow orchestration
- Provides analytics dashboard

---

## Team

Team Name: SumBa 101  
Hackathon: SOCAR Hackathon 2025  
Track: Data Engineering
