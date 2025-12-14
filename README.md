SOCAR Hackathon 2025 – SumBa 101
Data Engineering Track – Full Project Documentation

This repository documents the full data engineering solution developed by Team SumBa 101 for SOCAR Hackathon 2025.
The project demonstrates a production-style data pipeline handling corrupted data, legacy binary formats, analytical modeling, orchestration, and visualization — all running on the provided hackathon virtual machine.

1. Project Goal

The main objective of this project is to:

Process raw and corrupted datasets

Recover hidden or damaged parquet files

Parse legacy SGX seismic binary files

Transform raw data into a Data Vault model

Build analytical data marts

Orchestrate the entire pipeline using Airflow

Provide a web-based pipeline launcher and analytics dashboard

The solution is designed to be scalable, modular, and fully reproducible.

2. High-Level Architecture

The system follows a layered enterprise data architecture:

Raw Data Sources
(.parquet, .sgx)
        |
        v
Recovery & Parsing Layer
- Corrupted parquet recovery
- SGX binary parsing
        |
        v
Raw Data Vault
- Hubs
- Links
- Satellites
        |
        v
Dimensional Models / Data Marts
        |
        v
Analytics & Visualization
        |
        v
Airflow Orchestration + Web Pipeline Launcher

3. Technologies Used
Core Technologies

Python 3 – main data processing language

Bash – pipeline automation and hackathon-required scripts

Parquet – columnar storage format

DuckDB – analytical processing

Apache Iceberg – table abstraction for marts

Docker & Docker Compose – Airflow deployment

Streamlit – web application & dashboard

Apache Airflow – pipeline orchestration

Data Modeling

Data Vault 2.0

Hubs (business keys)

Links (relationships)

Satellites (descriptive attributes)

4. Raw Data Handling
4.1 Corrupted Parquet Files

Some parquet files were intentionally corrupted with oversized footers and hidden binary content.

We implemented:

A parquet repair script that:

Detects corrupted metadata

Extracts valid row groups

Recovers embedded information

Output is written to:

processed_data/


Script:

solutions/corrupted_parquet.sh --data-dir <DATA_DIR>

4.2 SGX Legacy Binary Files

SGX files are legacy seismic binary files containing structured records.

We implemented:

A custom SGX parser in Python

Binary decoding using struct

Conversion of SGX traces into structured parquet format

Script:

python src/sgx_to_parquet.py --data-dir <DATA_DIR> --out-dir <OUT_DIR>


Output:

processed_data/sgx_traces.parquet

5. Data Vault Implementation

After parsing and recovery, the data is transformed into a Raw Data Vault model.

Vault Components

Hubs

hub_well

hub_sensor

hub_survey

Links

link_well_sensor_survey

Satellites

sat_readings

Vault build scripts:

solutions/build_vault.sh --data-dir <DATA_DIR>


Output structure:

processed_data/dv/
  hub_*.parquet
  link_*.parquet
  sat_*.parquet


Snapshots of vault states are stored under:

snapshots/<timestamp>/

6. Analytical Data Marts

From the Data Vault layer, analytical marts are created:

Star-schema style fact and dimension tables

Optimized for analytical queries

Published using DuckDB and Iceberg-compatible layouts

Script:

python scripts/iceberg_publish.py


Output example:

mart_sensor_analysis/

7. Web Application – Pipeline Launcher

To simplify execution and demonstrate usability, we built a web-based pipeline launcher.

Key Features

Built with Streamlit

No dataset upload (supports very large datasets)

User inputs a directory path

Runs full pipeline end-to-end

Displays real-time logs

Port:

8090


Path:

/home/hackathon/webapp/


Pipeline execution is handled by:

webapp/scripts/run_pipeline.sh

8. Airflow Orchestration

The pipeline is orchestrated using Apache Airflow, deployed via Docker Compose.

Features

DAG triggers the same pipeline used by the web app

Manual triggering with custom parameters

Centralized scheduling and monitoring

DAG:

airflow/dags/sumba_pipeline_dag.py


Trigger example:

docker-compose exec airflow-webserver airflow dags trigger sumba_pipeline \
  --conf '{"data_dir":"/home/hackathon/data"}'


Port:

80

9. Analytics Dashboard

An analytics dashboard was built using Streamlit to visualize results from the marts.

Features:

Interactive charts

Aggregated sensor and survey analytics

Reads directly from processed data

Port:

8080

10. Ports Overview
Component	Port
Airflow Web UI	80
Analytics Dashboard	8080
Web Pipeline Launcher	8090
11. Repository & Directory Structure
solutions/
  corrupted_parquet.sh
  flag_parquet.sh
  load_sgx.sh
  build_vault.sh

src/
  sgx_to_parquet.py
  dv/
    build_hubs.py
    build_links.py
    build_sats.py

webapp/
  app.py
  scripts/run_pipeline.sh

airflow/
  dags/sumba_pipeline_dag.py

processed_data/
snapshots/
README.md

12. Hackathon Requirements Compliance

✔ Uses provided virtual machine

✔ No large file uploads

✔ Bash scripts accept --data-dir

✔ Outputs parquet files

✔ Recovers corrupted parquet files

✔ Parses legacy SGX format

✔ Implements Data Vault methodology

✔ Uses Airflow for orchestration

✔ Provides analytics dashboard

✔ End-to-end reproducible pipeline

13. Demo Flow (For Jury)

Open Airflow UI
http://<vm-ip>:80

Open Web Pipeline Launcher
http://<vm-ip>:8090

Enter dataset directory and start pipeline

Show logs:

Parquet recovery

SGX parsing

Data Vault build

Mart publishing

Open Dashboard
http://<vm-ip>:8080

Present analytical insights

14. Team Information

Team Name: SumBa 101

Hackathon: SOCAR Hackathon 2025

Track: Data Engineering