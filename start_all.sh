#!/usr/bin/env bash
set -euo pipefail

# English comments only:
# Starts Streamlit webapp + dashboard (if streamlit) in background.
# Airflow usually runs separately; keep it as-is if already running.

source /home/hackathon/venv/bin/activate

# Start webapp (8090)
nohup streamlit run /home/hackathon/webapp/app.py --server.port 8090 --server.address 0.0.0.0 \
  > /home/hackathon/webapp/logs/webapp.log 2>&1 &

# Start dashboard (8080) - CHANGE PATH if your dashboard file is different
nohup streamlit run /home/hackathon/dashboard.py --server.port 8080 --server.address 0.0.0.0 \
  > /home/hackathon/webapp/logs/dashboard.log 2>&1 &

echo "WEBAPP:   http://sumba101de-vm-ip.polandcentral.cloudapp.azure.com:8090"
echo "DASH:     http://sumba101de-vm-ip.polandcentral.cloudapp.azure.com:8080"
echo "AIRFLOW:  http://sumba101de-vm-ip.polandcentral.cloudapp.azure.com:80"
