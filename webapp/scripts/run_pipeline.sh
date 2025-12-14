#!/usr/bin/env bash
set -euo pipefail

# English comments only:
# This script runs the pipeline steps in order and writes logs for the web UI.

DATA_DIR="${1:?DATA_DIR is required}"
RUN_ID="${2:?RUN_ID is required}"
OUT_ROOT="/home/hackathon/webapp/runs/${RUN_ID}"

mkdir -p "${OUT_ROOT}"

LOG="${OUT_ROOT}/pipeline.log"
echo "RUN_ID=${RUN_ID}" | tee -a "${LOG}"
echo "DATA_DIR=${DATA_DIR}" | tee -a "${LOG}"
echo "Started at: $(date -Is)" | tee -a "${LOG}"

# Step 1: repair parquet
echo "== Step 1: repair parquet ==" | tee -a "${LOG}"
bash /home/hackathon/solutions/corrupted_parquet.sh --data-dir "${DATA_DIR}" 2>&1 | tee -a "${LOG}"

# Step 2: sgx -> parquet (FIX THIS PATH AFTER STEP 9)
echo "== Step 2: sgx -> parquet ==" | tee -a "${LOG}"
python /home/hackathon/scripts/SGX_SCRIPT_BURAYA_YAZ.py --data-dir "${DATA_DIR}" --out-dir "${OUT_ROOT}/sgx_parquet" 2>&1 | tee -a "${LOG}"

# Step 3: vault load (FIX THIS PATH AFTER STEP 9)
echo "== Step 3: vault load ==" | tee -a "${LOG}"
python /home/hackathon/scripts/VAULT_SCRIPT_BURAYA_YAZ.py --in-dir "${OUT_ROOT}/sgx_parquet" --run-id "${RUN_ID}" 2>&1 | tee -a "${LOG}"

# Step 4: publish marts/iceberg
echo "== Step 4: mart publish ==" | tee -a "${LOG}"
PIPELINE_RUN_ID="${RUN_ID}" python /home/hackathon/scripts/iceberg_publish.py 2>&1 | tee -a "${LOG}"

echo "Finished at: $(date -Is)" | tee -a "${LOG}"
echo "DONE" | tee -a "${OUT_ROOT}/DONE"
