#!/bin/bash
set -e

echo "--- Worker Job: running on $(hostname) ---"
echo "--- Worker Job: current working directory: $PWD ---"

if [ -z "$1" ]; then
    echo "Shell Error: You must provide a calibration file path to this script."
    exit 1
fi

if [ -z "$2" ]; then
    echo "Shell Error: You must provide a result path to this script."
    exit 1
fi

CAL_FILE=$1
OUT_PATH=$2

echo "Shell script received path: $CAL_FILE $OUT_PATH"

echo "--- Step 1: Decompressing Base Model and observation data ---"
tar -xzf TxtInOut.tar.gz
tar -xzf observed.tar.gz
echo "Decompression complete."

echo "--- Step 2: Copy SWAT+ executable to TxtInOut ---"
cp /app/swatplus-61.0.2.11-ifx-lin_x86_64-Rel TxtInOut/

PYTHON_EXEC="/opt/conda/envs/pyswatplus_util/bin/python3"

echo "--- Step 3: Running SWAT+ model and calculating model performance---"
$PYTHON_EXEC -u sensitivity/worker_runmodel.py "$CAL_FILE" "$OUT_PATH"

echo "--- Worker Job: Job completed successfully ---"
exit 0