#!/bin/bash
set -e

echo "--- Worker Job: running on $(hostname) ---"
echo "--- Worker Job: current working directory: $PWD ---"

echo "--- Step 1: Decompressing Base Model and observation data ---"
tar -xzf TxtInOut.tar.gz
tar -xzf observed.tar.gz
echo "Decompression complete."

echo "--- Step 2: Copy SWAT+ executable to TxtInOut ---"
cp /app/swatplus-61.0.2.11-ifx-lin_x86_64-Rel TxtInOut/

PYTHON_EXEC="/opt/conda/envs/pyswatplus_util/bin/python3"

echo "--- Step 3: Generate samples for sensitivity analysis ---"
$PYTHON_EXEC -u sensitivity/ctrl_sensitivity_sample.py

echo "--- Worker Job: Job completed successfully ---"
exit 0