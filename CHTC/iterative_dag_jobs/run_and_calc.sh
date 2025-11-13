#!/bin/bash
set -e

echo "--- Worker Job: running on $(hostname) ---"
echo "--- Worker Job: current working directory: $PWD ---"

echo "--- Worker Job: Step 0: Decompressing Base Model ---"
tar -xzf TxtInOut.tar.gz
echo "Decompression complete."

PYTHON_EXEC="/opt/conda/envs/pyswatplus_util/bin/python3"
echo "--- Worker Job: Step 1: Running Dummy Model ---"
$PYTHON_EXEC /app/test_chtc/dummy_model.py

echo "--- Worker Job: Step 2: Calculating Efficiency ---"
$PYTHON_EXEC /app/test_chtc/calculate_efficiency.py

echo "--- Worker Job: Job completed successfully ---"
exit 0