#!/bin/bash

# reset_and_run.sh
# Cleans up previous DAG runs, initializes state, and submits a fresh DAG.

echo "--- Starting Workflow Reset and Run ---"

# 1. Remove all existing HTCondor jobs for this user
echo "Step 1: Removing existing HTCondor jobs..."
condor_rm -all
echo "Done removing jobs."
echo ""

# 2. Clean up DAGMan files, logs, and generated files
#    Using "rm -f" suppresses errors if files don't exist.
echo "Step 2: Cleaning up old DAG files, logs, and outputs..."
DAG_LOG_DIR="dag_logs"
rm -f *.dag.* *.lock  # DAGMan state and rescue files
rm -f post_script.log continue_signal.txt # POST script log and signal file
rm -f worker_jobs_*.dag worker_jobs_current.dag # Generated sub-DAGs
rm -rf multi_runs     # Worker job output directories
rm -rf "$DAG_LOG_DIR"
rm -f all_results.csv # Accumulated results
rm -f iteration.state # Iteration state
echo "Done cleaning files."
echo ""

# 3. Initialize state files for a fresh run
echo "Step 3: Initializing state files..."
echo "0" > iteration.state
touch all_results.csv
echo "State initialized (iteration=0, empty results file)."
echo ""

# 4. Ensure the DAG log directory exists
echo "Step 4: Ensuring DAG log directory exists..."
mkdir -p "$DAG_LOG_DIR"
echo "Directory '$DAG_LOG_DIR' ensured."
echo ""

# 5. Submit the main DAG workflow
echo "Step 5: Submitting the main DAG workflow..."
MAIN_DAG_FILE="main_workflow.dag"
condor_submit_dag -outfile_dir "$DAG_LOG_DIR" "$MAIN_DAG_FILE"
SUBMIT_EXIT_CODE=$?
echo ""

# 6. Report status
if [ $SUBMIT_EXIT_CODE -eq 0 ]; then
    echo "--- Successfully submitted DAG: $MAIN_DAG_FILE ---"
    echo "Use 'condor_q' to monitor."
else
    echo "--- ERROR: Failed to submit DAG: $MAIN_DAG_FILE ---"
    echo "Check the output above for errors from condor_submit_dag."
fi

exit $SUBMIT_EXIT_CODE
