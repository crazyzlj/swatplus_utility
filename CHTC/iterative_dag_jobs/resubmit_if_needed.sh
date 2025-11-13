#!/bin/bash
# resubmit_if_needed.sh
# Called by SCRIPT POST, runs on the submit node (in the main project directory)

# --- Add Logging ---
LOG_FILE="post_script.log"
echo "----------------------------------------" >> "$LOG_FILE"
echo "$(date): POST Script starting in $(pwd)" >> "$LOG_FILE"
# --- End Add ---

SIGNAL_FILE="continue_signal.txt"
DAG_FILE_TO_SUBMIT="main_workflow.dag"
DAG_LOG_DIR="dag_logs"

# Log check
echo "POST Script: Checking signal file: $SIGNAL_FILE" >> "$LOG_FILE" 

# Use 'grep -q' which quietly checks if the pattern exists.
# It returns exit code 0 (true for 'if') if "continue" is found.
if grep -q "continue" "$SIGNAL_FILE"; then
    echo "POST Script: Signal file contains 'continue'. Resubmitting DAG: $DAG_FILE_TO_SUBMIT with force overwrite." >> "$LOG_FILE" # Log action

    # --- Add Logging Around Submit ---
    echo "$(date): Executing: condor_submit_dag -f -outfile_dir $DAG_LOG_DIR $DAG_FILE_TO_SUBMIT" >> "$LOG_FILE"
    # Clean up files that trigger recovery mode ---
    echo "POST Script: Cleaning up DAG state files..."
    rm -f ${DAG_FILE_TO_SUBMIT}.lock
    rm -f ${DAG_FILE_TO_SUBMIT}.nodes.log 
    # Optionally remove rescue files too, though -f should handle this
    rm -f ${DAG_FILE_TO_SUBMIT}.rescue*

    # Resubmit, directing logs to the subdirectory
    # -f means force to overwrite submission files, otherwise, errors will be raised:
    #    ERROR: "main_workflow.dag.condor.sub" already exists.
    #    ERROR: "main_workflow.dag.lib.out" already exists.
    #    ...
    #    Some file(s) needed by condor_dagman already exist.
    condor_submit_dag -f -outfile_dir "$DAG_LOG_DIR" "$DAG_FILE_TO_SUBMIT" >> "$LOG_FILE" 2>&1
    SUBMIT_EXIT_CODE=$?
    echo "$(date): condor_submit_dag finished with exit code $SUBMIT_EXIT_CODE" >> "$LOG_FILE"
    # --- End Add ---

    if [ $SUBMIT_EXIT_CODE -eq 0 ]; then
         echo "POST Script: DAG resubmitted successfully. Removing signal file." >> "$LOG_FILE" # Log success
         rm "$SIGNAL_FILE"
    else
         echo "POST Script: ERROR - condor_submit_dag failed with exit code $SUBMIT_EXIT_CODE. Signal file retained for debugging." >> "$LOG_FILE" # Log failure
         # Optionally exit with an error code to make the POST script fail in DAGMan's view
         # exit 1 
    fi
else
    echo "POST Script: Signal file is empty or does not contain 'continue'. Max iterations likely reached. Workflow finished." >> "$LOG_FILE" # Log finish
    # Optionally remove the empty signal file here too for cleanup
    # rm "$SIGNAL_FILE"
fi

echo "$(date): POST Script finished." >> "$LOG_FILE" # Log end
exit 0 # Always exit 0 so DAGMan thinks the POST script itself succeeded
