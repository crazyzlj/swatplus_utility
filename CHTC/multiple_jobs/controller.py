import os
import random

NUM_JOBS = 5
RUNS_DIR = "multi_runs"
DAG_FILE_NAME = "worker_jobs.dag"

print(f"--- Controller Script Started (DAG Generator) ---")
os.makedirs(RUNS_DIR, exist_ok=True)

with open(DAG_FILE_NAME, 'w') as dag_f:
    for i in range(NUM_JOBS):
        run_dir = os.path.join(RUNS_DIR, f"run_{i}")
        param_file = os.path.join(run_dir, "params.txt")
        os.makedirs(run_dir, exist_ok=True)
        with open(param_file, "w") as f:
            f.write(f"{random.randint(1, 100)}\n")
            f.write(f"{random.randint(1, 100)}\n")

        dag_f.write(f"JOB run_{i} worker.sub\n")

        dag_f.write(f"VARS run_{i} ParamFile=\"{param_file}\"\n")
        dag_f.write(f"VARS run_{i} RunDir=\"{run_dir}\"\n")
        dag_f.write("\n")

print(f"Successfully generated {NUM_JOBS} parameter files and {DAG_FILE_NAME}.")
print("--- Controller Script Finished ---")
