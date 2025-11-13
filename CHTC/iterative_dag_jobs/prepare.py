import os
import random
import sys

NUM_JOBS_PER_ITER = 5
RUNS_BASE_DIR = "multi_runs"
STATE_FILE = "iteration.state"
WORKER_DAG_TEMPLATE = "worker_jobs_gen_{}.dag"
WORKER_DAG_CURRENT_SYMLINK = "worker_jobs_current.dag"

# --- Function: Read or initialize state ---
def get_current_generation():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    else:
        return 0

# --- Function: Update state file ---
def update_generation(gen_num):
    with open(STATE_FILE, 'w') as f:
        f.write(str(gen_num))

# --- Function: Prepare inputs and sub-DAG for one generation ---
def prepare_generation(gen_num):
    print(f"--- Preparing Generation {gen_num} ---")
    gen_dir = os.path.join(RUNS_BASE_DIR, f"gen_{gen_num}")
    os.makedirs(gen_dir, exist_ok=True)

    worker_dag_file = WORKER_DAG_TEMPLATE.format(gen_num)

    with open(worker_dag_file, 'w') as dag_f:
        for i in range(NUM_JOBS_PER_ITER):
            run_dir = os.path.join(gen_dir, f"run_{i}")
            param_file = os.path.join(run_dir, "params.txt")
            node_name = f"run_{i}"
            os.makedirs(run_dir, exist_ok=True)

            with open(param_file, "w") as f:
                f.write(f"{random.randint(1, 100)}\n")
                f.write(f"{random.randint(1, 100)}\n")

            # Write sub-DAG entry (worker.sub is in same directory)
            dag_f.write(f"JOB {node_name} worker.sub\n")
            dag_f.write(f"VARS {node_name} NodeArgs=\"{node_name}\"\n") # Pass node name as arg
            dag_f.write(f"VARS {node_name} ParamFile=\"{param_file}\"\n")
            dag_f.write(f"VARS {node_name} RunDir=\"{run_dir}\"\n")
            dag_f.write(f"VARS {node_name} GenNum=\"{gen_num}\"\n")
            dag_f.write("\n")

    # Update symbolic link to the current sub-DAG
    if os.path.lexists(WORKER_DAG_CURRENT_SYMLINK):
        os.remove(WORKER_DAG_CURRENT_SYMLINK)
    os.symlink(worker_dag_file, WORKER_DAG_CURRENT_SYMLINK)

    print(f"Generated {NUM_JOBS_PER_ITER} param files in {gen_dir}")
    print(f"Generated worker DAG: {worker_dag_file}")
    print(f"Updated symlink: {WORKER_DAG_CURRENT_SYMLINK}")

# --- Main logic for prepare step ---
if __name__ == "__main__":
    current_gen = get_current_generation()
    next_gen = current_gen + 1
    prepare_generation(next_gen)
    update_generation(next_gen) # Update state file to the *next* generation number
    print(f"State file updated to generation {next_gen}")
