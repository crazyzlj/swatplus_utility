import os
import random
import sys
import csv

# --- Configuration ---
NUM_JOBS_PER_ITER = 5
MAX_ITERATIONS = 3
RUNS_BASE_DIR = "multi_runs"
STATE_FILE = "iteration.state"  # Records current generation number
RESULTS_FILE = "all_results.csv" # Accumulates all results
SIGNAL_FILE = "continue_signal.txt" # Continue signal
WORKER_DAG_TEMPLATE = "worker_jobs_gen_{}.dag"
WORKER_DAG_CURRENT_SYMLINK = "worker_jobs_current.dag"

# --- Function: Read or initialize state ---
def get_current_generation():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0 # Start from 0 if file content is invalid
    else:
        return 0 # Start from 0 if file doesn't exist

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
            os.makedirs(run_dir, exist_ok=True)
            node_name = f"run_{i}"

            with open(param_file, "w") as f:
                f.write(f"{random.randint(1, 100)}\n")
                f.write(f"{random.randint(1, 100)}\n")

            # Write sub-DAG entry
            dag_f.write(f"JOB run_{i} worker.sub\n") # worker.sub is in parent directory
            dag_f.write(f"VARS run_{i} ParamFile=\"{param_file}\"\n")
            dag_f.write(f"VARS run_{i} RunDir=\"{run_dir}\"\n")
            dag_f.write(f"VARS run_{i} GenNum=\"{gen_num}\"\n")
            dag_f.write(f"VARS {node_name} NodeName=\"{node_name}\"\n")
            dag_f.write("\n")

    # Update symbolic link to the current sub-DAG
    if os.path.lexists(WORKER_DAG_CURRENT_SYMLINK):
        os.remove(WORKER_DAG_CURRENT_SYMLINK)
    os.symlink(worker_dag_file, WORKER_DAG_CURRENT_SYMLINK)

    print(f"Generated {NUM_JOBS_PER_ITER} param files in {gen_dir}")
    print(f"Generated worker DAG: {worker_dag_file}")
    print(f"Updated symlink: {WORKER_DAG_CURRENT_SYMLINK}")

# --- Function: Gather results for one generation and append to the main file ---
def gather_and_append_results(gen_num):
    print(f"--- Gathering Results for Generation {gen_num} ---")
    gen_dir = os.path.join(RUNS_BASE_DIR, f"gen_{gen_num}")

    # Check if the main results file exists, write header if not
    write_header = not os.path.exists(RESULTS_FILE)

    with open(RESULTS_FILE, 'a', newline='') as csvfile:
        fieldnames = ['generation', 'run_id', 'efficiency']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        for i in range(NUM_JOBS_PER_ITER):
            run_dir = os.path.join(gen_dir, f"run_{i}")
            result_file = os.path.join(run_dir, "efficiency.txt")
            efficiency = "NA" # Default value
            try:
                with open(result_file, 'r') as f:
                    efficiency = f.read().strip()
            except FileNotFoundError:
                print(f"  Warning: Result file not found for run {i} in {result_file}!")

            writer.writerow({'generation': gen_num, 'run_id': i, 'efficiency': efficiency})
            print(f"  Gen {gen_num}, Run {i}: Efficiency = {efficiency}")

    print(f"Appended results to {RESULTS_FILE}")

# --- Function: Check if iterations should continue ---
def check_continue(gen_num):
    print(f"--- Checking if workflow should continue after Generation {gen_num} ---")

    # Always create/touch the file first
    with open(SIGNAL_FILE, "w") as f:
        f.write("") # Create an empty file initially

    if gen_num < MAX_ITERATIONS:
        print(f"Current generation {gen_num} is less than max {MAX_ITERATIONS}. Continuing.")
        # Overwrite the empty file with "continue"
        with open(SIGNAL_FILE, "w") as f:
            f.write("continue")
        print(f"Created signal file: {SIGNAL_FILE} with content.")
    else:
        print(f"Reached max generation {gen_num}. Stopping.")
        print(f"Signal file: {SIGNAL_FILE} remains empty.")
        # Do not write "continue" to the file

# --- Main control logic ---
if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] != "--step":
        print("Usage: python controller.py --step [prepare|check_continue]")
        exit(1)

    step = sys.argv[2]

    if step == "prepare":
        current_gen = get_current_generation()
        next_gen = current_gen + 1
        prepare_generation(next_gen)
        update_generation(next_gen) # Update state file to the *next* generation number
    elif step == "check_continue":
        current_gen = get_current_generation() # Get the generation that just finished
        if current_gen == 0:
            print("Error: State file indicates generation 0, cannot gather/check.")
            exit(1)
        gather_and_append_results(current_gen)
        check_continue(current_gen)
    else:
        print(f"Error: Unknown step '{step}'")
        exit(1)
