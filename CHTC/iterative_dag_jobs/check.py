import os
import sys
import csv

NUM_JOBS_PER_ITER = 5
MAX_ITERATIONS = 3
RUNS_BASE_DIR = "multi_runs"
STATE_FILE = "iteration.state"
RESULTS_FILE = "all_results.csv"
SIGNAL_FILE = "continue_signal.txt"

# --- Function: Read state ---
def get_current_generation():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            try:
                # This now reflects the generation that just finished running
                return int(f.read().strip())
            except ValueError:
                 print(f"Error: Invalid content in {STATE_FILE}. Cannot determine generation.")
                 exit(1) # Exit if state is corrupted
    else:
        print(f"Error: {STATE_FILE} not found. Cannot determine generation.")
        exit(1) # Exit if state file is missing

# --- Function: Gather results and append ---
def gather_and_append_results(gen_num):
    print(f"--- Gathering Results for Generation {gen_num} ---")
    gen_dir = os.path.join(RUNS_BASE_DIR, f"gen_{gen_num}")

    # Check if results file exists, write header if not
    write_header = not os.path.exists(RESULTS_FILE)

    with open(RESULTS_FILE, 'a', newline='') as csvfile:
        fieldnames = ['generation', 'run_id', 'efficiency']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        for i in range(NUM_JOBS_PER_ITER):
            run_dir = os.path.join(gen_dir, f"run_{i}")
            result_file = os.path.join(run_dir, "efficiency.txt")
            efficiency = "NA"
            try:
                with open(result_file, 'r') as f:
                    efficiency = f.read().strip()
            except FileNotFoundError:
                print(f"  Warning: Result file not found for run {i}!")

            writer.writerow({'generation': gen_num, 'run_id': i, 'efficiency': efficiency})
            print(f"  Gen {gen_num}, Run {i}: Efficiency = {efficiency}")

    print(f"Appended results to {RESULTS_FILE}")

# --- Function: Check if iterations should continue ---
def check_continue(gen_num):
    print(f"--- Checking if workflow should continue after Generation {gen_num} ---")

    # Always create/touch the signal file first
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

# --- Main logic for check step ---
if __name__ == "__main__":
    current_gen = get_current_generation()
    if current_gen == 0:
         print("Error: State file indicates generation 0, cannot gather/check.")
         exit(1)
    gather_and_append_results(current_gen)
    check_continue(current_gen)
