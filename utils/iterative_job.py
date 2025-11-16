from __future__ import absolute_import
import os
import sys

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))


STATE_FILE = "iteration.state"
SIGNAL_FILE = "continue_signal.txt"


def get_current_generation(base_dir):
    state_fpath = base_dir / STATE_FILE
    if os.path.exists(state_fpath):
        with open(state_fpath, 'r') as f:
            try:
                # This now reflects the generation that just finished running
                return int(f.read().strip())
            except ValueError:
                print(f"Error: Invalid content in {STATE_FILE}. Cannot determine generation.")
                exit(1)  # Exit if state is corrupted
    else:
        print(f"Info: {STATE_FILE} not found. Cannot determine generation.")
        exit(1)  # Exit if state file is missing


def update_generation(base_dir, gen_num):
    state_fpath = base_dir / STATE_FILE
    with open(state_fpath, 'w') as f:
        f.write(str(gen_num))


def check_continue(base_dir, gen_num, max_num):
    print(f"--- Checking if workflow should continue after Generation {gen_num} ---")
    signal_fpath = base_dir / SIGNAL_FILE
    # Always create/touch the signal file first
    with open(signal_fpath, "w") as f:
        f.write("")  # Create an empty file initially

    if gen_num < max_num:
        print(f"Current generation {gen_num} is less than max {max_num}. Continuing.")
        # Overwrite the empty file with "continue"
        with open(signal_fpath, "w") as f:
            f.write("continue")
        print(f"Created signal file: {SIGNAL_FILE} with content.")
    else:
        print(f"Reached max generation {gen_num}. Stopping.")
        print(f"Signal file: {SIGNAL_FILE} remains empty.")

