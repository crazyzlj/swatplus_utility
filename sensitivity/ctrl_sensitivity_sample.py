import os
import shutil
import time
import pathlib
import copy
import numpy as np
from SALib.sample import fast_sampler, morris

import pySWATPlus
import pySWATPlus.utils as utils
import pySWATPlus.validators as validators

def parse_parameter_file(filepath: str) -> list[dict]:
    """
    Reads a definition file and parses it into a list of dictionaries containing parameters to be considered.

    The file format is assumed to be: name,change_type,lower_bound,upper_bound
    - Lines starting with '#' (comments) are ignored.
    - Empty lines are ignored.

    Args:
        filepath (str): The path to the input text file.

    Returns:
        list[dict]: A list of dictionaries containing parameter information.
                   e.g.: [{'name': 'esco', 'change_type': 'absval',
                           'lower_bound': 0.0, 'upper_bound': 1.0}, ...]
    """
    parameters = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                # 1. Strip leading/trailing whitespace (like newlines)
                line = line.strip()

                # 2. Ignore empty lines or comment lines
                if not line or line.startswith('#'):
                    continue

                # 3. Split the data
                try:
                    parts = line.split(',')

                    # Ensure there are exactly 4 parts after splitting
                    if len(parts) == 4:
                        # 4. Create the dictionary and perform type conversion
                        param_dict = {
                            'name': parts[0].strip(),
                            'change_type': parts[1].strip(),
                            'lower_bound': float(parts[2].strip()),  # Convert to float
                            'upper_bound': float(parts[3].strip())  # Convert to float
                        }
                        parameters.append(param_dict)
                    else:
                        print(f"Warning: Skipping malformed line: {line}")

                except ValueError:
                    # Handle failures during float() conversion
                    print(f"Warning: Skipping line with data type error: {line}")
                except Exception as e:
                    print(f"Warning: Unknown error processing line '{line}': {e}")

    except FileNotFoundError:
        print(f"Error: File not found: {filepath}")
        return []  # Return an empty list
    except Exception as e:
        print(f"Error: An error occurred while reading the file: {e}")
        return []  # Return an empty list

    return parameters


# Sensitivity simulation
if __name__ == '__main__':
    # Use 'Morris' first when too many parameters are considered, and then use FAST.
    METHOD = 'morris'
    # --- FAST ---
    # Total model runs = N * D, M can be 4 (by default) or 8 and N > 4M^2 (N > 64)
    #   D is the count of considered parameters
    N_fast = 100  # Must > 4 * M^2, recommend 1024, 2048, ...
    M_fast = 4

    # --- Morris ---
    # total model runs = N * (D + 1), D is the count of considered parameters
    morris_trajectories = 50  # N: recommend 20-50
    morris_levels = 4  # p: sample levels, recommend 4 or 8

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Text file to define multiple parameters to be considered
    #  the format of each parameter MUST be "name,chang_type,lower_bound,upper_bound".
    # param_def_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\param_defs.txt'
    param_def_file = script_dir + '/../param_defs_test.txt'
    # TxtInOut folder
    tio_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    # tio_dir = script_dir + '/../TxtInOut'
    # Actual simulation folder for every model runs
    # sim_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\testsensitivity2'
    sim_dir_name = 'multi_runs'
    sim_dir_path = script_dir + '/../' + sim_dir_name

    tio_dir = pathlib.Path(tio_dir).resolve()
    sim_dir = pathlib.Path(sim_dir_path).resolve()
    # obs_dir = pathlib.Path(obs_dir).resolve()
    if not os.path.exists(sim_dir):
        os.makedirs(sim_dir, exist_ok=True)

    # Start time
    start_time = time.time()

    # Submit job of reading parameters definitions, generating samples, and saving to separated files
    param_def = parse_parameter_file(param_def_file)

    # Initialize TxtinoutReader with the simulation directory
    txtinout_reader = pySWATPlus.TxtinoutReader(
        tio_dir=tio_dir
    )
    # List of BoundDict objects
    params_bounds = utils._parameters_bound_dict_list(
        parameters=param_def
    )
    # Validate configuration of simulation parameters
    validators._simulation_preliminary_setup(
        sim_dir=sim_dir,
        tio_dir=tio_dir,
        parameters=params_bounds
    )
    # Create an object of pySWATPlus.SensitivityAnalyzer()
    sensitivity_obj = pySWATPlus.SensitivityAnalyzer()
    # problem dictionary
    problem = sensitivity_obj._create_sobol_problem(
        params_bounds=params_bounds
    )
    copy_problem = copy.deepcopy(x=problem)

    # Generate sample array
    print(f"--- Using {METHOD} method to generate samples ---")

    if METHOD.lower() == 'fast':
        sample_array = fast_sampler.sample(
                problem=copy_problem,
                N=N_fast,
                M=M_fast
        )
        sample_out_file_name = 'fast_samples.npz'
        print(f"FAST method: N={N_fast}, M={M_fast}, parameters D={problem['num_vars']}")

    elif METHOD.lower() == 'morris':
        sample_array = morris.sample(
                problem=copy_problem,
                N=morris_trajectories,
                num_levels=morris_levels,
                optimal_trajectories=None  # use default trajectories
        )
        sample_out_file_name = 'morris_samples.npz'
        print(f"Morris: N (trajectories)={morris_trajectories}, "
              f"Levels={morris_levels}, parameters D={problem['num_vars']}")
    else:
        raise ValueError(f"Unsupported METHOD: '{METHOD}'. Please use 'FAST' or 'Morris'.")

    # sample_array = fast_sampler.sample(copy_problem, N_fast, M=M_fast)

    # Number of unique simulations
    num_sim = sample_array.shape[0]

    sample_out_file = sim_dir / sample_out_file_name
    np.savez_compressed(sample_out_file, samples=sample_array)
    print(f"Samples are saved to {sample_out_file}")

    # Output sensitivity analysis data (without simulation results)
    required_time = time.time() - start_time
    time_stats = {
        'sample_length': len(sample_array),
        'time_sec': round(required_time),
        'time_per_sample_sec': round(required_time / len(sample_array), 1),
    }
    sim_dict = {}
    sensim_output = {
        'time': time_stats,
        'problem': problem,
        'sample': sample_array,
        'simulation': sim_dict
    }

    # Write output to the file 'sensitivity_simulation.json' in simulation folder
    sensitivity_obj._write_simulation_in_json(
            sensim_dir=sim_dir,
            sensim_output=sensim_output
    )

    # Write calibration.cal file
    for idx, arr in enumerate(sample_array, start=1):
        # Dictionary mapping for sensitivity simulation name and variable
        var_names = copy_problem['names']
        var_dict = {
            var_names[i]: float(arr[i]) for i in range(len(var_names))
        }
        # Create ParameterType dictionary to write calibration.cal file
        params_sim = []
        for i, param in enumerate(params_bounds):
            params_sim.append(
                    {
                        'name': param.name,
                        'change_type': param.change_type,
                        'value': var_dict[var_names[i]],
                        'units': param.units,
                        'conditions': param.conditions
                    }
            )
        # List of ModifyDict objects
        params = utils._parameters_modify_dict_list(
                parameters=params_sim,
        )
        txtinout_reader._write_calibration_file(
                parameters=params
        )
        # Remove and rename calibration.cal file to sim_<i>.cal
        shutil.move(tio_dir / 'calibration.cal', sim_dir / f'sim_{idx}.cal')

    print(f"--- Controller Script Started (DAG Generator) ---")
    DAG_FILE_NAME = "worker_jobs.dag"
    with open(DAG_FILE_NAME, 'w') as dag_f:
        for i in range(1, num_sim + 1):
            param_file = f'{sim_dir_name}/sim_{i}.cal'
            dag_f.write(f"JOB run_{i} worker.sub\n")
            dag_f.write(f"VARS run_{i} ParamFile=\"{param_file}\"\n")
            dag_f.write(f"VARS run_{i} ResultDir=\"{sim_dir_name}/OutletsResults_{i}\"\n")
            dag_f.write("\n")

    print(f"Successfully generated {num_sim} parameter files and {DAG_FILE_NAME}.")
    print("--- Controller Script Finished ---")


