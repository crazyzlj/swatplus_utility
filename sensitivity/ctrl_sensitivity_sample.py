from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import shutil
import time
import pathlib
import copy
import json
import numpy as np
from typing import List, Dict, Any, Optional
from SALib.sample import fast_sampler, morris

import pySWATPlus
import pySWATPlus.utils as utils
import pySWATPlus.validators as validators

from utils.cal_param_def import write_calibration_files, parse_parameter_file


if __name__ == '__main__':
    # Use 'Morris' first when too many parameters are considered, and then use FAST.
    METHOD = 'morris'
    # --- FAST ---
    # Total model runs = N * D, M can be 4 (by default) or 8 and N > 4M^2 (N > 64)
    #   D is the count of considered parameters
    N_fast = 1024  # Must > 4 * M^2, recommend 1024, 2048, ...
    M_fast = 4

    # --- Morris ---
    # total model runs = N * (D + 1), D is the count of considered parameters
    morris_trajectories = 100  # N: recommend 20-50
    morris_levels = 4  # p: sample levels, recommend 4 or 8

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Text file to define multiple parameters to be considered
    #  the format of each parameter MUST be "name,chang_type,lower_bound,upper_bound".
    # param_def_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\param_defs_morris-2025-11-19.txt'
    # hru_grp_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\hru_combinations.json'
    # rte_grp_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\channel_combinations.json'
    # aqu_grp_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\aqu_combinations.json'
    param_def_file = script_dir + '/../param_defs.txt'
    hru_grp_file = script_dir + '/../hru_combinations.json'
    rte_grp_file = script_dir + '/../channel_combinations.json'
    aqu_grp_file = script_dir + '/../aqu_combinations.json'
    id_field_map = {
        'hru': 'hru_ids',
        'rte': 'channel_ids',
        'aqu': 'aqu_ids'
    }
    # TxtInOut folder
    # tio_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    tio_dir = script_dir + '/../TxtInOut'
    # Actual simulation folder for every model runs
    sim_dir_name = 'multi_runs'
    sim_dir_path = script_dir + '/../' + sim_dir_name

    tio_dir = pathlib.Path(tio_dir).resolve()
    sim_dir = pathlib.Path(sim_dir_path).resolve()

    if not os.path.exists(sim_dir):
        os.makedirs(sim_dir, exist_ok=True)

    # Start time
    start_time = time.time()

    # read hru and channel group information
    # 构建 'spatial_group_data' 配置字典
    #    键 'hru' 和 'rte' 必须与 param_defs.txt 中
    #    '|' 分隔的第二部分匹配。
    spatial_data_config = {}
    hru_grp_data = None
    if hru_grp_file is not None and os.path.exists(hru_grp_file):
        with open(hru_grp_file, 'r') as f:
            loaded_hru_data = json.load(f)
            spatial_data_config['hru'] = loaded_hru_data
    rte_grp_data = None
    if rte_grp_file is not None and os.path.exists(rte_grp_file):
        with open(rte_grp_file, 'r') as f:
            loaded_channel_data = json.load(f)
            spatial_data_config['rte'] = loaded_channel_data

    aqu_grp_data = None
    if aqu_grp_file is not None and os.path.exists(aqu_grp_file):
        with open(aqu_grp_file, 'r') as f:
            loaded_aqu_data = json.load(f)
            spatial_data_config['aqu'] = loaded_aqu_data

    param_def = parse_parameter_file(param_def_file, spatial_data_config, id_field_map)

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

    write_calibration_files(sample_array, copy_problem, params_bounds,
                            txtinout_reader, tio_dir, sim_dir)

    print(f"--- Controller Script Started (DAG Generator) ---")
    DAG_FILE_NAME = script_dir + "/../worker_jobs.dag"
    with open(DAG_FILE_NAME, 'w') as dag_f:
        for i in range(1, num_sim + 1):
            param_file = f'{sim_dir_name}/sim_{i}.cal'
            dag_f.write(f"JOB run_{i} worker.sub\n")
            dag_f.write(f"VARS run_{i} ParamFile=\"{param_file}\"\n")
            dag_f.write(f"VARS run_{i} ResultDir=\"{sim_dir_name}/OutletsResults_{i}\"\n")
            dag_f.write("\n")

    print(f"Successfully generated {num_sim} parameter files and {DAG_FILE_NAME}.")
    print("--- Controller Script Finished ---")
