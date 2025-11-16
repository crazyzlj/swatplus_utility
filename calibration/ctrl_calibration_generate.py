from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import pathlib
import json
import pickle
import copy

from pymoo.util.ref_dirs import get_reference_directions
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.algorithms.moo.nsga3 import NSGA3
from pymoo.core.problem import Problem
from pymoo.termination import get_termination
import numpy as np

import pySWATPlus
import pySWATPlus.utils as utils
import pySWATPlus.validators as validators

from utils import RUNS_BASE_DIR
from utils.iterative_job import get_current_generation, update_generation, SIGNAL_FILE
from utils.cal_param_def import parse_parameter_file, write_calibration_files
from utils.optimization import SWATPlusProblem, get_algorithm

from config import N_POP, MAX_GENERATIONS, OBJECTIVES, WORKER_DAG_TEMPLATE, POP_FILE, GENSTATE_FILE, WORKER_DAG_CURRENT_SYMLINK

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = script_dir + '/../'
    base_dir = pathlib.Path(base_dir).resolve()
    current_gen = get_current_generation(base_dir)

    print(f"--- Running ctrl_calibration_generate.py for Generation {current_gen} ---")
    N_OBJ = len(OBJECTIVES)
    N_POP = N_POP

    # Text file to define multiple parameters to be considered
    #  the format of each parameter MUST be "name,chang_type,lower_bound,upper_bound".
    # param_def_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\param_defs-cali-up1up2-2025-11-15.txt'
    # hru_grp_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\hru_combinations.json'
    # rte_grp_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\channel_combinations.json'
    param_def_file = script_dir + '/../param_defs.txt'
    hru_grp_file = script_dir + '/../hru_combinations.json'
    rte_grp_file = script_dir + '/../channel_combinations.json'
    # TxtInOut folder
    # tio_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    tio_dir = script_dir + '/../TxtInOut'
    # Actual simulation folder for every model runs
    sim_dir_name = RUNS_BASE_DIR
    sim_dir_path = script_dir + '/../' + sim_dir_name

    tio_dir = pathlib.Path(tio_dir).resolve()
    sim_dir = pathlib.Path(sim_dir_path).resolve()

    if not os.path.exists(sim_dir):
        os.makedirs(sim_dir, exist_ok=True)

    gen_sim_dirname = f"{sim_dir_name}/gen_{current_gen}"
    gen_dir = base_dir / gen_sim_dirname
    os.makedirs(gen_dir, exist_ok=True)

    if not os.path.exists(gen_dir):
        os.makedirs(gen_dir, exist_ok=True)

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

    # 此映射表告诉函数在 'hru' 组中查找 'hru_ids' 键，
    # 在 'rte' 组中查找 'channel_ids' 键。
    # 您可以根据需要扩展此映射。
    id_field_map = {
        'hru': 'hru_ids',
        'rte': 'channel_ids'
    }

    param_def = parse_parameter_file(param_def_file, spatial_data_config, id_field_map)
    # Initialize TxtinoutReader with the simulation directory
    txtinout_reader = pySWATPlus.TxtinoutReader(
            tio_dir=tio_dir
    )
    # List of BoundDict objects
    params_bounds = utils._parameters_bound_dict_list(
            parameters=param_def
    )
    # Create an object of pySWATPlus.SensitivityAnalyzer()
    sensitivity_obj = pySWATPlus.SensitivityAnalyzer()
    # problem dictionary
    problem = sensitivity_obj._create_sobol_problem(
            params_bounds=params_bounds
    )
    copy_problem = copy.deepcopy(x=problem)

    pop_filename = POP_FILE.format(current_gen)
    pop_fpath = gen_dir / pop_filename
    genstate_filename = GENSTATE_FILE.format(current_gen)
    genstate_fpath = gen_dir / genstate_filename

    if current_gen == 0:
        # --- Initialization (Generation 0) ---
        print("Initialization (iter_index=0): Creating initial population...")

        # 1. Define optimization problem
        problem = SWATPlusProblem(copy_problem, n_obj=N_OBJ)

        # 2. Determine optimization algorithm based the count of objectives
        algorithm = get_algorithm(problem.n_obj, N_POP)
        algorithm.setup(problem,
                        termination=get_termination("n_gen", MAX_GENERATIONS),
                        seed=1,  # To guarantee the initial population can be reproduced
                        verbose=False)

        # 3. "ask"
        print(f"Asking algorithm for initial population (n_pop = {N_POP})...")
        pop = algorithm.ask()
        X = pop.get("X")

        # 4. Save
        np.save(pop_fpath, X)
        print(f"Saved initial population to {pop_fpath}.")
        with open(genstate_fpath, 'wb') as f:
            pickle.dump(algorithm, f)
        print(f"Saved initial algorithm state to {genstate_fpath}.")

    else:
        # --- Iteration: Generation k > 0 ---
        print(f"Loading population from previous step: {POP_FILE}")
        if not os.path.exists(pop_fpath):
            print(f"Error: Population file '{pop_fpath}' not found!")
            exit(-1)
        X = np.load(pop_fpath)

    # --- Generation .cal files ---
    n_pop_loaded, n_var = X.shape

    if n_pop_loaded != N_POP:
        print(f"Warning: N_POP in config ({N_POP}) does not match loaded population ({n_pop_loaded})")
        raise ValueError("Population size mismatch between config and loaded file.")

    print(f"Generating {n_pop_loaded} .cal files for Gen {current_gen}...")

    write_calibration_files(X, copy_problem, params_bounds,
                            txtinout_reader, tio_dir, gen_dir)

    print(f"Successfully generated {n_pop_loaded} .cal files for Gen {current_gen}.")

    print(f"--- Preparing model runs' DAG Generation {current_gen} ---")

    worker_dag_filename = WORKER_DAG_TEMPLATE.format(current_gen)
    worker_dag_file = base_dir / worker_dag_filename

    print(f"--- Controller Script Started (DAG Generator) ---")
    with open(worker_dag_file, 'w') as dag_f:
        for i in range(1, N_POP + 1):
            param_file = f'{gen_sim_dirname}/sim_{i}.cal'
            dag_f.write(f"JOB run_{i} worker.sub\n")
            dag_f.write(f"VARS run_{i} ParamFile=\"{param_file}\"\n")
            dag_f.write(f"VARS run_{i} ResultDir=\"{gen_sim_dirname}/OutletsResults_{i}\"\n")
            dag_f.write("\n")

    print(f"Successfully generated {N_POP} parameter files and {worker_dag_file}.")
    print("--- Controller Script Finished ---")

    print(f"Generated {N_POP} param files in {gen_dir}")
    print(f"Generated worker DAG: {worker_dag_file}")

    # Update symbolic link to the current sub-DAG
    cur_worker_dag_file = base_dir / WORKER_DAG_CURRENT_SYMLINK
    if os.path.lexists(cur_worker_dag_file):
        os.remove(cur_worker_dag_file)
    os.symlink(worker_dag_file, cur_worker_dag_file)
