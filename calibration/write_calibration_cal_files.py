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

from config import WORKER_DAG_TEMPLATE


class ParamWrapper:
    def __init__(self, d):
        self.name = d['name']
        self.change_type = d['change_type']
        self.units = d['units']
        self.conditions = d.get('conditions', None)


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = script_dir + '/../'
    base_dir = pathlib.Path(base_dir).resolve()

    # Text file to define multiple parameters to be considered
    #  the format of each parameter MUST be "name,chang_type,lower_bound,upper_bound".
    param_def_file = script_dir + '/../param_defs_models.txt'
    hru_grp_file = script_dir + '/../hru_combinations.json'
    rte_grp_file = script_dir + '/../channel_combinations.json'
    aqu_grp_file = script_dir + '/../aqu_combinations.json'
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

    gen_sim_dirname = f"{sim_dir_name}/selected_param_sets"
    gen_dir = base_dir / gen_sim_dirname
    if not os.path.exists(gen_dir):
        os.makedirs(gen_dir, exist_ok=True)

    gen_dir = pathlib.Path(gen_dir).resolve()

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


    # 此映射表告诉函数在 'hru' 组中查找 'hru_ids' 键，
    # 在 'rte' 组中查找 'channel_ids' 键。
    # 您可以根据需要扩展此映射。
    id_field_map = {
        'hru': 'hru_ids',
        'rte': 'channel_ids',
        'aqu': 'aqu_ids'
    }
    # 使用示例
    params_list, mode = parse_parameter_file(param_def_file, spatial_data_config, id_field_map)

    if params_list and mode == 'discrete':
        # 提取矩阵: (N_params, N_models) -> (N_models, N_params)
        n_models = len(params_list[0]['values'])
        values_matrix = [p['values'] for p in params_list]
        sample_array = np.array(values_matrix).T

        # 转换字典列表为对象列表
        # params_bounds_objs = [ParamWrapper(p) for p in params_list]

        # 构造虚假 problem
        # problem = {'names': [p['name'] for p in params_list]}

        # Initialize TxtinoutReader with the simulation directory
        txtinout_reader = pySWATPlus.TxtinoutReader(
                tio_dir=tio_dir
        )

        params_list_adapted = []
        for p in params_list:
            param = {}
            param['name'] = p['name']
            param['change_type'] = p['change_type']
            param['lower_bound'] = p['lower_bound']
            param['upper_bound'] = p['upper_bound']
            param['units'] = p['units']
            params_list_adapted.append(param)
        # # List of BoundDict objects
        params_bounds = utils._parameters_bound_dict_list(
                parameters=params_list_adapted
        )
        # Create an object of pySWATPlus.SensitivityAnalyzer()
        sensitivity_obj = pySWATPlus.SensitivityAnalyzer()
        # problem dictionary
        problem = sensitivity_obj._create_sobol_problem(
                params_bounds=params_bounds
        )

        write_calibration_files(sample_array, problem, params_bounds,
                                txtinout_reader, tio_dir, gen_dir)


        worker_dag_filename = WORKER_DAG_TEMPLATE.format(9999)
        worker_dag_file = base_dir / worker_dag_filename

        print(f"--- Controller Script Started (DAG Generator) ---")
        with open(worker_dag_file, 'w') as dag_f:
            for i in range(1, n_models + 1):
                param_file = f'{gen_sim_dirname}/sim_{i}.cal'
                dag_f.write(f"JOB run_{i} worker.sub\n")
                dag_f.write(f"VARS run_{i} ParamFile=\"{param_file}\"\n")
                dag_f.write(f"VARS run_{i} ResultDir=\"{gen_sim_dirname}/OutletsResults_{i}\"\n")
                dag_f.write("\n")

        print(f"Successfully generated {n_models} parameter files and {worker_dag_file}.")
        print("--- Controller Script Finished ---")
