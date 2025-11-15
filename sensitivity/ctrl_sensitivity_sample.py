import os
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

def parse_parameter_file(filepath: str,
                         spatial_group_data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    读取参数定义文件，并将其解析为包含参数信息的字典列表。

    新功能:
    - 能够解析 'name|object_type|group_name' 格式的参数名称。
    - 'object_type' (e.g., 'hru', 'rte') 用作在 'spatial_group_data' 中的一级键。
    - 'group_name' (e.g., 'down1_agri_allsoil') 用作二级键。
    - 查找到的 ID 列表 (hru_ids, channel_ids) 会被赋给 'units' 键。
    - 全局参数 (如 'esco') 的 'units' 键为 None。

    Args:
        filepath (str): 输入的 .txt 文件路径。
        spatial_group_data (dict): 一个字典，包含从 JSON 文件加载的空间分组数据。
            结构示例:
            {
                'hru': {'down1_agri_allsoil': {'hru_ids': [1,2]}, ...},
                'rte': {'all_headwater': {'channel_ids': [10]}, ...}
            }

    Returns:
        list[dict]: 参数信息字典的列表。
            e.g.: [{'name': 'cn2', 'change_type': 'pctchg', ..., 'units': [101, 102]},
                   {'name': 'esco', 'change_type': 'absval', ..., 'units': None}]
    """

    parameters = []

    # --- 关键配置 ---
    # 此映射表告诉函数在 'hru' 组中查找 'hru_ids' 键，
    # 在 'rte' 组中查找 'channel_ids' 键。
    # 您可以根据需要扩展此映射。
    id_field_map = {
        'hru': 'hru_ids',
        'rte': 'channel_ids'
    }
    # ------------------

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()

                if not line or line.startswith('#'):
                    continue

                try:
                    parts = line.split(',')
                    if len(parts) != 4:
                        print(f"警告: 跳过格式错误的行 (需要4个部分): {line}")
                        continue

                    raw_name = parts[0].strip()
                    param_dict = {
                        'change_type': parts[1].strip(),
                        'lower_bound': float(parts[2].strip()),
                        'upper_bound': float(parts[3].strip()),
                        'units': None  # 默认 'units' 为 None (全局参数)
                    }

                    # --- 核心扩展逻辑 ---
                    if '|' in raw_name:
                        name_parts = raw_name.split('|')

                        # 1. 验证格式
                        if len(name_parts) != 3:
                            print(f"警告: 跳过格式错误的参数名 (需要 3 个 '|' 分隔的部分): {line}")
                            continue

                        param_name = name_parts[0].strip()
                        object_type = name_parts[1].strip()  # e.g., 'hru'
                        group_name = name_parts[2].strip()  # e.g., 'down1_agri_allsoil'

                        param_dict['name'] = param_name

                        # 2. 开始查找 ID 列表
                        try:
                            # 2.1 检查 object_type 是否在配置中 (e.g., 'hru' in spatial_group_data)
                            if object_type not in spatial_group_data:
                                print(f"警告: 在 '{line}' 中, "
                                      f"对象类型 '{object_type}' 未在 spatial_group_data 中找到。")
                                continue

                            data_source = spatial_group_data[object_type]

                            # 2.2 检查 group_name 是否在对应的 JSON 数据中 (e.g., 'down1_agri_allsoil' in hru_data)
                            if group_name not in data_source:
                                print(f"警告: 在 '{line}' 中, "
                                      f"组名 '{group_name}' 未在 {object_type} 数据中找到。")
                                continue

                            group_data = data_source[group_name]

                            # 2.3 检查我们是否知道要查找哪个ID字段 (e.g., 'hru' in id_field_map)
                            if object_type not in id_field_map:
                                print(f"警告: 在 '{line}' 中, "
                                      f"对象类型 '{object_type}' 没有在 id_field_map 中配置。")
                                continue

                            id_field = id_field_map[object_type]  # 'hru_ids' or 'channel_ids'

                            # 2.4 检查 'hru_ids' 或 'channel_ids' 是否在 JSON 的该条目中
                            if id_field not in group_data:
                                print(f"警告: 在 '{line}' 中, "
                                      f"字段 '{id_field}' 未在组 '{group_name}' 中找到。")
                                continue

                            # 2.5 成功！获取ID列表
                            id_list = group_data[id_field]
                            param_dict['units'] = id_list

                        except Exception as e_lookup:
                            print(f"警告: 在为行 '{line}' 查找空间单元时出错: {e_lookup}")
                            continue

                    else:  # 如果没有 '|'
                        param_dict['name'] = raw_name
                        # param_dict['units'] 已经是 None, 保持不变

                    parameters.append(param_dict)
                    # --- 逻辑结束 ---

                except ValueError:
                    print(f"警告: 跳过数据类型错误的行 (float转换失败): {line}")
                except Exception as e:
                    print(f"警告: 处理行 '{line}' 时发生未知错误: {e}")

    except FileNotFoundError:
        print(f"错误: 文件未找到: {filepath}")
        return []
    except Exception as e:
        print(f"错误: 读取文件时发生错误: {e}")
        return []

    return parameters

# Sensitivity simulation
if __name__ == '__main__':
    # Use 'Morris' first when too many parameters are considered, and then use FAST.
    METHOD = 'FAST'
    # --- FAST ---
    # Total model runs = N * D, M can be 4 (by default) or 8 and N > 4M^2 (N > 64)
    #   D is the count of considered parameters
    N_fast = 1024  # Must > 4 * M^2, recommend 1024, 2048, ...
    M_fast = 4

    # --- Morris ---
    # total model runs = N * (D + 1), D is the count of considered parameters
    morris_trajectories = 50  # N: recommend 20-50
    morris_levels = 4  # p: sample levels, recommend 4 or 8

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Text file to define multiple parameters to be considered
    #  the format of each parameter MUST be "name,chang_type,lower_bound,upper_bound".
    # param_def_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\param_defs-fast-2025-11-14.txt'
    # hru_grp_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\hru_combinations.json'
    # rte_grp_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\channel_combinations.json'
    param_def_file = script_dir + '/../param_defs.txt'
    hru_grp_file = script_dir + '/../hru_combinations.json'
    rte_grp_file = script_dir + '/../channel_combinations.json'
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

    param_def = parse_parameter_file(param_def_file, spatial_data_config)

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
