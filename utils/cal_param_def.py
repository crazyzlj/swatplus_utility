from __future__ import absolute_import
import os
import sys

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

from typing import List, Dict, Any, Optional
import shutil

import pySWATPlus
import pySWATPlus.utils as utils
import pySWATPlus.validators as validators

def parse_parameter_file(filepath: str,
                         spatial_group_data: Dict[str, Dict[str, Any]],
                         id_field_map: Dict[str, str]) -> List[Dict[str, Any]]:
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
    # id_field_map = {
    #     'hru': 'hru_ids',
    #     'rte': 'channel_ids'
    # }
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

def write_calibration_files(sample_array, problem, params_bounds, txtinout_reader, tio_dir, sim_dir):
    # Write calibration.cal file
    for idx, arr in enumerate(sample_array, start=1):
        # Dictionary mapping for sensitivity simulation name and variable
        var_names = problem['names']
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
