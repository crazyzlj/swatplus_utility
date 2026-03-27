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

from typing import List, Dict, Any
import sys

def parse_parameter_file(filepath: str,
                         spatial_group_data: Dict[str, Dict[str, Any]],
                         id_field_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    读取参数定义文件。

    支持两种模式 (建议在文件首行指定):
    1. PARAM_RANGES (默认): 定义参数优化范围 (4列: name, type, lower, upper)
    2. PARAM_SETS: 定义多组确定的参数值 (N列: name, type, val1, val2, ... valN)

    Args:
        filepath (str): 输入文件路径。
        spatial_group_data (dict): 空间分组数据。
        id_field_map (dict): 对象类型到ID字段名的映射。

    Returns:
        list[dict]: 参数字典列表。
            Sets模式包含键: 'values' (list of floats), 'definition_type': 'discrete'
            Range模式包含键: 'lower_bound', 'upper_bound', 'definition_type': 'range'
    """
    parameters = []
    mode = 'range'  # 默认为范围模式

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # --- 1. 预扫描确定文件模式 ---
        # 查找第一行有效数据前的标识
        for line in lines:
            line = line.strip().upper()
            if not line or line.startswith('#'):
                continue

            if 'PARAM_SETS' in line:
                mode = 'discrete'
                print(f"提示: 检测到模式标记 {line}, 使用 [参数集/数值] 模式解析。")
                break
            elif 'PARAM_RANGES' in line:
                mode = 'range'
                print(f"提示: 检测到模式标记 {line}, 使用 [范围] 模式解析。")
                break
            elif line.startswith('PARAM_COMB'):  # 兼容你提供的文件头
                mode = 'discrete'
                print("提示: 检测到表头 'param_comb', 自动切换为 [参数集/数值] 模式。")
                break

        # --- 2. 逐行解析数据 ---
        for line_idx, line in enumerate(lines):
            line = line.strip()

            # 跳过空行、注释行
            if not line or line.startswith('#'):
                continue

            # 跳过模式标记行 和 标题行
            line_upper = line.upper()
            if 'PARAM_SETS' in line_upper or 'PARAM_RANGES' in line_upper:
                continue
            if (line_upper.startswith('PARAM_COMB') or
                    line_upper.startswith('NAME,') or line_upper.startswith('NAME|')):
                continue

            try:
                parts = [p.strip() for p in line.split(',')]

                # 基础检查：无论哪种模式，至少要有 name 和 change_type
                if len(parts) < 2:
                    continue

                raw_name = parts[0]
                change_type = parts[1]

                param_dict = {
                    'name': None,  # 后续填充
                    'change_type': change_type,
                    'units': None,  # 默认全局
                    # 'definition_type': mode
                }

                # === 根据模式解析数值 ===
                if mode == 'discrete':
                    # PARAM_SETS 模式: 读取从第3列开始的所有值
                    if len(parts) < 3:
                        print(f"警告: 行 {line_idx + 1} 在 SETS 模式下缺少数值列: {line}")
                        continue

                    try:
                        vals = [float(x) for x in parts[2:]]
                        param_dict['values'] = vals
                        # 为了兼容性，填充极值
                        param_dict['lower_bound'] = min(vals)
                        param_dict['upper_bound'] = max(vals)
                    except ValueError:
                        print(f"警告: 行 {line_idx + 1} 包含非数值数据: {line}")
                        continue

                else:
                    # PARAM_RANGES 模式: 严格读取第3、4列
                    if len(parts) < 4:
                        print(f"警告: 行 {line_idx + 1} 在 RANGES 模式下格式错误 (需4列): {line}")
                        continue

                    try:
                        lb = float(parts[2])
                        ub = float(parts[3])
                        param_dict['lower_bound'] = lb
                        param_dict['upper_bound'] = ub
                        param_dict['values'] = None
                    except ValueError:
                        print(f"警告: 行 {line_idx + 1} 上下界数值无效: {line}")
                        continue

                # === 解析空间单元名称 (name|type|group) ===
                if '|' in raw_name:
                    name_parts = raw_name.split('|')
                    if len(name_parts) == 3:
                        param_name = name_parts[0].strip()
                        object_type = name_parts[1].strip()
                        group_name = name_parts[2].strip()

                        param_dict['name'] = param_name

                        # 查找空间 ID
                        try:
                            # 简化查找逻辑，增加鲁棒性
                            if (object_type in spatial_group_data and
                                    group_name in spatial_group_data[object_type] and
                                    object_type in id_field_map):

                                id_field = id_field_map[object_type]
                                group_data = spatial_group_data[object_type][group_name]

                                if id_field in group_data:
                                    param_dict['units'] = group_data[id_field]
                                else:
                                    # 静默处理或轻微提示，防止刷屏
                                    pass
                            else:
                                pass  # 找不到组ID，默认为全局或保持None
                        except Exception:
                            pass
                    else:
                        print(f"警告: 参数名格式错误: {raw_name}")
                        continue
                else:
                    param_dict['name'] = raw_name

                parameters.append(param_dict)

            except Exception as e:
                print(f"错误: 解析行 {line_idx + 1} 失败: {e}")

    except FileNotFoundError:
        print(f"错误: 文件未找到: {filepath}")
        return []

    return parameters, mode
#
# def parse_parameter_file(filepath: str,
#                          spatial_group_data: Dict[str, Dict[str, Any]],
#                          id_field_map: Dict[str, str]) -> List[Dict[str, Any]]:
#     """
#     读取参数定义文件，并将其解析为包含参数信息的字典列表。
#
#     新功能:
#     - 能够解析 'name|object_type|group_name' 格式的参数名称。
#     - 'object_type' (e.g., 'hru', 'rte') 用作在 'spatial_group_data' 中的一级键。
#     - 'group_name' (e.g., 'down1_agri_allsoil') 用作二级键。
#     - 查找到的 ID 列表 (hru_ids, channel_ids) 会被赋给 'units' 键。
#     - 全局参数 (如 'esco') 的 'units' 键为 None。
#
#     Args:
#         filepath (str): 输入的 .txt 文件路径。
#         spatial_group_data (dict): 一个字典，包含从 JSON 文件加载的空间分组数据。
#             结构示例:
#             {
#                 'hru': {'down1_agri_allsoil': {'hru_ids': [1,2]}, ...},
#                 'rte': {'all_headwater': {'channel_ids': [10]}, ...}
#             }
#
#     Returns:
#         list[dict]: 参数信息字典的列表。
#             e.g.: [{'name': 'cn2', 'change_type': 'pctchg', ..., 'units': [101, 102]},
#                    {'name': 'esco', 'change_type': 'absval', ..., 'units': None}]
#     """
#
#     parameters = []
#
#     # --- 关键配置 ---
#     # 此映射表告诉函数在 'hru' 组中查找 'hru_ids' 键，
#     # 在 'rte' 组中查找 'channel_ids' 键。
#     # 您可以根据需要扩展此映射。
#     # id_field_map = {
#     #     'hru': 'hru_ids',
#     #     'rte': 'channel_ids'
#     # }
#     # ------------------
#
#     try:
#         with open(filepath, 'r', encoding='utf-8') as f:
#             for line in f:
#                 line = line.strip()
#
#                 if not line or line.startswith('#'):
#                     continue
#
#                 try:
#                     parts = line.split(',')
#                     if len(parts) != 4:
#                         print(f"警告: 跳过格式错误的行 (需要4个部分): {line}")
#                         continue
#
#                     raw_name = parts[0].strip()
#                     param_dict = {
#                         'change_type': parts[1].strip(),
#                         'lower_bound': float(parts[2].strip()),
#                         'upper_bound': float(parts[3].strip()),
#                         'units': None  # 默认 'units' 为 None (全局参数)
#                     }
#
#                     # --- 核心扩展逻辑 ---
#                     if '|' in raw_name:
#                         name_parts = raw_name.split('|')
#
#                         # 1. 验证格式
#                         if len(name_parts) != 3:
#                             print(f"警告: 跳过格式错误的参数名 (需要 3 个 '|' 分隔的部分): {line}")
#                             continue
#
#                         param_name = name_parts[0].strip()
#                         object_type = name_parts[1].strip()  # e.g., 'hru'
#                         group_name = name_parts[2].strip()  # e.g., 'down1_agri_allsoil'
#
#                         param_dict['name'] = param_name
#
#                         # 2. 开始查找 ID 列表
#                         try:
#                             # 2.1 检查 object_type 是否在配置中 (e.g., 'hru' in spatial_group_data)
#                             if object_type not in spatial_group_data:
#                                 print(f"警告: 在 '{line}' 中, "
#                                       f"对象类型 '{object_type}' 未在 spatial_group_data 中找到。")
#                                 continue
#
#                             data_source = spatial_group_data[object_type]
#
#                             # 2.2 检查 group_name 是否在对应的 JSON 数据中 (e.g., 'down1_agri_allsoil' in hru_data)
#                             if group_name not in data_source:
#                                 print(f"警告: 在 '{line}' 中, "
#                                       f"组名 '{group_name}' 未在 {object_type} 数据中找到。")
#                                 continue
#
#                             group_data = data_source[group_name]
#
#                             # 2.3 检查我们是否知道要查找哪个ID字段 (e.g., 'hru' in id_field_map)
#                             if object_type not in id_field_map:
#                                 print(f"警告: 在 '{line}' 中, "
#                                       f"对象类型 '{object_type}' 没有在 id_field_map 中配置。")
#                                 continue
#
#                             id_field = id_field_map[object_type]  # 'hru_ids' or 'channel_ids'
#
#                             # 2.4 检查 'hru_ids' 或 'channel_ids' 是否在 JSON 的该条目中
#                             if id_field not in group_data:
#                                 print(f"警告: 在 '{line}' 中, "
#                                       f"字段 '{id_field}' 未在组 '{group_name}' 中找到。")
#                                 continue
#
#                             # 2.5 成功！获取ID列表
#                             id_list = group_data[id_field]
#                             param_dict['units'] = id_list
#
#                         except Exception as e_lookup:
#                             print(f"警告: 在为行 '{line}' 查找空间单元时出错: {e_lookup}")
#                             continue
#
#                     else:  # 如果没有 '|'
#                         param_dict['name'] = raw_name
#                         # param_dict['units'] 已经是 None, 保持不变
#
#                     parameters.append(param_dict)
#                     # --- 逻辑结束 ---
#
#                 except ValueError:
#                     print(f"警告: 跳过数据类型错误的行 (float转换失败): {line}")
#                 except Exception as e:
#                     print(f"警告: 处理行 '{line}' 时发生未知错误: {e}")
#
#     except FileNotFoundError:
#         print(f"错误: 文件未找到: {filepath}")
#         return []
#     except Exception as e:
#         print(f"错误: 读取文件时发生错误: {e}")
#         return []
#
#     return parameters

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
