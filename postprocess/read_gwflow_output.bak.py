from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import csv
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import shutil

from postprocess.config import *


def day_of_year_to_date_str(year: int, day_of_year: int) -> str:
    """
    将 年 + 年内第几天 转换为 yyyy/m/d 格式字符串
    例如：2002, 1 -> '2002/1/1'
    """
    dt = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    return f"{dt.year}/{dt.month}/{dt.day}"


def is_daily_data_line(tokens: List[str], n_cells: int, n_vars: int) -> bool:
    """
    判断当前行是否可能是逐日数据行。
    规则：
    1. 前两个字段必须能转成整数：year, day_of_year
    2. day_of_year 必须在 1~366 之间
    3. 后续数值个数至少为 n_cells * n_vars
    4. 后续这些值必须都能转成 float
    """
    if len(tokens) < 2 + n_cells * n_vars:
        return False

    try:
        year = int(tokens[0])
        day_of_year = int(tokens[1])
    except ValueError:
        return False

    if year < 1000 or year > 9999:
        return False

    if day_of_year < 1 or day_of_year > 366:
        return False

    needed_values = tokens[2:2 + n_cells * n_vars]
    try:
        for x in needed_values:
            float(x)
    except ValueError:
        return False

    return True


def parse_cell_ids_from_line(line: str) -> List[int]:
    """
    从类似下面这一行中提取 cell 编号：
    cell:        6938        6939        6940 ...
    """
    tokens = line.strip().split()
    if not tokens:
        return []

    # 允许第一列是 'cell:' 或 'cell'
    first = tokens[0].lower()
    if first not in ("cell:", "cell"):
        return []

    cell_ids = []
    for tk in tokens[1:]:
        try:
            cell_ids.append(int(tk))
        except ValueError:
            # 忽略异常字段
            pass
    return cell_ids


def extract_simulation_to_csv(
    file_variables: List[List[str]],
    grid_ids: List[int],
    well_ids: List[str],
    output_directory: str
) -> None:
    """
    按指定格式读取模拟文本文件，并输出为多个 CSV 文件。

    Parameters
    ----------
    file_variables : List[List[str]]
        每个元素代表一个输入文件及其中包含的变量名。
        例如：
        [
            [GW_HEAD_FILE, 'gw_head'],
            [GW_CONC_FILE, 'gw_no3', 'gw_p']
        ]

    grid_ids : List[int]
        目标井对应的 GRID_ID 列表，与 well_ids 一一对应。

    well_ids : List[str]
        井编号列表，与 grid_ids 一一对应。

    output_directory : str
        输出 CSV 文件目录。
    """
    if len(grid_ids) != len(well_ids):
        raise ValueError("GRID_IDS 和 WELL_IDS 的长度必须一致。")

    os.makedirs(output_directory, exist_ok=True)

    # 一个井可能对应同一个 grid，因此这里保留全部映射，不去重
    targets: List[Tuple[int, str]] = list(zip(grid_ids, well_ids))

    for item in file_variables:
        if len(item) < 2:
            raise ValueError(f"FILE_VARIABLES 中的元素格式错误：{item}")

        input_file = item[0]
        variable_names = item[1:]
        n_vars = len(variable_names)

        if not os.path.isfile(input_file):
            raise FileNotFoundError(f"输入文件不存在：{input_file}")

        print(f"Processing file: {input_file}")

        cell_ids_in_file = None
        cell_index_map: Dict[int, int] = {}

        # 为当前输入文件中的所有“变量-井”预先打开输出文件
        out_handles = {}  # key = (var_name, well_id)
        try:
            for var_name in variable_names:
                for _, well_id in targets:
                    out_csv = os.path.join(
                        output_directory,
                        f"simu_{var_name}_day_{well_id}.csv"
                    )
                    f = open(out_csv, mode="w", newline="", encoding="utf-8")
                    writer = csv.writer(f)
                    writer.writerow(["Date", "Value"])
                    out_handles[(var_name, well_id)] = (f, writer)

            with open(input_file, mode="r", encoding="utf-8", errors="ignore") as fin:
                for raw_line in fin:
                    line = raw_line.strip()
                    if not line:
                        continue

                    # 先找 cell 定义行
                    if cell_ids_in_file is None:
                        maybe_cells = parse_cell_ids_from_line(line)
                        if maybe_cells:
                            cell_ids_in_file = maybe_cells
                            cell_index_map = {cid: idx for idx, cid in enumerate(cell_ids_in_file)}
                            print(f"  Found {len(cell_ids_in_file)} cells in header.")
                        continue

                    # cell 行找到之后，开始尝试识别逐日数据行
                    tokens = line.split()
                    n_cells = len(cell_ids_in_file)

                    if not is_daily_data_line(tokens, n_cells, n_vars):
                        continue

                    year = int(tokens[0])
                    day_of_year = int(tokens[1])
                    date_str = day_of_year_to_date_str(year, day_of_year)

                    values = tokens[2:2 + n_cells * n_vars]

                    # 对每个变量分别切片
                    # 例如 n_cells = 15, n_vars = 2
                    # 第1段 0:15 -> gw_no3
                    # 第2段 15:30 -> gw_p
                    for var_idx, var_name in enumerate(variable_names):
                        start = var_idx * n_cells
                        end = (var_idx + 1) * n_cells
                        var_values = values[start:end]

                        for grid_id, well_id in targets:
                            if grid_id not in cell_index_map:
                                # 当前井对应的 grid 不在该文件 header 中，跳过
                                continue

                            col_idx = cell_index_map[grid_id]
                            raw_value = var_values[col_idx]

                            # 转成 float，再写出成类似 3.0 的格式
                            value = float(raw_value)
                            _, writer = out_handles[(var_name, well_id)]
                            writer.writerow([date_str, str(value)])

            if cell_ids_in_file is None:
                raise ValueError(f"文件中未找到 'cell:' 定义行：{input_file}")

            missing_grid_ids = sorted(set(grid_ids) - set(cell_ids_in_file))
            if missing_grid_ids:
                print(f"  Warning: 下列 GRID_ID 未在文件 {os.path.basename(input_file)} 中找到：{missing_grid_ids}")

        finally:
            for f, _ in out_handles.values():
                f.close()

        print(f"Finished: {input_file}")


def read_gwflow_outputs(txtinout_dir, out_dir, gridids, wellids):
    GW_HEAD_FILE = os.path.join(txtinout_dir, 'gwflow_state_obs_head')
    GW_CONC_FILE = os.path.join(txtinout_dir, 'gwflow_state_obs_conc')

    FILE_VARIABLES = [
        [GW_HEAD_FILE, 'gw_head'],
        [GW_CONC_FILE, 'gw_no3', 'gw_p']
    ]

    shutil.copy2(GW_HEAD_FILE, out_dir)
    shutil.copy2(GW_CONC_FILE, out_dir)

    # output file name format:
    # simu_<Variable>_day_<WELL_ID>.csv
    # e.g., simu_gw_head_day_PK237.csv

    extract_simulation_to_csv(
        file_variables=FILE_VARIABLES,
        grid_ids=gridids,
        well_ids=wellids,
        output_directory=out_dir
    )

if __name__ == '__main__':
    txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut'
    out_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\Results\OutletsResultsTest'
    read_gwflow_outputs(txtinout_dir, out_dir, GRID_IDS, WELL_IDS)
