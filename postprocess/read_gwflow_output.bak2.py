from __future__ import absolute_import
import os
import sys
import csv
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Set

# 自动处理路径，确保能导入项目内其他模块
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

try:
    from postprocess.config import *
except ImportError:
    # 如果 config 导入失败，此处需手动定义测试变量
    GRID_IDS = []
    WELL_IDS = []


def day_of_year_to_date_str(year: int, day_of_year: int) -> str:
    """将年内天数转换为 yyyy/m/d 格式"""
    dt = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    return f"{dt.year}/{dt.month}/{dt.day}"


def is_daily_data_line_old(tokens: List[str], n_cells: int, n_vars: int) -> bool:
    """判断旧格式数据的有效性"""
    if len(tokens) < 2 + n_cells * n_vars:
        return False
    try:
        year, doy = int(tokens[0]), int(tokens[1])
        if not (1000 <= year <= 9999 and 1 <= doy <= 366):
            return False
        for x in tokens[2:2 + n_cells * n_vars]:
            float(x)
        return True
    except ValueError:
        return False


def parse_cell_ids_from_line(line: str) -> List[int]:
    """从旧格式 Header 中提取 cell 编号"""
    tokens = line.strip().split()
    if not tokens or tokens[0].lower() not in ("cell:", "cell"):
        return []
    cell_ids = []
    for tk in tokens[1:]:
        try:
            cell_ids.append(int(tk))
        except ValueError:
            pass
    return cell_ids


def extract_new_format(input_file: str, grid_ids: List[int], well_ids: List[str], output_dir: str):
    """
    解析新格式 gwflow_obs_day.txt (窄表格式)
    列定义: jday, mon, day, yr, unit, gis_id, name, head, wt_depth, temp, no3, p
    """
    print(f"Processing new format file: {os.path.basename(input_file)}")

    target_map = {gid: wid for gid, wid in zip(grid_ids, well_ids)}
    target_gids = set(grid_ids)
    found_gids = set()
    out_handles = {}

    # 定义列索引
    COL_YR, COL_MON, COL_DAY = 3, 1, 2
    COL_GID = 5
    COL_VARS = {'gw_head': 7, 'gw_no3': 10, 'gw_p': 11}

    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                tokens = line.strip().split()
                # 跳过非数据行（前几个字段必须是数字）
                if not tokens or not tokens[0].isdigit():
                    continue

                try:
                    gid = int(tokens[COL_GID])
                    if gid in target_gids:
                        found_gids.add(gid)
                        well_id = target_map[gid]
                        date_str = f"{tokens[COL_YR]}/{tokens[COL_MON]}/{tokens[COL_DAY]}"

                        for var_name, col_idx in COL_VARS.items():
                            key = (var_name, well_id)
                            if key not in out_handles:
                                out_path = os.path.join(output_dir,
                                                        f"simu_{var_name}_day_{well_id}.csv")
                                fh = open(out_path, 'w', newline='', encoding='utf-8')
                                writer = csv.writer(fh)
                                writer.writerow(["Date", "Value"])
                                out_handles[key] = (fh, writer)

                            val = float(tokens[col_idx])
                            out_handles[key][1].writerow([date_str, val])
                except (ValueError, IndexError):
                    continue

        missing = target_gids - found_gids
        if missing:
            print(f"  Warning: The following GRID_IDs were not found: {sorted(list(missing))}")

    finally:
        for fh, _ in out_handles.values():
            fh.close()


def extract_old_format(file_variables: List[List[str]], grid_ids: List[int], well_ids: List[str],
                       output_dir: str):
    """解析旧格式 gwflow_state_obs_* (宽表格式)"""
    targets = list(zip(grid_ids, well_ids))

    for item in file_variables:
        input_file, variable_names = item[0], item[1:]
        if not os.path.isfile(input_file):
            continue

        print(f"Processing old format file: {os.path.basename(input_file)}")
        cell_ids_in_file, cell_index_map = None, {}
        out_handles = {}

        try:
            for var_name in variable_names:
                for _, well_id in targets:
                    out_path = os.path.join(output_dir, f"simu_{var_name}_day_{well_id}.csv")
                    f = open(out_path, mode="w", newline="", encoding="utf-8")
                    writer = csv.writer(f)
                    writer.writerow(["Date", "Value"])
                    out_handles[(var_name, well_id)] = (f, writer)

            with open(input_file, mode="r", encoding="utf-8", errors='ignore') as fin:
                for raw_line in fin:
                    line = raw_line.strip()
                    if not line: continue

                    if cell_ids_in_file is None:
                        maybe_cells = parse_cell_ids_from_line(line)
                        if maybe_cells:
                            cell_ids_in_file = maybe_cells
                            cell_index_map = {cid: idx for idx, cid in enumerate(cell_ids_in_file)}
                        continue

                    tokens = line.split()
                    n_cells = len(cell_ids_in_file)
                    if not is_daily_data_line_old(tokens, n_cells, len(variable_names)):
                        continue

                    date_str = day_of_year_to_date_str(int(tokens[0]), int(tokens[1]))
                    values = tokens[2:2 + n_cells * len(variable_names)]

                    for var_idx, var_name in enumerate(variable_names):
                        var_values = values[var_idx * n_cells: (var_idx + 1) * n_cells]
                        for grid_id, well_id in targets:
                            if grid_id in cell_index_map:
                                val = float(var_values[cell_index_map[grid_id]])
                                out_handles[(var_name, well_id)][1].writerow([date_str, val])

            missing_ids = set(grid_ids) - set(cell_ids_in_file if cell_ids_in_file else [])
            if missing_ids:
                print(
                    f"  Warning: GRID_IDs not found in {os.path.basename(input_file)}: {sorted(list(missing_ids))}")

        finally:
            for f, _ in out_handles.values():
                f.close()


def read_gwflow_outputs(txtinout_dir, out_dir, gridids, wellids):
    """主控函数：识别格式并执行提取"""
    if not gridids or not wellids:
        print("Error: GRID_IDS or WELL_IDS is empty.")
        return

    os.makedirs(out_dir, exist_ok=True)

    new_file = os.path.join(txtinout_dir, 'gwflow_obs_day.txt')
    old_head = os.path.join(txtinout_dir, 'gwflow_state_obs_head')
    old_conc = os.path.join(txtinout_dir, 'gwflow_state_obs_conc')

    # 1. 尝试新格式
    if os.path.exists(new_file):
        extract_new_format(new_file, gridids, wellids, out_dir)
        print(f"Finished processing new format.")

    # 2. 尝试旧格式
    elif os.path.exists(old_head) or os.path.exists(old_conc):
        file_vars = [
            [old_head, 'gw_head'],
            [old_conc, 'gw_no3', 'gw_p']
        ]
        extract_old_format(file_vars, gridids, wellids, out_dir)
        print(f"Finished processing old format.")

    # 3. 文件都不存在
    else:
        print("No gwflow output files found (gwflow_obs_day.txt or gwflow_state_obs_*). Skipping.")


if __name__ == '__main__':
    txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0512-snow15surf4gwf7lat3-hole-cdut'
    out_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0512-snow15surf4gwf7lat3-hole-cdut\OutletsResults'

    read_gwflow_outputs(txtinout_dir, out_dir, GRID_IDS, WELL_IDS)