from __future__ import absolute_import, annotations
import os
import sys
import csv
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Iterable, Tuple, Set
import pandas as pd

# 自动处理路径，确保能导入项目内其他模块
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

try:
    from postprocess.config import *
except ImportError:
    # 如果 config 导入失败，此处需手动定义测试变量
    GRID_IDS = []
    WELL_IDS = []

# =================================================================
# 1. 基础工具与核心读取模块 (供外部引用或内部调用)
# =================================================================

def day_of_year_to_date(year: int, day_of_year: int) -> datetime:
    """将年内天数转换为 datetime"""
    return datetime(year, 1, 1) + timedelta(days=day_of_year - 1)


def parse_old_header(line: str) -> List[int]:
    """解析旧版文件的Cell ID Header"""
    tokens = line.strip().split()
    if not tokens or tokens[0].lower() not in ("cell:", "cell"):
        return []
    return [int(tk) for tk in tokens[1:] if tk.isdigit()]


def read_gwflow_to_df(
        txtinout_dir: str,
        grid_ids: Iterable[int],
        grid_labels: Optional[Dict[int, str]] = None,
        variable: str = 'gw_head'
) -> pd.DataFrame:
    """
    核心库函数：自动识别新旧格式，提取指定变量，返回标准长表 DataFrame。
    返回列: ['Date', 'GridID', 'GridLabel', 'Value']
    """
    target_ids = set(grid_ids)
    labels = grid_labels or {}
    rows = []

    new_file = os.path.join(txtinout_dir, 'gwflow_obs_day.txt')
    old_head = os.path.join(txtinout_dir, 'gwflow_state_obs_head')
    old_conc = os.path.join(txtinout_dir, 'gwflow_state_obs_conc')

    # 1. 处理新格式 (长表)
    if os.path.exists(new_file):
        col_map = {'gw_head': 7, 'gw_no3': 10, 'gw_p': 11}
        val_idx = col_map.get(variable)
        if val_idx is None:
            return pd.DataFrame()  # 不支持的变量

        with open(new_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                tokens = line.strip().split()
                if not tokens or not tokens[0].isdigit(): continue
                try:
                    gid = int(tokens[5])
                    if gid in target_ids:
                        rows.append({
                            'Date': datetime(int(tokens[3]), int(tokens[1]), int(tokens[2])),
                            'GridID': gid,
                            'GridLabel': labels.get(gid, str(gid)),
                            'Value': float(tokens[val_idx])
                        })
                except (ValueError, IndexError):
                    continue

    # 2. 处理旧格式 (宽表)
    elif os.path.exists(old_head) or os.path.exists(old_conc):
        current_file = old_head if variable == 'gw_head' else old_conc
        if not os.path.exists(current_file):
            return pd.DataFrame()

        cell_ids, cell_map = None, {}
        # 确定多变量文件中的偏移索引 (旧格式 conc 文件中，0 是 no3, 1 是 p)
        var_block_idx = 0
        if variable == 'gw_p': var_block_idx = 1

        with open(current_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if cell_ids is None:
                    cell_ids = parse_old_header(line)
                    cell_map = {cid: i for i, cid in enumerate(cell_ids)}
                    continue

                tokens = line.split()
                if len(tokens) < 2: continue
                try:
                    dt = day_of_year_to_date(int(tokens[0]), int(tokens[1]))
                    vals = tokens[2:]
                    n_cells = len(cell_ids)

                    for gid in target_ids:
                        if gid in cell_map:
                            # 根据区块和 cell_id 计算一维数组中的索引位置
                            val_pos = var_block_idx * n_cells + cell_map[gid]
                            if val_pos < len(vals):
                                rows.append({
                                    'Date': dt,
                                    'GridID': gid,
                                    'GridLabel': labels.get(gid, str(gid)),
                                    'Value': float(vals[val_pos])
                                })
                except (ValueError, IndexError):
                    continue

    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values(['GridID', 'Date'], inplace=True)
    return df


# =================================================================
# 2. 独立运行模块 (向下兼容原有输出单站独立CSV功能)
# =================================================================

def export_individual_csvs(df: pd.DataFrame, out_dir: str, variable: str,
                           grid_to_well: Dict[int, str]):
    """将合并的 DataFrame 按井拆分，并写入独立 CSV 文件"""
    os.makedirs(out_dir, exist_ok=True)

    for grid_id, group_df in df.groupby('GridID'):
        if grid_id not in grid_to_well:
            continue
        well_id = grid_to_well[grid_id]
        out_file = os.path.join(out_dir, f"simu_{variable}_day_{well_id}.csv")

        export_df = group_df[['Date', 'Value']].copy()
        # 还原原脚本中的 yyyy/m/d 日期格式
        export_df['Date'] = export_df['Date'].dt.strftime('%Y/%m/%d')
        # 去除前导零的格式要求可以用 pandas 进行转换，或者直接留存标准的 yyyy/mm/dd
        # 此处使用 pandas 的字符串去前导零替换法，贴合你原脚本的输出
        export_df['Date'] = export_df['Date'].str.replace(r'/0', '/', regex=True)

        export_df.to_csv(out_file, index=False, encoding='utf-8')


def read_gwflow_outputs(txtinout_dir: str, out_dir: str, gridids: List[int], wellids: List[str]):
    """主控函数：调用基础核心，并将结果保存为单独的 CSV 文件"""
    if not gridids or not wellids:
        print("Error: GRID_IDS or WELL_IDS is empty.")
        return

    grid_to_well = dict(zip(gridids, wellids))
    variables = ['gw_head', 'gw_no3', 'gw_p']

    for var in variables:
        print(f"Extracting variable: {var} ...")
        df = read_gwflow_to_df(
                txtinout_dir=txtinout_dir,
                grid_ids=gridids,
                grid_labels=grid_to_well,
                variable=var
        )

        if df.empty:
            print(f"  No data found or unsupported format for {var}. Skipping.")
        else:
            export_individual_csvs(df, out_dir, var, grid_to_well)
            print(f"  Finished processing and exporting {var}.")


if __name__ == '__main__':
    txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0517-all-2'
    out_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0517-all-2\OutletsResults'


    read_gwflow_outputs(txtinout_dir, out_dir, GRID_IDS, WELL_IDS)