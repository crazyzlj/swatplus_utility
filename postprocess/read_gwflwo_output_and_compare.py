from __future__ import annotations
import os
import sys
import csv
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Iterable, Tuple, Set
import pandas as pd
import matplotlib.pyplot as plt

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import read_gwflow_output as rgo

def sanitize_filename(text: str) -> str:
    bad_chars = '<>:"/\\|?*'
    for ch in bad_chars:
        text = text.replace(ch, '_')
    return text

def collect_scenario_head_data(
        scenarios: Dict[str, str],
        grid_ids: Iterable[int],
        grid_labels: Optional[Dict[int, str]] = None,
        output_csv: Optional[str] = None,
) -> pd.DataFrame:
    """
    通过调用 read_gwflow_output 模块，收集多场景数据。
    """
    frames = []
    for scenario_name, path in scenarios.items():
        print(f'Processing scenario: {scenario_name}')

        # 自动解析 TxtInOut 路径（兼容直接路径或父目录路径）
        txtinout = path if os.path.basename(path).lower() == 'txtinout' \
            else os.path.join(path, 'TxtInOut')
        search_dir = txtinout if os.path.isdir(txtinout) else path

        # 调用复用的读取函数
        df = rgo.read_gwflow_to_df(
                txtinout_dir=search_dir,
                grid_ids=grid_ids,
                grid_labels=grid_labels
        )

        if not df.empty:
            df['Scenario'] = scenario_name
            frames.append(df)
        else:
            print(f'  Warning: No data found for scenario {scenario_name}')

    if not frames:
        raise ValueError('No scenario data were collected.')

    merged_df = pd.concat(frames, ignore_index=True)

    if output_csv:
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    return merged_df


# =========================
# 后续绘图与导出函数保持不变
# =========================
def plot_each_grid_cell(
    merged_df: pd.DataFrame,
    output_dir: str,
    figure_dpi: int = 150,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
) -> None:
    """
    Plot one figure per grid cell, with one line per scenario.
    """
    os.makedirs(output_dir, exist_ok=True)

    plot_df = merged_df.copy()
    plot_df['Date'] = pd.to_datetime(plot_df['Date'])

    if date_start:
        plot_df = plot_df[plot_df['Date'] >= pd.to_datetime(date_start)]
    if date_end:
        plot_df = plot_df[plot_df['Date'] <= pd.to_datetime(date_end)]

    grouped = plot_df.groupby(['GridID', 'GridLabel'], sort=True)

    for (grid_id, grid_label), df_grid in grouped:
        plt.figure(figsize=(12, 5))

        for scenario_name, df_scen in df_grid.groupby('Scenario', sort=False):
            df_scen = df_scen.sort_values('Date')
            plt.plot(df_scen['Date'], df_scen['Value'], label=scenario_name, linewidth=1.2)

        plt.xlabel('Date')
        plt.ylabel('Groundwater head')
        plt.title(f'Grid {grid_label} ({grid_id})')
        plt.legend()
        plt.tight_layout()

        out_png = os.path.join(
            output_dir,
            f'gw_head_grid_{sanitize_filename(str(grid_label))}_{grid_id}.png'
        )
        plt.savefig(out_png, dpi=figure_dpi)
        plt.close()
        print(f'Figure written to: {out_png}')


# =========================
# Optional wide CSV export
# =========================

def export_wide_csv_per_grid(merged_df: pd.DataFrame, output_dir: str) -> None:
    """
    Export one wide-format CSV per grid:
        Date | scenario1 | scenario2 | ...
    """
    os.makedirs(output_dir, exist_ok=True)

    tmp = merged_df.copy()
    tmp['Date'] = pd.to_datetime(tmp['Date'])

    for (grid_id, grid_label), df_grid in tmp.groupby(['GridID', 'GridLabel'], sort=True):
        wide_df = (
            df_grid.pivot_table(
                index='Date',
                columns='Scenario',
                values='Value',
                aggfunc='first',
            )
            .sort_index()
            .reset_index()
        )

        out_csv = os.path.join(
            output_dir,
            f'gw_head_grid_{sanitize_filename(str(grid_label))}_{grid_id}.csv'
        )
        wide_df.to_csv(out_csv, index=False, encoding='utf-8-sig')
        print(f'Wide CSV written to: {out_csv}')


if __name__ == '__main__':
    SCENARIOS = {
        'base': r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0513-snow-2',
        'hole': r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0513-snow-hole-2',
        'hole-cdut': r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0513-snow-hole-cdut-2'
    }
    GRID_IDS = [7698, 8470, 8599, 8735, 8992]
    GRID_LABELS = {7698: '7698', 8470: '8470', 8599: '8599', 8735: '8735', 8992: '8992'}
    OUTPUT_DIR = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\gwhead_comp'

    # 执行流程
    merged_df = collect_scenario_head_data(SCENARIOS, GRID_IDS, GRID_LABELS)

    # 绘图逻辑
    fig_dir = os.path.join(OUTPUT_DIR, 'figures')
    plot_each_grid_cell(merged_df, fig_dir)
    print('Comparison Done.')