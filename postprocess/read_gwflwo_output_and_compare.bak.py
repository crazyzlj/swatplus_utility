from __future__ import annotations

import os
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Iterable, Tuple, Optional

import matplotlib.pyplot as plt
import pandas as pd


# =========================
# Utilities for gwflow file
# =========================

def day_of_year_to_date(year: int, day_of_year: int) -> datetime:
    """Convert (year, day_of_year) to datetime."""
    return datetime(year, 1, 1) + timedelta(days=day_of_year - 1)



def is_daily_data_line(tokens: List[str], n_cells: int, n_vars: int = 1) -> bool:
    """
    Check whether a tokenized line is a valid daily data line.

    Expected layout:
        year day_of_year value_1 value_2 ... value_(n_cells * n_vars)
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

    try:
        for x in tokens[2:2 + n_cells * n_vars]:
            float(x)
    except ValueError:
        return False

    return True



def parse_cell_ids_from_line(line: str) -> List[int]:
    """
    Parse cell ids from a header line like:
        cell: 6938 6939 6940 ...
    """
    tokens = line.strip().split()
    if not tokens:
        return []

    first = tokens[0].lower()
    if first not in ("cell:", "cell"):
        return []

    cell_ids: List[int] = []
    for tk in tokens[1:]:
        try:
            cell_ids.append(int(tk))
        except ValueError:
            pass
    return cell_ids


# =========================
# Core readers
# =========================

def resolve_gwflow_head_file(path_or_dir: str, filename: str = 'gwflow_state_obs_head') -> str:
    """
    Resolve gwflow_state_obs_head from either:
    1. a direct file path, or
    2. a directory containing the file, or
    3. a scenario directory that contains TxtInOut/gwflow_state_obs_head.
    """
    if os.path.isfile(path_or_dir):
        return path_or_dir

    cand1 = os.path.join(path_or_dir, filename)
    if os.path.isfile(cand1):
        return cand1

    cand2 = os.path.join(path_or_dir, 'TxtInOut', filename)
    if os.path.isfile(cand2):
        return cand2

    raise FileNotFoundError(
        f'Cannot find {filename} from: {path_or_dir}\n'
        f'Tried:\n  {cand1}\n  {cand2}'
    )



def read_gwflow_head_for_grids(
    gwflow_head_file: str,
    grid_ids: Iterable[int],
    grid_labels: Optional[Dict[int, str]] = None,
) -> pd.DataFrame:
    """
    Read daily groundwater head time series for selected grid cells.

    Returns a long-format DataFrame with columns:
        Date, GridID, GridLabel, Value
    """
    target_grid_ids = list(grid_ids)
    target_grid_set = set(target_grid_ids)
    grid_labels = grid_labels or {}

    rows: List[Dict[str, object]] = []
    cell_ids_in_file: Optional[List[int]] = None
    cell_index_map: Dict[int, int] = {}

    with open(gwflow_head_file, mode='r', encoding='utf-8', errors='ignore') as fin:
        for raw_line in fin:
            line = raw_line.strip()
            if not line:
                continue

            if cell_ids_in_file is None:
                maybe_cells = parse_cell_ids_from_line(line)
                if maybe_cells:
                    cell_ids_in_file = maybe_cells
                    cell_index_map = {cid: idx for idx, cid in enumerate(cell_ids_in_file)}
                continue

            tokens = line.split()
            n_cells = len(cell_ids_in_file)
            if not is_daily_data_line(tokens, n_cells, n_vars=1):
                continue

            year = int(tokens[0])
            day_of_year = int(tokens[1])
            date_value = day_of_year_to_date(year, day_of_year)
            values = tokens[2:2 + n_cells]

            for grid_id in target_grid_ids:
                if grid_id not in cell_index_map:
                    continue
                col_idx = cell_index_map[grid_id]
                value = float(values[col_idx])
                rows.append({
                    'Date': date_value,
                    'GridID': grid_id,
                    'GridLabel': grid_labels.get(grid_id, str(grid_id)),
                    'Value': value,
                })

    if cell_ids_in_file is None:
        raise ValueError(f"Did not find a 'cell:' header line in file: {gwflow_head_file}")

    missing_grid_ids = sorted(target_grid_set - set(cell_ids_in_file))
    if missing_grid_ids:
        print(
            f'Warning: the following GridID(s) were not found in '
            f'{os.path.basename(gwflow_head_file)}: {missing_grid_ids}'
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values(['GridID', 'Date'], inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df


# =========================
# Scenario aggregation
# =========================

def collect_scenario_head_data(
    scenarios: Dict[str, str],
    grid_ids: Iterable[int],
    grid_labels: Optional[Dict[int, str]] = None,
    output_csv: Optional[str] = None,
) -> pd.DataFrame:
    """
    Collect selected grid-cell daily gw head data from multiple scenarios.

    Parameters
    ----------
    scenarios : dict
        Mapping: scenario_name -> directory_or_file_path
    grid_ids : iterable[int]
        Grid cells to extract.
    grid_labels : dict[int, str], optional
        Friendly labels for plotting/file naming.
    output_csv : str, optional
        Save merged long-format CSV if provided.
    """
    frames: List[pd.DataFrame] = []
    for scenario_name, path_or_dir in scenarios.items():
        gwflow_head_file = resolve_gwflow_head_file(path_or_dir)
        print(f'Processing scenario: {scenario_name}')
        print(f'  File: {gwflow_head_file}')

        df = read_gwflow_head_for_grids(
            gwflow_head_file=gwflow_head_file,
            grid_ids=grid_ids,
            grid_labels=grid_labels,
        )
        df['Scenario'] = scenario_name
        frames.append(df)

    if not frames:
        raise ValueError('No scenario data were collected.')

    merged_df = pd.concat(frames, ignore_index=True)
    merged_df.sort_values(['GridID', 'Scenario', 'Date'], inplace=True)
    merged_df.reset_index(drop=True, inplace=True)

    if output_csv:
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f'Merged CSV written to: {output_csv}')

    return merged_df


# =========================
# Plotting
# =========================

def sanitize_filename(text: str) -> str:
    bad_chars = '<>:"/\\|?*'
    for ch in bad_chars:
        text = text.replace(ch, '_')
    return text



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


# =========================
# Main user settings
# =========================
if __name__ == '__main__':
    # 1. Set scenario names and their folders (or direct gwflow_state_obs_head file paths)
    SCENARIOS = {
        'pump10': r'D:\tmp\20260414\addpumpex10run',
        'pump20': r'D:\tmp\20260414\addpumpex20run',
        'pump50': r'D:\tmp\20260414\addpumpex50run',
        'pump80': r'D:\tmp\20260414\addpumpex80run',
        'pump100': r'D:\tmp\20260414\addpumpex100run',
    }

    # 2. Grid cells to extract
    GRID_IDS = [5627, 7798, 3944]

    # 3. Optional friendly labels shown in titles/file names
    GRID_LABELS = {
        5627: 'ABT836',
        7798: 'AAV456',
        3944: 'AAJ835',
    }

    # 4. Output directory
    OUTPUT_DIR = r'D:\tmp\20260414\pumpex_comp'

    # 5. Optional date subset. Use None to keep all dates.
    DATE_START = None      # e.g. '2024-01-01'
    DATE_END = None        # e.g. '2025-12-31'

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    merged_csv = os.path.join(OUTPUT_DIR, 'gw_head_all_scenarios_long.csv')
    wide_csv_dir = os.path.join(OUTPUT_DIR, 'csv_per_grid')
    fig_dir = os.path.join(OUTPUT_DIR, 'figures')

    merged_df = collect_scenario_head_data(
        scenarios=SCENARIOS,
        grid_ids=GRID_IDS,
        grid_labels=GRID_LABELS,
        output_csv=merged_csv,
    )

    export_wide_csv_per_grid(merged_df, wide_csv_dir)
    plot_each_grid_cell(
        merged_df=merged_df,
        output_dir=fig_dir,
        date_start=DATE_START,
        date_end=DATE_END,
    )

    print('Done.')
