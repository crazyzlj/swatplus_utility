from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))
import glob
import shutil
import time
import pathlib
import logging

from postprocess.config import *
from postprocess.read_basin_precip import read_basin_precipitation
from postprocess.read_channel_sd_output import read_channel_daily_monthly_outputs
from postprocess.read_gwflow_output import read_gwflow_outputs
from postprocess.eval_model_performance_v2 import evaluate_performance
import numpy.typing
import pySWATPlus


def delete_files_by_suffix_glob(folder_path: str,
                                suffix: str,
                                dry_run: bool = True):
    if not suffix.startswith('.'):
        suffix = '.' + suffix

    # 1. 构建搜索模式
    # 'os.path.join' 会正确处理路径分隔符 (e.g., / 或 \)
    # '*' 是通配符, 匹配任何字符
    search_pattern = os.path.join(folder_path, f"*{suffix}")

    if dry_run:
        print(f"*** [空运行] 模式。搜索模式: {search_pattern} ***\n")
    else:
        print(f"*** [正式运行] 模式。搜索模式: {search_pattern} ***\n")

    deleted_count = 0

    # 2. glob.glob 会返回所有匹配文件的完整路径列表
    for file_path in glob.glob(search_pattern):
        try:
            # 3. 仍然检查它是否是文件 (glob 也会匹配文件夹, 如果它们以 .csv 结尾)
            if os.path.isfile(file_path):
                if dry_run:
                    print(f"[空运行] 将删除: {file_path}")
                else:
                    print(f"正在删除: {file_path}")
                    os.remove(file_path)

                deleted_count += 1

        except OSError as e:
            print(f"无法删除 {file_path}: {e}")

    if dry_run:
        print(f"\n--- [空运行] 结束。找到 {deleted_count} 个文件。---")
    else:
        print(f"\n--- [正式运行] 结束。删除 {deleted_count} 个文件。---")


if __name__ == '__main__':
    cal_file = None
    if len(sys.argv) >= 2:
        # Calibration file
        cal_file = sys.argv[1]
    results_dir = None
    if len(sys.argv) >= 3:
        # Result folder for extracted simulation results and calculated model performances
        results_dir = sys.argv[2]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # TxtInOut folder
    tio_dir = script_dir + '/../TxtInOut'
    # Observation folder
    obs_dir = script_dir + '/../observed'
    if results_dir is None:
        results_dir = script_dir + '/../TxtInOut/OutletsResults'

    # Start time
    start_time = time.time()
    tio_dir = pathlib.Path(tio_dir).resolve()
    obs_dir = pathlib.Path(obs_dir).resolve()
    if cal_file is not None:
        cal_file = pathlib.Path(cal_file).resolve()
    results_dir = pathlib.Path(results_dir).resolve()
    os.makedirs(results_dir, exist_ok=True)

    log_file_path = results_dir / "swatplus_model.log"

    try:
        logging.basicConfig(
                filename=log_file_path,
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                filemode='w')
    except ValueError:
        print("Logging already configured, skipping basicConfig.")

    # 2. Submit each single model job and receive results_dir
    # Initialize TxtinoutReader with the simulation directory
    txtinout_reader = pySWATPlus.TxtinoutReader(
        tio_dir=tio_dir
    )

    # Run SWAT+ model and calculate model performances

    # Only when new cal_file is specified, remove and rename sim_<i>.cal to cpu_path/calibration.cal
    cal_file_act = tio_dir / 'calibration.cal'
    if cal_file is not None:
        if os.path.exists(cal_file_act):
            os.remove(cal_file_act)
        shutil.move(cal_file, cal_file_act)

    # Run SWAT+ model in each directory
    txtinout_reader.run_swat(
        parameters=None,
        begin_date='01-Jan-2002',
        end_date='31-Dec-2024',
        warmup=6
    )
    # Extract interested simulation results to the result folder
    # 1. Read precipitation
    read_basin_precipitation(tio_dir, CHANNEL_NUMBER, results_dir)

    # 2. Read daily and monthly channel outputs
    read_channel_daily_monthly_outputs(tio_dir, results_dir, CHANNEL_NUMBER, SUFFIX,
                                       CHANNEL_NUMBERS, SUFFIXES)

    # 3. Read groundwater head and solute outputs
    read_gwflow_outputs(tio_dir, results_dir, GRID_IDS, WELL_IDS)

    # 4. Calculate model performance metrics
    evaluate_performance(CONF, results_dir, obs_dir, results_dir, PLOT_STIME,
                         PLOT_ETIME, plot_flag=False)

    copy_files = ['basin_pw_day.txt', 'basin_wb_day.txt']
    for cpfile in copy_files:
        abs_file = tio_dir / cpfile
        if os.path.exists(abs_file):
            shutil.copy2(abs_file, results_dir)

    # delete the extracted simulation data in csv format
    delete_files_by_suffix_glob(results_dir, '.csv', True)
    delete_files_by_suffix_glob(results_dir, '.csv', False)
