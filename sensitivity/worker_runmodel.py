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

from postprocess.read_channel_sd_output import process_swat_output_memory_efficient
from postprocess.eval_model_performance_v2 import evaluate_performance

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


# Sensitivity simulation
if __name__ == '__main__':
    # Calibration file
    cal_file = sys.argv[1]
    # Result folder for extracted simulation results and calculated model performances
    results_dir = sys.argv[2]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # TxtInOut folder
    # tio_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    tio_dir = script_dir + '/../TxtInOut'
    # Observation folder
    # obs_dir = r'D:\data_m\manitowoc\observed'
    obs_dir = script_dir + '/../observed'

    CHANNEL_NUMBER = [68]
    SUFFIX = ['_usgs04085427']

    CHANNEL_NUMBERS = [68, 170, 157, 74]
    SUFFIXES = ['_usgs04085427', '_363375', '_10020782', '_363313']

    # Configuration for calculating model performances
    plot_stime = '2008/1/1'
    plot_etime = '2024/12/31'
    plot_flag = False
    # For flow only
    conf = {'usgs04085427': {'flo_out': {'day': {'ylabel': 'Q(m^3/s)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '2008/1/1',
                                                 'vali_etime': '2013/12/31'},
                                         'mon': {'ylabel': 'Q(m^3/s)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1',
                                                 'cali_etime': '2024/12',
                                                 'vali_stime': '2008/1',
                                                 'vali_etime': '2013/12'}}
                             },
            '363375': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/5',
                                           'vali_stime': '',
                                           'vali_etime': ''}}
                       },
            '10020782': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                             'plot_style': 'dotline',
                                             'cali_stime': '2017/7',
                                             'cali_etime': '2019/10',
                                             'vali_stime': '',
                                             'vali_etime': ''}}
                         },
            '363313': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/10',
                                           'vali_stime': '',
                                           'vali_etime': ''}}
                       }
            }

    # Start time
    start_time = time.time()
    tio_dir = pathlib.Path(tio_dir).resolve()
    obs_dir = pathlib.Path(obs_dir).resolve()
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

    # Remove and rename sim_<i>.cal to cpu_path/calibration.cal
    cal_file_act = tio_dir / 'calibration.cal'
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
    INPUT_FILE = tio_dir / 'channel_sd_day.txt'
    MON_INPUT_FILE = tio_dir / 'channel_sd_mon.txt'
    CHANNEL_NUMBER = [68]
    SUFFIX = ['_usgs04085427']

    CHANNEL_NUMBERS = [68, 170, 157, 74]
    SUFFIXES = ['_usgs04085427', '_363375', '_10020782', '_363313']

    process_swat_output_memory_efficient(
            input_file_path=INPUT_FILE, skiplines=3,
            channel_id=CHANNEL_NUMBER,
            output_folder=results_dir,
            fname_suffix=SUFFIX
    )

    process_swat_output_memory_efficient(
            input_file_path=MON_INPUT_FILE, skiplines=3,
            channel_id=CHANNEL_NUMBERS,
            output_folder=results_dir,
            fname_suffix=SUFFIXES, is_daily=False
    )

    # Calculate model performance indices
    evaluate_performance(conf, results_dir, obs_dir, results_dir, '',
                         plot_stime, plot_etime, plot_flag=plot_flag)

    # delete the extracted simulation data in csv format
    delete_files_by_suffix_glob(results_dir, '.csv', True)
    delete_files_by_suffix_glob(results_dir, '.csv', False)
