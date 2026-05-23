from __future__ import absolute_import
import os
import sys

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from typing import Dict, Any
import logging
import json

from pygeoc.utils import MathClass
from postprocess.config import *
from convert_txt_to_excel import swat_txt_to_excel
from read_basin_precip import read_basin_precipitation
from read_gwflow_output import read_gwflow_outputs
from read_channel_sd_output import read_channel_daily_monthly_outputs
from eval_model_performance_v2 import evaluate_performance

if __name__ == '__main__':
    txtinout_dirs = [
        r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0522-snow-cc3.19',
        r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0522-snow-cc3.20',
        r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0522-snow-cc3.21'
                    ]
    OBS_DATA_DIR = r'D:\data_m\manitowoc\observed'
    plot_flag = True

    for txtinout_dir in txtinout_dirs:
        out_dir = r'%s\OutletsResults' % txtinout_dir
        files_to_convert = [r'%s\basin_pw_day.txt' % txtinout_dir,
                            r'%s\basin_wb_day.txt' % txtinout_dir
                            ]
        for file in files_to_convert:
            swat_txt_to_excel(file)

        read_basin_precipitation(txtinout_dir, CHANNEL_NUMBER, out_dir)

        read_gwflow_outputs(txtinout_dir, out_dir, GRID_IDS, WELL_IDS)

        read_channel_daily_monthly_outputs(txtinout_dir, out_dir, CHANNEL_NUMBER, SUFFIX,
                                           CHANNEL_NUMBERS, SUFFIXES)

        evaluate_performance(CONF, out_dir, OBS_DATA_DIR, out_dir, PLOT_STIME,
                             PLOT_ETIME, plot_flag=plot_flag)

