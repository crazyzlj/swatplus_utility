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

# 假设 'pygeoc' 已经安装。如果未安装，请使用 pip install pygeoc
# 或者用您自己的实现替换指标计算函数。
from pygeoc.utils import MathClass


def process_date_column(df, date_col):
    """
    自动检测日期列是 'YYYY/MM/DD' 还是 'YYYY/MM' 格式，
    并应用用户指定的相应转换规则。

    - 如果是 YYYY/MM/DD, 转换为 datetime64[ns] (使用 errors='coerce')
    - 如果是 YYYY/MM, 转换为 Period[M]
    """

    # 1. 提取一个非空的样本值用于“嗅探”
    #    .dropna() 确保我们跳过开头的任何 NaN/NaT
    sample_series = df[date_col].dropna()

    if sample_series.empty:
        # 如果该列全是空值，则无需操作
        print(f"列 '{date_col}' 为空，跳过转换。")
        return df

    # 我们只需要测试第一个非空值
    sample_value = sample_series.iloc[0]

    try:
        # 2. 尝试用 *严格* 格式 (YYYY/MM/DD) 解析 *样本*
        #    注意：这里我们用默认的 errors='raise' 来 *触发* except
        pd.to_datetime(sample_value, format='%Y/%m/%d')

        # 3. 如果成功，说明整个文件是 YYYY/MM/DD 格式
        print(f"检测到格式: YYYY/MM/DD。应用规则 (to_datetime, coerce)。")

        # 应用您为 YYYY/MM/DD 指定的规则
        df[date_col] = pd.to_datetime(df[date_col],
                                      format='%Y/%m/%d',
                                      errors='coerce')

    except ValueError:
        # 4. 如果失败，我们假定它就是 YYYY/MM 格式
        print(f"检测到格式: YYYY/MM。应用规则 (to_period('M'))。")

        # 应用您为 YYYY/MM 指定的规则
        # (%Y/%m 格式可以正确处理 '2023/5' 和 '2023/11')
        df[date_col] = pd.to_datetime(df[date_col],
                                      format='%Y/%m',
                                      errors='coerce')

    return df

def load_data(file_path: str, value_col: str = 'Value') -> pd.DataFrame:
    """
    从CSV文件加载时间序列数据。
    自动检测日期列，假定其列名为'Date'或为文件中的第一列。
    """
    if not os.path.exists(file_path):
        print(f"  - 警告: 文件未找到于 {file_path}")
        return None
    try:
        df = pd.read_csv(file_path)

        # 自动确定日期列
        if 'Date' in df.columns:
            date_col = 'Date'
        else:
            date_col = df.columns[0]
            print(
                    f"  - 信息: 在 {os.path.basename(file_path)} 中未找到'Date'列。使用第一列 '{date_col}' 作为日期索引。")

        # df[date_col] = pd.to_datetime(df[date_col])
        df = process_date_column(df, date_col)
        df.set_index(date_col, inplace=True)

        # 为保持一致性重命名主要的数值列
        if value_col not in df.columns and len(df.columns) > 0:
            value_col = df.columns[0]  # 设置索引后剩下的第一列

        return df[[value_col]].rename(columns={value_col: 'Value'})

    except Exception as e:
        print(f"  - 错误: 加载 {file_path} 时出错: {e}")
        return None


def calculate_metrics(df: pd.DataFrame, start_time: str, end_time: str) -> Dict[str, float]:
    """为指定时间段计算模型性能指标。"""
    try:
        period_df = df.loc[start_time:end_time]
        if period_df.empty or len(period_df) < 2:
            print(f"  - 警告: 在时间段 {start_time}-{end_time} 内数据不足，无法计算指标。")
            return {}
    except KeyError:
        print(f"  - 警告: 日期范围 {start_time}-{end_time} 在数据中不存在。")
        return {}

    obs_array = period_df['Obs'].values
    sim_array = period_df['Sim'].values

    if np.isnan(obs_array).any() or np.isnan(sim_array).any():
        print(f"  - 警告: 在时间段 {start_time}-{end_time} 内发现NaN值。指标可能不准确。")
        return {}

    metrics = {
        'NSE': MathClass.nashcoef(obs_array, sim_array),
        'RSR': MathClass.rsr(obs_array, sim_array),
        'PBIAS': MathClass.pbias(obs_array, sim_array),
        'R_square': MathClass.rsquare(obs_array, sim_array)
    }
    return {k: round(float(v), 2) for k, v in metrics.items()}


def plot_time_series(sim_df: pd.DataFrame, obs_df: pd.DataFrame, config: Dict[str, Any],
                     metrics: Dict[str, Any], output_path: str, precip_df: pd.DataFrame,
                     plot_stime: str, plot_etime: str):
    """
    生成并保存观测值与模拟值的对比图，并附带降水数据。
    模拟值会绘制在整个全局时间范围内。
    """
    fig, ax = plt.subplots(figsize=(17, 8))

    # --- MODIFIED SECTION ---
    # 准备用于绘图的数据，确保它们在全局时间范围内
    sim_plot_df = sim_df.loc[plot_stime:plot_etime]
    obs_plot_df = obs_df.loc[plot_stime:plot_etime]
    # --- END MODIFIED SECTION ---

    plot_style = config.get('plot_style', 'dotline')

    # 绘制观测值与模拟值
    if plot_style == 'dotline':
        ax.plot(obs_plot_df.index, obs_plot_df['Value'], 'r-', label='Observed', linewidth=1.5)
        ax.plot(sim_plot_df.index, sim_plot_df['Value'], 'b-', label='Simulated', linewidth=1.5,
                alpha=0.8)
    elif plot_style == 'bar':
        ax.bar(obs_plot_df.index, obs_plot_df['Value'], width=20, color='r', label='Observed')
        ax.plot(sim_plot_df.index, sim_plot_df['Value'], 'b-', label='Simulated', linewidth=1.5)
    elif plot_style == 'point':
        ax.plot(obs_plot_df.index, obs_plot_df['Value'], 'ro', label='Observed', markersize=4)
        ax.plot(sim_plot_df.index, sim_plot_df['Value'], 'b-', label='Simulated', linewidth=1.5,
                alpha=0.7)

    # 标示率定和验证期
    cali_label = "Calibration"
    if 'cali' in metrics and metrics['cali']:
        cali_label += f" (NSE={metrics['cali'].get('NSE', 'N/A')})"
        ax.axvspan(pd.to_datetime(config['cali_stime']), pd.to_datetime(config['cali_etime']),
                   color='grey', alpha=0.2, label=cali_label)

    vali_label = "Validation"
    if 'vali' in metrics and metrics['vali']:
        vali_label += f" (NSE={metrics['vali'].get('NSE', 'N/A')})"
        ax.axvspan(pd.to_datetime(config['vali_stime']), pd.to_datetime(config['vali_etime']),
                   color='lightblue', alpha=0.3, label=vali_label)

    ax.set_ylabel(config.get('ylabel', 'Value'), fontsize=14)
    ax.set_title(f"Model Performance for {os.path.basename(output_path).replace('.jpg', '')}",
                 fontsize=16)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    # 为所有图设置统一的X轴范围
    ax.set_xlim(pd.to_datetime(plot_stime), pd.to_datetime(plot_etime))

    # 在次坐标轴上添加降水
    if precip_df is not None:
        ax2 = ax.twinx()
        precip_subset = precip_df.loc[plot_stime:plot_etime]
        ax2.bar(precip_subset.index, precip_subset['Value'], width=1.0, color='deepskyblue',
                alpha=0.6, label='Precipitation')
        ax2.set_ylabel('Precipitation (mm)', fontsize=14)
        ax2.invert_yaxis()
        ax2.set_ylim((precip_subset['Value'].max() * 4), 0)
        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines + lines2, labels + labels2, loc='upper left')
    else:
        ax.legend(loc='upper left')

    # 格式化X轴日期
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

    fig.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close(fig)
    print(f"  - 图像已保存至: {output_path}")


def evaluate_performance(conf: Dict[str, Any], sim_dir: str, obs_dir: str, fig_dir: str,
                         precip_file: str, plot_stime: str, plot_etime: str,
                         plot_flag : bool = True):
    """
    主函数，用于遍历配置、计算指标并生成图表。
    """
    if not os.path.exists(fig_dir):
        os.makedirs(fig_dir)
        print(f"已创建输出目录: {fig_dir}")

    # 一次性加载降水数据
    precip_path = os.path.join(sim_dir, precip_file)
    precip_df = load_data(precip_path, value_col='precip')
    if precip_df is None:
        print("警告: 未找到降水文件。图表将在没有降水数据的情况下生成。")

    logger = logging.getLogger('eval_model_performance')
    logger.setLevel(logging.INFO)
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(message)s')

    log_file = os.path.join(sim_dir, 'model_performance.log')
    file_handler = logging.FileHandler(log_file, 'w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    all_indicators = {}
    for site_id, site_conf in conf.items():
        for variable, var_conf in site_conf.items():
            for time_step, settings in var_conf.items():
                logger.info(f"\n正在处理: 站点={site_id}, 变量={variable}, 时间步长={time_step}")

                primary_key = f'{site_id}_{variable}_{time_step}'

                sim_file = f"simu_{variable}_{time_step}_{site_id}.csv"
                obs_file = f"{variable}_{time_step}_{site_id}.csv"
                fig_file = f"{variable}_{time_step}_{site_id}.jpg"

                sim_path = os.path.join(sim_dir, sim_file)
                obs_path = os.path.join(obs_dir, obs_file)
                fig_path = os.path.join(fig_dir, fig_file)

                # 加载完整的数据集
                sim_df = load_data(sim_path, value_col=variable)
                obs_df = load_data(obs_path, value_col=variable)

                if sim_df is None or obs_df is None:
                    logger.warning("  - 因缺少数据文件而跳过。")
                    continue

                # 创建用于指标计算的合并数据集
                merged_df = pd.merge(obs_df.rename(columns={'Value': 'Obs'}),
                                     sim_df.rename(columns={'Value': 'Sim'}),
                                     left_index=True, right_index=True, how='inner')
                merged_df.dropna(inplace=True)

                if merged_df.empty:
                    logger.warning("  - 因找不到匹配的时间序列数据而跳过。")
                    continue

                all_metrics = {}
                if settings.get('cali_stime') and settings.get('cali_etime'):
                    cali_metrics = calculate_metrics(merged_df, settings['cali_stime'],
                                                     settings['cali_etime'])
                    all_metrics['cali'] = cali_metrics
                    for k, v in cali_metrics.items():
                        uniq_key = f'{primary_key}_cali_{k}'
                        all_indicators[uniq_key] = v
                    logger.info(f"  - 率定期指标 ({settings['cali_stime']} - {settings['cali_etime']}): {cali_metrics}")

                if settings.get('vali_stime') and settings.get('vali_etime'):
                    vali_metrics = calculate_metrics(merged_df, settings['vali_stime'],
                                                     settings['vali_etime'])
                    all_metrics['vali'] = vali_metrics
                    for k, v in vali_metrics.items():
                        uniq_key = f'{primary_key}_vali_{k}'
                        all_indicators[uniq_key] = v
                    logger.info(f"  - 验证期指标 ({settings['vali_stime']} - {settings['vali_etime']}): {vali_metrics}")

                if not plot_flag:
                    continue
                plot_time_series(sim_df, obs_df, settings, all_metrics, fig_path, precip_df,
                                 plot_stime, plot_etime)
    file_handler.close()
    logger.removeHandler(file_handler)
    logger.removeHandler(console_handler)
    # Path to the JSON file
    json_file = os.path.join(sim_dir, 'model_performance.json')

    # Write output to the JSON file
    with open(json_file, 'w') as output_write:
        json.dump(all_indicators, output_write, indent=4)


if __name__ == '__main__':
    # --- 配置 ---
    SIM_DATA_DIR = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\Results\OutletsResults'
    OBS_DATA_DIR = r'D:\data_m\manitowoc\observed'
    precip_file = os.path.join(SIM_DATA_DIR, 'precip.csv')
    plot_stime = '2008/1/1'
    plot_etime = '2024/12/31'

    FIGURES_DIR = SIM_DATA_DIR
    plot_flag = False

    # 主配置字典
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
                                                 'vali_etime': '2013/12'}},
                             'sed_out': {'day': {'ylabel': 'Sed(tons)',
                                                 'plot_style': 'point',
                                                 'cali_stime': '2011/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '',
                                                 'vali_etime': ''},
                                         'mon': {'ylabel': 'Sed(tons)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1',
                                                 'cali_etime': '2019/12',
                                                 'vali_stime': '2008/1',
                                                 'vali_etime': '2013/12'}},
                             'no3_out': {'day': {'ylabel': 'NO3 (Kg N)',
                                                 'plot_style': 'point',
                                                 'cali_stime': '2008/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '',
                                                 'vali_etime': ''}},
                             'nh3_out': {'day': {'ylabel': 'NH3 (Kg N)',
                                                 'plot_style': 'point',
                                                 'cali_stime': '2008/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '',
                                                 'vali_etime': ''}},
                             'orgn_out': {'day': {'ylabel': 'OrgN (Kg N)',
                                                  'plot_style': 'point',
                                                  'cali_stime': '2008/1/1',
                                                  'cali_etime': '2023/12/31',
                                                  'vali_stime': '',
                                                  'vali_etime': ''}},
                             'tn_out': {'day': {'ylabel': 'TN (Kg N)',
                                                'plot_style': 'point',
                                                'cali_stime': '2008/1/1',
                                                'cali_etime': '2024/12/31',
                                                'vali_stime': '',
                                                'vali_etime': ''}},
                             'solp_out': {'day': {'ylabel': 'SolP (Kg P)',
                                                  'plot_style': 'point',
                                                  'cali_stime': '2011/1/1',
                                                  'cali_etime': '2024/12/31',
                                                  'vali_stime': '',
                                                  'vali_etime': ''}},
                             'tp_out': {'day': {'ylabel': 'TP (Kg P)',
                                                'plot_style': 'point',
                                                'cali_stime': '2011/1/1',
                                                'cali_etime': '2024/12/31',
                                                'vali_stime': '',
                                                'vali_etime': ''},
                                        'mon': {'ylabel': 'TP (Kg P)',
                                                'plot_style': 'dotline',
                                                'cali_stime': '2014/1',
                                                'cali_etime': '2019/12',
                                                'vali_stime': '2008/1',
                                                'vali_etime': '2013/12'}},
                             },
            '363375': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/5',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       'sed_out': {'mon': {'ylabel': 'Sed (tons)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/5',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       'tp_out': {'mon': {'ylabel': 'TP (Kg P)',
                                          'plot_style': 'dotline',
                                          'cali_stime': '2017/7',
                                          'cali_etime': '2019/5',
                                          'vali_stime': '',
                                          'vali_etime': ''}},
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
                                           'vali_etime': ''}},
                       'sed_out': {'mon': {'ylabel': 'Sed (tons)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/10',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       'tp_out': {'mon': {'ylabel': 'TP (Kg P)',
                                          'plot_style': 'dotline',
                                          'cali_stime': '2017/7',
                                          'cali_etime': '2019/10',
                                          'vali_stime': '',
                                          'vali_etime': ''}},
                       }
            }

    # 运行评估流程
    evaluate_performance(conf, SIM_DATA_DIR, OBS_DATA_DIR, FIGURES_DIR, precip_file, plot_stime,
                         plot_etime, plot_flag=plot_flag)
    print("\n--- 评估完成 ---")
