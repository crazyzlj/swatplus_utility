import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from typing import Union, List, Dict, Any
from pygeoc.utils import MathClass

def load_data(file_path: str, date_col: str = 'Date', value_col: str = 'Value') -> pd.DataFrame:
    """从CSV文件加载时间序列数据。"""
    if not os.path.exists(file_path):
        print(f"警告: 文件未找到 {file_path}")
        return None
    df = pd.read_csv(file_path)
    df[date_col] = pd.to_datetime(df[date_col])
    df.set_index(date_col, inplace=True)
    return df.rename(columns={value_col: os.path.basename(file_path)})


def calculate_metrics(df: pd.DataFrame, start_time: str, end_time: str) -> Dict[str, float]:
    """为指定时间段计算模型性能指标。"""
    period_df = df.loc[start_time:end_time]
    if period_df.empty:
        return {}

    obs_array = period_df['Obs'].values
    sim_array = period_df['Sim'].values

    metrics = {
        'NSE': MathClass.nashcoef(obs_array, sim_array),
        'RSR': MathClass.rsr(obs_array, sim_array),
        'PBIAS': MathClass.pbias(obs_array, sim_array),
        'R_square': MathClass.rsquare(obs_array, sim_array),
        'RMSE': MathClass.rmse(obs_array, sim_array)
    }
    return metrics


def plot_time_series(
        var_name: str,
        config: Dict[str, Any],
        sim_data: pd.DataFrame,  # 完整的模拟数据
        merged_data: pd.DataFrame,  # 匹配后的数据
        precip_data: pd.DataFrame,
        cali_metrics: Dict[str, float],
        vali_metrics: Dict[str, float],
        plot_start: str,
        plot_end: str,
        output_dir: str
):
    """
    绘制模拟与实测对比图，并标注性能指标。
    此版本修正了Y轴自动缩放问题。
    """
    fig, ax1 = plt.subplots(figsize=(15, 5))

    # --- 1. 绘制变量 ---
    p_sim, = ax1.plot(sim_data.index, sim_data.iloc[:, 0], color='red', linestyle='-',
                      linewidth=1.2, label='Simulation')
    p_obs = None
    if config['plot_style'] == 'dotline':
        p_obs, = ax1.plot(merged_data.index, merged_data['Obs'], color='black', marker='.',
                          markersize=4, linestyle='-', linewidth=1, label='Observation')
    elif config['plot_style'] == 'bar':
        p_obs = ax1.bar(merged_data.index, merged_data['Obs'], width=1.5, color='black',
                        label='Observation', alpha=0.8)
    elif config['plot_style'] == 'point':
        p_obs = ax1.scatter(merged_data.index, merged_data['Obs'], color='black', marker='o',
                            s=15, label='Observation', alpha=0.8, zorder=10)

    ax1.set_ylabel(f"{var_name} ({config['unit']})", fontsize=14)
    ax1.set_xlabel("Date", fontsize=14)
    ax1.tick_params(axis='both', which='major', labelsize=12)
    ax1.set_xlim(pd.to_datetime(plot_start), pd.to_datetime(plot_end))

    # --- 2. 绘制降水 ---
    ax2 = ax1.twinx()
    p_precip = ax2.bar(precip_data.index, precip_data.iloc[:, 0], color='blue',
                       label='Precipitation', width=1.0)
    ax2.set_ylabel("Precipitation (mm)", fontsize=14)
    ax2.invert_yaxis()
    ax2.tick_params(axis='y', which='major', labelsize=12)
    max_precip = precip_data.iloc[:, 0].max()
    if max_precip > 0:
        ax2.set_ylim(max_precip * 4, 0)

    # --- 3. X轴日期格式 ---
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=5, maxticks=10))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    fig.autofmt_xdate()

    # --- 4. 手动设置左Y轴范围 (关键修正) ---
    start_date = pd.to_datetime(plot_start)
    end_date = pd.to_datetime(plot_end)

    # 筛选出可视范围内的模拟和实测数据
    visible_sim = sim_data.loc[start_date:end_date]
    visible_obs = merged_data.loc[start_date:end_date]

    # 计算可视范围内的最大值
    max_val_sim = visible_sim.iloc[:, 0].max()
    max_val_obs = 0
    if not visible_obs.empty:
        max_val_obs = visible_obs['Obs'].max()

    overall_max = max(max_val_sim, max_val_obs)

    # 设置Y轴上限为最大值的1.1倍，下限为0（适用于非负数据）
    if pd.notna(overall_max):
        ax1.set_ylim(bottom=0, top=overall_max * 1.1)

    # --- 5. 文本标注 ---
    has_validation = config.get('vali_stime') and config.get('vali_etime')
    text_box_style = dict(facecolor='white', alpha=0.8, edgecolor='none', boxstyle='round,pad=0.3')

    if has_validation:
        cali_start = pd.to_datetime(config['cali_stime'])
        vali_start = pd.to_datetime(config['vali_stime'])
        separator_date = vali_start if cali_start < vali_start else cali_start
        ax1.axvline(x=separator_date, color='dimgray', linestyle='--', linewidth=2)

        ax1.text(0.25, 0.95, "Validation", transform=ax1.transAxes, ha='center', va='top',
                 fontsize=14, fontweight='bold')
        ax1.text(0.75, 0.95, "Calibration", transform=ax1.transAxes, ha='center', va='top',
                 fontsize=14, fontweight='bold')

        if vali_metrics:
            vali_stats_text = (f"NSE: {vali_metrics.get('NSE', 'N/A'):.3f}\n"
                               f"PBIAS: {vali_metrics.get('PBIAS', 'N/A'):.3f}\n"
                               f"RSR: {vali_metrics.get('RSR', 'N/A'):.3f}")
            ax1.text(0.25, 0.85, vali_stats_text, transform=ax1.transAxes, ha='center', va='top',
                     fontsize=12, color='red', bbox=text_box_style)
        if cali_metrics:
            cali_stats_text = (f"NSE: {cali_metrics.get('NSE', 'N/A'):.3f}\n"
                               f"PBIAS: {cali_metrics.get('PBIAS', 'N/A'):.3f}\n"
                               f"RSR: {cali_metrics.get('RSR', 'N/A'):.3f}")
            ax1.text(0.75, 0.85, cali_stats_text, transform=ax1.transAxes, ha='center', va='top',
                     fontsize=12, color='red', bbox=text_box_style)
    else:
        if cali_metrics:
            stats_text = (f"Calibration\n\n"
                          f"NSE: {cali_metrics.get('NSE', 'N/A'):.3f}\n"
                          f"PBIAS: {cali_metrics.get('PBIAS', 'N/A'):.3f}\n"
                          f"RSR: {cali_metrics.get('RSR', 'N/A'):.3f}")
            ax1.text(0.5, 0.85, stats_text, transform=ax1.transAxes, ha='center', va='center',
                     fontsize=12, color='red', bbox=text_box_style)

    # --- 6. 图例和布局 ---
    handles = [p_precip, p_obs, p_sim]
    labels = [h.get_label() for h in handles if h is not None]
    ax1.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.18), ncol=3, fontsize=12,
               frameon=False)

    fig.tight_layout(rect=[0, 0, 1, 0.93])

    # --- 7. 保存图像 ---
    output_filename = f"{var_name}_performance.png"
    plt.savefig(os.path.join(output_dir, output_filename), dpi=300)
    print(f"图表已保存至: {os.path.join(output_dir, output_filename)}")
    plt.close(fig)


if __name__ == '__main__':
    sim_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\Results\OutletsResults'
    obs_dir = r'D:\data_m\manitowoc\observed'
    precip_file = os.path.join(sim_dir, 'precip.csv')
    plot_stime = '2008/1/1'
    plot_etime = '2024/12/31'

    output_plot_dir = sim_dir
    os.makedirs(output_plot_dir, exist_ok=True)

    conf = {'Q': {'sim_file': 'simu_q.csv',  # file located in sim_dir
                  'obs_file': 'flow_cms_usgs04085427.csv',  # file located in obs_dir
                  'unit': 'm^3/s',  # unit, so the left Y-axes label will be Q(m^/s)
                  'plot_style': 'dotline', # dotline, bar, or point
                  'cali_stime': '2014/1/1',
                  # Start datetime of calibration period, in format YYYY/MM/DD
                  'cali_etime': '2024/12/31',
                  # End datetime of calibration period, in format YYYY/MM/DD
                  'vali_stime': '2008/1/1',
                  # Start datetime of validation period, in format YYYY/MM/DD
                  'vali_etime': '2013/12/31'  # End datetime of validation period, in format YYYY/MM/DD
                  },
            'Sed': {'sim_file': 'simu_sed.csv',
                    'obs_file': 'sed_usgs04085427.csv',
                    'unit': 'tons',
                    'plot_style': 'point',
                    'cali_stime': '2011/1/1',
                    'cali_etime': '2024/12/31',
                    'vali_stime': '',  # one of vali_stime and vali_etime is '' or None means
                    'vali_etime': ''  # no validation period is set for model performance and plotting
                    },
            'NO3': {'sim_file': 'simu_no3.csv',
                    'obs_file': 'no3_usgs04085427.csv',
                    'unit': 'kg N',
                    'plot_style': 'point',
                    'cali_stime': '2008/1/1',
                    'cali_etime': '2024/12/31',
                    'vali_stime': '',
                    'vali_etime': ''},
            'NH3': {'sim_file': 'simu_nh3.csv',
                    'obs_file': 'nh3_usgs04085427.csv',
                    'unit': 'kg N',
                    'plot_style': 'point',
                    'cali_stime': '2008/1/1',
                    'cali_etime': '2024/12/31',
                    'vali_stime': '',
                    'vali_etime': ''},
            'OrgN': {'sim_file': 'simu_orgn.csv',
                     'obs_file': 'orgn_usgs04085427.csv',
                     'unit': 'kg N',
                     'plot_style': 'point',
                     'cali_stime': '2008/1/1',
                     'cali_etime': '2023/12/31',
                     'vali_stime': '',
                     'vali_etime': ''},
            'TN': {'sim_file': 'simu_tn.csv',
                   'obs_file': 'tn_usgs04085427.csv',
                   'unit': 'kg N',
                   'plot_style': 'point',
                   'cali_stime': '2008/1/1',
                   'cali_etime': '2024/12/31',
                   'vali_stime': '',
                   'vali_etime': ''},
            'SolP': {'sim_file': 'simu_solp.csv',
                     'obs_file': 'solp_usgs04085427.csv',
                     'unit': 'kg P',
                     'plot_style': 'point',
                     'cali_stime': '2011/1/1',
                     'cali_etime': '2024/12/31',
                     'vali_stime': '',
                     'vali_etime': ''},
            'TP': {'sim_file': 'simu_tp.csv',
                   'obs_file': 'tp_usgs04085427.csv',
                   'unit': 'kg P',
                   'plot_style': 'point',
                   'cali_stime': '2011/1/1',
                   'cali_etime': '2024/12/31',
                   'vali_stime': '',
                   'vali_etime': ''}
            }

    precip_df = load_data(precip_file)
    if precip_df is None:
        raise FileNotFoundError(f"降水文件未找到: {precip_file}")

    for var_name, config in conf.items():
        print(f"\n--- 正在处理变量: {var_name} ---")

        sim_path = os.path.join(sim_dir, config['sim_file'])
        obs_path = os.path.join(obs_dir, config['obs_file'])

        sim_df = load_data(sim_path)
        obs_df = load_data(obs_path)

        if sim_df is None or obs_df is None:
            print(f"跳过变量 {var_name}，因为缺少模拟或实测数据文件。")
            continue

        merged_df = pd.merge(obs_df, sim_df, left_index=True, right_index=True, how='inner')
        merged_df.columns = ['Obs', 'Sim']
        merged_df.dropna(inplace=True)

        if merged_df.empty:
            print(f"跳过变量 {var_name}，因为没有时间上匹配的模拟和实测数据。")
            continue

        cali_metrics = calculate_metrics(merged_df, config['cali_stime'], config['cali_etime'])
        print(f"率定期 ({config['cali_stime']} - {config['cali_etime']}) 指标: {cali_metrics}")

        vali_metrics = {}
        if config.get('vali_stime') and config.get('vali_etime'):
            vali_metrics = calculate_metrics(merged_df, config['vali_stime'], config['vali_etime'])
            print(f"验证期 ({config['vali_stime']} - {config['vali_etime']}) 指标: {vali_metrics}")

        plot_time_series(
                var_name=var_name,
                config=config,
                sim_data=sim_df,
                merged_data=merged_df,
                precip_data=precip_df,
                cali_metrics=cali_metrics,
                vali_metrics=vali_metrics,
                plot_start=plot_stime,
                plot_end=plot_etime,
                output_dir=output_plot_dir
        )