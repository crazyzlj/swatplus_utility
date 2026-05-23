from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import pandas as pd

from postprocess.config import *

BASE_COLS = [
        'jday', 'mon', 'day', 'yr', 'unit', 'gis_id', 'name', 'area', 'precip', 'evap', 'seep',
        'flo_stor', 'sed_stor', 'orgn_stor', 'sedp_stor', 'no3_stor', 'solp_stor', 'chla_stor',
        'nh3_stor', 'no2_stor', 'cbod_stor', 'dox_stor', 'san_stor', 'sil_stor', 'cla_stor',
        'sag_stor', 'lag_stor', 'grv_stor', 'temp_1', 'flo_in', 'sed_in', 'orgn_in', 'sedp_in',
        'no3_in', 'solp_in', 'chla_in', 'nh3_in', 'no2_in', 'cbod_in', 'dox_in', 'san_in',
        'sil_in', 'cla_in', 'sag_in', 'lag_in', 'grv_in', 'temp_2', 'flo_out', 'sed_out',
        'orgn_out', 'sedp_out', 'no3_out', 'solp_out', 'chla_out', 'nh3_out', 'no2_out',
        'cbod_out', 'dox_out', 'san_out', 'sil_out', 'cla_out', 'sag_out', 'lag_out',
        'grv_out', 'temp_3', 'water_temp'
    ]
def parse_swat_records(input_file_path: str, skip_lines=0):
    """
    使用生成器逐条解析SWAT+文件中的记录，以节省内存。
    这个函数一次只在内存中处理一条完整的记录。

    Args:
        input_file_path (str): 输入文件的路径。

    Yields:
        list: 包含一条完整记录的所有数据值的列表。
    """
    with open(input_file_path, 'r') as f:
        # 跳过文件头
        for _ in range(skip_lines):
            next(f)

        record_values = []
        for line in f:
            # 跳过空行
            if not line.strip():
                continue

            values_in_line = line.strip().split()

            # 判断是否是新记录的开始
            # 新记录的开头通常是整数（jday, mon, day, yr），而续行通常是科学记数法
            # 这里我们用一个启发式规则：如果第一个值不含'E'或'.'，则认为是新记录的开始
            is_new_record_start = False
            try:
                # 尝试将第一个值转为整数，如果成功，则是新记录的开始
                int(values_in_line[0])
                if '.' not in values_in_line[0] and 'E' not in values_in_line[0].upper():
                    is_new_record_start = True
            except (ValueError, IndexError):
                is_new_record_start = False

            if is_new_record_start and record_values:
                # 如果是新记录的开始，并且旧记录已有数据，则yield旧记录
                yield record_values
                record_values = values_in_line  # 开始收集新记录
            else:
                # 否则，是当前记录的续行，追加数据
                record_values.extend(values_in_line)

        # yield文件中的最后一条记录
        if record_values:
            yield record_values


def process_swat_output_memory_efficient(input_file_path: str, skiplines,
                                         channel_id: list[int], output_folder: str,
                                         fname_suffix: list[str], is_daily: bool = True):
    """
    内存优化版的SWAT+结果处理函数。
    """
    print("--- 正在以内存优化模式运行 ---")
    # ... (文件检查和文件夹创建代码与之前相同) ...
    os.makedirs(output_folder, exist_ok=True)

    # col_names = [
    #     'jday', 'mon', 'day', 'yr', 'unit', 'gis_id', 'name', 'area', 'precip', 'evap', 'seep',
    #     'flo_stor', 'sed_stor', 'orgn_stor', 'sedp_stor', 'no3_stor', 'solp_stor', 'chla_stor',
    #     'nh3_stor', 'no2_stor', 'cbod_stor', 'dox_stor', 'san_stor', 'sil_stor', 'cla_stor',
    #     'sag_stor', 'lag_stor', 'grv_stor', 'temp_1', 'flo_in', 'sed_in', 'orgn_in', 'sedp_in',
    #     'no3_in', 'solp_in', 'chla_in', 'nh3_in', 'no2_in', 'cbod_in', 'dox_in', 'san_in',
    #     'sil_in', 'cla_in', 'sag_in', 'lag_in', 'grv_in', 'temp_2', 'flo_out', 'sed_out',
    #     'orgn_out', 'sedp_out', 'no3_out', 'solp_out', 'chla_out', 'nh3_out', 'no2_out',
    #     'cbod_out', 'dox_out', 'san_out', 'sil_out', 'cla_out', 'sag_out', 'lag_out',
    #     'grv_out', 'temp_3', 'water_temp'
    # ]
    if is_daily:
        col_names = BASE_COLS +  ['water_temp_prx', 'icej_cover', 'icej_stor', 'icej_block', 'icej_release',
                      'icej_qraw', 'icej_qadj', 'icej_qratio', 'icej_qrise', 'icej_susc',
                      'icej_flag']
    else:
        col_names = BASE_COLS
    name_col_index = col_names.index('name')  # 获取 'name' 列的索引
    target_names = [f"cha{str(cid).zfill(3)}" for cid in channel_id]

    print(f"开始从大文件中筛选河道 '{','.join(target_names)}' 的数据...")

    # 逐条记录读取，只保留需要的记录
    required_records = [list() for i in range(len(target_names))]
    for record in parse_swat_records(input_file_path, skiplines):
        # 直接通过索引检查name，避免创建完整的DataFrame
        if len(record) != len(col_names):
            continue
        for i, tname in enumerate(target_names):
            if record[name_col_index].strip() == tname:
                required_records[i].append(record)
    all_none = True
    for i, tname in enumerate(target_names):
        if not required_records[i]:
            print(f"错误: 在文件中未找到河道 '{tname}' 的数据。")
        else:
            all_none = False
            print(f"筛选完成，找到{tname}: {len(required_records[i])} 条相关记录。正在创建DataFrame...")
    if all_none:
        return

    # 仅用需要的记录创建DataFrame，这将占用非常小的内存
    df_channels = [pd.DataFrame(recs, columns=col_names) for recs in required_records]

    # --- 后续处理与之前的代码完全相同 ---
    # 拼接日期、转换类型、计算TN/TP、输出文件等
    def export_to_csv(data, variable_name, filename):
        output_df = data[['Date', variable_name]].copy()
        output_df.rename(columns={variable_name: 'Value'}, inplace=True)
        output_path = os.path.join(output_folder, filename)
        output_df.to_csv(output_path, index=False)
        print(f"已生成文件: {output_path}")

    cols_to_convert = ['flo_in', 'flo_out', 'sed_out', 'no3_out', 'no2_out', 'nh3_out',
                       'orgn_out', 'solp_out', 'sedp_out', 'tn_out', 'tp_out',
                       'water_temp', 'water_temp_prx',
                       'icej_cover', 'icej_stor', 'icej_block', 'icej_release',
                       'icej_qraw', 'icej_qadj', 'icej_qratio', 'icej_qrise', 'icej_susc',
                       'icej_flag']
    for i, df_channel in enumerate(df_channels):
        if is_daily:
            df_channel['Date'] = df_channel['yr'].astype(str) + '/' + \
                                 df_channel['mon'].astype(str) + '/' + \
                                 df_channel['day'].astype(str)
        else:
            df_channel['Date'] = df_channel['yr'].astype(str) + '/' + \
                                 df_channel['mon'].astype(str)

        for col in cols_to_convert:
            if col != 'tn_out' and col != 'tp_out':
                if  col not in df_channel:
                    continue
                df_channel[col] = pd.to_numeric(df_channel[col], errors='coerce')
            if col == 'tn_out':
                df_channel['tn_out'] = df_channel['no3_out'] + df_channel['nh3_out'] + \
                                       df_channel['no2_out'] + df_channel['orgn_out']
            if col == 'tp_out':
                df_channel['tp_out'] = df_channel['sedp_out'] + df_channel['solp_out']
            fname = f'simu_{col}_'
            if is_daily:
                fname += 'day'
            else:
                fname += 'mon'
            if fname_suffix[i] != '':
                fname += fname_suffix[i]
            fname += '.csv'
            export_to_csv(df_channel, col, fname)

    print("\n所有任务处理完成！")


def aggregate_all_channels_attributes(input_file_path: str, skiplines: int,
                                      output_folder: str, vars_to_sum: list[str],
                                      fname_suffix: str = '_all_sum', is_daily: bool = True):
    """
    内存优化版：遍历全文件，按日期汇总（求和）所有河道（channel）的指定属性。

    Args:
        input_file_path (str): 输入文件路径
        skiplines (int): 跳过的表头行数
        output_folder (str): 输出文件夹
        vars_to_sum (list[str]): 需要求和的变量名列表，如 ['flo_out', 'sed_out', 'tn_out']
        fname_suffix (str): 输出文件名的后缀
        is_daily (bool): 是否为日值文件
    """
    print(f"--- 正在按{'日' if is_daily else '月'}汇总所有河道的指定属性 ---")
    os.makedirs(output_folder, exist_ok=True)

    if is_daily:
        col_names = BASE_COLS + ['water_temp_prx', 'icej_cover', 'icej_stor', 'icej_block',
                                 'icej_release',
                                 'icej_qraw', 'icej_qadj', 'icej_qratio', 'icej_qrise', 'icej_susc',
                                 'icej_flag']
    else:
        col_names = BASE_COLS

    # 处理TN和TP这种衍生变量（需要由基础变量相加而得）
    derived_vars = {
        'tn_out': ['no3_out', 'nh3_out', 'no2_out', 'orgn_out'],
        'tp_out': ['sedp_out', 'solp_out']
    }

    # 确定实际需要从文件中提取并累加的基础变量
    actual_vars_to_extract = set(vars_to_sum)
    for var in vars_to_sum:
        if var in derived_vars:
            actual_vars_to_extract.update(derived_vars[var])
            actual_vars_to_extract.remove(var)

    # 获取索引字典，加速按列名查找
    var_indices = {var: col_names.index(var) for var in actual_vars_to_extract if var in col_names}
    yr_idx = col_names.index('yr')
    mon_idx = col_names.index('mon')
    day_idx = col_names.index('day') if is_daily else -1

    # 累加器字典: { "YYYY/M/D" : { "flo_out": 123.4, "sed_out": 5.6 } }
    aggregated_data = {}

    print(f"正在扫描并累加变量: {', '.join(actual_vars_to_extract)}...")

    for record in parse_swat_records(input_file_path, skiplines):
        if len(record) != len(col_names):
            continue

        # 提取日期构建Key
        yr, mon = record[yr_idx], record[mon_idx]
        date_key = f"{yr}/{mon}/{record[day_idx]}" if is_daily else f"{yr}/{mon}"

        # 初始化当天的字典
        if date_key not in aggregated_data:
            aggregated_data[date_key] = {var: 0.0 for var in actual_vars_to_extract}

        # 累加各个需要的基础变量
        for var, idx in var_indices.items():
            try:
                val = float(record[idx])
                aggregated_data[date_key][var] += val
            except ValueError:
                pass  # 忽略无法转为float的异常值（如空值或异常字符）

    print("累加完成，正在生成输出文件...")

    # 转换为 DataFrame 方便统一输出
    records_for_df = []
    for date_key, sums in aggregated_data.items():
        row = {'Date': date_key}
        row.update(sums)
        records_for_df.append(row)

    if not records_for_df:
        print("警告: 未能在文件中解析到有效数据。")
        return

    df_sum = pd.DataFrame(records_for_df)

    # 计算衍生变量
    if 'tn_out' in vars_to_sum:
        df_sum['tn_out'] = df_sum['no3_out'] + df_sum['nh3_out'] + df_sum['no2_out'] + df_sum[
            'orgn_out']
    if 'tp_out' in vars_to_sum:
        df_sum['tp_out'] = df_sum['sedp_out'] + df_sum['solp_out']

    # 内部函数：导出CSV
    def export_to_csv(data, variable_name, filename):
        output_df = data[['Date', variable_name]].copy()
        output_df.rename(columns={variable_name: 'Value'}, inplace=True)
        output_path = os.path.join(output_folder, filename)
        output_df.to_csv(output_path, index=False)
        print(f"已生成汇总文件: {output_path}")

    # 分别导出用户请求的变量
    for var in vars_to_sum:
        fname = f'aggregate_simu_{var}_{"day" if is_daily else "mon"}{fname_suffix}.csv'
        export_to_csv(df_sum, var, fname)

    print("所有汇总任务处理完成！\n")

def read_channel_daily_monthly_outputs(txtinout_dir, out_dir, dailychannel, dailysuffix,
                                       monthlychannel, monthlysuffix):
    daily_input = os.path.join(txtinout_dir, 'channel_sd_day.txt')
    monthly_input = os.path.join(txtinout_dir, 'channel_sd_mon.txt')

    process_swat_output_memory_efficient(
            input_file_path=daily_input, skiplines=3,
            channel_id=dailychannel,
            output_folder=out_dir,
            fname_suffix=dailysuffix
    )

    process_swat_output_memory_efficient(
            input_file_path=monthly_input, skiplines=3,
            channel_id=monthlychannel,
            output_folder=out_dir,
            fname_suffix=monthlysuffix, is_daily=False
    )

if __name__ == '__main__':
    txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0517-all-2'
    out_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0517-all-2\OutletsResults'

    read_channel_daily_monthly_outputs(txtinout_dir, out_dir, CHANNEL_NUMBER, SUFFIX,
                                       CHANNEL_NUMBERS, SUFFIXES)
    # 新增的：汇总全流域所有河道的特定属性
    daily_input = os.path.join(txtinout_dir, 'channel_sd_day.txt')

    # 例如：我们想汇总每天/月所有河网内的无机氮总蓄存量，或者总流量输出
    vars_to_aggregate = ['icej_cover', 'icej_stor', 'icej_block',
                                 'icej_release',
                                 'icej_qraw', 'icej_qadj']

    aggregate_all_channels_attributes(
            input_file_path=daily_input,
            skiplines=3,
            output_folder=out_dir,
            vars_to_sum=vars_to_aggregate,
            fname_suffix='_all_channels_sum',
            is_daily=True
    )