from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import pandas as pd

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

    col_names = [
        'jday', 'mon', 'day', 'yr', 'unit', 'gis_id', 'name', 'area', 'precip', 'evap', 'seep',
        'flo_stor', 'sed_stor', 'orgn_stor', 'sedp_stor', 'no3_stor', 'solp_stor', 'chla_stor',
        'nh3_stor', 'no2_stor', 'cbod_stor', 'dox_stor', 'san_stor', 'sil_stor', 'cla_stor',
        'sag_stor', 'lag_stor', 'grv_stor', 'null_1', 'flo_in', 'sed_in', 'orgn_in', 'sedp_in',
        'no3_in', 'solp_in', 'chla_in', 'nh3_in', 'no2_in', 'cbod_in', 'dox_in', 'san_in',
        'sil_in', 'cla_in', 'sag_in', 'lag_in', 'grv_in', 'null_2', 'flo_out', 'sed_out',
        'orgn_out', 'sedp_out', 'no3_out', 'solp_out', 'chla_out', 'nh3_out', 'no2_out',
        'cbod_out', 'dox_out', 'san_out', 'sil_out', 'cla_out', 'sag_out', 'lag_out',
        'grv_out', 'null_3', 'water_temp'
    ]
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

    cols_to_convert = ['flo_out', 'sed_out', 'no3_out', 'no2_out', 'nh3_out',
                       'orgn_out', 'solp_out', 'sedp_out', 'tn_out', 'tp_out']
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


# --- 使用示例 ---
if __name__ == '__main__':
    txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    INPUT_FILE = txtinout_dir + os.sep + 'channel_sd_day.txt'
    MON_INPUT_FILE = txtinout_dir + os.sep + 'channel_sd_mon.txt'
    CHANNEL_NUMBER = [68]
    SUFFIX = ['_usgs04085427']

    CHANNEL_NUMBERS = [68, 170, 157, 74]
    SUFFIXES = ['_usgs04085427', '_363375', '_10020782', '_363313']

    OUTPUT_DIRECTORY = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\Results\OutletsResults'

    process_swat_output_memory_efficient(
            input_file_path=INPUT_FILE, skiplines=3,
            channel_id=CHANNEL_NUMBER,
            output_folder=OUTPUT_DIRECTORY,
            fname_suffix=SUFFIX
    )

    process_swat_output_memory_efficient(
            input_file_path=MON_INPUT_FILE, skiplines=3,
            channel_id=CHANNEL_NUMBERS,
            output_folder=OUTPUT_DIRECTORY,
            fname_suffix=SUFFIXES, is_daily=False
    )

