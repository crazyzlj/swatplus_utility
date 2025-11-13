import pandas as pd
import os
import sys


def parse_swat_records(input_file_path: str, skip_rows: int = 9):
    """
    使用生成器逐条解析SWAT+文件中的记录，以节省内存。
    """
    try:
        with open(input_file_path, 'r') as f:
            for _ in range(skip_rows):
                next(f)

            record_values = []
            for line in f:
                if not line.strip():
                    continue

                values_in_line = line.strip().split()

                is_new_record_start = False
                try:
                    if values_in_line[0].isdigit():
                        is_new_record_start = True
                except (ValueError, IndexError):
                    is_new_record_start = False

                if is_new_record_start and record_values:
                    yield record_values
                    record_values = values_in_line
                else:
                    record_values.extend(values_in_line)

            if record_values:
                yield record_values
    except FileNotFoundError:
        print(f"警告: 文件未找到 {input_file_path}，跳过解析。")
        return


def process_swat_file(
        input_file_path: str,
        column_names: list,
        output_folder: str,
        target_column: str,
        output_filename: str,
        skip_rows: int,
        filter_by_name: str = None,
        perform_unit_conversion: bool = False  # 新增参数
):
    """
    通用化的SWAT+结果文件处理函数，增加了单位转换功能。
    """
    print(f"--- 正在处理文件: {input_file_path} ---")

    required_records = []
    name_col_index = column_names.index('name') if 'name' in column_names else -1

    for record in parse_swat_records(input_file_path, skip_rows):
        if len(record) == len(column_names):
            if filter_by_name:
                if name_col_index != -1 and record[name_col_index].strip() == filter_by_name:
                    required_records.append(record)
            else:
                required_records.append(record)
        else:
            print(f"警告: 跳过格式不匹配的记录。预期 {len(column_names)} 列，实际 {len(record)} 列。")

    if not required_records:
        print(f"未在 {input_file_path} 中找到符合条件的数据。")
        return False

    print(f"筛选完成，找到 {len(required_records)} 条相关记录。正在创建DataFrame...")

    df_filtered = pd.DataFrame(required_records, columns=column_names)

    df_filtered['Date'] = df_filtered['yr'].astype(str) + '/' + \
                          df_filtered['mon'].astype(str).str.zfill(2) + '/' + \
                          df_filtered['day'].astype(str).str.zfill(2)

    # --- 新增：单位转换逻辑 ---
    if perform_unit_conversion:
        print("正在执行单位转换 (m^3 -> mm)...")
        # 确保参与计算的列是数值类型
        df_filtered['precip'] = pd.to_numeric(df_filtered['precip'], errors='coerce')
        df_filtered['area'] = pd.to_numeric(df_filtered['area'], errors='coerce')

        # 应用公式: mm = m^3 / (ha * 10)
        # 增加一个检查，防止area为0导致除零错误
        if (df_filtered['area'] == 0).any():
            print("警告: 'area' 列中存在0值，转换结果中可能出现inf(无穷大)。")

        # 将计算结果放入目标列，即使目标列也叫'precip'，它会被新值覆盖
        df_filtered[target_column] = df_filtered['precip'] / df_filtered['area'] / 10

    # 提取目标列并输出
    df_output = df_filtered[['Date', target_column]].copy()
    df_output.rename(columns={target_column: 'Value'}, inplace=True)
    df_output['Value'] = pd.to_numeric(df_output['Value'], errors='coerce')

    output_path = os.path.join(output_folder, output_filename)
    os.makedirs(output_folder, exist_ok=True)
    df_output.to_csv(output_path, index=False)

    print(f"成功生成文件: {output_path}")
    return True


if __name__ == '__main__':
    # 1. SWAT+输出文件所在的文件夹
    # 假设您的 basin_wb_day.txt 或 channel_sd_day.txt 在这个文件夹里
    txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    INPUT_FILE = txtinout_dir + os.sep + 'channel_sd_day.txt'
    CHANNEL_NUMBER = 68
    output_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\Results\OutletsResults'

    basin_wb_file = os.path.join(txtinout_dir, 'basin_wb_day.txt')
    channel_sd_file = os.path.join(txtinout_dir, 'channel_sd_day.txt')

    # basin_wb_day.txt 的列名和文件头信息
    basin_wb_cols = [
        'jday', 'mon', 'day', 'yr', 'unit', 'gis_id', 'name', 'precip', 'snofall', 'snomlt',
        'surq_gen', 'latq', 'wateryld', 'perc', 'et', 'ecanopy', 'eplant', 'esoil', 'surq_cont',
        'cn', 'sw_init', 'sw_final', 'sw_ave', 'sw_300', 'sno_init', 'sno_final', 'snopack',
        'pet', 'qtile', 'irr', 'surq_runon', 'latq_runon', 'overbank', 'surq_cha', 'surq_res',
        'surq_ls', 'latq_cha', 'latq_res', 'latq_ls', 'gwsoilq', 'satex', 'satex_chan',
        'sw_change', 'lagsurf', 'laglatq', 'lagsatex', 'wet_evap', 'wet_oflo', 'wet_stor'
    ]
    basin_skip_rows = 3

    # channel_sd_day.txt 的列名和文件头信息
    channel_sd_cols = [
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
    channel_skip_rows = 3

    # 逻辑判断：优先使用 basin_wb_day.txt
    if os.path.exists(basin_wb_file):
        print(f"找到文件: {basin_wb_file}，将从此文件提取降水数据。")
        process_swat_file(
                input_file_path=basin_wb_file,
                column_names=basin_wb_cols,
                output_folder=output_dir,
                target_column='precip',
                output_filename='precip.csv',
                skip_rows=basin_skip_rows,
                filter_by_name=None,  # basin_wb通常只有一条记录每天，无需筛选
                perform_unit_conversion=False
        )
    elif os.path.exists(channel_sd_file):
        print(f"未找到 basin_wb_day.txt，但找到 {channel_sd_file}。")
        print("将从流域出口（通常是最后一条河道记录）提取降水数据。")
        # 注意：从channel文件中提取降水时，所有河道记录的precip值都相同
        # 我们只需任选一条记录即可，这里默认不筛选，提取文件中的第一条记录
        process_swat_file(
                input_file_path=channel_sd_file,
                column_names=channel_sd_cols,
                output_folder=output_dir,
                target_column='precip',
                output_filename='precip.csv',
                skip_rows=channel_skip_rows,
                filter_by_name=f"cha{str(CHANNEL_NUMBER).zfill(3)}",
                perform_unit_conversion=True
        )
    else:
        print("错误: 未在指定目录下找到 basin_wb_day.txt 或 channel_sd_day.txt。")


