import pandas as pd
import os
import pytz
import numpy as np


def process_water_quality_data(input_file_paths: list, output_folder: str, target_timezone: str):
    """
    处理从 waterqualitydata.us 下载的多种格式的水质数据，进行合并、汇总和格式化。

    Args:
        input_file_paths (list): 包含一个或多个原始CSV文件绝对路径的列表。
        output_folder (str): 用于存放输出文件的文件夹路径。
        target_timezone (str): 指定输出数据采用的目标时区。
    """
    print(f"--- 开始处理 {len(input_file_paths)} 个文件 ---")

    # --- 1. 标准化列名并合并文件 ---

    # 定义新旧列名的映射关系。键是旧名称，值是我们要统一成的标准名称。
    COLUMN_MAP = {
        'USGSPcode': 'USGSpcode',  # 处理 P 的大小写差异
        'Result_Measure': 'ResultMeasureValue',
        'Result_Characteristic': 'CharacteristicName',
        'Location_Identifier': 'MonitoringLocationIdentifier',
        'Activity_StartDate': 'ActivityStartDate',
        'Activity_StartTime': 'ActivityStartTime/Time',
        'Activity_StartTimeZone': 'ActivityStartTime/TimeZoneCode',
        'Result_MeasureUnit': 'ResultMeasure/MeasureUnitCode'
    }

    all_dfs = []  # 用于存放读取的每个DataFrame

    for file_path in input_file_paths:
        if not os.path.exists(file_path):
            print(f"警告: 输入文件未找到，已跳过 -> {file_path}")
            continue

        print(f"正在读取文件: {file_path}")
        try:
            # 步骤 1: 以字符串形式读取CSV
            temp_df = pd.read_csv(file_path, dtype=str, low_memory=False)

            # 步骤 2: 标准化列名
            # 创建一个要重命名的列的字典
            rename_dict = {col: COLUMN_MAP[col] for col in temp_df.columns if col in COLUMN_MAP}
            if rename_dict:
                temp_df.rename(columns=rename_dict, inplace=True)
                print(f"  > 已标准化 {len(rename_dict)} 个列名。")

            all_dfs.append(temp_df)

        except Exception as e:
            print(f"错误: 读取或处理文件 {file_path} 时失败 -> {e}")
            continue

    if not all_dfs:
        print("错误: 没有成功读取任何文件，程序终止。")
        return

    # 步骤 3: 合并所有DataFrame
    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\n成功合并所有文件，共 {len(df)} 条记录。")

    # --- 后续处理流程与之前版本相同 ---

    try:
        os.makedirs(output_folder, exist_ok=True)
        print(f"数据将输出到文件夹: {output_folder}")
    except OSError as e:
        print(f"错误: 创建输出文件夹失败 -> {e}")
        return

    # ... [其余代码与上一版完全相同] ...
    value_column = 'ResultMeasureValue'
    df[value_column] = pd.to_numeric(df[value_column], errors='coerce')
    original_rows = len(df)
    df.dropna(subset=[value_column], inplace=True)
    print(f"已舍弃 {original_rows - len(df)} 条无效观测值记录。剩余 {len(df)} 条有效记录。")

    unit_column = 'ResultMeasure/MeasureUnitCode'
    if unit_column in df.columns:
        df[unit_column] = df[unit_column].fillna('')
        df['ValueWithUnit'] = np.where(df[unit_column] != '',
                                       df[value_column].astype(str) + '(' + df[unit_column] + ')',
                                       df[value_column].astype(str))
    else:
        df['ValueWithUnit'] = df[value_column].astype(str)

    df['ParameterName'] = df['USGSpcode'].fillna(df['CharacteristicName'])

    df['datetime_str'] = df['ActivityStartDate'] + ' ' + df['ActivityStartTime/Time']

    tz_map = {'CST': 'America/Chicago', 'CDT': 'America/Chicago', 'EST': 'America/New_York',
              'EDT': 'America/New_York', 'MST': 'America/Denver', 'MDT': 'America/Denver',
              'PST': 'America/Los_Angeles', 'PDT': 'America/Los_Angeles', 'UTC': 'UTC'}

    def convert_timezone(row):
        try:
            source_tz_str = row['ActivityStartTime/TimeZoneCode'] if pd.notna(
                    row['ActivityStartTime/TimeZoneCode']) else 'UTC'
            ianatz_name = tz_map.get(source_tz_str, source_tz_str)
            source_tz = pytz.timezone(ianatz_name)
            aware_datetime = source_tz.localize(pd.to_datetime(row['datetime_str']))
            target_tz = pytz.timezone(target_timezone)
            return aware_datetime.astimezone(target_tz)
        except (pytz.UnknownTimeZoneError, TypeError, ValueError):
            return pd.NaT

    print("正在处理日期和时间...")
    df['DATETIME_aware'] = df.apply(convert_timezone, axis=1)
    df.dropna(subset=['DATETIME_aware'], inplace=True)
    df['DATETIME'] = df['DATETIME_aware'].dt.strftime('%Y-%m-%d %H:%M:%S')

    locations = df['MonitoringLocationIdentifier'].unique()
    print(f"\n发现 {len(locations)} 个站点，将分别为每个站点创建文件...")

    for i, loc_id in enumerate(locations):
        print(f"  ({i + 1}/{len(locations)}) 正在处理站点: {loc_id}")
        site_df = df[df['MonitoringLocationIdentifier'] == loc_id].copy()
        try:
            pivoted_df = site_df.pivot_table(index='DATETIME', columns='ParameterName',
                                             values='ValueWithUnit', aggfunc='first').reset_index()
            safe_filename = "".join(c for c in loc_id if c.isalnum() or c in ('-', '_')).rstrip()
            output_path = os.path.join(output_folder, f"{safe_filename}.csv")
            pivoted_df.to_csv(output_path, index=False)
        except Exception as e:
            print(f"    处理站点 {loc_id} 时发生错误: {e}")

    print("\n--- 所有操作完成！ ---")


if __name__ == '__main__':
    # ========================== 用户配置 ==========================
    # 将所有要处理的文件路径放入这个列表中
    # input_csv_paths = [
    #     r'C:\Users\ljzhu\Downloads\wqx2.x_data_demo.csv',
    #     r'C:\Users\ljzhu\Downloads\wqx3.0_data_demo.csv'
    # ]
    input_csv_paths = [
        r'C:\Users\ljzhu\Downloads\narrowresult_wqx3.0_since2024-03-11.csv',
        r'C:\Users\ljzhu\Downloads\narrowresult_wqx2.x.csv'
    ]

    output_directory = r'C:\Users\ljzhu\Downloads\wqx_processed'
    target_tz = 'America/Chicago'
    # =============================================================

    process_water_quality_data(input_csv_paths, output_directory, target_tz)
