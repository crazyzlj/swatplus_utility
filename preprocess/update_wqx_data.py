import pandas as pd
import os
import pytz
import numpy as np


# 复用之前的核心处理逻辑，确保处理方式一致
def process_raw_data_for_site(raw_df: pd.DataFrame, site_id: str,
                              target_timezone: str) -> pd.DataFrame:
    """
    一个内部辅助函数，用于处理原始数据框并为特定站点生成格式化的宽表。
    这是之前大脚本的核心逻辑的精简版。
    """
    # --- 筛选特定站点的数据 ---
    location_col = 'MonitoringLocationIdentifier'
    if location_col not in raw_df.columns:
        print("错误：在原始数据中找不到 'MonitoringLocationIdentifier' 列。")
        return pd.DataFrame()

    site_df = raw_df[raw_df[location_col] == site_id].copy()
    if site_df.empty:
        return pd.DataFrame()

    # --- 数据清洗与格式化 (与之前的脚本逻辑相同) ---
    value_column = 'ResultMeasureValue'
    site_df[value_column] = pd.to_numeric(site_df[value_column], errors='coerce')
    site_df.dropna(subset=[value_column], inplace=True)

    unit_column = 'ResultMeasure/MeasureUnitCode'
    if unit_column in site_df.columns:
        site_df[unit_column] = site_df[unit_column].fillna('')
        site_df['ValueWithUnit'] = np.where(
                site_df[unit_column] != '',
                site_df[value_column].astype(str) + '(' + site_df[unit_column] + ')',
                site_df[value_column].astype(str)
        )
    else:
        site_df['ValueWithUnit'] = site_df[value_column].astype(str)

    site_df['ParameterName'] = site_df['USGSpcode'].fillna(site_df['CharacteristicName'])

    # 时间处理
    site_df['datetime_str'] = site_df['ActivityStartDate'] + ' ' + site_df['ActivityStartTime/Time']
    tz_map = {'CST': 'America/Chicago', 'CDT': 'America/Chicago', 'EST': 'America/New_York',
              'EDT': 'America/New_York', 'UTC': 'UTC'}
    timezone_col = 'ActivityStartTime/TimeZoneCode'

    def convert_timezone(row):
        try:
            source_tz_str = row[timezone_col] if pd.notna(row[timezone_col]) else 'UTC'
            ianatz_name = tz_map.get(source_tz_str, source_tz_str)
            source_tz = pytz.timezone(ianatz_name)
            aware_datetime = source_tz.localize(pd.to_datetime(row['datetime_str']))
            target_tz = pytz.timezone(target_timezone)
            return aware_datetime.astimezone(target_tz)
        except Exception:
            return pd.NaT

    site_df['DATETIME_aware'] = site_df.apply(convert_timezone, axis=1)
    site_df.dropna(subset=['DATETIME_aware'], inplace=True)
    site_df['DATETIME'] = site_df['DATETIME_aware'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # --- 数据透视 ---
    pivoted_df = site_df.pivot_table(
            index='DATETIME',
            columns='ParameterName',
            values='ValueWithUnit',
            aggfunc='first'
    ).reset_index()

    return pivoted_df


def update_site_csv(existing_site_csv_path: str, new_raw_file_paths: list, target_timezone: str):
    """
    用新的原始数据文件更新一个已经处理好的站点CSV文件。

    Args:
        existing_site_csv_path (str): 需要更新的、已处理好的站点CSV文件路径。
        new_raw_file_paths (list): 包含新下载的原始数据的文件路径列表。
        target_timezone (str): 目标时区，用于处理新数据。
    """
    print(f"--- 开始更新文件: {existing_site_csv_path} ---")

    # --- 1. 识别站点ID并读取现有数据 ---
    if not os.path.exists(existing_site_csv_path):
        print(f"错误: 现有站点文件未找到 -> {existing_site_csv_path}")
        return

    site_id_to_update = os.path.splitext(os.path.basename(existing_site_csv_path))[0]
    print(f"目标站点ID为: {site_id_to_update}")

    try:
        old_df = pd.read_csv(existing_site_csv_path)
        print(f"成功读取现有数据，包含 {len(old_df)} 条记录。")
    except Exception as e:
        print(f"错误: 读取现有站点文件失败 -> {e}")
        return

    # --- 2. 读取并合并所有新的原始数据文件 ---
    COLUMN_MAP = {'USGSPcode': 'USGSpcode', 'Result_Measure': 'ResultMeasureValue',
                  'Result_Characteristic': 'CharacteristicName',
                  'Location_Identifier': 'MonitoringLocationIdentifier',
                  'Activity_StartDate': 'ActivityStartDate',
                  'Activity_StartTime': 'ActivityStartTime/Time',
                  'Activity_StartTimeZone': 'ActivityStartTime/TimeZoneCode',
                  'Result_MeasureUnit': 'ResultMeasure/MeasureUnitCode'}
    new_raw_dfs = []
    for file_path in new_raw_file_paths:
        if os.path.exists(file_path):
            try:
                temp_df = pd.read_csv(file_path, dtype=str, low_memory=False)
                rename_dict = {col: COLUMN_MAP[col] for col in temp_df.columns if col in COLUMN_MAP}
                if rename_dict:
                    temp_df.rename(columns=rename_dict, inplace=True)
                new_raw_dfs.append(temp_df)
            except Exception as e:
                print(f"警告: 读取新原始文件 {file_path} 失败，已跳过。错误: {e}")
        else:
            print(f"警告: 新原始文件 {file_path} 未找到，已跳过。")

    if not new_raw_dfs:
        print("未找到任何有效的新数据文件，无需更新。")
        return

    combined_new_raw_df = pd.concat(new_raw_dfs, ignore_index=True)
    print(f"成功合并 {len(new_raw_dfs)} 个新文件，共 {len(combined_new_raw_df)} 条原始记录。")

    # --- 3. 处理新数据 ---
    print("\n正在以相同逻辑处理新数据...")
    new_processed_df = process_raw_data_for_site(combined_new_raw_df, site_id_to_update,
                                                 target_timezone)

    if new_processed_df.empty:
        print("在新数据中未找到属于该站点的有效记录，无需更新。")
        return

    print(f"从新数据中为站点 {site_id_to_update} 处理得到 {len(new_processed_df)} 条记录。")

    # --- 4. 合并新旧数据并去重 ---
    print("\n正在合并新旧数据...")

    # 确保 DATETIME 列是 datetime 对象以便排序和去重
    old_df['DATETIME'] = pd.to_datetime(old_df['DATETIME'])
    new_processed_df['DATETIME'] = pd.to_datetime(new_processed_df['DATETIME'])

    # 使用 concat 合并，然后按 DATETIME 去重，并保留最后一条记录
    # 这能确保新数据覆盖掉旧数据中任何重叠的日期
    combined_df = pd.concat([old_df, new_processed_df], ignore_index=True)

    # 按时间排序，然后去重
    combined_df.sort_values(by='DATETIME', ascending=True, inplace=True)
    updated_df = combined_df.drop_duplicates(subset=['DATETIME'], keep='last').copy()

    # --- 5. 保存最终结果 ---
    # 将DATETIME列格式化回字符串以便保存
    updated_df['DATETIME'] = updated_df['DATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        updated_df.to_csv(existing_site_csv_path, index=False)
        print("\n--- 更新成功！---")
        print(f"文件 '{existing_site_csv_path}' 已更新。")
        print(
            f"旧记录数: {len(old_df)}, 新记录数: {len(new_processed_df)}, 最终总记录数: {len(updated_df)}")
    except Exception as e:
        print(f"错误: 保存更新文件失败 -> {e}")


# --- 如何使用这个脚本 ---
if __name__ == '__main__':
    # ========================== 用户配置 ==========================

    # 1. 指定您想要更新的、已经处理好的站点文件名
    #    这个文件应该在之前脚本的输出文件夹里
    existing_file = r'C:\Users\ljzhu\Downloads\wqx_processed\selected\USGS-04085427.csv'

    # 2. 将所有新下载的原始数据文件名放入这个列表
    wp = r'C:\Users\ljzhu\OneDrive\工作\实时工作文件\modeling-Manitowoc\USGS-04085427'
    new_files = [
        'total-nitrogen(nitrate+nitrite+totalammonia+organicnitrogen)-2008-05-28-2025-08-05.csv',
        'suspended-sediment-smaller-than-0.0625-1973-03-07-2025-04-08.csv',
        'suspended-sediment-smaller-than-0.063-dry-mass-2018-09-11-2025-04-08.csv',
        'suspended-sediment-larger-than-0.063-dry-mass-2018-09-11-2025-04-08.csv',
        'suspended-sediment-flux-1972-11-30-2012-09-12.csv',
        'suspended-sediment-dry-mass-2018-09-11-2025-07-08.csv',
        'suspended-sediment-concentration-1972-11-30-2025-07-08.csv',
        'phosphorus-unfiltered-1979-03-29-2025-08-05.csv',
        'phosphorus-filtered-1979-03-29-2008-09-16.csv',
        'orthophosphate-unfiltered-1990-11-21-1992-10-22.csv',
        'orthophosphate-unfiltered-1979-04-17-1987-03-25.csv',
        'orthophosphate-filtered-1980-05-07-2025-08-05.csv'
        # ... 如果有更多文件，继续添加
    ]
    new_files2 = [wp + os.sep + f for f in new_files]

    # 3. 确认目标时区
    tz = 'America/Chicago'

    # =============================================================

    update_site_csv(existing_site_csv_path=existing_file,
                    new_raw_file_paths=new_files2,
                    target_timezone=tz)