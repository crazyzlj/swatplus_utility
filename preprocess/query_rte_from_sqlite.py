# 用Python实现如下功能：
# 1. 从QSWAT+构建的sqlite数据库中，按要求查询符合条件的channel，并输出channel的编号
# 2. 用户指定一个或多个子流域编号，如[30, 31, 32]
# 3. 在sqlite数据库的gis_channels表中，根据subbasin字段（即子流域编号）查询对应的channel编号（即id）
# 5. 将符合条件的channel编号排序,输出个数和编号
import sqlite3
import os
import json
from typing import List, Optional, Tuple, Dict, Any

def query_channels_by_strahler(db_path: str,
                               subbasins: List[int],
                               strahler_orders: List[int]) -> Tuple[int, List[int]]:
    """
    从 QSWAT+ SQLite 数据库中，根据子流域编号 和 Strahler 级别
    查询对应的 Channel 编号。
    """

    if not subbasins:
        print(" -> 警告: 'subbasins' 列表为空, 跳过查询。")
        return 0, []
    if not strahler_orders:
        print(" -> 警告: 'strahler_orders' 列表为空, 跳过查询。")
        return 0, []

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        sub_placeholders = ','.join('?' for _ in subbasins)
        str_placeholders = ','.join('?' for _ in strahler_orders)

        # 假设Strahler级别字段名为 'strahler'
        sql_query = f"""
            SELECT id
            FROM gis_channels
            WHERE 
                subbasin IN ({sub_placeholders}) 
                AND strahler IN ({str_placeholders})
            ORDER BY id ASC
        """

        params = subbasins + strahler_orders

        cursor.execute(sql_query, params)

        results = cursor.fetchall()
        channel_ids = [row[0] for row in results]

        return len(channel_ids), channel_ids

    except sqlite3.Error as e:
        if "no such column" in str(e):
            print(f"数据库查询错误: {e}")
            print("错误提示: 请检查 'gis_channels' 表中是否"
                  "存在名为 'strahler' 的字段。")
        else:
            print(f"数据库查询时发生错误: {e}")
        return 0, []
    except Exception as e:
        print(f"发生意外错误: {e}")
        return 0, []
    finally:
        if conn:
            conn.close()

def run_and_save_channel_combinations(db_path: str,
                                      sub_dict: Dict[str, Optional[List[int]]],
                                      strahler_dict: Dict[str, List[int]],
                                      output_file_path: str):
    """
    遍历所有子流域和Strahler级别组合，运行查询，并保存为 JSON。

    参数:
    db_path (str): SQLite 数据库文件路径。
    sub_dict (dict): 子流域组合字典。
    strahler_dict (dict): Strahler 级别组合字典。
    output_file_path (str): 输出 JSON 文件的路径。
    """

    print(f"--- 开始批量处理 Channel 组合 ---")

    if not os.path.exists(db_path):
        print(f"错误: 数据库文件未找到: {db_path}")
        return

    # 复制字典以避免修改原始输入
    processed_sub_dict = sub_dict.copy()

    # *** 智能处理 'all': None ***
    # 如果 'all' 键的值为 None, 则自动汇集所有其他的 subbasin 列表
    if 'all' in processed_sub_dict and processed_sub_dict['all'] is None:
        print("检测到 'all': None。正在自动汇集所有其他子流域...")
        all_subs_set = set()
        for key, sub_list in processed_sub_dict.items():
            if key != 'all' and sub_list is not None:
                all_subs_set.update(sub_list)

        # 替换 None 为排序后的
        processed_sub_dict['all'] = sorted(list(all_subs_set))
        print(f" -> 'all' 组已填充 {len(processed_sub_dict['all'])} 个唯一子流域。")

    all_results = {}

    # 1. 遍历所有组合
    for sub_key, sub_values in processed_sub_dict.items():
        for str_key, str_values in strahler_dict.items():
            # 2. 生成组合名称
            combination_name = f"{sub_key}_{str_key}"

            print(f"正在处理: {combination_name}...")

            # 3. 执行查询
            count, channel_list = query_channels_by_strahler(
                    db_path,
                    sub_values,  # 使用处理过的 sub_values
                    str_values
            )
            if count == 0:
                continue
            # 4. 存储结果
            all_results[combination_name] = {
                "count": count,
                "channel_ids": channel_list,
                # 存储输入参数用于追溯
                "inputs": {
                    "subbasins_count": len(sub_values) if sub_values else 0,
                    # "subbasins": sub_values, # 列表可能很长, 存入json会导致文件很大, 暂时注释
                    "strahler_orders": str_values
                }
            }
            print(f" -> 完成: {combination_name} | 找到 {count} 个 Channel")

    # 5. 写入 JSON 文件
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=4)

        print(f"\n--- 批量处理完成 ---")
        print(f"所有组合已成功保存到: {output_file_path}")

    except IOError as e:
        print(f"写入 JSON 文件时出错: {e}")
    except Exception as e:
        print(f"发生意外错误: {e}")


if __name__ == "__main__":
    DEMO_DB_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite"
    OUTPUT_JSON_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\channel_combinations.json"

    sub_inputs = {
        'up1': [59, 55, 56, 66, 67, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82],
        'up2': [15, 1, 2, 3, 4, 6, 7, 8, 9, 13, 21, 26, 27],
        'mid1': [42, 5, 10, 11, 12, 14, 16, 17, 18, 19, 20, 22, 23, 24, 25, 28, 29, 33, 34,
                 35, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
                 57, 58, 60, 61, 62, 63, 64, 65, 68, 83, 84, 85],
        'down1': [31, 30, 32],
        'all': None}

    strahler_group = {'headwater': [1, 2],
                      'midbranch': [3, 4],
                      'mainstream': [5]}

    run_and_save_channel_combinations(
            DEMO_DB_FILE,
            sub_inputs,
            strahler_group,
            OUTPUT_JSON_FILE
    )

    print(f"\n--- 正在读取生成的 {OUTPUT_JSON_FILE} (用于验证) ---")
    try:
        with open(OUTPUT_JSON_FILE, 'r') as f:
            data = json.load(f)
            # 格式化打印 JSON 内容
            print(json.dumps(data, indent=2))

            # 检查一个 'all' 组的结果
            key_to_check = "all_headwater"
            if key_to_check in data:
                print(f"\n[验证] {key_to_check} (应包含up_demo的headwater): "
                      f"{data[key_to_check]['channel_ids']}")
                # 预期: [1, 2, 3, 4, 5]

    except FileNotFoundError:
        print("未找到 JSON 文件。")
