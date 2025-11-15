# 用Python实现如下功能：
# 1. 从QSWAT+构建的sqlite数据库中，按要求查询符合条件的hru，并按规定格式输出hru的编号
# 2. 用户指定一个或多个子流域编号，如[30, 31, 32]，指定landuse（为None表示所有landuse），如['dairy1t1', 'dairy1t2']，指定soil（为None表示所有soil），如['426184','426201']
# 3. 首先在sqlite数据库的gis_lsus表中，根据subbasin字段（即子流域编号）查询对应的lsu编号（即id）
# 4. 然后在sqlite数据库的gis_hrus表中，根据上一步查询到的lsu编号（lsu字段）和用户指定的landuse、soil查询符合条件的hru编号（id字段）
# 5. 最后将符合条件的hru编号排序,输出hru个数，和编号列表
import sqlite3
import os
import json
from typing import List, Optional, Tuple, Dict, Any


def query_hrus_from_db(db_path: str,
                       subbasins: List[int],
                       landuses: Optional[List[str]],
                       soils: Optional[List[str]]) -> Tuple[int, List[int]]:
    """
    从 QSWAT+ SQLite 数据库中查询符合条件的 HRU 编号。

    参数:
    db_path (str): SQLite 数据库文件 (.sqlite) 的路径。
    subbasins (List[int]): 必须的子流域编号列表, 例如 [30, 31]。
    landuses (Optional[List[str]]): 土地利用类型列表, 或 None (表示所有)。
    soils (Optional[List[str]]): 土壤类型列表, 或 None (表示所有)。

    返回:
    Tuple[int, List[int]]: (符合条件的HRU数量, 排序后的HRU编号列表)
    """

    if not subbasins:
        print("错误: 必须至少提供一个子流域编号 (subbasins)")
        return 0, []

    conn = None
    try:
        # 1. 连接数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 2. 构建基础查询语句 (连接 gis_lsus 和 gis_hrus)
        # T1 代表 gis_lsus (用于按 subbasin 过滤 lsu)
        # T2 代表 gis_hrus (用于按 lsu, landuse, soil 过滤 hru)
        base_query = """
            SELECT T2.id
            FROM gis_lsus AS T1
            JOIN gis_hrus AS T2 ON T1.id = T2.lsu
        """

        # 3. 动态构建 WHERE 子句和参数
        where_clauses = []
        params = []

        # 3.1 添加 Subbasin 条件 (必须)
        # 使用 IN (?, ?, ...) 语法安全地插入列表
        sub_placeholders = ','.join('?' for _ in subbasins)
        where_clauses.append(f"T1.subbasin IN ({sub_placeholders})")
        params.extend(subbasins)

        # 3.2 添加 Landuse 条件 (可选)
        if landuses:  # 列表不为 None 且不为空
            lu_placeholders = ','.join('?' for _ in landuses)
            where_clauses.append(f"T2.landuse IN ({lu_placeholders})")
            params.extend(landuses)

        # 3.3 添加 Soil 条件 (可选)
        if soils:  # 列表不为 None 且不为空
            soil_placeholders = ','.join('?' for _ in soils)
            where_clauses.append(f"T2.soil IN ({soil_placeholders})")
            params.extend(soils)

        # 4. 组合最终查询
        # 使用 "AND" 连接所有条件，并按 HRU ID 排序
        final_query = base_query + " WHERE " + " AND ".join(where_clauses)
        final_query += " ORDER BY T2.id ASC"

        # print(f"执行查询: {final_query}") # 用于调试
        # print(f"使用参数: {params}") # 用于调试

        # 5. 执行查询
        cursor.execute(final_query, params)

        # 提取结果 (fetchall 返回的是元组列表, e.g., [(101,), (102,)])
        results = cursor.fetchall()
        hru_ids = [row[0] for row in results]

        # 6. 返回结果
        return len(hru_ids), hru_ids

    except sqlite3.Error as e:
        print(f"数据库查询时发生错误: {e}")
        return 0, []
    except Exception as e:
        print(f"发生意外错误: {e}")
        return 0, []
    finally:
        # 确保数据库连接被关闭
        if conn:
            conn.close()


def run_and_save_combinations(db_path: str,
                              sub_dict: Dict[str, List[int]],
                              lu_dict: Dict[str, Optional[List[str]]],
                              soil_dict: Dict[str, Optional[List[str]]],
                              output_file_path: str):
    """
    遍历所有输入字典的组合，运行查询，并将结果保存为 JSON 文件。

    参数:
    db_path (str): SQLite 数据库文件路径。
    sub_dict (dict): 子流域组合字典, e.g., {"up1": [59], "up2": [15, 14]}
    lu_dict (dict): 土地利用组合字典, e.g., {"alllu": None, "agri": ["corn"]}
    soil_dict (dict): 土壤组合字典, e.g., {"allsoil": None}
    output_file_path (str): 输出 JSON 文件的路径。
    """

    print(f"--- 开始批量处理 HRU 组合 ---")

    # 检查数据库是否存在
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件未找到: {db_path}")
        return

    all_results = {}

    # 1. 使用三层嵌套循环遍历所有组合
    for sub_key, sub_values in sub_dict.items():
        for lu_key, lu_values in lu_dict.items():
            for soil_key, soil_values in soil_dict.items():
                # 2. 生成组合名称
                combination_name = f"{sub_key}_{lu_key}_{soil_key}"

                # 3. 执行查询
                print(f"正在处理: {combination_name}...")
                count, hru_list = query_hrus_from_db(
                        db_path,
                        sub_values,
                        lu_values,
                        soil_values
                )
                if count == 0:
                    continue

                # 4. 将结果存入主字典
                all_results[combination_name] = {
                    "count": count,
                    "hru_ids": hru_list,
                    # 也可在此存储输入参数用于追溯
                    "inputs": {
                        "subbasins": sub_values,
                        "landuses": lu_values,
                        "soils": soil_values
                    }
                }
                print(f" -> 完成: {combination_name} | 找到 {count} 个 HRU")

    # 5. 所有组合完成后，将结果写入 JSON 文件
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            # indent=4 让 JSON 文件格式化，更易读
            json.dump(all_results, f, indent=4)

        print(f"\n--- 批量处理完成 ---")
        print(f"所有组合已成功保存到: {output_file_path}")

    except IOError as e:
        print(f"写入 JSON 文件时出错: {e}")
    except Exception as e:
        print(f"发生意外错误: {e}")

if __name__ == "__main__":

    DEMO_DB_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite"
    OUTPUT_JSON_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships\hru_combinations.json"

    # 输入1：子流域列表集
    sub_inputs = {'up1': [59, 55, 56, 66, 67, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82],
                  'up2': [15, 1, 2, 3, 4, 6, 7, 8, 9, 13, 21, 26, 27],
                  'mid1': [42, 5, 10, 11, 12, 14, 16, 17, 18, 19, 20, 22, 23, 24, 25, 28, 29, 33, 34,
                           35, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
                           57, 58, 60, 61, 62, 63, 64, 65, 68, 83, 84, 85],
                  'down1': [31, 30, 32]}

    lu_inputs = {
        'alllu': None,
        'dairy': ['dairy', 'dairy1t1', 'dairy1t2', 'dairy1t3', 'dairy2t1', 'dairy2t2', 'dairy2t3'],
        'cashgrain': ['cashgrain', 'cashgraint1', 'cashgraint2', 'cashgraint3', 'cashgraint4'],
        'contcorn': ['contcorn', 'contcornt1', 'contcornt2', 'contcornt3'],
        'conthay': ['conthay'],
        'urban': ['urhd', 'urld'],
        'grass': ['gras'],
        'forest': ['frst'],
        'wet': ['watr', 'wetl']
    }
    lu_inputs['agri'] = lu_inputs['dairy'] + lu_inputs['cashgrain'] + lu_inputs['contcorn'] + lu_inputs['conthay']
    lu_inputs['nonagri'] = lu_inputs['urban'] + lu_inputs['grass'] + lu_inputs['forest'] + lu_inputs['wet']
    lu_inputs['nonurban'] = lu_inputs['agri'] + lu_inputs['grass'] + lu_inputs['forest'] + lu_inputs['wet']
    lu_inputs['nonagrinonurban'] = lu_inputs['grass'] + lu_inputs['forest'] + lu_inputs['wet']

    soil_inputs = {
        "allsoil": None
    }

    run_and_save_combinations(
            DEMO_DB_FILE,
            sub_inputs,
            lu_inputs,
            soil_inputs,
            OUTPUT_JSON_FILE
    )

    # print(f"\n--- 正在读取生成的 {OUTPUT_JSON_FILE} (用于验证) ---")
    # try:
    #     with open(OUTPUT_JSON_FILE, 'r') as f:
    #         data = json.load(f)
    #         print(json.dumps(data, indent=2))
    #
    #         key_to_check = "down1_alllu_allsoil"
    #         if key_to_check in data:
    #             print(f"\n[验证] {key_to_check}: {data[key_to_check]['hru_ids']}")
    # except FileNotFoundError:
    #     print("未找到 JSON 文件。")
    # except json.JSONDecodeError:
    #     print("JSON 文件格式错误。")
