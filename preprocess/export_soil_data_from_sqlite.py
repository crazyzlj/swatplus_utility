import sqlite3
import pandas as pd
import numpy as np


def export_swat_soils(sqlite_path, output_excel):
    # 1. 建立数据库连接
    conn = sqlite3.connect(sqlite_path)

    # 2. 读取基础表 soils_sol
    query_sol = "SELECT id, name as MUID, hyd_grp as HYDGRP, dp_tot as SOL_ZMX, " \
                "anion_excl as ANION_EXCL, perc_crk as SOL_CRK, texture as TEXTURE " \
                "FROM soils_sol"
    df_sol = pd.read_sql_query(query_sol, conn)

    # 3. 读取土壤层表 soils_sol_layer
    query_layer = "SELECT * FROM soils_sol_layer"
    df_layer = pd.read_sql_query(query_layer, conn)

    # 4. 统计每个土壤的层数 (NLAYERS)
    df_nlayers = df_layer.groupby('soil_id').size().reset_index(name='NLAYERS')
    df_sol = pd.merge(df_sol, df_nlayers, left_on='id', right_on='soil_id', how='left')
    df_sol['NLAYERS'] = df_sol['NLAYERS'].fillna(0).astype(int)

    # 5. 定义属性映射关系 (数据库字段: Excel前缀)
    attr_map = {
        'dp': 'SOL_Z', 'bd': 'SOL_BD', 'awc': 'SOL_AWC', 'soil_k': 'SOL_K',
        'carbon': 'SOL_CBN', 'clay': 'CLAY', 'silt': 'SILT', 'sand': 'SAND',
        'rock': 'ROCK', 'alb': 'SOL_ALB', 'usle_k': 'USLE_K', 'ec': 'SOL_EC',
        'caco3': 'SOL_CAL', 'ph': 'SOL_PH'
    }

    # 6. 处理层数据 (1 to 10层)
    # 创建一个空的DataFrame用于存储展开后的层属性
    layers_expanded = pd.DataFrame(index=df_sol['id'])

    for layer_idx in range(1, 11):
        # 筛选当前层的数据
        current_layer_data = df_layer[df_layer['layer_num'] == layer_idx]
        current_layer_data = current_layer_data.set_index('soil_id')

        for db_col, excel_prefix in attr_map.items():
            col_name = f"{excel_prefix}{layer_idx}"
            # 将当前属性映射到对应的 soil_id 上，缺失层自动补 0
            layers_expanded[col_name] = current_layer_data[db_col]

    # 填充空值为 0
    layers_expanded = layers_expanded.fillna(0)

    # 7. 合并主表与展开后的层表
    # 按照要求的表头顺序组织最终 DataFrame
    final_cols = ['MUID', 'NLAYERS', 'HYDGRP', 'SOL_ZMX', 'ANION_EXCL', 'SOL_CRK', 'TEXTURE']
    # 动态添加层属性列
    for i in range(1, 11):
        for prefix in attr_map.values():
            final_cols.append(f"{prefix}{i}")

    df_final = pd.merge(df_sol, layers_expanded, left_on='id', right_index=True, how='left')

    # 确保列顺序正确，只取需要的列
    df_output = df_final[final_cols]

    # 8. 导出为 Excel
    df_output.to_excel(output_excel, index=False)

    conn.close()
    print(f"Success: Soil data exported to {output_excel}")


# 使用示例
if __name__ == "__main__":
    db_file = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite"  # 替换为你的SQLite文件路径
    output_file = r"D:\data_m\manitowoc\soil\soils_manitowoc.xlsx"
    export_swat_soils(db_file, output_file)