# 用Python实现对SWAT+工程的sqlite数据库的对比、修改。
# manitowoc_test30mv4.sqlite是QSWAT+创建的工程数据库（简称proj_db），是基于基础数据库swatplus_datasets.sqlite（简称base_db）创建的，
# 但是我发现，base_db中的一些表，并没有正确地复制到proj_db。因此，现在需要根据base_db对proj_db进行修正。
#
# 两个数据库均有plant_ini和plant_ini_item表，但是proj_db中的plant_ini_item表数据有误。
# 现在需要读取base_db中plant_ini的name，确保name在proj_db中都存在，且description等内容不同的字段也要更新，两个数据库中的plant_ini中的id允许不同；
# plant_ini_item表中的plant_ini_id来自于plant_ini表，需要对应将base_db中数据更正到proj_db中。
#
# 接下来，在刚才的脚本中，再加上一个函数，用于更正landuse_lum表，更正的方法为：
# 读取proj_db的表中的每一条记录，按照name去base_db中查找对应记录，根据base_db表中的plant_com_id和mgt_id，分别在plant_ini和management_sch表中找到对应的name，
# 然后再在base_db的plant_ini和management_sch表中找到对应的id，以此更正base_db表数据。
#
# 请你复述对功能需求的理解，并写出代码。

import sqlite3
import pandas as pd
import os

# --- 配置 ---
# 基础数据库 (源)
BASE_DB_PATH = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\swatplus_datasets.sqlite'
# 工程数据库 (目标)
PROJ_DB_PATH = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite'


def sync_plant_community_data(base_conn, proj_conn):
    """
    根据基础数据库 (base_db) 来修正工程数据库 (proj_db) 中的
    plant_ini 和 plant_ini_item 表。
    只更新 proj_db 中已存在的记录。
    """
    # --------------------------------------------------------------------
    # 步骤 1: 更新 plant_ini 表
    # --------------------------------------------------------------------
    print("\n--- 步骤 1: 正在更新 'plant_ini' 表 ---")

    plant_ini_base_df = pd.read_sql_query("SELECT * FROM plant_ini", base_conn)
    proj_cursor = proj_conn.cursor()

    columns = plant_ini_base_df.columns.drop('id').tolist()

    update_count = 0
    skip_count = 0

    proj_cursor.execute('BEGIN TRANSACTION')
    for _, row in plant_ini_base_df.iterrows():
        # 检查 proj_db 中是否已存在同名记录
        proj_cursor.execute("SELECT id FROM plant_ini WHERE name = ?", (row['name'],))
        existing_record = proj_cursor.fetchone()

        # 只在记录已存在时才执行更新
        if existing_record:
            values = tuple(row.drop('id'))
            set_clause = ', '.join([f'"{col}" = ?' for col in columns])
            sql = f"UPDATE plant_ini SET {set_clause} WHERE name = ?"
            proj_cursor.execute(sql, values + (row['name'],))
            update_count += 1
        else:
            # 如果记录不存在，则跳过
            skip_count += 1

    proj_conn.commit()
    print(
        f"'plant_ini' 表同步完成。更新: {update_count} 条, 跳过 (不存在于项目中): {skip_count} 条。")

    # --------------------------------------------------------------------
    # 步骤 2: 重新生成 plant_ini_item 表 (仅针对项目中存在的 plant_ini)
    # --------------------------------------------------------------------
    print("\n--- 步骤 2: 正在同步 'plant_ini_item' 表 ---")

    # 建立ID映射
    base_map_df = pd.read_sql_query("SELECT id, name FROM plant_ini", base_conn)
    base_id_map = pd.Series(base_map_df.id.values, index=base_map_df.name).to_dict()
    proj_map_df = pd.read_sql_query("SELECT id, name FROM plant_ini", proj_conn)
    proj_id_map = pd.Series(proj_map_df.id.values, index=proj_map_df.name).to_dict()
    base_to_proj_id_map = {base_id: proj_id_map[name] for name, base_id in base_id_map.items() if
                           name in proj_id_map}
    print(f"ID 映射建立完成，共找到 {len(base_to_proj_id_map)} 个共同的植物群落。")

    # 同步 plant_ini_item
    plant_item_base_df = pd.read_sql_query("SELECT * FROM plant_ini_item", base_conn)

    # 只保留那些其 plant_ini_id 存在于映射表中的记录
    valid_base_ids = base_to_proj_id_map.keys()
    plant_item_to_sync_df = plant_item_base_df[
        plant_item_base_df['plant_ini_id'].isin(valid_base_ids)].copy()

    # 替换 'plant_ini_id'
    plant_item_to_sync_df['plant_ini_id'] = plant_item_to_sync_df['plant_ini_id'].map(
        base_to_proj_id_map)

    proj_cursor.execute('BEGIN TRANSACTION')
    proj_cursor.execute("DELETE FROM plant_ini_item")  # 清空旧数据
    plant_item_to_sync_df.to_sql('plant_ini_item', proj_conn, if_exists='append',
                                 index=False)  # 写入新数据
    proj_conn.commit()
    print(
        f"'plant_ini_item' 表同步完成。为项目中存在的植物群落共写入 {len(plant_item_to_sync_df)} 条新记录。")


def sync_landuse_lum(base_conn, proj_conn):
    """
    根据基础数据库 (base_db) 来修正工程数据库 (proj_db) 中的
    landuse_lum 表。
    只更新 proj_db 中已存在的记录。
    """
    print("\n--- 新增步骤: 正在更新 'landuse_lum' 表 ---")

    # --- 准备映射关系 ---
    base_plnt_map = pd.read_sql_query("SELECT id, name FROM plant_ini", base_conn).set_index('id')[
        'name'].to_dict()
    base_mgt_map = \
    pd.read_sql_query("SELECT id, name FROM management_sch", base_conn).set_index('id')[
        'name'].to_dict()
    proj_plnt_map = \
    pd.read_sql_query("SELECT id, name FROM plant_ini", proj_conn).set_index('name')['id'].to_dict()
    proj_mgt_map = \
    pd.read_sql_query("SELECT id, name FROM management_sch", proj_conn).set_index('name')[
        'id'].to_dict()
    print("'landuse_lum' 关联表的ID/Name映射关系已创建。")

    # --- 开始同步 ---
    landuse_base_df = pd.read_sql_query("SELECT * FROM landuse_lum", base_conn)
    proj_cursor = proj_conn.cursor()
    columns = landuse_base_df.columns.drop('id').tolist()

    update_count = 0
    skip_count = 0

    proj_cursor.execute('BEGIN TRANSACTION')

    for _, row in landuse_base_df.iterrows():
        # 检查 proj_db 中是否已存在同名记录
        proj_cursor.execute("SELECT id FROM landuse_lum WHERE name = ?", (row['name'],))
        existing_record = proj_cursor.fetchone()

        # 只在记录已存在时才执行更新
        if existing_record:
            base_plnt_id = row['plnt_com_id']
            base_mgt_id = row['mgt_id']

            plnt_name = base_plnt_map.get(base_plnt_id)
            mgt_name = base_mgt_map.get(base_mgt_id)

            proj_plnt_id = proj_plnt_map.get(plnt_name)
            proj_mgt_id = proj_mgt_map.get(mgt_name)

            if plnt_name and proj_plnt_id is None:
                print(
                    f"  - 警告: 在 proj_db.plant_ini 中找不到名为 '{plnt_name}' 的记录。跳过 landuse '{row['name']}' 的更新。")
                skip_count += 1
                continue
            if mgt_name and proj_mgt_id is None:
                print(
                    f"  - 警告: 在 proj_db.management_sch 中找不到名为 '{mgt_name}' 的记录。跳过 landuse '{row['name']}' 的更新。")
                skip_count += 1
                continue

            new_row = row.copy()
            new_row['plnt_com_id'] = proj_plnt_id
            new_row['mgt_id'] = proj_mgt_id

            values = tuple(new_row.drop('id'))

            set_clause = ', '.join([f'"{col}" = ?' for col in columns])
            sql = f"UPDATE landuse_lum SET {set_clause} WHERE name = ?"
            proj_cursor.execute(sql, values + (row['name'],))
            update_count += 1
        else:
            # 如果记录不存在，则跳过
            skip_count += 1

    proj_conn.commit()
    print(
        f"'landuse_lum' 表同步完成。更新: {update_count} 条, 跳过 (不存在于项目中): {skip_count} 条。")


def main():
    """
    主执行函数
    """
    if not os.path.exists(BASE_DB_PATH):
        print(f"错误: 基础数据库文件未找到: '{BASE_DB_PATH}'")
        return
    if not os.path.exists(PROJ_DB_PATH):
        print(f"错误: 工程数据库文件未找到: '{PROJ_DB_PATH}'")
        return

    print(f"开始从 '{BASE_DB_PATH}' 更新数据到 '{PROJ_DB_PATH}'...")

    base_conn = None
    proj_conn = None

    try:
        base_conn = sqlite3.connect(BASE_DB_PATH)
        proj_conn = sqlite3.connect(PROJ_DB_PATH)

        sync_plant_community_data(base_conn, proj_conn)
        sync_landuse_lum(base_conn, proj_conn)

        print("\n所有更新操作成功完成！")

    except sqlite3.Error as e:
        print(f"\n发生数据库错误: {e}")
        if proj_conn:
            proj_conn.rollback()
            print("操作已回滚，工程数据库未做任何更改。")
    except Exception as e:
        print(f"\n发生未知错误: {e}")
        if proj_conn:
            proj_conn.rollback()
            print("操作已回滚。")
    finally:
        if base_conn:
            base_conn.close()
        if proj_conn:
            proj_conn.close()
        print("\n数据库连接已关闭。")


if __name__ == '__main__':
    main()