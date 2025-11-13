# 用Python写程序读取新增的landuse.mgt数据、并更新至swatplus_datasets.sqlite数据库的landuse_lum表。
# 示例文件详见已上传的new_landuse.lum.csv
# 该文件的头信息是name,ref_name,plnt_com,mgt
# 以第一条数据为例：cashgraint1_lum,corn120_lum,cashgraint1_comm,cashgrain_t1
# cashgraint1_lum是新增的lum数据的name，name的前缀cashgraint1必须能存在于plants_plt表中；
# corn120_lum是新增lum数据的参考数据（ref_name），即在导入数据库之前，先读取ref_name的数据，然后把name改成cashgraint1_lum；
# cashgraint1_comm是该lum数据对应的植物群落（plant community； plnt_comm）,必须存在于plant_ini表中；
# cashgrain_t1则是该lum数据对应的management schedule （mgt），必须存在于management_sch表中，并把该表中的id赋值给landuse_mgt数据的mgt_id；
# 以上是new_landuse.lum.csv文件目前已有的内容，根据landuse_lum表的结构，以后还会新增cn2、cons_prac、ov_mann、tile、sep、vfs、grww和bmp这几列属性，
# 分别对应landuse_mgt表中的cn2_id、cons_prac_id、ov_mann_id、tile_id、sep_id、vfs_id、grww_id和bmp_id，
# 分别需要去这些表中查找id：cntable_lum、cons_prac_lum、ovn_table_lum、tiledrain_str、septic_str、filterstrip_str、grassedww_str和bmpuser_str。
#
# 以上介绍完了输入数据，接下来是landuse_lum表的结构：
#
# create table landuse_lum
# (
#     id           INTEGER      not null
#         primary key,
#     name         VARCHAR(255) not null,
#     cal_group    VARCHAR(255),
#     plnt_com_id  INTEGER
#                               references plant_ini
#                                   on delete set null,
#     mgt_id       INTEGER
#                               references management_sch
#                                   on delete set null,
#     cn2_id       INTEGER
#                               references cntable_lum
#                                   on delete set null,
#     cons_prac_id INTEGER
#                               references cons_prac_lum
#                                   on delete set null,
#     urban_id     INTEGER
#                               references urban_urb
#                                   on delete set null,
#     urb_ro       VARCHAR(255),
#     ov_mann_id   INTEGER
#                               references ovn_table_lum
#                                   on delete set null,
#     tile_id      INTEGER
#                               references tiledrain_str
#                                   on delete set null,
#     sep_id       INTEGER
#                               references septic_str
#                                   on delete set null,
#     vfs_id       INTEGER
#                               references filterstrip_str
#                                   on delete set null,
#     grww_id      INTEGER
#                               references grassedww_str
#                                   on delete set null,
#     bmp_id       INTEGER
#                               references bmpuser_str
#                                   on delete set null,
#     description  TEXT
# );
#
# create index landuse_lum_bmp_id
#     on landuse_lum (bmp_id);
#
# create index landuse_lum_cn2_id
#     on landuse_lum (cn2_id);
#
# create index landuse_lum_cons_prac_id
#     on landuse_lum (cons_prac_id);
#
# create index landuse_lum_grww_id
#     on landuse_lum (grww_id);
#
# create index landuse_lum_mgt_id
#     on landuse_lum (mgt_id);
#
# create unique index landuse_lum_name
#     on landuse_lum (name);
#
# create index landuse_lum_ov_mann_id
#     on landuse_lum (ov_mann_id);
#
# create index landuse_lum_plnt_com_id
#     on landuse_lum (plnt_com_id);
#
# create index landuse_lum_sep_id
#     on landuse_lum (sep_id);
#
# create index landuse_lum_tile_id
#     on landuse_lum (tile_id);
#
# create index landuse_lum_urban_id
#     on landuse_lum (urban_id);
#
# create index landuse_lum_vfs_id
#     on landuse_lum (vfs_id);
#
# 以上就是我的功能需求，和数据与数据库表对应关系的详细解释。请理解并写出代码。

import sqlite3
import pandas as pd
import os


DB_FILE_PATH = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\swatplus_datasets.sqlite'
# DB_FILE_PATH = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite"
CSV_FILE_PATH = r'D:\data_m\manitowoc\landcover\landusemanagement(LUM)\new_landuse.lum.csv'

# 定义CSV列名与数据库表和ID列的映射关系
# 格式: 'csv_column_name': {'table': 'db_table_name', 'id_col': 'foreign_key_id_col_in_landuse_lum'}
COLUMN_TO_TABLE_MAP = {
    'plnt_com': {'table': 'plant_ini', 'id_col': 'plnt_com_id'},
    'mgt': {'table': 'management_sch', 'id_col': 'mgt_id'},
    'cn2': {'table': 'cntable_lum', 'id_col': 'cn2_id'},
    'cons_prac': {'table': 'cons_prac_lum', 'id_col': 'cons_prac_id'},
    'ov_mann': {'table': 'ovn_table_lum', 'id_col': 'ov_mann_id'},
    'tile': {'table': 'tiledrain_str', 'id_col': 'tile_id'},
    'sep': {'table': 'septic_str', 'id_col': 'sep_id'},
    'vfs': {'table': 'filterstrip_str', 'id_col': 'vfs_id'},
    'grww': {'table': 'grassedww_str', 'id_col': 'grww_id'},
    'bmp': {'table': 'bmpuser_str', 'id_col': 'bmp_id'}
}


def get_id_by_name(cursor, table, name):
    """根据名称从指定表中查询ID"""
    try:
        cursor.execute(f"SELECT id FROM {table} WHERE name = ?", (name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"  - 错误: 在表 '{table}' 中未找到名称为 '{name}' 的记录。")
            return None
    except sqlite3.Error as e:
        print(f"  - 数据库查询错误 (表: {table}, 名称: {name}): {e}")
        return None


def update_landuse_from_csv(csv_path, db_path):
    """
    从CSV文件读取新的土地利用数据，并更新到SWAT+ SQLite数据库中。
    """
    if not os.path.exists(csv_path):
        print(f"错误: CSV文件未找到: {csv_path}")
        return
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件未找到: {db_path}")
        return

    print(f"开始处理文件 '{csv_path}' 并更新数据库 '{db_path}'...")

    try:
        # 使用pandas读取CSV
        df = pd.read_csv(csv_path)
        # 将空值替换为None，以便后续处理
        df = df.where(pd.notna(df), None)
    except FileNotFoundError:
        print(f"错误: 无法找到CSV文件 at {csv_path}")
        return
    except Exception as e:
        print(f"读取CSV文件时出错: {e}")
        return

    conn = None
    try:
        # 连接到SQLite数据库
        conn = sqlite3.connect(db_path)
        # 设置row_factory以字典形式访问列，便于操作
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 获取 landuse_lum 表的列名，用于动态构建INSERT语句
        cursor.execute("PRAGMA table_info(landuse_lum)")
        landuse_lum_columns = [row[1] for row in cursor.fetchall()]

        success_count = 0
        fail_count = 0

        # 遍历CSV文件的每一行
        for index, row in df.iterrows():
            print(f"\n正在处理第 {index + 1} 行: name = {row['name']}")

            # --- 1. 获取基本信息 ---
            new_name = row['name']
            ref_name = row['ref_name']

            if not new_name or not ref_name:
                print("  - 错误: 'name' 或 'ref_name' 列为空，跳过此行。")
                fail_count += 1
                continue

            # --- 2. 获取参考数据作为模板 ---
            cursor.execute("SELECT * FROM landuse_lum WHERE name = ?", (ref_name,))
            ref_data = cursor.fetchone()

            if ref_data is None:
                print(
                    f"  - 错误: 在 'landuse_lum' 表中未找到参考数据 'ref_name' = '{ref_name}'。跳过此行。")
                fail_count += 1
                continue

            # 将模板数据转换为可修改的字典
            new_record = dict(ref_data)

            # --- 3. 更新字段 ---
            # 更新 name
            new_record['name'] = new_name

            # 用于标记本行是否处理成功
            is_row_valid = True

            # 遍历所有需要通过名称转换ID的列
            for csv_col, db_info in COLUMN_TO_TABLE_MAP.items():
                # 检查CSV中是否存在该列，并且值不为空
                if csv_col in row and row[csv_col] is not None:
                    value_name = row[csv_col]
                    table_name = db_info['table']
                    id_col_name = db_info['id_col']

                    print(f"  - 正在查询 '{value_name}' 在表 '{table_name}' 中的 ID...")
                    found_id = get_id_by_name(cursor, table_name, value_name)

                    if found_id is not None:
                        new_record[id_col_name] = found_id
                        print(f"    ... 成功，ID = {found_id}")
                    else:
                        is_row_valid = False
                        break  # 如果任何一个ID查找失败，则终止处理本行

            if not is_row_valid:
                print(f"  - 由于查找ID失败，跳过 '{new_name}' 的处理。")
                fail_count += 1
                continue

            # --- 4. 准备并执行数据库插入/更新操作 ---
            # 过滤掉不存在于目标表中的键（如'id'）
            final_data = {k: v for k, v in new_record.items() if
                          k in landuse_lum_columns and k != 'id'}

            columns_str = ', '.join(final_data.keys())
            placeholders_str = ', '.join(['?'] * len(final_data))
            values = tuple(final_data.values())

            # 使用 INSERT OR REPLACE 来插入或更新记录
            # 这会根据UNIQUE约束(name)来判断是插入还是替换
            sql_command = f"INSERT OR REPLACE INTO landuse_lum ({columns_str}) VALUES ({placeholders_str})"

            try:
                cursor.execute(sql_command, values)
                print(f"  - 成功将 '{new_name}' 写入数据库。")
                success_count += 1
            except sqlite3.Error as e:
                print(f"  - 数据库写入错误 for '{new_name}': {e}")
                fail_count += 1

        # 提交所有更改
        conn.commit()
        print(f"\n--- 处理完成 ---")
        print(f"成功处理: {success_count} 条记录")
        print(f"失败/跳过: {fail_count} 条记录")

    except sqlite3.Error as e:
        print(f"发生数据库错误: {e}")
        if conn:
            conn.rollback()  # 如果出错则回滚
    except Exception as e:
        print(f"发生未知错误: {e}")
    finally:
        if conn:
            conn.close()  # 确保数据库连接被关闭
            print("数据库连接已关闭。")


if __name__ == '__main__':
    # 运行主函数
    update_landuse_from_csv(CSV_FILE_PATH, DB_FILE_PATH)