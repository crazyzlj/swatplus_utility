# 用Python写程序实现从一个sqlite数据库中复制表到另一个sqlite数据库中。
# 需要复制的表包括：
# tillage_till
# d_table_dtl
# d_table_dtl_act
# d_table_dtl_act_out
# d_table_dtl_con
# d_table_dtl_cond_alt
# management_sch
# management_sch_auto
# plants_plt
# plant_ini
# plant_ini_item
# landuse_lum
#
# 如果目标数据库中该表已经存在，则将源数据库中的数据更新至目标数据库中。
import sqlite3
import pandas as pd
import os

# 源数据库文件路径
SOURCE_DB_PATH = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\swatplus_datasets.sqlite'
# 目标数据库文件路径
DESTINATION_DB_PATH = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite'

# 需要复制的表名列表
TABLES_TO_COPY = [
    'tillage_til',
    'd_table_dtl',
    'd_table_dtl_act',
    'd_table_dtl_act_out',
    'd_table_dtl_cond',
    'd_table_dtl_cond_alt',
    'management_sch',
    'management_sch_auto',
    'plants_plt',
    'plant_ini',
    'plant_ini_item',
    'landuse_lum'
]


def copy_tables(source_db, dest_db, table_list):
    """
    将指定的表从源SQLite数据库复制到目标SQLite数据库。
    如果目标表已存在，则先删除再创建（替换）。

    Args:
        source_db (str): 源数据库的文件路径。
        dest_db (str): 目标数据库的文件路径。
        table_list (list): 需要复制的表名字符串列表。
    """
    # 检查源数据库文件是否存在
    if not os.path.exists(source_db):
        print(f"错误: 源数据库文件未找到: '{source_db}'")
        return

    print(f"开始从 '{source_db}' 复制数据到 '{dest_db}'...")

    src_conn = None
    dest_conn = None

    try:
        # 连接到源数据库和目标数据库
        src_conn = sqlite3.connect(source_db)
        dest_conn = sqlite3.connect(dest_db)

        # 检查源数据库中是否存在所有指定的表
        cursor = src_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = [row[0] for row in cursor.fetchall()]

        missing_tables = [tbl for tbl in table_list if tbl not in existing_tables]
        if missing_tables:
            print(f"警告: 源数据库中缺少以下表，将跳过它们: {', '.join(missing_tables)}")
            table_list = [tbl for tbl in table_list if tbl in existing_tables]

        if not table_list:
            print("没有找到任何可复制的表，操作终止。")
            return

        # 开始事务
        dest_conn.execute('BEGIN TRANSACTION')

        # 遍历要复制的每个表
        for table_name in table_list:
            print(f"  - 正在处理表: '{table_name}'...")

            # 1. 从源数据库读取整个表到 pandas DataFrame
            # 使用 chunksize 可以处理非常大的表，防止内存溢出，但对于一般大小的表，直接读取更简单
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", src_conn)

            # 2. 将 DataFrame 写入目标数据库
            # if_exists='replace' 会在写入前删除已存在的表，然后创建新表并插入数据
            # 这确保了结构和数据都与源表完全一致
            df.to_sql(table_name, dest_conn, if_exists='replace', index=False)

            print(f"    '{table_name}' 已成功复制并更新。共 {len(df)} 条记录。")

        # 提交事务，保存所有更改
        dest_conn.commit()
        print("\n所有指定的表都已成功复制！")

    except sqlite3.Error as e:
        print(f"\n发生数据库错误: {e}")
        if dest_conn:
            # 如果发生错误，回滚所有更改
            dest_conn.rollback()
            print("操作已回滚，目标数据库未做任何更改。")
    except pd.errors.DatabaseError as e:
        print(f"\nPandas读取数据库时发生错误: {e}")
        if dest_conn:
            dest_conn.rollback()
            print("操作已回滚。")
    except Exception as e:
        print(f"\n发生未知错误: {e}")
        if dest_conn:
            dest_conn.rollback()
            print("操作已回滚。")
    finally:
        # 确保数据库连接被关闭
        if src_conn:
            src_conn.close()
        if dest_conn:
            dest_conn.close()
        print("数据库连接已关闭。")


if __name__ == '__main__':
    # 运行主函数
    # 在运行前，请确保 SOURCE_DB_PATH 和 DESTINATION_DB_PATH 已正确设置
    # 如果目标数据库文件不存在，脚本会自动创建它
    copy_tables(SOURCE_DB_PATH, DESTINATION_DB_PATH, TABLES_TO_COPY)