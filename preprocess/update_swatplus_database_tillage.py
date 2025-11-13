# Prepare new tillage operations in a csv file using the following format:
#
# name,mix_eff,mix_dp,rough,ridge_ht,ridge_sp,description
# verticaltill,0.10000,50.00000,10.00000,0.00000,0.00000,Vertical tillage
#
# This script will import new tillage operations to "tillage_til" table of swatplus_datasets.sqlite
#
import sqlite3
import csv
import os

def create_database_and_table(db_file, tbl_name):
    """
    连接到SQLite数据库并创建所需的表和索引（如果它们不存在）。
    """
    # 连接到数据库，如果文件不存在，会自动创建
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 创建表的SQL语句，使用IF NOT EXISTS确保不会在表已存在时报错
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {tbl_name} (
        id          INTEGER      PRIMARY KEY,
        name        VARCHAR(255) NOT NULL,
        mix_eff     REAL         NOT NULL,
        mix_dp      REAL         NOT NULL,
        rough       REAL         NOT NULL,
        ridge_ht    REAL         NOT NULL,
        ridge_sp    REAL         NOT NULL,
        description TEXT
    );
    """

    # 创建唯一索引的SQL语句
    create_index_sql = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {tbl_name}_name_idx
    ON {tbl_name} (name);
    """

    # 执行SQL
    cursor.execute(create_table_sql)
    cursor.execute(create_index_sql)

    # 提交更改并关闭连接
    conn.commit()
    conn.close()
    print(f"数据库 '{db_file}' 和表 '{tbl_name}' 已准备就绪。")


def update_db_from_csv(db_file, tbl_name, csv_file):
    """
    从CSV文件读取数据并更新到SQLite数据库中。
    使用 "UPSERT" (INSERT ON CONFLICT) 逻辑。
    """
    if not os.path.exists(csv_file):
        print(f"错误: CSV文件 '{csv_file}' 不存在。")
        return

    # 连接到数据库
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # SQL "UPSERT" 语句
    # - ON CONFLICT(name): 指定唯一键列
    # - DO UPDATE SET: 如果'name'冲突（即已存在），则执行更新操作
    # - 'excluded.' 前缀引用了试图插入但失败的新行数据
    upsert_sql = f"""
    INSERT INTO {tbl_name} (name, mix_eff, mix_dp, rough, ridge_ht, ridge_sp, description)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(name) DO UPDATE SET
        mix_eff = excluded.mix_eff,
        mix_dp = excluded.mix_dp,
        rough = excluded.rough,
        ridge_ht = excluded.ridge_ht,
        ridge_sp = excluded.ridge_sp,
        description = excluded.description;
    """

    # 读取CSV文件并执行更新
    try:
        with open(csv_file, mode='r', encoding='utf-8') as csvfile:
            # DictReader可以让我们通过列名访问数据，更方便
            reader = csv.DictReader(csvfile)

            # 准备要插入/更新的数据列表
            data_to_upsert = []
            for row in reader:
                data_to_upsert.append((
                    row['name'],
                    float(row['mix_eff']),
                    float(row['mix_dp']),
                    float(row['rough']),
                    float(row['ridge_ht']),
                    float(row['ridge_sp']),
                    row['description']
                ))

            # executemany比在循环中逐条执行更高效
            if data_to_upsert:
                cursor.executemany(upsert_sql, data_to_upsert)
                conn.commit()
                print(f"成功处理了 {len(data_to_upsert)} 条记录。")
            else:
                print("CSV文件为空或只有表头，没有数据需要处理。")

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        conn.rollback()  # 如果出错，回滚所有更改
    finally:
        conn.close()  # 确保数据库连接总是被关闭


def view_data(db_file, tbl_name):
    """
    查询并打印表中的所有数据以供验证。
    """
    if not os.path.exists(db_file):
        print("数据库文件不存在，无法查看数据。")
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    print("\n--- 数据库当前数据 ---")
    cursor.execute(f"SELECT * FROM {tbl_name};")
    rows = cursor.fetchall()

    if not rows:
        print("表中没有数据。")
    else:
        for row in rows:
            print(row)

    conn.close()


# --- 主程序入口 ---
if __name__ == "__main__":
    db_file = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\swatplus_datasets.sqlite"
    tbl_name = "tillage_til"
    csv_file = r"D:\data_m\manitowoc\landcover\landusemanagement(LUM)\new_tillage.til.csv"

    # 1. 确保数据库和表结构存在
    create_database_and_table(db_file, tbl_name)

    # 2. 查看执行前的数据（第一次运行时表是空的）
    print("\n[运行前]")
    view_data(db_file, tbl_name)

    # 3. 从CSV更新数据
    print("\n[开始从CSV更新...]")
    update_db_from_csv(db_file, tbl_name, csv_file)

    # 4. 查看执行后的数据以验证结果
    print("\n[运行后]")
    view_data(db_file, tbl_name)
