# 用Python写程序读取新增的management schedule数据、并更新至swatplus_datasets.sqlite数据库。
# 示例文件详见已上传的new_management.sch.csv
# 该文件的格式为：
# name,numb_auto
# cashgrain_t1,"2,pl_hv_cashgrain_6yrs,tillage_1"
#
# name为management schedule名，如cashgrain_t1，numb_auto为该schedule包含的Decision table的数量和名称，如上述例子中包含2个，即pl_hv_cashgrain_6yrs和tillage_1
#
# 该数据需要更新至数据库中的2张表：management_sch和management_sch_auto，这两张表的结构如下：
#
# create table management_sch
# (
#     id   INTEGER      not null
#         primary key,
#     name VARCHAR(255) not null
# );
# create unique index management_sch_name
#     on management_sch (name);
#
# create table management_sch_auto
# (
#     id                INTEGER not null
#         primary key,
#     management_sch_id INTEGER not null
#         references management_sch
#             on delete cascade,
#     d_table_id        INTEGER not null
#         references d_table_dtl
#             on delete cascade,
#     plant1            VARCHAR(255),
#     plant2            VARCHAR(255)
# );
# create index management_sch_auto_d_table_id
#     on management_sch_auto (d_table_id);
# create index management_sch_auto_management_sch_id
#     on management_sch_auto (management_sch_id);
# create index management_sch_auto_plant1_id
#     on management_sch_auto (plant1_id);
# create index management_sch_auto_plant2_id
#     on management_sch_auto (plant2_id);
#
# 以刚才的数据为例，在management_sch表中新增1条记录，id为递增赋值的1，name为cashgrain_t1。
# 在management_sch_auto表中新增2条记录，id为递增赋值的1和2，management_sch_id为management_sch表中刚新增的1，d_table_id为d_table_dtl表中搜索name得到的id
#
# 以上就是我的功能需求，和数据与数据库表对应关系的详细解释。请理解并写出代码。
import sqlite3
import csv
import os

# --- 配置 ---
# DB_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\swatplus_datasets.sqlite"
DB_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite"
CSV_FILE = r"D:\data_m\manitowoc\landcover\landusemanagement(LUM)\new_management.sch.csv"


def create_tables(cursor):
    """
    如果表不存在，则在数据库中创建 management_sch 和 management_sch_auto 表。
    """
    print("正在检查并创建 management schedule 表结构...")

    # management_sch
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS management_sch
    (
        id   INTEGER      NOT NULL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    );
    """)
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS management_sch_name
        ON management_sch (name);
    """)

    # management_sch_auto
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS management_sch_auto
    (
        id                INTEGER NOT NULL PRIMARY KEY,
        management_sch_id INTEGER NOT NULL
            REFERENCES management_sch
                ON DELETE CASCADE,
        d_table_id        INTEGER NOT NULL
            REFERENCES d_table_dtl
                ON DELETE CASCADE,
        plant1            VARCHAR(255),
        plant2            VARCHAR(255)
    );
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS management_sch_auto_management_sch_id
        ON management_sch_auto (management_sch_id);
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS management_sch_auto_d_table_id
        ON management_sch_auto (d_table_id);
    """)
    print("数据库表结构准备就绪。")


def update_db_from_csv(cursor):
    """
    从CSV文件读取 management schedule 数据并更新到数据库。
    """
    print(f"开始从 '{CSV_FILE}' 读取数据...")

    try:
        with open(CSV_FILE, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                sch_name = row['name']
                numb_auto_raw = row['numb_auto']

                print(f"\n--- 正在处理 Schedule: {sch_name} ---")

                # 1. "Upsert" management_sch 表
                # 首先检查是否存在，如果存在则删除，级联删除会自动处理 management_sch_auto 中的旧记录
                cursor.execute("SELECT id FROM management_sch WHERE name = ?", (sch_name,))
                existing = cursor.fetchone()
                if existing:
                    print(f"'{sch_name}' 已存在。正在删除旧记录以便更新...")
                    cursor.execute("DELETE FROM management_sch WHERE id = ?", (existing[0],))

                # 插入新的 schedule 记录
                cursor.execute("INSERT INTO management_sch (name) VALUES (?)", (sch_name,))
                management_sch_id = cursor.lastrowid
                print(f"已插入/更新 '{sch_name}' 到 management_sch (ID: {management_sch_id})")

                # 2. 解析 numb_auto 并更新 management_sch_auto 表
                parts = [p.strip() for p in numb_auto_raw.split(',')]
                # 第一个元素是数量，后面的都是 decision table 的名称
                d_table_names = parts[1:]

                for d_table_name in d_table_names:
                    # 根据名称在 d_table_dtl 表中查找对应的 id
                    cursor.execute("SELECT id FROM d_table_dtl WHERE name = ?", (d_table_name,))
                    d_table_row = cursor.fetchone()

                    if d_table_row:
                        d_table_id = d_table_row[0]
                        # 插入到 management_sch_auto 表
                        cursor.execute(
                                """INSERT INTO management_sch_auto (management_sch_id, d_table_id)
                                   VALUES (?, ?)""",
                                (management_sch_id, d_table_id)
                        )
                    else:
                        # 如果在 d_table_dtl 中找不到对应的 decision table，则打印警告
                        print(
                            f"!! 警告: 在 d_table_dtl 表中未找到名为 '{d_table_name}' 的 decision table。已跳过此条目。")

                print(f"为 '{sch_name}' 成功关联了 {len(d_table_names)} 个 decision tables。")

    except FileNotFoundError:
        print(f"错误: CSV文件 '{CSV_FILE}' 未找到。")
        raise
    except Exception as e:
        print(f"处理CSV文件时发生错误: {e}")
        raise


def main():
    """
    主执行函数
    """
    if not os.path.exists(DB_FILE):
        print(f"错误: 数据库文件 '{DB_FILE}' 不存在。请先运行上一个脚本生成数据库。")
        return
    if not os.path.exists(CSV_FILE):
        print(f"错误: 输入文件 '{CSV_FILE}' 不存在。")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # 启用外键约束，这对于 ON DELETE CASCADE 至关重要
        cursor.execute("PRAGMA foreign_keys = ON;")

        # 1. 确保表结构存在
        create_tables(cursor)

        # 2. 从CSV文件更新数据
        update_db_from_csv(cursor)

        # 3. 提交事务
        conn.commit()
        print(f"\n成功！数据库 '{DB_FILE}' 已更新。")

    except Exception as e:
        print(f"\n操作失败: {e}")
        if conn:
            conn.rollback()
            print("数据库更改已回滚。")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()