# 用Python实现将读取SWAT+的Decision Table格式的文件、并更新至swatplus_datasets.sqlite数据库。
# 示例文件详见已上传的new_lum.2025-10-08v2.dtl
# Decision Table格式的文件均存在数据库中的5张表里：d_table_dtl, d_table_dtl_act, d_table_dtl_act_out, d_table_dtl_con, d_table_dtl_cond_alt
# 接下来我会先介绍各个表的结构，然后会以一个例子说明*.dtl文件中的数据和数据库中5张表的对应关系，你需要理解之后，写代码实现功能。
#
# d_table_dtl的结构：
# create table d_table_dtl
# (
#     id          INTEGER      not null
#         primary key,
#     name        VARCHAR(255) not null,
#     file_name   VARCHAR(255) not null,
#     description VARCHAR(255)
# );
# create unique index d_table_dtl_name
#     on d_table_dtl (name);
#
# d_table_dtl_act的结构：
# create table d_table_dtl_act
# (
#     id         INTEGER      not null
#         primary key,
#     d_table_id INTEGER      not null
#         references d_table_dtl
#             on delete cascade,
#     act_typ    VARCHAR(255) not null,
#     obj        VARCHAR(255) not null,
#     obj_num    INTEGER      not null,
#     name       VARCHAR(255) not null,
#     option     VARCHAR(255) not null,
#     const      REAL         not null,
#     const2     REAL         not null,
#     fp         VARCHAR(255) not null
# );
#
# d_table_dtl_act_out的结构：
# create table d_table_dtl_act_out
# (
#     id      INTEGER not null
#         primary key,
#     act_id  INTEGER not null
#         references d_table_dtl_act
#             on delete cascade,
#     outcome INTEGER not null
# );
# create index d_table_dtl_act_out_act_id
#     on d_table_dtl_act_out (act_id);
#
# d_table_dtl_con的结构：
# create table d_table_dtl_cond
# (
#     id          INTEGER      not null
#         primary key,
#     d_table_id  INTEGER      not null
#         references d_table_dtl
#             on delete cascade,
#     var         VARCHAR(255) not null,
#     obj         VARCHAR(255) not null,
#     obj_num     INTEGER      not null,
#     lim_var     VARCHAR(255) not null,
#     lim_op      VARCHAR(255) not null,
#     lim_const   REAL         not null,
#     description VARCHAR(255)
# );
# create index d_table_dtl_cond_d_table_id
#    on d_table_dtl_cond (d_table_id);
#
# d_table_dtl_cond_alt的结构：
# create table d_table_dtl_cond_alt
# (
#     id      INTEGER      not null
#         primary key,
#     cond_id INTEGER      not null
#         references d_table_dtl_cond
#             on delete cascade,
#     alt     VARCHAR(255) not null
# );
# create index d_table_dtl_cond_alt_cond_id
#     on d_table_dtl_cond_alt (cond_id);
#
# new_lum.2025-10-08v2.dtl文件的第1行是本文件所有数据的来源文件名“lum.dtl”, :之后的内容忽略，该文件中!之后的内容通常会作为description字段的内容
# 第2行是一个整数，代表本文件有多少个decision table配置
# 从遇到第一个name开始，便是一个decision table的开始
# 第1行是表头，即name                     conds      alts      acts       !Tillage 3: Vertical tillage before planting in spring and none tillage in fall
# 第2行是该decision table的总体信息，如tillage_3_dairys1s2          2         1         1，表示name是tillage_3_dairys1s2，包含2个conditions（条件判断项），1个alternatives，1个action
#   d_table_dtl表中则会新增一条记录，id是递增赋值的58，name是tillage_3_dairys1s2，file_name是lum.dtl，description是Tillage 3: Vertical tillage before planting in spring and none tillage in fall
# 第3行是conditions的表头，即var                        obj   obj_num           lim_var            lim_op     lim_const      alt1 ，对应的是d_table_dtl_cond和d_table_dtl_cond_alt两张表
# 第4-5行是2个conditions行，比如phu_base0                  hru         0              null                 -       0.12000         >       !vertical tillage just before planting if base0 heat units > this value
#   d_table_dtl_cond表中则会新增一条记录，id是递增复制的596，d_table_id是d_table_dtl刚新增的58，var是phu_base0，obj是hru，obj_num是0，lim_var是null，lim_op是-，lim_const是0.12000
#   d_table_dtl_cond_alt表中则会新增1条记录（alts为几，则新增几条记录），id为递增赋值的3976，cond_id为d_table_dtl_cond中刚新增的596，alt为>
# 第6行是action的表头，即act_typ                    obj   obj_num              name            option         const        const2                fp  outcome，对应的是table d_table_dtl_act和table d_table_dtl_act_out两张表
# 第7行是本decision table包含的唯一一个action，即till                       hru         0     vertical_till      verticaltill       0.00000       1.00000              null  y
#   table d_table_dtl_act表中则会新增一条记录，id是递增的375，d_table_id是58，act_typ是till，obj是hru，obj_num是0，name是vertical_till，option是verticaltill，const是0.0000，const2是1.0000，fp是null
#   table d_table_dtl_act_out表中则会新增一条记录，id是递增的2397，act_id是table d_table_dtl_act中刚新增的375，outcome是1（即1代表y，0代表n），alts为几，则新增几条记录
#
# 以上就是我的功能需求，和数据与数据库表对应关系的详细解释。请理解并写出代码。
import sqlite3
import os
import re

# --- 配置 ---
DB_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\swatplus_datasets.sqlite"
# DB_FILE = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite"
DTL_FILE = r"D:\data_m\manitowoc\landcover\landusemanagement(LUM)\new_lum.2025-10-08v2.dtl"


def create_tables(cursor):
    """
    如果表不存在，则在数据库中创建所有需要的表和索引。
    """
    print("正在检查并创建数据库表结构...")

    # d_table_dtl
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS d_table_dtl
    (
        id          INTEGER      NOT NULL PRIMARY KEY,
        name        VARCHAR(255) NOT NULL,
        file_name   VARCHAR(255) NOT NULL,
        description VARCHAR(255)
    );
    """)
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS d_table_dtl_name
        ON d_table_dtl (name);
    """)

    # d_table_dtl_cond
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS d_table_dtl_cond
    (
        id          INTEGER      NOT NULL PRIMARY KEY,
        d_table_id  INTEGER      NOT NULL
            REFERENCES d_table_dtl
                ON DELETE CASCADE,
        var         VARCHAR(255) NOT NULL,
        obj         VARCHAR(255) NOT NULL,
        obj_num     INTEGER      NOT NULL,
        lim_var     VARCHAR(255) NOT NULL,
        lim_op      VARCHAR(255) NOT NULL,
        lim_const   REAL         NOT NULL,
        description VARCHAR(255)
    );
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS d_table_dtl_cond_d_table_id
       ON d_table_dtl_cond (d_table_id);
    """)

    # d_table_dtl_cond_alt
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS d_table_dtl_cond_alt
    (
        id      INTEGER      NOT NULL PRIMARY KEY,
        cond_id INTEGER      NOT NULL
            REFERENCES d_table_dtl_cond
                ON DELETE CASCADE,
        alt     VARCHAR(255) NOT NULL
    );
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS d_table_dtl_cond_alt_cond_id
        ON d_table_dtl_cond_alt (cond_id);
    """)

    # d_table_dtl_act
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS d_table_dtl_act
    (
        id         INTEGER      NOT NULL PRIMARY KEY,
        d_table_id INTEGER      NOT NULL
            REFERENCES d_table_dtl
                ON DELETE CASCADE,
        act_typ    VARCHAR(255) NOT NULL,
        obj        VARCHAR(255) NOT NULL,
        obj_num    INTEGER      NOT NULL,
        name       VARCHAR(255) NOT NULL,
        option     VARCHAR(255) NOT NULL,
        const      REAL         NOT NULL,
        const2     REAL         NOT NULL,
        fp         VARCHAR(255) NOT NULL
    );
    """)

    # d_table_dtl_act_out
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS d_table_dtl_act_out
    (
        id      INTEGER NOT NULL PRIMARY KEY,
        act_id  INTEGER NOT NULL
            REFERENCES d_table_dtl_act
                ON DELETE CASCADE,
        outcome INTEGER NOT NULL
    );
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS d_table_dtl_act_out_act_id
        ON d_table_dtl_act_out (act_id);
    """)
    print("数据库表结构准备就绪。")


def parse_and_update(cursor, dtl_filepath):
    """
    解析.dtl文件并更新数据库。
    """
    print(f"开始解析文件: {dtl_filepath}")

    with open(dtl_filepath, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]

    file_name = lines[0].split(':')[0].strip()
    line_iter = iter(lines[2:])

    while True:
        try:
            line = next(line_iter)

            if line.lower().startswith('name'):
                # <<< 修正开始 >>>
                # 主描述信息在表头行（当前的 'line' 变量）
                desc_match = re.search(r'!\s*(.*)', line)
                description = desc_match.group(1).strip() if desc_match else None

                # Decision Table 的数据在下一行
                dt_def_line = next(line_iter)
                # <<< 修正结束 >>>

                # 1. 解析Decision Table主信息
                data_str = re.sub(r'!\s*.*', '', dt_def_line).strip()
                parts = data_str.split()

                dt_name, num_conds, num_alts, num_acts = parts[0], int(parts[1]), int(
                        parts[2]), int(parts[3])

                print(f"\n--- 正在处理 Decision Table: {dt_name} ---")

                # 2. 更新 d_table_dtl (Upsert)
                cursor.execute("SELECT id FROM d_table_dtl WHERE name = ?", (dt_name,))
                existing = cursor.fetchone()
                if existing:
                    print(f"'{dt_name}' 已存在。正在删除旧记录以便更新...")
                    cursor.execute("DELETE FROM d_table_dtl WHERE id = ?", (existing[0],))

                cursor.execute(
                        "INSERT INTO d_table_dtl (name, file_name, description) VALUES (?, ?, ?)",
                        (dt_name, file_name, description)  # 使用从表头行提取的description
                )
                d_table_id = cursor.lastrowid
                print(f"已插入/更新 '{dt_name}' 到 d_table_dtl (ID: {d_table_id})")

                # 3. 解析 Conditions (此部分逻辑原本就是正确的)
                next(line_iter)
                for _ in range(num_conds):
                    cond_line = next(line_iter)
                    cond_desc_match = re.search(r'!\s*(.*)', cond_line)
                    cond_description = cond_desc_match.group(1).strip() if cond_desc_match else None
                    cond_data_str = re.sub(r'!\s*.*', '', cond_line).strip()
                    cond_parts = cond_data_str.split()

                    var, obj, obj_num, lim_var, lim_op, lim_const = cond_parts[0:6]
                    alts = cond_parts[6:]

                    cursor.execute(
                            """INSERT INTO d_table_dtl_cond 
                               (d_table_id, var, obj, obj_num, lim_var, lim_op, lim_const, description) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (d_table_id, var, obj, int(obj_num), lim_var, lim_op, float(lim_const),
                             cond_description)
                    )
                    cond_id = cursor.lastrowid

                    for alt_val in alts:
                        cursor.execute(
                                "INSERT INTO d_table_dtl_cond_alt (cond_id, alt) VALUES (?, ?)",
                                (cond_id, alt_val)
                        )
                print(f"已为 '{dt_name}' 插入 {num_conds} 条条件记录。")

                # 4. 解析 Actions
                next(line_iter)
                for _ in range(num_acts):
                    act_line = next(line_iter)
                    act_parts = act_line.split()

                    act_typ, obj, obj_num, name, option, const, const2, fp = act_parts[0:8]
                    outcomes = act_parts[8:]

                    cursor.execute(
                            """INSERT INTO d_table_dtl_act
                               (d_table_id, act_typ, obj, obj_num, name, option, const, const2, fp)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (d_table_id, act_typ, obj, int(obj_num), name, option, float(const),
                             float(const2), fp)
                    )
                    act_id = cursor.lastrowid

                    outcome_count = 0
                    for i, outcome_char in enumerate(outcomes):
                        if outcome_char.lower() == 'y':
                            cursor.execute(
                                    "INSERT INTO d_table_dtl_act_out (act_id, outcome) VALUES (?, ?)",
                                    (act_id, 1)
                            )
                        if outcome_char.lower() == 'n':
                            cursor.execute(
                                    "INSERT INTO d_table_dtl_act_out (act_id, outcome) VALUES (?, ?)",
                                    (act_id, 0)
                            )
                        outcome_count += 1
                print(f"已为 '{dt_name}' 插入 {num_acts} 条动作记录。")

        except StopIteration:
            break
        except Exception as e:
            print(f"处理文件时发生错误: {e}")
            raise


def main():
    """
    主执行函数
    """
    if not os.path.exists(DTL_FILE):
        print(f"错误: 输入文件 '{DTL_FILE}' 不存在。")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        create_tables(cursor)
        parse_and_update(cursor, DTL_FILE)
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