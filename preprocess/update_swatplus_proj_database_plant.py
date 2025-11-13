# 用Python实现一个sqlite数据库中用plants_plt表的内容更新plant表。
# 这两张表的结构几乎一致，plants_plt的结构是：
# create table plants_plt
# (
#     id          INTEGER,
#     name        TEXT,
#     plnt_typ    TEXT,
#     gro_trig    TEXT,
#     nfix_co     REAL,
#     days_mat    REAL,
#     bm_e        REAL,
#     harv_idx    REAL,
#     lai_pot     REAL,
#     frac_hu1    REAL,
#     lai_max1    REAL,
#     frac_hu2    REAL,
#     lai_max2    REAL,
#     hu_lai_decl REAL,
#     dlai_rate   REAL,
#     can_ht_max  REAL,
#     rt_dp_max   REAL,
#     tmp_opt     REAL,
#     tmp_base    REAL,
#     frac_n_yld  REAL,
#     frac_p_yld  REAL,
#     frac_n_em   REAL,
#     frac_n_50   REAL,
#     frac_n_mat  REAL,
#     frac_p_em   REAL,
#     frac_p_50   REAL,
#     frac_p_mat  REAL,
#     harv_idx_ws REAL,
#     usle_c_min  REAL,
#     stcon_max   REAL,
#     vpd         REAL,
#     frac_stcon  REAL,
#     ru_vpd      REAL,
#     co2_hi      REAL,
#     bm_e_hi     REAL,
#     plnt_decomp REAL,
#     lai_min     REAL,
#     bm_tree_acc REAL,
#     yrs_mat     REAL,
#     bm_tree_max REAL,
#     ext_co      REAL,
#     leaf_tov_mn REAL,
#     leaf_tov_mx REAL,
#     bm_dieoff   REAL,
#     rt_st_beg   REAL,
#     rt_st_end   REAL,
#     plnt_pop1   REAL,
#     frac_lai1   REAL,
#     plnt_pop2   REAL,
#     frac_lai2   REAL,
#     frac_sw_gro REAL,
#     aeration    REAL,
#     rsd_pctcov  REAL,
#     rsd_covfac  REAL,
#     description TEXT
# );
#
# plant表的结构是：
# create table plant
# (
#     id          INTEGER      not null
#         primary key,
#     name        VARCHAR(255) not null,
#     plnt_typ    VARCHAR(255) not null,
#     gro_trig    VARCHAR(255) not null,
#     nfix_co     REAL         not null,
#     days_mat    REAL         not null,
#     bm_e        REAL         not null,
#     harv_idx    REAL         not null,
#     lai_pot     REAL         not null,
#     frac_hu1    REAL         not null,
#     lai_max1    REAL         not null,
#     frac_hu2    REAL         not null,
#     lai_max2    REAL         not null,
#     hu_lai_decl REAL         not null,
#     dlai_rate   REAL         not null,
#     can_ht_max  REAL         not null,
#     rt_dp_max   REAL         not null,
#     tmp_opt     REAL         not null,
#     tmp_base    REAL         not null,
#     frac_n_yld  REAL         not null,
#     frac_p_yld  REAL         not null,
#     frac_n_em   REAL         not null,
#     frac_n_50   REAL         not null,
#     frac_n_mat  REAL         not null,
#     frac_p_em   REAL         not null,
#     frac_p_50   REAL         not null,
#     frac_p_mat  REAL         not null,
#     harv_idx_ws REAL         not null,
#     usle_c_min  REAL         not null,
#     stcon_max   REAL         not null,
#     vpd         REAL         not null,
#     frac_stcon  REAL         not null,
#     ru_vpd      REAL         not null,
#     co2_hi      REAL         not null,
#     bm_e_hi     REAL         not null,
#     plnt_decomp REAL         not null,
#     lai_min     REAL         not null,
#     bm_tree_acc REAL         not null,
#     yrs_mat     REAL         not null,
#     bm_tree_max REAL         not null,
#     ext_co      REAL         not null,
#     leaf_tov_mn REAL         not null,
#     leaf_tov_mx REAL         not null,
#     bm_dieoff   REAL         not null,
#     rt_st_beg   REAL         not null,
#     rt_st_end   REAL         not null,
#     plnt_pop1   REAL         not null,
#     frac_lai1   REAL         not null,
#     plnt_pop2   REAL         not null,
#     frac_lai2   REAL         not null,
#     frac_sw_gro REAL         not null,
#     aeration    REAL         not null,
#     rsd_pctcov  REAL         not null,
#     rsd_covfac  REAL         not null,
#     description TEXT
# );
#
# create unique index plants_name
#     on plant (name);
#
# 现在需要将plants_plt中是数据更新至plant表中，如果某条记录的name在plant表中已存在，则更新其参数
import sqlite3
import pandas as pd
import os

# --- 配置 ---
# 请将 'your_database.sqlite' 替换为您的数据库文件名
DB_PATH =  r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\manitowoc_test30mv4.sqlite'
SOURCE_TABLE = 'plants_plt'
DESTINATION_TABLE = 'plant'


def sync_plant_tables(db_path, src_table, dest_table):
    """
    将源表中的数据同步（插入或更新）到目标表中。

    Args:
        db_path (str): SQLite数据库的文件路径。
        src_table (str): 源表名 (e.g., 'plants_plt').
        dest_table (str): 目标表名 (e.g., 'plant').
    """
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件未找到: '{db_path}'")
        return

    print(f"开始将数据从 '{src_table}' 同步到 '{dest_table}'...")

    conn = None
    try:
        # 1. 连接到数据库并读取源表数据
        conn = sqlite3.connect(db_path)

        # 检查源表是否存在
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (src_table,))
        if cursor.fetchone() is None:
            print(f"错误: 源表 '{src_table}' 在数据库中不存在。")
            return

        # 使用pandas高效读取整个源表
        df = pd.read_sql_query(f"SELECT * FROM {src_table}", conn)

        if df.empty:
            print(f"源表 '{src_table}' 为空，无需同步。")
            return

        # 将NaN值（pandas中的空值）替换为None，以兼容SQLite
        df = df.where(pd.notna(df), None)

        # 2. 准备将数据写入目标表
        # 获取列名，用于构建SQL语句
        columns = df.columns.tolist()
        columns_str = ', '.join(f'"{col}"' for col in columns)  # 使用引号以防列名是SQL关键字
        placeholders_str = ', '.join(['?'] * len(columns))

        # 准备批量插入的数据 (元组列表)
        data_to_insert = [tuple(row) for row in df.itertuples(index=False)]

        # 3. 执行数据库写入操作
        print(f"共找到 {len(df)} 条记录需要同步...")

        # 开始事务
        cursor.execute('BEGIN TRANSACTION')

        # 使用 INSERT OR REPLACE 语句
        # 它会根据 'name' 列的 UNIQUE 约束来决定是插入新行还是替换现有行
        sql_command = f"INSERT OR REPLACE INTO {dest_table} ({columns_str}) VALUES ({placeholders_str})"

        # 使用 executemany 高效执行批量操作
        cursor.executemany(sql_command, data_to_insert)

        # 提交事务
        conn.commit()

        print(f"\n同步成功！{len(df)} 条记录已插入或更新到 '{dest_table}' 表中。")

    except sqlite3.Error as e:
        print(f"\n发生数据库错误: {e}")
        if conn:
            # 如果出错，回滚所有更改
            conn.rollback()
            print("操作已回滚，数据库未做任何更改。")
    except Exception as e:
        print(f"\n发生未知错误: {e}")
        if conn:
            conn.rollback()
            print("操作已回滚。")
    finally:
        # 确保数据库连接被关闭
        if conn:
            conn.close()
            print("数据库连接已关闭。")


if __name__ == '__main__':
    # 运行主函数
    # 在运行前，请确保 DB_PATH 指向正确的数据库文件
    sync_plant_tables(DB_PATH, SOURCE_TABLE, DESTINATION_TABLE)
