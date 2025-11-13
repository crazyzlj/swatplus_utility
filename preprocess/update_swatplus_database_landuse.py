import sqlite3
import csv
import os

# --- Configuration ---
wp = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4'
db_path = wp + os.sep + 'swatplus_datasets.sqlite'
csv_path = r'D:\data_m\manitowoc\landcover\landusemanagement(LUM)\new_landuse_lookup_manitowoc.csv'
# ---------------------

# Column lists from the database schema
PLANTS_COLUMNS = ["name", "plnt_typ", "gro_trig", "nfix_co", "days_mat", "bm_e", "harv_idx",
                  "lai_pot", "frac_hu1", "lai_max1", "frac_hu2", "lai_max2", "hu_lai_decl",
                  "dlai_rate", "can_ht_max", "rt_dp_max", "tmp_opt", "tmp_base", "frac_n_yld",
                  "frac_p_yld", "frac_n_em", "frac_n_50", "frac_n_mat", "frac_p_em", "frac_p_50",
                  "frac_p_mat", "harv_idx_ws", "usle_c_min", "stcon_max", "vpd", "frac_stcon",
                  "ru_vpd", "co2_hi", "bm_e_hi", "plnt_decomp", "lai_min", "bm_tree_acc", "yrs_mat",
                  "bm_tree_max", "ext_co", "leaf_tov_mn", "leaf_tov_mx", "bm_dieoff", "rt_st_beg",
                  "rt_st_end", "plnt_pop1", "frac_lai1", "plnt_pop2", "frac_lai2", "frac_sw_gro",
                  "aeration", "rsd_pctcov", "rsd_covfac", "description"]
PLANT_INI_ITEM_COLUMNS = ["plnt_name_id", "lc_status", "lai_init", "bm_init", "phu_init",
                          "plnt_pop", "yrs_init", "rsd_init"]
LANDUSE_COLUMNS = ["name", "cal_group", "plnt_com_id", "mgt_id", "cn2_id", "cons_prac_id",
                   "urban_id", "urb_ro", "ov_mann_id", "tile_id", "sep_id", "vfs_id", "grww_id",
                   "bmp_id", "description"]

def check_template_exists(cursor, table, name):
    """Checks if a template name exists in the specified table."""
    query = f"SELECT COUNT(*) FROM {table} WHERE name = ?"
    cursor.execute(query, (name,))
    return cursor.fetchone()[0] > 0


def add_plant(cursor, new_id, new_name, template_name):
    """Adds a new entry to the plants_plt table, checking for existence first."""
    # --- 新增：检查新条目是否已存在 ---
    cursor.execute("SELECT COUNT(*) FROM plants_plt WHERE id = ? OR name = ?", (new_id, new_name))
    if cursor.fetchone()[0] > 0:
        print(
            f"  - INFO: Plant with ID '{new_id}' or Name '{new_name}' already exists in plants_plt. Skipping.")
        return  # 如果已存在，则直接退出函数

    if not check_template_exists(cursor, 'plants_plt', template_name):
        raise ValueError(
            f"Error: Template plant '{template_name}' not found in 'plants_plt' table.")

    cols_to_select_str = ", ".join(PLANTS_COLUMNS)
    cols_to_insert_str = ", ".join(["id", "name"] + PLANTS_COLUMNS)

    sql = f"""
        INSERT INTO plants_plt ({cols_to_insert_str})
        SELECT ?, ?, {cols_to_select_str}
        FROM plants_plt
        WHERE name = ?;
    """
    cursor.execute(sql, (new_id, new_name, template_name))
    print(f"  - Successfully added '{new_name}' (ID: {new_id}) to plants_plt.")


def add_plant_ini(cursor, new_id, new_comm_name):
    cursor.execute("SELECT COUNT(*) FROM plant_ini WHERE id = ? OR name = ?",
                   (new_id, new_comm_name))
    if cursor.fetchone()[0] > 0:
        print(
            f"  - INFO: Plant community '{new_comm_name}' (ID: {new_id}) already exists in plant_ini. Skipping.")
        return
    sql = "INSERT INTO plant_ini (id, name, rot_yr_ini, description) VALUES (?, ?, 1, ?)"
    cursor.execute(sql,
                   (new_id, new_comm_name, f"Initial conditions for {new_comm_name} community"))
    print(f"  - OK: Added '{new_comm_name}' (ID: {new_id}) to plant_ini.")


def add_plant_ini_items(cursor, new_ini_id, community_plants):
    # 获取当前plant_ini_item表中最大的ID，用于自增
    cursor.execute("SELECT MAX(id) FROM plant_ini_item")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0

    for plant_name in community_plants:
        plant_name = plant_name.strip()
        cursor.execute("SELECT id FROM plants_plt WHERE name = ?", (plant_name,))
        plant_id_res = cursor.fetchone()
        if not plant_id_res:
            raise ValueError(f"Plant '{plant_name}' from community not found in plants_plt table.")
        plant_id = plant_id_res[0]

        cursor.execute(
            "SELECT COUNT(*) FROM plant_ini_item WHERE plant_ini_id = ? AND plnt_name_id = ?",
            (new_ini_id, plant_id))
        if cursor.fetchone()[0] > 0:
            print(
                f"  - INFO: Item for community ID '{new_ini_id}' and plant '{plant_name}' already exists. Skipping.")
            continue

        cursor.execute(
            f'SELECT {", ".join(PLANT_INI_ITEM_COLUMNS)} FROM plant_ini_item WHERE plnt_name_id = ? LIMIT 1',
            (plant_id,))
        template_item = cursor.fetchone()
        if not template_item:
            raise ValueError(
                f"No template found in plant_ini_item for plant '{plant_name}' (ID: {plant_id}).")

        max_id += 1
        new_item_values = (max_id, new_ini_id) + template_item

        sql = f'INSERT INTO plant_ini_item (id, plant_ini_id, {", ".join(PLANT_INI_ITEM_COLUMNS)}) VALUES ({",".join(["?"] * (len(PLANT_INI_ITEM_COLUMNS) + 2))})'
        cursor.execute(sql, new_item_values)
        print(f"    - OK: Added item for plant '{plant_name}' to community ID '{new_ini_id}'.")

#
# def add_landuse_lum(cursor, new_id, new_name, template_name):
#     new_lum_name = new_name + '_lum'
#     template_lum_name = template_name + '_lum'
#     cursor.execute("SELECT COUNT(*) FROM landuse_lum WHERE id = ? OR name = ?",
#                    (new_id, new_lum_name))
#     if cursor.fetchone()[0] > 0:
#         print(
#             f"  - INFO: Landuse '{new_lum_name}' (ID: {new_id}) already exists in landuse_lum. Skipping.")
#         return
#     cursor.execute("SELECT COUNT(*) FROM landuse_lum WHERE name = ?", (template_lum_name,))
#     if cursor.fetchone()[0] == 0:
#         raise ValueError(f"Template landuse '{template_lum_name}' not found in 'landuse_lum'.")
#
#     # 在复制时，直接将plnt_com_id设置为新的ID
#     cols_to_select = [c for c in LANDUSE_COLUMNS if c != 'plnt_com_id' and c != 'name']
#     sql = f'INSERT INTO landuse_lum (id, name, plnt_com_id, {", ".join(cols_to_select)}) SELECT ?, ?, ?, {", ".join(cols_to_select)} FROM landuse_lum WHERE name = ?'
#     cursor.execute(sql, (new_id, new_lum_name, new_id, template_lum_name))
#     print(f"  - OK: Added '{new_lum_name}' (ID: {new_id}) to landuse_lum.")


def main():
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at '{db_path}'")
        return
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at '{csv_path}'")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("BEGIN TRANSACTION;")

        with open(csv_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            expected_headers = {'new_landuse_id', 'new_name', 'swat_code', 'plant_comm'}
            reader_headers_lower = {h.lower().strip() for h in reader.fieldnames}
            if not expected_headers.issubset(reader_headers_lower):
                missing = expected_headers - reader_headers_lower
                raise ValueError(f"CSV file is missing required headers: {list(missing)}")

            print(f"Reading from '{csv_path}' and updating '{db_path}'...")

            for row_orig in reader:
                row = {k.lower().strip(): v.strip() for k, v in row_orig.items()}
                new_id = int(row['new_landuse_id'])
                new_plant_name = row['new_name']
                template_plant_name = row['swat_code'].lower()
                community_plants = [p.strip().lower() for p in row['plant_comm'].split('-')]

                print(
                    f"\nProcessing: ID={new_id}, Name='{new_plant_name}', Community={community_plants}")

                # 按顺序执行四个步骤
                add_plant(cur, new_id, new_plant_name, template_plant_name)
                add_plant_ini(cur, new_id, new_plant_name + '_comm')
                add_plant_ini_items(cur, new_id, community_plants)
                # add_landuse_lum(cur, new_id, new_plant_name, template_plant_name)

        conn.commit()
        print("\nDatabase update successful. All changes have been saved.")

    except (sqlite3.Error, ValueError, KeyError) as e:
        if conn:
            conn.rollback()
        print(f"\nAn error occurred: {e}")
        print("Database update failed. All changes have been rolled back.")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
