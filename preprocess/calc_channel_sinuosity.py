import os
import numpy as np
import pandas as pd
import geopandas as gpd


def update_swatplus_cha_file(input_cha, output_cha, df, id_col="Channel", mann_col="n_dynamic",
                             sinu_col="SI"):
    """
    根据计算结果更新 SWAT+ 的 hyd-sed-lte.cha 文件。

    参数:
        input_cha (str): 原始 hyd-sed-lte.cha 文件路径
        output_cha (str): 修改后生成的新文件路径
        df (pd.DataFrame): 包含最新参数的 DataFrame
        id_col (str): DataFrame 中存储 Channel ID 的列名
        mann_col (str): DataFrame 中需要写入 mann 字段的列名
        sinu_col (str): DataFrame 中需要写入 sinu 字段的列名
    """
    if not os.path.exists(input_cha):
        print(f"错误: 找不到输入文件 {input_cha}")
        return

    # 1. 构建字典查找表，将 ID 转换为 SWAT+ 的命名规范，如 68 -> 'hydcha068'
    lookup = {}
    for _, row in df.iterrows():
        try:
            cha_id = int(row[id_col])
            cha_name = f"hydcha{cha_id:03d}"
            lookup[cha_name] = {
                'mann': row[mann_col] if pd.notna(row[mann_col]) else None,
                'sinu': row[sinu_col] if pd.notna(row[sinu_col]) else None
            }
        except ValueError:
            # 略过无法转换为整数的 ID
            continue

    # 2. 读取并修改文件
    with open(input_cha, 'r', encoding='utf-8') as f_in, \
            open(output_cha, 'w', encoding='utf-8') as f_out:

        lines = f_in.readlines()

        if len(lines) < 2:
            print("错误: cha 文件行数过少，格式异常。")
            return

        # 第一行: 版本信息，原样保留
        f_out.write(lines[0])

        # 第二行: 解析表头，动态获取目标列的索引
        headers = lines[1].strip().split()
        try:
            mann_idx = headers.index('mann')
            sinu_idx = headers.index('sinu')
        except ValueError as e:
            print(f"错误: 无法在表头中找到指定的列名 ({e})。请检查文件格式。")
            return

        # 重建第二行表头，保持对齐（name 字段占 30 字符宽度，其他字段占 15 字符宽度）
        header_line = f"{headers[0]:<30}" + "".join([f"{h:>15}" for h in headers[1:]]) + "\n"
        f_out.write(header_line)

        updated_count = 0

        # 3. 逐行修改数据区
        for line in lines[2:]:
            if not line.strip():  # 忽略空行
                f_out.write(line)
                continue

            tokens = line.strip().split()
            if len(tokens) < max(mann_idx, sinu_idx) + 1:
                f_out.write(line)  # 行数据不全，原样跳过
                continue

            name = tokens[0]

            # 如果该行名称在我们的计算结果字典中，进行替换
            if name in lookup:
                new_mann = lookup[name]['mann']
                new_sinu = lookup[name]['sinu']

                # 统一保留 5 位小数，这符合 SWAT+ 默认编辑器的输出精度
                if new_mann is not None:
                    tokens[mann_idx] = f"{new_mann:.5f}"
                if new_sinu is not None:
                    tokens[sinu_idx] = f"{new_sinu:.5f}"

                updated_count += 1

            # 重新组装该行，强制所有数据列使用相同的宽度进行排版
            data_line = f"{tokens[0]:<30}" + "".join([f"{t:>15}" for t in tokens[1:]]) + "\n"
            f_out.write(data_line)

    print(f"\n[SWAT+ 文件更新完成]")
    print(f"目标文件: {output_cha}")
    print(f"成功更新了 {updated_count} 条 Channel 记录。")

def calculate_qswat_hydro_parameters(input_shp, id_field, output_csv,
                                     wid_field="Wid2", slo_field="Slo2",
                                     dep_field="Dep2", len_field="Len2",
                                     wid_multiplier=4.0, min_tolerance=30.0,
                                     base_n0=0.035, slo_threshold=0.002):
    """
    计算线要素的 SI、SI_c，并动态计算 Manning's n。

    参数:
        base_n0 (float): Cowan 法的基础糙率。QSWAT+ 默认是 0.05，但纯自然顺直河道
                         通常在 0.03~0.04 之间。这里默认取 0.035。
        slo_threshold (float): 触发 Jarrett 公式的坡度阈值，默认为 0.002。
    """
    if not os.path.exists(input_shp):
        raise FileNotFoundError(f"找不到矢量文件: {input_shp}")

    gdf = gpd.read_file(input_shp)
    if id_field not in gdf.columns:
        raise ValueError(f"错误: 字段 '{id_field}' 不存在。")

    avail_fields = gdf.columns
    use_wid = wid_field in avail_fields
    use_slo = slo_field in avail_fields
    use_dep = dep_field in avail_fields

    results = []

    for index, row in gdf.iterrows():
        geom = row.geometry
        feature_id = row[id_field]

        # 提取水力协变量
        val_wid = row[wid_field] if use_wid else np.nan
        val_slo = row[slo_field] if use_slo else np.nan
        val_dep = row[dep_field] if use_dep else np.nan
        val_len2 = row[len_field] if len_field in avail_fields else np.nan

        if geom is None or geom.is_empty or geom.geom_type not in ['LineString', 'MultiLineString']:
            results.append({
                id_field: feature_id, 'SI': np.nan, 'SI_c': np.nan,
                'n_dynamic': np.nan, 'n_method': 'Invalid Geom'
            })
            continue

        # --- 1. 计算弯曲度 (SI & SI_c) ---
        L_c_geom = geom.length
        coords = list(geom.coords) if geom.geom_type == 'LineString' else [pt for part in geom.geoms
                                                                           for pt in part.coords]
        start_pt, end_pt = coords[0], coords[-1]
        D = np.sqrt((start_pt[0] - end_pt[0]) ** 2 + (start_pt[1] - end_pt[1]) ** 2)

        SI = L_c_geom / D if D > 0 else (np.inf if L_c_geom > 0 else 1.0)

        current_tolerance = min_tolerance
        if use_wid and pd.notnull(val_wid) and val_wid > 0:
            current_tolerance = max(min_tolerance, val_wid * wid_multiplier)

        geom_simplified = geom.simplify(current_tolerance, preserve_topology=False)
        L_v = max(geom_simplified.length, D)

        SI_c = L_c_geom / L_v if L_v > 0 else np.nan
        if not np.isnan(SI_c) and SI_c < 1.0:
            SI_c = 1.0

        # --- 2. 动态计算 Manning's n (引入真实水力半径与约束) ---
        n_val = np.nan
        n_method = "Unknown"

        has_valid_slo = pd.notnull(val_slo) and val_slo > 0
        has_valid_dep = pd.notnull(val_dep) and val_dep > 0
        has_valid_wid = pd.notnull(val_wid) and val_wid > 0

        # Jarrett 公式的绝对物理上限
        MAX_N_LIMIT = 0.12

        if has_valid_slo and val_slo > slo_threshold and has_valid_dep and has_valid_wid:
            # 策略 A: 高坡降河段 -> Jarrett 公式 (使用精确水力半径)

            # 1. 计算矩形断面的真实水力半径 R
            R_rect = (val_wid * val_dep) / (val_wid + 2 * val_dep)

            # 2. 约束输入变量在 Jarrett 经验公式的安全边界内
            eff_slo = min(val_slo, 0.04)  # 坡度上限约束为 0.04
            eff_R = max(R_rect, 0.15)  # 水力半径下限约束为 0.15 米

            # 3. 使用公制系数 0.32 计算
            n_raw = 0.32 * (eff_slo ** 0.38) * (eff_R ** -0.16)

            # 4. 结果绝对值截断
            n_val = min(n_raw, MAX_N_LIMIT)

            if n_raw > MAX_N_LIMIT:
                n_method = f"Jarrett (Capped at {MAX_N_LIMIT})"
            else:
                n_method = "Jarrett (Bounded)"

        else:
            # 策略 B: 平缓河段或缺失宽深数据 -> Cowan 弯曲度修正法
            m = 1.00  # 顺直默认乘数
            if not np.isnan(SI_c):
                if SI_c > 1.5:
                    m = 1.30
                elif SI_c >= 1.2:
                    m = 1.15

            n_val = base_n0 * m
            n_method = "Cowan"

        # 记录结果
        results.append({
            id_field: feature_id,
            'SI': round(SI, 4),
            'SI_c': round(SI_c, 4) if not np.isnan(SI_c) else '',
            'n_dynamic': round(n_val, 4) if not np.isnan(n_val) else '',
            'n_method': n_method,
            'Tolerance_m': round(current_tolerance, 2),
            wid_field: round(val_wid, 2) if pd.notnull(val_wid) else '',
            slo_field: round(val_slo, 4) if pd.notnull(val_slo) else '',
            dep_field: round(val_dep, 2) if pd.notnull(val_dep) else '',
            len_field: round(val_len2, 3) if pd.notnull(val_len2) else ''
        })

    # --- 3. 输出处理 ---
    df_out = pd.DataFrame(results)

    cols_to_drop = [col for col in [wid_field, slo_field, dep_field, len_field]
                    if col not in avail_fields]
    df_out = df_out.drop(columns=cols_to_drop, errors='ignore')

    df_out.to_csv(output_csv, index=False, encoding='utf-8')
    print(f"计算完成。包含 {len(results)} 条记录。")
    print(f"曼宁系数统计: \n{df_out['n_method'].value_counts()}")
    print(f"结果已保存至: {output_csv}")


if __name__ == "__main__":
    shp_path = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Watershed\Shapes\rivs1.shp"
    id_col = "Channel"
    csv_out = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Watershed\channel_sinuosity.csv"
    # --- 2. 更新 SWAT+ cha 文件 ---
    original_cha = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut\hyd-sed-lte.bak.cha"  # 原始 SWAT+ 输入文件
    new_cha = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut\hyd-sed-lte.cha"  # 生成的新文件（建议另存为，避免破坏原始文件）

    calculate_qswat_hydro_parameters(
            input_shp=shp_path,
            id_field=id_col,
            output_csv=csv_out,
            base_n0=0.035,  # 基础糙率 (可根据研究区土壤类型微调)
            slo_threshold=0.002  # 触发 Jarrett 的坡度界限
    )

    df_results = pd.read_csv(csv_out)


    update_swatplus_cha_file(
            input_cha=original_cha,
            output_cha=new_cha,
            df=df_results,
            id_col="Channel",  # Shapefile 和 CSV 中的 ID 字段
            mann_col="n_dynamic",  # 刚才综合计算并做了阈值截断的曼宁系数
            sinu_col="SI"  # 使用“沿河道弯曲度”。如果没有提供 DEM 计算该值，可以改传 "SI"
    )
