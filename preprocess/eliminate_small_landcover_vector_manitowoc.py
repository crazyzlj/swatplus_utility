import geopandas as gpd
import pandas as pd

# ==============================================================================
# 用户配置区域
# ==============================================================================
# --- 输入/输出文件 ---
# 只需要这一个输入文件
input_shp = "D:\data_m\manitowoc_test30m\manitowoc_test30mv4\processlandcover\landcover.shp"
output_shp = "D:\data_m\manitowoc_test30m\manitowoc_test30mv4\processlandcover\landcover_eliminated.shp"

# --- Shapefile中的字段名 ---
# !!! 请确保这里的字段名与您shapefile中的完全一致 !!!
lc_code_field = 'gridcode'  # 土地利用类型字段
sub_id_field = 'Subbasin'  # 子流域ID字段


# --- 处理逻辑配置 ---
# 定义需要分组处理的土地利用类型
ag_codes = [2110, 2120, 2130, 3110]
natural_codes = [3000, 4000, 6000]

AREA_THRESHOLD = 0.05


# ==============================================================================


def process_vector_group_proportional(gdf, group_codes, total_sub_area, lc_field):
    """
    一个可复用的函数，用于处理一个特定组（如农业或自然）内的所有小面积地块。
    (此核心函数逻辑与上一版完全相同)
    """
    MAX_ITERATIONS = 10  # 在函数内部定义，因为它是处理逻辑的一部分
    while True:
        group_gdf = gdf[gdf[lc_field].isin(group_codes)]
        if len(group_gdf[lc_field].unique()) <= 1:
            break

        area_by_lc = group_gdf.groupby(lc_field)['geometry'].apply(lambda g: g.area.sum())

        minor_codes = {code for code, area in area_by_lc.items() if
                       (area / total_sub_area) < AREA_THRESHOLD}

        if not minor_codes:
            # print("    - No minor parcels to process in this group.")
            break

        print(f"    - Reclassifying minor types: {list(minor_codes)}")

        target_codes = [c for c in group_codes if c not in minor_codes]
        minor_polygons_gdf = gdf[gdf[lc_field].isin(minor_codes)]
        total_minor_area = minor_polygons_gdf.geometry.area.sum()

        target_areas = area_by_lc.reindex(target_codes).fillna(0)
        total_target_area = target_areas.sum()

        if total_target_area == 0:
            print("    - No target parcels to merge into. Skipping.")
            break

        area_budget = (target_areas / total_target_area * total_minor_area).to_dict()
        # print(f"    - Area budget: { {k: round(v, 2) for k, v in area_budget.items()} }")

        merge_candidates = []
        gdf_sindex = gdf.sindex

        for idx, poly in minor_polygons_gdf.iterrows():
            possible_neighbors_idx = list(gdf_sindex.intersection(poly.geometry.bounds))
            neighbors = gdf.iloc[possible_neighbors_idx]
            real_neighbors = neighbors[neighbors.geometry.touches(poly.geometry)]
            target_neighbors = real_neighbors[real_neighbors[lc_field].isin(target_codes)]

            for n_idx, neighbor in target_neighbors.iterrows():
                shared_border = poly.geometry.intersection(neighbor.geometry)
                if shared_border.geom_type == 'LineString':
                    score = shared_border.length
                    merge_candidates.append({
                        'minor_idx': idx, 'minor_area': poly.geometry.area,
                        'target_idx': n_idx, 'target_code': neighbor[lc_field],
                        'score': score
                    })

        merge_candidates.sort(key=lambda x: x['score'], reverse=True)

        processed_minor_indices = set()
        for candidate in merge_candidates:
            minor_idx, target_code, minor_area = candidate['minor_idx'], candidate['target_code'], \
            candidate['minor_area']
            if minor_idx in processed_minor_indices: continue
            if area_budget[target_code] >= minor_area:
                gdf.loc[minor_idx, lc_field] = target_code
                area_budget[target_code] -= minor_area
                processed_minor_indices.add(minor_idx)

        remaining_indices = set(minor_polygons_gdf.index) - processed_minor_indices
        if remaining_indices:
            # print(f"    - {len(remaining_indices)} parcels remain after budget assignment. Merging them to best available neighbors.")
            dominant_target_code = target_areas.idxmax()
            for idx in remaining_indices:
                poly = gdf.loc[idx]
                neighbors = gdf[gdf.geometry.touches(poly.geometry)]
                target_neighbors = neighbors[neighbors[lc_field].isin(target_codes)]
                if not target_neighbors.empty:
                    shared_border_lengths = {n_idx: poly.geometry.intersection(n.geometry).length
                                             for n_idx, n in target_neighbors.iterrows()}
                    best_neighbor_idx = max(shared_border_lengths, key=shared_border_lengths.get)
                    gdf.loc[idx, lc_field] = gdf.loc[best_neighbor_idx, lc_field]
                else:
                    # print(f"      - Parcel {idx} is an island, merging to dominant type {dominant_target_code}.")
                    gdf.loc[idx, lc_field] = dominant_target_code
    return gdf


def reclassify_vector_landcover(shp_path, out_path):
    print("Step 1: Loading and preparing data...")
    source_gdf = gpd.read_file(shp_path)

    required_fields = [lc_code_field, sub_id_field, 'geometry']
    for field in required_fields:
        if field not in source_gdf.columns:
            raise ValueError(f"Input shapefile must contain the field: '{field}'")

    # --- 新增：将数据一分为二 ---
    gdf_to_process = source_gdf[source_gdf[sub_id_field] > 0].copy()
    gdf_to_keep = source_gdf[source_gdf[sub_id_field] <= 0].copy()
    print(f"Found {len(gdf_to_process)} features to process (Subbasin ID > 0).")
    print(f"Found {len(gdf_to_keep)} features to keep unmodified (Subbasin ID <= 0).")

    processed_polygons = []
    unique_sub_ids = gdf_to_process[sub_id_field].unique()

    for sub_id in unique_sub_ids:
        print(f"\n--- Processing Subbasin ID: {sub_id} ---")

        sub_gdf = gdf_to_process[gdf_to_process[sub_id_field] == sub_id].copy()
        sub_area = sub_gdf.geometry.area.sum()

        if sub_area == 0: continue

        print("  - Processing agricultural group...")
        sub_gdf = process_vector_group_proportional(sub_gdf, ag_codes, sub_area, lc_code_field)

        print("  - Processing natural group...")
        sub_gdf = process_vector_group_proportional(sub_gdf, natural_codes, sub_area, lc_code_field)

        processed_polygons.append(sub_gdf)

    print("\nStep 3: Merging all results...")
    # 将处理过的部分和保留的部分合并回来
    if processed_polygons:
        processed_gdf = gpd.GeoDataFrame(pd.concat(processed_polygons, ignore_index=True),
                                         crs=source_gdf.crs)
        final_gdf = pd.concat([processed_gdf, gdf_to_keep], ignore_index=True)
    else:  # 如果没有任何需要处理的数据
        final_gdf = gdf_to_keep

    # 只保留必要的列以进行融合
    final_gdf = final_gdf[[lc_code_field, sub_id_field, 'geometry']]

    # print("Dissolving polygons by subbasin and land use code...")
    # 修正：按子流域和地类双重标准进行融合，避免跨流域合并
    # dissolved_gdf = final_gdf.dissolve(by=[sub_id_field, lc_code_field]).reset_index()

    print(f"Step 4: Saving final shapefile to {out_path}...")
    final_gdf.to_file(out_path, driver='ESRI Shapefile', encoding='utf-8')
    print("Processing complete.")


if __name__ == "__main__":
    reclassify_vector_landcover(input_shp, output_shp)
