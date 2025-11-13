import os
from osgeo import gdal
import numpy as np
from collections import Counter

# ==============================================================================
# 用户配置区域
# ==============================================================================
# 定义需要分组处理的土地利用类型
ag_codes = [2110, 2120, 2130, 3110]
natural_codes = [3000, 4000, 6000]
# 如果您的数据中还有其他农业或自然类型，请添加到上面的列表中

# 迭代分配时的最大迭代次数
MAX_ITERATIONS = 10
# 小面积阈值
AREA_THRESHOLD = 0.05


# ==============================================================================


def reclassify_land_cover(landcover_path, subbasins_path, output_path):
    print("Step 1: Reading raster files...")
    lc_ds = gdal.Open(landcover_path)
    sb_ds = gdal.Open(subbasins_path)
    if lc_ds is None or sb_ds is None:
        raise FileNotFoundError("One or both input raster files could not be opened.")

    lc_band = lc_ds.GetRasterBand(1)
    lc_nodata_val = lc_band.GetNoDataValue()
    print(f"Detected NoData value for landcover: {lc_nodata_val}")
    if lc_nodata_val is None:
        raise ValueError("Input landcover raster must have a NoData value set.")

    lc_array = lc_band.ReadAsArray()
    sb_array = sb_ds.GetRasterBand(1).ReadAsArray()

    # ==============================================================================
    # 新增步骤：在处理开始前，基于subbasins.tif的范围，预先准备好处理数组
    # ==============================================================================
    print("Initializing processing array based on valid subbasin extent...")
    # 1. 创建一个全局的、有效的子流域范围掩膜 (所有子流域ID > 0 的区域)
    analysis_extent_mask = (sb_array > 0)

    # 2. 创建一个默认填满NoData值的、干净的输出数组
    lc_processed = np.full(lc_array.shape, lc_nodata_val, dtype=lc_array.dtype)

    # 3. 将原始土地利用数据中，位于有效范围内的值，复制到新的处理数组中
    lc_processed[analysis_extent_mask] = lc_array[analysis_extent_mask]
    # ==============================================================================

    # 创建一个布尔掩膜，标记所有原始土地利用数据中的有效像元
    valid_data_mask_original_lc = (lc_array != lc_nodata_val)

    subbasin_ids = np.unique(sb_array[analysis_extent_mask])
    print(f"Found {len(subbasin_ids)} subbasins to process.")

    for sub_id in subbasin_ids:
        print(f"\n--- Processing Subbasin ID: {sub_id} ---")
        # 这里的掩膜现在结合了：子流域ID + 原始土地利用数据有效性
        # 确保我们不会处理那些在子流域内但原始数据是NoData的像元
        sub_mask = (sb_array == sub_id) & valid_data_mask_original_lc
        sub_area_pixels = np.sum(sub_mask)

        if sub_area_pixels == 0:
            print("Subbasin contains no valid land cover data. Skipping.")
            continue

        print("Processing agricultural lands...")
        lc_processed = process_land_group(
            lc_array=lc_processed,
            sub_mask=sub_mask,
            sub_area_pixels=sub_area_pixels,
            group_codes=ag_codes
        )

        print("Processing natural lands...")
        lc_processed = process_land_group(
            lc_array=lc_processed,
            sub_mask=sub_mask,
            sub_area_pixels=sub_area_pixels,
            group_codes=natural_codes
        )

    # 由于我们从一开始就在正确的范围内操作，不再需要最后的掩膜步骤

    print("\n--- Final Land Cover Statistics per Subbasin ---")
    final_stats = calculate_final_stats(lc_processed, sb_array, subbasin_ids, lc_nodata_val)
    for sub_id, stats in final_stats.items():
        print(f"Subbasin {sub_id}:")
        total_pixels = sum(stats.values())
        if total_pixels == 0: continue
        for lc_code, count in sorted(stats.items()):
            percentage = (count / total_pixels) * 100
            print(f"  - Code {lc_code}: {count} pixels ({percentage:.2f}%)")

    print(f"\nSaving processed raster to {output_path}...")
    write_raster(output_path, lc_processed, lc_ds)

    lc_ds = None
    sb_ds = None
    print("Processing complete.")


def process_land_group(lc_array, sub_mask, sub_area_pixels, group_codes):
    # 此函数逻辑保持不变
    while True:
        lc_in_sub = lc_array[sub_mask]
        group_mask_in_sub = np.isin(lc_in_sub, group_codes)
        group_codes_in_sub = lc_in_sub[group_mask_in_sub]

        if len(group_codes_in_sub) <= 1:  # 如果组内只剩一种或零种类型，无需再处理
            break

        type_counts = Counter(group_codes_in_sub)
        smallest_type = min(type_counts.items(), key=lambda item: item[1])
        lc_code, count = smallest_type

        if (count / sub_area_pixels) < AREA_THRESHOLD:
            print(f"  - Reclassifying minor type {lc_code} ({count} pixels)...")

            target_codes = [code for code in group_codes if code != lc_code]
            if not target_codes: break

            target_pixels_in_sub = lc_array[np.isin(lc_array, target_codes) & sub_mask]
            target_counts = Counter(target_pixels_in_sub)
            if not target_counts: break

            total_target_area = sum(target_counts.values())
            target_assignments_budget = {}
            pixels_to_assign = count
            assigned_so_far = 0

            for target_code, target_area in target_counts.most_common():
                proportion = target_area / total_target_area
                num_to_assign = round(proportion * pixels_to_assign)
                target_assignments_budget[target_code] = num_to_assign
                assigned_so_far += num_to_assign

            diff = pixels_to_assign - assigned_so_far
            if diff != 0:
                most_common_target = target_counts.most_common(1)[0][0]
                target_assignments_budget[most_common_target] += diff

            pixels_to_reclassify_mask = (lc_array == lc_code) & sub_mask
            rows, cols = np.where(pixels_to_reclassify_mask)
            unassigned_coords = list(zip(rows, cols))

            for i in range(MAX_ITERATIONS):
                if not unassigned_coords: break
                num_unassigned_before = len(unassigned_coords)

                still_unassigned = []
                np.random.shuffle(unassigned_coords)

                for r, c in unassigned_coords:
                    r_min, r_max = max(0, r - 1), min(lc_array.shape[0], r + 2)
                    c_min, c_max = max(0, c - 1), min(lc_array.shape[1], c + 2)
                    window = lc_array[r_min:r_max, c_min:c_max]
                    valid_neighbors = [n for n in window.flatten() if
                                       n in target_assignments_budget and target_assignments_budget[
                                           n] > 0]

                    assigned = False
                    if valid_neighbors:
                        neighbor_counts = Counter(valid_neighbors)
                        codes = list(neighbor_counts.keys())
                        counts = list(neighbor_counts.values())
                        probabilities = np.array(counts, dtype=float) / sum(counts)
                        chosen_neighbor = np.random.choice(codes, p=probabilities)
                        lc_array[r, c] = chosen_neighbor
                        target_assignments_budget[chosen_neighbor] -= 1
                        assigned = True
                    if not assigned:
                        still_unassigned.append((r, c))

                unassigned_coords = still_unassigned
                if len(unassigned_coords) == num_unassigned_before: break

            if unassigned_coords:
                remainder_list = []
                for code, num in target_assignments_budget.items():
                    remainder_list.extend([code] * num)
                np.random.shuffle(remainder_list)
                for i, (r, c) in enumerate(unassigned_coords):
                    if i < len(remainder_list): lc_array[r, c] = remainder_list[i]

            continue

        else:
            print(f"  - Smallest type '{lc_code}' is >= threshold. Group processing complete.")
            break

    return lc_array


# --- 辅助函数 (无需修改) ---
def calculate_final_stats(lc_array, sb_array, subbasin_ids, nodata_val):
    stats = {}
    if nodata_val is not None:
        valid_data_mask = (lc_array != nodata_val)
    else:
        valid_data_mask = np.ones_like(lc_array, dtype=bool)
    for sub_id in subbasin_ids:
        sub_mask = (sb_array == sub_id) & valid_data_mask
        lc_in_sub = lc_array[sub_mask]
        counts = Counter(lc_in_sub)
        stats[sub_id] = dict(counts)
    return stats


def write_raster(output_path, array, reference_ds):
    driver = gdal.GetDriverByName("GTiff")
    rows, cols = array.shape
    out_ds = driver.Create(output_path, cols, rows, 1, reference_ds.GetRasterBand(1).DataType)
    out_ds.SetGeoTransform(reference_ds.GetGeoTransform())
    out_ds.SetProjection(reference_ds.GetProjection())
    out_band = out_ds.GetRasterBand(1)
    out_band.WriteArray(array)
    nodata_val = reference_ds.GetRasterBand(1).GetNoDataValue()
    if nodata_val is not None:
        out_band.SetNoDataValue(nodata_val)
    out_band.FlushCache()
    out_ds = None


if __name__ == "__main__":
    landcover_input = "D:\data_m\manitowoc\landcover\wiscland2_30m_modified4swat_with_hay_without_water.tif"
    subbasins_input = "D:\data_m\manitowoc_test30m\subbasins_30m.tif"
    processed_output = "D:\data_m\manitowoc\landcover\wiscland2_30m_modified4swat_eliminated.tif"

    if not os.path.exists(landcover_input) or not os.path.exists(subbasins_input):
        print(
            f"Error: Input file not found. Please ensure '{landcover_input}' and '{subbasins_input}' are present.")
    else:
        reclassify_land_cover(landcover_input, subbasins_input, processed_output)
