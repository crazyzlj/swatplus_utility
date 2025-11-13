import geopandas as gpd
import pandas as pd

# ==============================================================================
# 用户配置区域
# ==============================================================================
# --- 输入/输出文件 ---
input_shp = "D:\data_m\manitowoc_test30m\manitowoc_test30mv4\processlandcover\landcover_eliminated_with_county.shp"
output_shp = "D:\data_m\manitowoc_test30m\manitowoc_test30mv4\processlandcover\landcover_eliminated_reclassified.shp"

# --- Shapefile中的字段名 ---
# !!! 请确保这里的字段名与您shapefile中的完全一致 !!!
lc_field = 'gridcode'  # 土地利用类型字段
sub_id_field = 'Subbasin'  # 子流域ID字段
county_field = 'AREASYMBOL'  # 县代码字段
new_lc_field = 'newlandcov'  # 新增的土地利用类型字段名

# --- 重分类规则定义 ---
RECLASS_RULES = {
    2110: {  # Cash grain
        'targets': {2111: 0.45, 2112: 0.14, 2113: 0.14, 2114: 0.27}
    },
    2120: {  # Continuous corn
        'targets': {2121: 0.64, 2122: 0.25, 2123: 0.11}
    },
    2130: {
        'conditional': True,  # 标记这是一个需要按条件判断的规则
        'conditions': [
            {
                'counties': ['WI071'],
                'targets': {2134: 0.65, 2135: 0.26, 2136: 0.09}
            },
            {
                'counties': ['WI009', 'WI015', 'WI039'],
                'targets': {2131: 0.65, 2132: 0.26, 2133: 0.09}
            }
        ]
    }
}


# ==============================================================================


def reclassify_group(sub_gdf, lc_code, target_map, lc_field_name, new_lc_field_name):
    """
    对一个子流域内的一种土地利用类型，按面积比例进行重分类。

    :param sub_gdf: 单个子流域的GeoDataFrame。
    :param lc_code: 需要重分类的原始土地利用代码。
    :param target_map: 目标代码及其面积比例的字典。
    :param lc_field_name: 原始土地利用字段名。
    :param new_lc_field_name: 新的土地利用字段名。
    :return: 包含新编码的Pandas Series，索引与sub_gdf对应。
    """
    # 筛选出需要处理的地块
    parcels_to_reclass = sub_gdf[sub_gdf[lc_field_name] == lc_code].copy()

    if parcels_to_reclass.empty:
        return None

    print(f"    - Reclassifying {len(parcels_to_reclass)} parcels of type {lc_code}...")

    # 计算总面积和每个目标的具体面积
    total_area = parcels_to_reclass.geometry.area.sum()
    target_areas = {code: total_area * ratio for code, ratio in target_map.items()}

    # 初始化当前已分配面积
    current_areas = {code: 0 for code in target_map.keys()}

    # 按面积从大到小排序
    # 1. 创建一个临时列来存储每个地块的面积
    parcels_to_reclass['temp_area'] = parcels_to_reclass.geometry.area
    # 2. 按这个新列的名称进行排序
    parcels_to_reclass = parcels_to_reclass.sort_values(by='temp_area', ascending=False)

    # 准备一个Series来存储结果
    result_series = pd.Series(index=parcels_to_reclass.index, dtype=int)

    # 遍历地块进行分配
    for idx, parcel in parcels_to_reclass.iterrows():
        # 计算每个目标的“欠缺”面积
        deficits = {code: target_areas[code] - current_areas[code] for code in target_map.keys()}

        # 找到欠缺面积最大的目标
        best_target_code = max(deficits, key=deficits.get)

        # 分配新编码
        result_series.loc[idx] = best_target_code

        # 更新已分配面积
        current_areas[best_target_code] += parcel.geometry.area

    # 打印最终的分配结果以供验证
    final_ratios = {code: current_areas[code] / total_area * 100 for code in target_map.keys()}
    print(f"      Final ratios: { {k: f'{v:.1f}%' for k, v in final_ratios.items()} }")

    return result_series


def main():
    """主执行函数"""
    print(f"Step 1: Loading shapefile '{input_shp}'...")
    gdf = gpd.read_file(input_shp)

    # 检查所需字段
    required_fields = [lc_field, sub_id_field, county_field, 'geometry']
    for field in required_fields:
        if field not in gdf.columns:
            raise ValueError(f"Input shapefile must contain the field: '{field}'")

    # --- Step 2: 初始化新字段 ---
    print(f"Step 2: Initializing new field '{new_lc_field}'...")
    # 默认将新字段的值设置为原始值
    gdf[new_lc_field] = gdf[lc_field]

    # --- Step 3: 筛选需要处理的要素 ---
    # Subbasin ID > 0 的为待处理要素
    gdf_to_process = gdf[gdf[sub_id_field] > 0]
    unique_sub_ids = gdf_to_process[sub_id_field].unique()
    print(f"Found {len(unique_sub_ids)} subbasins to process.")

    # --- Step 4: 逐子流域、逐地类进行处理 ---
    for sub_id in unique_sub_ids:
        print(f"\n--- Processing Subbasin ID: {sub_id} ---")

        # 获取当前子流域的所有地块
        sub_gdf = gdf_to_process[gdf_to_process[sub_id_field] == sub_id]

        # 遍历规则中定义的每一种需要重分类的土地利用类型
        for lc_code_to_process in RECLASS_RULES.keys():
            rule = RECLASS_RULES[lc_code_to_process]
            target_map = None

            # 如果是需要按条件判断的规则 (如2130)
            if rule.get('conditional'):
                # 获取当前子流域的县代码 (假定一个子流域内的县代码是唯一的)
                current_county = sub_gdf[county_field].iloc[0]
                # 寻找匹配的条件
                for condition in rule['conditions']:
                    if current_county in condition['counties']:
                        target_map = condition['targets']
                        break
            # 如果是普通规则
            else:
                target_map = rule['targets']

            # 如果找到了匹配的规则，则执行重分类
            if target_map:
                new_codes_series = reclassify_group(sub_gdf, lc_code_to_process, target_map,
                                                    lc_field, new_lc_field)

                # 如果有返回结果，则更新主GeoDataFrame
                if new_codes_series is not None:
                    gdf.loc[new_codes_series.index, new_lc_field] = new_codes_series

    # --- Step 5: 保存结果 ---
    print(f"\nStep 5: Saving reclassified shapefile to '{output_shp}'...")
    gdf.to_file(output_shp, driver='ESRI Shapefile', encoding='utf-8')
    print("Processing complete.")


if __name__ == "__main__":
    main()
