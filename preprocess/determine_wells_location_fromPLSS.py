import pandas as pd
import geopandas as gpt
from shapely.geometry import Point
import os

import pyproj
import pyproj.datadir

os.environ['PROJ_LIB'] = pyproj.datadir.get_data_dir()
print(os.environ['PROJ_LIB'])

# --- 配置路径 ---
TARGET_CSV = r"D:\tmp\selected_wells_without_pdf.csv"  # 存放需要处理的编号，每行一个
INVENTORY_EXCEL = r"D:\tmp\selected_wells_inventory.xlsx"
PLSS_SHP = r"D:\tmp\plss\Plss_Sections.shp"
OUTPUT_EXCEL = r"D:\tmp\wells_have_gwleveldata.xlsx"
OUTPUT_SHP = r"D:\tmp\wells_have_gwleveldata.shp"


def locate_wells_projected():
    # 1. 读取目标编号 (CSV)
    if not os.path.exists(TARGET_CSV):
        print(f"找不到 CSV 文件: {TARGET_CSV}")
        return
    target_ids = pd.read_csv(TARGET_CSV, header=None, dtype=str)[0].str.strip().tolist()

    # 2. 加载基础数据
    if not os.path.exists(INVENTORY_EXCEL):
        print(f"找不到名录文件: {INVENTORY_EXCEL}")
        return

    # 读取名录并将编号设为字符串
    inventory = pd.read_excel(INVENTORY_EXCEL, dtype={'WI Unique Well #': str})
    inventory['WI Unique Well #'] = inventory['WI Unique Well #'].str.strip()

    # 读取 PLSS SHP (保持其原始投影: EPSG:3857)
    gdf_plss = gpt.read_file(PLSS_SHP)

    # 3. 过滤出目标水井
    df = inventory[inventory['WI Unique Well #'].isin(target_ids)].copy()

    if df.empty:
        print("未匹配到任何水井编号。")
        return

    print(f"开始定位 {len(df)} 口水井...")

    # 4. 匹配 PLSS 并提取投影坐标 (X, Y)
    for idx, row in df.iterrows():
        try:
            # 数据清洗
            twp = int(float(row['Township']))
            rng = int(float(row['Range']))
            sec = int(float(row['Section']))
            rng_dir = str(row['Range Direction']).strip().upper()

            # 匹配对应多边形
            target = gdf_plss[
                (gdf_plss['TWP'].astype(int) == twp) &
                (gdf_plss['RNG'].astype(int) == rng) &
                (gdf_plss['SEC'].astype(int) == sec) &
                (gdf_plss['DIR_ALPHA'] == rng_dir)
                ]

            if not target.empty:
                # 直接获取投影坐标系的质心
                centroid = target.iloc[0].geometry.centroid

                # 将投影坐标直接写入 Excel
                df.at[idx, 'Proj_X'] = centroid.x
                df.at[idx, 'Proj_Y'] = centroid.y
            else:
                print(f"[{row['WI Unique Well #']}] 未能在地图中找到对应区域")

        except Exception as e:
            print(f"[{row['WI Unique Well #']}] 错误: {e}")

    # 5. 保存结果
    # 导出 Excel
    df.to_excel(OUTPUT_EXCEL, index=False)

    # 导出 Shapefile (使用与 PLSS 相同的坐标系)
    valid_df = df.dropna(subset=['Proj_X', 'Proj_Y'])
    if not valid_df.empty:
        geometry = [Point(xy) for xy in zip(valid_df['Proj_X'], valid_df['Proj_Y'])]
        # 这里的 crs 直接设为与输入 plss 一致
        final_gdf = gpt.GeoDataFrame(valid_df, geometry=geometry)
        final_gdf.to_file(OUTPUT_SHP)
        print(
            f"处理完成！\nExcel 已生成（包含 Proj_X/Y 列）\nShapefile 已保存，坐标系与原始 PLSS 一致。")


if __name__ == "__main__":
    locate_wells_projected()