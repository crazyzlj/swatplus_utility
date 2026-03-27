import os
import re
import pandas as pd
import pdfplumber
import geopandas as gpt
from shapely.geometry import Point, box
import logging
import pyproj
import pyproj.datadir

os.environ['PROJ_LIB'] = pyproj.datadir.get_data_dir()

# --- 路径配置 ---
EXCEL_PATH = r"D:\tmp\wdnr_wells\selected_wells_inventory.xlsx"
PDF_DIR = r"D:\tmp\wdnr_wells"
PLSS_SHP = r"D:\tmp\plss\Plss_Sections.shp"
OUTPUT_EXCEL = r"D:\tmp\well_updated.xlsx"
FAILED_LOG = r"D:\tmp\failed_wells.txt"
LOG_FILE = r"D:\tmp\process.log"

# --- 坐标系配置 ---
SRC_CRS = 'EPSG:3857'  # SHP的投影坐标系 (Web Mercator)
DST_CRS = 'EPSG:4326'  # 目标经纬度坐标系

# 日志设置
logging.basicConfig(level=logging.INFO, filename=LOG_FILE, filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)


def to_int(val):
    """安全转换为整数"""
    try:
        return int(float(str(val).strip()))
    except:
        return None


def get_quarter_center(geom, quarters_str):
    """根据方位在投影坐标下计算中心点 (返回 x, y)"""
    minx, miny, maxx, maxy = geom.bounds
    q_list = re.findall(r'(NE|NW|SE|SW)', quarters_str.upper())

    curr_minx, curr_miny, curr_maxx, curr_maxy = minx, miny, maxx, maxy
    for q in q_list:
        midx, midy = (curr_minx + curr_maxx) / 2, (curr_miny + curr_maxy) / 2
        if q == 'NE':
            curr_minx, curr_miny = midx, midy
        elif q == 'NW':
            curr_maxx, curr_miny = midx, midy
        elif q == 'SE':
            curr_minx, curr_maxy = midx, midy
        elif q == 'SW':
            curr_maxx, curr_maxy = midx, midy

    return (curr_minx + curr_maxx) / 2, (curr_miny + curr_maxy) / 2


def extract_pdf_info(pdf_path):
    res = {'twp': None, 'rng_num': None, 'rng_dir': None, 'sec': None,
           'lat': None, 'lon': None, 'quarters': ""}

    with pdfplumber.open(pdf_path) as pdf:
        page_text = pdf.pages[0].extract_text()
        if not page_text: return res
        clean_text = " ".join(page_text.split())

        # 1. 提取经纬度
        lat_m = re.search(r'(\d+\.\d+)\s*°\s*N', clean_text)
        lon_m = re.search(r'(-\d+\.\d+)\s*°\s*W', clean_text)
        if lat_m: res['lat'] = float(lat_m.group(1))
        if lon_m: res['lon'] = float(lon_m.group(1))

        # 2. 提取方位信息 (在 Section 之前)
        # 截取 "Well Plan Approval #" 到 "Section" 之间的部分
        q_zone_m = re.search(r'Well Plan Approval #\s+(.*?)\s+Section', clean_text)
        if q_zone_m:
            res['quarters'] = " ".join(re.findall(r'\b(NE|NW|SE|SW)\b', q_zone_m.group(1)))

        # 3. 提取 PLSS 数字序列 (在 or Govt Lot # 之后)
        # 目标文本示例: "or Govt Lot # 24 17 N 20 E"
        plss_zone_m = re.search(r'or Govt Lot #\s+(\d+)\s+(\d+)\s+[NS]\s+(\d+)\s+([EW])',
                                clean_text)
        if plss_zone_m:
            res['sec'] = int(plss_zone_m.group(1))
            res['twp'] = int(plss_zone_m.group(2))
            res['rng_num'] = int(plss_zone_m.group(3))
            res['rng_dir'] = plss_zone_m.group(4).upper()
        else:
            # 备选方案：如果格式略有偏差，按数字顺序抓取
            trail_m = re.search(r'or Govt Lot #\s+(.*)', clean_text)
            if trail_m:
                nums = re.findall(r'\b(\d+)\b', trail_m.group(1))
                dirs = re.findall(r'\b([EW])\b', trail_m.group(1))
                if len(nums) >= 3:
                    res['sec'], res['twp'], res['rng_num'] = int(nums[0]), int(nums[1]), int(
                            nums[2])
                if dirs: res['rng_dir'] = dirs[0].upper()

    return res


def get_quarter_center(geom, quarters_str):
    minx, miny, maxx, maxy = geom.bounds
    q_list = re.findall(r'(NE|NW|SE|SW)', quarters_str.upper())
    curr_minx, curr_miny, curr_maxx, curr_maxy = minx, miny, maxx, maxy
    for q in q_list:
        midx, midy = (curr_minx + curr_maxx) / 2, (curr_miny + curr_maxy) / 2
        if q == 'NE':
            curr_minx, curr_miny = midx, midy
        elif q == 'NW':
            curr_maxx, curr_miny = midx, midy
        elif q == 'SE':
            curr_minx, curr_maxy = midx, midy
        elif q == 'SW':
            curr_maxx, curr_maxy = midx, midy
    return (curr_minx + curr_maxx) / 2, (curr_miny + curr_maxy) / 2


def main():
    df = pd.read_excel(EXCEL_PATH)
    gdf_plss = gpt.read_file(PLSS_SHP)

    # 建立坐标转换器
    transformer_to_wgs = pyproj.Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    transformer_to_merc = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    failed_list = []

    for idx, row in df.iterrows():
        well_id = str(row['WI Unique Well #']).strip()
        pdf_path = os.path.join(PDF_DIR, f"{well_id}_Report.pdf")
        if not os.path.exists(pdf_path): continue

        try:
            pdf_data = extract_pdf_info(pdf_path)

            # 数据转换与匹配
            ex_twp, ex_rng, ex_sec = int(float(row['Township'])), int(float(row['Range'])), int(
                float(row['Section']))
            ex_dir = str(row['Range Direction']).strip().upper()

            if not (pdf_data['twp'] == ex_twp and pdf_data['rng_num'] == ex_rng and pdf_data[
                'sec'] == ex_sec):
                logging.error(f"[{well_id}] 属性不匹配")
                failed_list.append(well_id)
                continue

            # 定位 PLSS 几何
            target_sec = gdf_plss[(gdf_plss['TWP'].astype(int) == ex_twp) &
                                  (gdf_plss['RNG'].astype(int) == ex_rng) &
                                  (gdf_plss['SEC'].astype(int) == ex_sec) &
                                  (gdf_plss['DIR_ALPHA'] == ex_dir)]

            if target_sec.empty:
                failed_list.append(well_id)
                continue

            sec_geom = target_sec.iloc[0].geometry
            final_lon, final_lat = None, None

            # 优先使用 PDF DD 坐标并校验
            if pdf_data['lat'] and pdf_data['lon']:
                mx, my = transformer_to_merc.transform(pdf_data['lon'], pdf_data['lat'])
                if sec_geom.buffer(150).contains(Point(mx, my)):
                    final_lon, final_lat = pdf_data['lon'], pdf_data['lat']

            # 否则使用方位插值
            if final_lon is None:
                cx, cy = get_quarter_center(sec_geom, pdf_data['quarters'])
                final_lon, final_lat = transformer_to_wgs.transform(cx, cy)

            df.at[idx, 'Latitude'], df.at[idx, 'Longitude'] = final_lat, final_lon
            logging.info(f"[{well_id}] 处理成功")

        except Exception as e:
            logging.error(f"[{well_id}] 错误: {e}")
            failed_list.append(well_id)

    df.to_excel(OUTPUT_EXCEL, index=False)
    # 生成 Shapefile
    valid = df.dropna(subset=['Latitude', 'Longitude'])
    if not valid.empty:
        gpt.GeoDataFrame(valid, geometry=[Point(xy) for xy in zip(valid.Longitude, valid.Latitude)]).to_file(r"D:\tmp\wells.shp")


if __name__ == "__main__":
    main()