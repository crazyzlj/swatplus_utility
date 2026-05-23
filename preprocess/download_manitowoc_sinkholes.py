import requests
import geopandas as gpd
import json

# 构建请求全量数据的 URL
url = "https://services5.arcgis.com/ceC3pbsIeU7iCdFg/arcgis/rest/services/Areas_Contributing_Runoff_to_Direct_Conduits_to_Groundwater_new2/FeatureServer/0/query"
params = {
    "where": "1=1",
    "outFields": "*",
    "returnGeometry": "true",
    "f": "geojson",
    "outSR": "4326"
}

print("正在从 ArcGIS Feature Server 抓取天坑空间数据...")
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    features_count = len(data.get('features', []))
    print(f"成功获取数据！共包含 {features_count} 个天坑/汇水区多边形。")

    # 检查是否触发了最大记录限制
    if data.get('exceededTransferLimit'):
        print(
            "警告：触发了服务器的最大返回数量限制，数据可能不完整（需要分页抓取）。建议使用方案二中的 QGIS 加载。")

    # 将 JSON 保存到本地
    with open('manitowoc_sinkholes.geojson', 'w') as f:
        json.dump(data, f)

    # 可选：如果安装了 geopandas，直接转换为 Shapefile
    try:
        gdf = gpd.read_file('manitowoc_sinkholes.geojson')
        gdf.to_file('manitowoc_sinkholes.shp')
        print("已成功转换为 Shapefile (manitowoc_sinkholes.shp)！")
    except ImportError:
        print("未安装 geopandas，保留 GeoJSON 格式。")
else:
    print(f"请求失败，状态码: {response.status_code}")