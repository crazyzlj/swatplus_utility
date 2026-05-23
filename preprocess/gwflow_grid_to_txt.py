import geopandas as gpd
import pandas as pd


def fishnet_to_formatted_text(shp_path, id_field, value_field, out_txt_path, ncols=None):
    """
    将规则格网Shapefile的指定字段转换为格式化文本。

    参数:
    shp_path (str): Shapefile的路径
    id_field (str): 唯一ID字段名 (用于排序)
    value_field (str): 要输出的字段名
    out_txt_path (str): 输出文本文件路径
    ncols (int, optional): 如果需要还原为二维空间矩阵，请提供格网的列数。
                           默认为None（即每行只输出一个值）。
    """
    print(f"正在读取文件: {shp_path} ...")

    try:
        # 1. 加载Shapefile
        gdf = gpd.read_file(shp_path)

        # 2. 字段校验
        if id_field not in gdf.columns:
            raise ValueError(f"错误：Shapefile中未找到ID字段 '{id_field}'。")
        if value_field not in gdf.columns:
            raise ValueError(f"错误：Shapefile中未找到目标字段 '{value_field}'。")

        # 3. 按唯一ID排序，严格保证逐行输出顺序
        # 注：假设ID是数值型。如果是字符串型的数字（如"1", "10", "2"），直接排序会变成"1", "10", "2"。
        # 为确保安全，可强制转换为数值进行排序
        gdf['temp_sort_id'] = pd.to_numeric(gdf[id_field], errors='coerce')
        gdf_sorted = gdf.sort_values(by='temp_sort_id')

        # 4. 格式化并写入文本
        with open(out_txt_path, 'w', encoding='utf-8') as f:
            count = 0
            for val in gdf_sorted[value_field]:
                # 处理空值 (NoData)
                if pd.isna(val):
                    val_str = "0"  # 流域模型常用的NoData值
                else:
                    # 尝试去除浮点数末尾的 '.0' 以符合 "     1" 这样的整型示例
                    try:
                        if val>0:
                            val = val - 1.0
                        if float(val).is_integer():
                            val_str = str(int(float(val)))
                        else:
                            val_str = "%.3f" % val
                    except ValueError:
                        val_str = str(val)  # 处理本身就是文本的情况

                # 核心格式化：右对齐，占6个字符
                # 注意：如果数值本身长度超过6（例如"1234567"），Python默认会完整保留而不截断，这可能会破坏对齐。
                formatted_str = f"{val_str:>10}"

                # 5. 写入逻辑 (1D 列表 vs 2D 矩阵)
                if ncols is None:
                    # 默认情况：一维输出，每个值占一行
                    f.write(formatted_str + '\n')
                else:
                    # 二维矩阵输出：在同行写入，达到列数后换行
                    f.write(formatted_str)
                    count += 1
                    if count % ncols == 0:
                        f.write('\n')

        print(f"处理完成。数据已成功导出至: {out_txt_path}")

    except Exception as e:
        print(f"程序执行中断: {e}")


# ==========================================
# 调用示例
# ==========================================
if __name__ == "__main__":
    # 请替换为您的实际路径和字段名
    SHAPEFILE_PATH = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv5\gwflow\grids\grid6_sinkhole.shp"
    ID_FIELD = "Id"
    VALUE_FIELD = "elev_rev"
    ncols = 129
    OUTPUT_TEXT_PATH = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-modified\initialhead.txt"

    # 模式1：每个值占一行输出
    # fishnet_to_formatted_text(SHAPEFILE_PATH, ID_FIELD, VALUE_FIELD, OUTPUT_TEXT_PATH)

    # 模式2：如果您需要还原2D网格（例如模型需要ASCII Grid格式输入），可以传入列数
    fishnet_to_formatted_text(SHAPEFILE_PATH, ID_FIELD, VALUE_FIELD, OUTPUT_TEXT_PATH, ncols=ncols)