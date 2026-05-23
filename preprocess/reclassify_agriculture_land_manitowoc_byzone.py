import numpy as np
from osgeo import gdal


def subdivide_landuse_gdal_fixed(lu_path, target_values, zone_path, output_path):
    """
    使用 GDAL 实现：根据分区栅格中心坐标匹配，细分特定的土地利用类型。
    修复了 gdal.InvGeoTransform 在不同版本下的返回值解包问题。
    """
    # 1. 读取土地利用栅格
    ds_lu = gdal.Open(lu_path)
    if ds_lu is None: raise FileNotFoundError(f"无法打开文件: {lu_path}")

    gt_lu = ds_lu.GetGeoTransform()
    rb_lu = ds_lu.GetRasterBand(1)
    data_lu = rb_lu.ReadAsArray()
    nodata_lu = rb_lu.GetNoDataValue()

    # 2. 读取分区栅格
    ds_zone = gdal.Open(zone_path)
    if ds_zone is None: raise FileNotFoundError(f"无法打开文件: {zone_path}")

    gt_zone = ds_zone.GetGeoTransform()
    rb_zone = ds_zone.GetRasterBand(1)
    data_zone = rb_zone.ReadAsArray()
    nodata_zone = rb_zone.GetNoDataValue()

    # 3. 兼容性获取逆变换矩阵
    res = gdal.InvGeoTransform(gt_zone)
    if isinstance(res, tuple) and len(res) == 2:
        success, inv_gt_zone = res
        if not success: raise RuntimeError("逆矩阵计算失败")
    else:
        inv_gt_zone = res
        if inv_gt_zone is None: raise RuntimeError("逆矩阵计算失败")

    # 4. 寻找待处理像元的行列索引
    mask = np.isin(data_lu, list(target_values))
    rows, cols = np.where(mask)

    if len(rows) == 0:
        print("未找到指定的土地利用类型值。")
        return

    # 预准备输出数组 (使用 Int32)
    output_data = data_lu.astype(np.int32)
    z_rows_max, z_cols_max = data_zone.shape

    # 5. 遍历目标像元，进行空间坐标匹配
    # 预提取矩阵参数以微弱提升性能
    l0, l1, l2, l3, l4, l5 = gt_lu
    i0, i1, i2, i3, i4, i5 = inv_gt_zone

    for r, c in zip(rows, cols):
        # 计算土地利用栅格像元中心的地理坐标 (Xp, Yp)
        px = l0 + (c + 0.5) * l1 + (r + 0.5) * l2
        py = l3 + (c + 0.5) * l4 + (r + 0.5) * l5

        # 映射回分区栅格的行列号
        cz = int(i0 + px * i1 + py * i2)
        rz = int(i3 + px * i4 + py * i5)

        # 边界检查
        if 0 <= rz < z_rows_max and 0 <= cz < z_cols_max:
            z_val = data_zone[rz, cz]

            # 判断有效性：非 NoData 且在 1-9 范围内
            if z_val != nodata_zone and not np.isnan(z_val) and 1 <= z_val <= 9:
                original_val = data_lu[r, c]
                output_data[r, c] = int(original_val * 10 + z_val)

    # 6. 保存结果
    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(output_path, ds_lu.RasterXSize, ds_lu.RasterYSize, 1, gdal.GDT_Int32)
    out_ds.SetProjection(ds_lu.GetProjection())
    out_ds.SetGeoTransform(gt_lu)

    out_band = out_ds.GetRasterBand(1)
    if nodata_lu is not None:
        out_band.SetNoDataValue(nodata_lu)

    out_band.WriteArray(output_data)

    # 显式关闭
    out_band = None
    out_ds = None
    ds_lu = None
    ds_zone = None
    print(f"处理成功: {output_path}")


if __name__ == "__main__":
    subdivide_landuse_gdal_fixed(
            lu_path=r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\processlandcover\landcover_result.tif",
            target_values=[2110, 2111, 2112, 2113, 2114, 2120, 2121, 2122, 2123, 2130, 2131, 2132,
                          2133, 2134, 2135, 2136, 3110],
            zone_path=r"D:\data_m\manitowoc\groundwater\zone_by_karst_tiledrain.tif",
            output_path=r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\processlandcover\landcover_result_zone.tif"
    )

