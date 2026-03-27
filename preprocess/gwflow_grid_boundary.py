import geopandas as gpd
import numpy as np
import pandas as pd

def detect_boundary2(bsn_boundary_shp, grid_shp, out_grid_shp):
    # 1. 读取数据
    borders_gdf = gpd.read_file(bsn_boundary_shp)
    grid_gdf = gpd.read_file(grid_shp)

    # 确保坐标系一致
    if borders_gdf.crs != grid_gdf.crs:
        print("坐标系不一致，正在转换...")
        grid_gdf = grid_gdf.to_crs(borders_gdf.crs)

    # 初始化 boundary 列
    grid_gdf['boundary'] = 0

    # ---------------------------------------------------------
    # 核心修改部分
    # ---------------------------------------------------------

    # 步骤 A: 创建向内收缩的“内部面”
    # distance=-1e-5 (约等于0.01毫米或更小，取决于投影)
    inner_basin = borders_gdf.copy()
    inner_basin['geometry'] = inner_basin.buffer(-1e-5)

    # 步骤 B: 筛选出“位于流域内部”的格网 (第一次 sjoin)
    cells_inside = gpd.sjoin(grid_gdf, inner_basin, how='inner', predicate='intersects')

    # 【关键修复】：删除第一次 sjoin 产生的 index_right 列
    # 否则第二次 sjoin 会因为列名冲突报错
    cells_inside = cells_inside.drop(columns=['index_right'])

    # 步骤 C: 获取原始边界线
    boundary_line_gdf = borders_gdf.copy()
    boundary_line_gdf['geometry'] = boundary_line_gdf.boundary

    # 步骤 D: 在“内部格网”中，查找压盖“原始边界线”的格网 (第二次 sjoin)
    true_boundary_cells = gpd.sjoin(cells_inside, boundary_line_gdf, how='inner',
                                    predicate='intersects')

    valid_indices = true_boundary_cells.index.unique()
    grid_gdf.loc[valid_indices, 'boundary'] = 1

    # 4. 保存结果
    print(f"检测到 {len(valid_indices)} 个边界格网。正在保存...")
    grid_gdf.to_file(out_grid_shp)




def detect_boundary(bsn_boundary_shp, grid_shp, out_grid_shp):
    # Boundary Cell Information
    borders_gdf = gpd.read_file(bsn_boundary_shp)
    grid5_gdf = gpd.read_file(grid_shp)

    # first, create an internal boundary by contracting inwards 1e-5 unit (hopefully using a projected coordinate system)
    inner_basin = borders_gdf.copy()
    inner_basin['geometry'] = inner_basin.buffer(-1e-5)
    # second, get grids inside the contracted basin boundary
    cells_inside = gpd.sjoin(grid5_gdf, inner_basin, how='inner', predicate="intersects")
    del cells_inside['index_right']

    borders_gdf['geometry'] = borders_gdf.boundary
    # Getting the geometry of only the boundaries of the catchment
    borders_gdf['boundary'] = 1
    true_boundary_cells = gpd.sjoin(cells_inside, borders_gdf, how='inner', predicate='intersects')
    valid_indices = true_boundary_cells.index.unique()
    grid6_gdf = grid5_gdf.copy()
    grid6_gdf['boundary'] = 0
    grid6_gdf.loc[valid_indices, 'boundary'] = 1


    # GRID 6 CLEAN UP
    # Necessary clean up to replace nan values to 0, and deleting index_right to avoid warnings of attribute name truncation
    #grid6_gdf['boundary'] = grid6_gdf['boundary'].fillna(0)
    grid6_gdf['Avg_Thick'] = grid6_gdf['Avg_Thick'].fillna(0)
    grid6_gdf['Avg_elevat'] = grid6_gdf['Avg_elevat'].fillna(0)
    #del grid6_gdf['index_right']

    grid6_gdf.to_file(out_grid_shp)

def detect_active(bsn_boundary_shp, grid_shp, out_grid_shp):
    # Recognize active cells
    # Create new geodataframe called Grid 2 that is the same as Grid 1 for now
    basin = gpd.read_file(bsn_boundary_shp)
    grid1 = gpd.read_file(grid_shp)
    grid2 = grid1.copy()
    # Create the atributte Avg_active in grid 2 and the basin (At the moment, at the grid everything is 0)
    grid2['Avg_active'] = 0
    basin['Avg_active'] = 1

    # first, create an internal boundary by contracting inwards 1e-5 unit (hopefully using a projected coordinate system)
    inner_basin = basin.copy()
    inner_basin['geometry'] = inner_basin.buffer(-1e-5)
    # second, get grids inside the contracted basin boundary
    grid_join = gpd.sjoin(grid2, inner_basin, how='left', predicate="intersects")
    #del cells_inside['index_right']


    # Spatial join attributes from grid1 and the basin creating a new geodataframe from its combination (grid_join will repeat grid1 geometry, but get the basins attributes)
    # With this, all the cells that intersect the basin will now have a new attribute that is equal to 1, while for the rest the attribute value will be nan
    # grid_join = gpd.sjoin(grid2, basin, how="left", predicate='intersects')
    # Get the avg active values from the joined geodataframe and save into array
    active_array = grid_join['Avg_active_right'].to_numpy()  # This will take an array of the avg_active attribute for the positions where cells intersect the basin as 1, and the rest as nan
    active_array = np.nan_to_num(active_array, nan=0)  # This will change al nan in the array, to 0
    # Create array from Id values
    id_array = grid_join['Id'].to_numpy()
    # Create new empty dataframe
    grid2_df = pd.DataFrame()
    # Assign avg_active array values to data frame, as well as Ids
    grid2_df['Avg_active'] = active_array.tolist()
    grid2_df['Id'] = id_array.tolist()
    # If there is repetition, for example a cell intersects the basin twice (its in the border between 2 sub-catchmetnts) the cell will be duplicated
    # The next line of code merges the duplicated cells and takes the average of the numerical attributes (in this case if avg_active is 1, it will stay as 1 after the average)
    grid2_df = grid2_df.groupby('Id').mean().reset_index()
    # We assign the avg_active corrected values from the dataframe, into the GeoDataFrame called Grid 2
    grid2['Avg_active'] = grid2_df['Avg_active']
    # We save grid 2 as a new Shapefile
    grid2.to_file(out_grid_shp)


if __name__ == '__main__':
    gwflow_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\gwflow'
    basin_shp = '%s/supplementary/only_basin_boundary.shp' % gwflow_dir
    grid1_shp = '%s/grids/grid1.shp' % gwflow_dir
    grid2_shp = '%s/grids/grid2New.shp' % gwflow_dir
    detect_active(basin_shp, grid1_shp, grid2_shp)

    grid_shp = '%s/grids/grid5Copy.shp' % gwflow_dir
    gridnew_shp = '%s/grids/grid6New2.shp' % gwflow_dir

    # detect_boundary(basin_shp, grid_shp, gridnew_shp)
