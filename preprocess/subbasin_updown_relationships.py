# 用Python写程序，具体要求如下：
# 1. 输入是两个Shapefile，一个是河道数据stream.shp，一个是子流域数据subbasin.shp，每一条河道均对应一个子流域；
# 2. 读取stream.shp的2个字段：LINKNO和DSLINKNO，LINKNO是河道编号，DSLINKNO是下游河道编号，如果没有下游河道，则DSLINKNO等于-1，由此可构建河道的上下游关系；
# 3. 读取subbasin.shp的PolygonID和Subbasin两个字段，PolygonID对应于stream.shp中的LINKNO，此时，将上一步构建的河道上下游关系中的河道编号替换为Subbasin；
# 4. 利用JSON存储河道上下游关系数据，每个河道均有上游河道（1个或多个）和下游河道（1个或0个），并保存为subbasin_updown_relationship.json；
# 5. 程序允许用户指定1个或多个子流域编号，计算输出每个子流域的所有上游子流域编号，如果给定的子流域编号中存在嵌套情况，如子流域3是子流域5的上游，则输出子流域5的所有上游子流域时、排除所有子流域3的所有上游子流域（也包括子流域3本身）

# [59, 42, 15, 31]

import geopandas as gpd
import json
import os
from collections import deque


def find_all_upstream(start_subbasin, upstream_map):
    """
    Uses Breadth-First Search (BFS) to find all upstream subbasins
    for a given starting subbasin.

    Args:
    start_subbasin: The ID of the starting subbasin.
    upstream_map: A dictionary storing {subbasin: [immediate_upstream_subbasins]}

    Returns:
    A set containing all upstream subbasin IDs.
    """
    all_upstream = set()

    # Check if the starting subbasin exists in the topology map
    if start_subbasin not in upstream_map:
        return all_upstream

    # Use a deque (double-ended queue) for efficient BFS
    # The queue starts with the *immediate* upstream neighbors of the target subbasin
    queue = deque(upstream_map[start_subbasin])

    while queue:
        # Get the next subbasin from the left side of the queue
        current_sub = queue.popleft()

        # If this subbasin hasn't been processed yet
        if current_sub not in all_upstream:
            # 1. Add it to the set of all upstream subbasins
            all_upstream.add(current_sub)

            # 2. Add its *immediate* upstream neighbors to the right side of the queue
            #    for future processing.
            #    Use .get(current_sub, []) to safely handle headwater subbasins
            #    that have no upstream neighbors.
            for up_sub in upstream_map.get(current_sub, []):
                if up_sub not in all_upstream:
                    queue.append(up_sub)

    return all_upstream


def calculate_and_print_upstream(user_specified_subbasins, all_subbasins, upstream_relationship):
    """
    (Step 5)
    Calculates and prints all upstream subbasins for a specified list,
    handling nested cases by excluding upstream members of other specified subbasins.

    Args:
    user_specified_subbasins (list): The list of subbasin IDs to query.
    all_subbasins (set): A set of all valid subbasin IDs in the topology.
    upstream_relationship (dict): The topology map {sub: [immediate_upstream_subs]}.
    """
    print("\n--- 正在计算指定子流域的上游 ---")

    if not user_specified_subbasins:
        print("未指定子流域，跳过步骤 5。")
        return

    # 5a. First, calculate the *complete* set of upstream subbasins for each specified ID
    all_upstream_sets = {}
    valid_input_subs = []
    for sub_id in user_specified_subbasins:
        if sub_id not in all_subbasins:
            print(f"警告: 子流域 '{sub_id}' 不在拓扑图中，将跳过。")
            continue
        valid_input_subs.append(sub_id)
        # Call the helper function defined earlier
        all_upstream_sets[sub_id] = find_all_upstream(sub_id, upstream_relationship)

    # 5b. Apply the exclusion rule
    final_output_upstream = {}
    for target_sub in valid_input_subs:
        # Copy the full upstream set to modify it
        current_upstream_set = set(all_upstream_sets[target_sub])

        # Check against all *other* specified subbasins
        for other_sub in valid_input_subs:
            if target_sub == other_sub:
                continue

            # Core rule: If 'other_sub' is in 'target_sub's upstream list...
            if other_sub in current_upstream_set:
                # ...then remove 'other_sub' itself
                current_upstream_set.discard(other_sub)

                # ...and also remove *all* of 'other_sub's upstream subbasins
                current_upstream_set.difference_update(all_upstream_sets[other_sub])

        final_output_upstream[target_sub] = current_upstream_set

    # 5c. Print the final results
    print("\n--- 最终计算结果 (已应用排除规则) ---")
    for sub_id, upstream_set in final_output_upstream.items():
        # Try to sort numerically if possible, otherwise sort as strings
        try:
            sorted_list = sorted(list(int(x) for x in upstream_set))
        except ValueError:
            sorted_list = sorted(list(upstream_set))

        print(f"子流域 {sub_id} 的所有上游: {sorted_list if sorted_list else '无'}")


def process_watershed_topology(stream_shp_path, subbasin_shp_path, output_dir,
                               user_specified_subbasins):
    """
    Main function to execute all steps of processing the watershed topology.

    Args:
    stream_shp_path (str): Path to the stream.shp file.
    subbasin_shp_path (str): Path to the subbasin.shp file.
    output_dir (str): Path to the folder where the output JSON will be saved.
    user_specified_subbasins (list): A list of subbasin IDs to query.
    """

    # --- Steps 1 & 2: Read Shapefiles and build stream topology ---
    print(f"正在读取 {stream_shp_path} 和 {subbasin_shp_path}...")
    try:
        gdf_stream = gpd.read_file(stream_shp_path)
        gdf_subbasin = gpd.read_file(subbasin_shp_path)
    except Exception as e:
        print(f"文件读取错误: {e}")
        print("请确保文件路径正确，并且已安装 geopandas 及其依赖 (如 fiona)。")
        return

    # Check for required columns
    required_stream_cols = ['LINKNO', 'DSLINKNO']
    required_subbasin_cols = ['PolygonId', 'Subbasin']

    if not all(col in gdf_stream.columns for col in required_stream_cols):
        print(f"错误: 'stream.shp' 必须包含字段: {required_stream_cols}")
        return
    if not all(col in gdf_subbasin.columns for col in required_subbasin_cols):
        print(f"错误: 'subbasin.shp' 必须包含字段: {required_subbasin_cols}")
        return

    print("文件读取成功。")

    # --- Step 3: Build LINKNO to Subbasin mapping and convert to subbasin topology ---
    print("正在构建子流域拓扑关系...")

    # Create a mapping: {PolygonID: Subbasin}
    try:
        linkno_to_subbasin_map = gdf_subbasin.set_index('PolygonId')['Subbasin'].to_dict()
    except Exception as e:
        print(f"构建映射失败: {e}。请检查 'subbasin.shp' 中的 'PolygonID' 是否有重复值。")
        return

    # Initialize downstream and upstream relationship dictionaries
    downstream_relationship = {}  # {subbasin: downstream_subbasin}
    upstream_relationship = {}  # {subbasin: [list_of_upstream_subbasins]}

    # Initialize all known subbasins
    all_subbasins = set(linkno_to_subbasin_map.values())
    for sub in all_subbasins:
        upstream_relationship[sub] = []

    # Iterate over the stream data (stream.shp)
    for _, row in gdf_stream.iterrows():
        current_linkno = row['LINKNO']
        ds_linkno = row['DSLINKNO']

        current_sub = linkno_to_subbasin_map.get(current_linkno)
        if not current_sub:
            continue

        ds_sub = None
        if ds_linkno != -1:
            ds_sub = linkno_to_subbasin_map.get(ds_linkno)

        downstream_relationship[current_sub] = ds_sub

        if ds_sub in upstream_relationship:
            upstream_relationship[ds_sub].append(current_sub)
        elif ds_sub is not None:
            print(
                f"警告：下游河道 {ds_linkno} 对应的子流域 {ds_sub} 未在 'subbasin.shp' 中找到 PolygonId。")

    print("拓扑关系构建完成。")

    # --- Step 4: Store relationship data in JSON format ---
    os.makedirs(output_dir, exist_ok=True)
    json_output_filename = "subbasin_updown_relationship.json"
    json_output_path = os.path.join(output_dir, json_output_filename)

    print(f"正在将拓扑关系保存到 {json_output_path}...")

    full_topology_json = {}
    for sub in all_subbasins:
        full_topology_json[sub] = {
            "downstream": downstream_relationship.get(sub),
            "upstream": upstream_relationship.get(sub, [])
        }

    try:
        with open(json_output_path, 'w', encoding='utf-8') as f:
            json.dump(full_topology_json, f, indent=4, ensure_ascii=False, sort_keys=True)
        print("JSON 文件保存成功。")
    except Exception as e:
        print(f"JSON 文件保存失败: {e}")

    # --- Step 5: Calculate and output upstream subbasins (handling nesting) ---
    # Call the dedicated function for this step
    calculate_and_print_upstream(
            user_specified_subbasins,
            all_subbasins,
            upstream_relationship
    )


# --- Main execution block ---
if __name__ == "__main__":
    # --- User Configuration ---

    # 1. Define file paths
    SHP_FOLDER = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Watershed\Shapes"  # Use "." for the same directory as the script
    STREAM_SHP = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Watershed\Shapes\manitowoc_basin_30m_clip_newstream\manitowoc_basin_30m_clip_newstream.shp"
    SUBBASIN_SHP = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Watershed\Shapes\manitowoc_basin_30m_clip_newsubbasins.shp"

    # 2. !! 指定输出文件夹 !!
    OUTPUT_DIRECTORY = r"D:\data_m\manitowoc_test30m\manitowoc_test30mv4\subbasin_updown_relationships"

    # 3. !! 在这里指定您要查询的子流域编号 !!
    SPECIFIED_SUBBASINS = [59, 42, 15, 31]

    # --- End of Configuration ---

    # Execute the main function
    process_watershed_topology(
            STREAM_SHP,
            SUBBASIN_SHP,
            OUTPUT_DIRECTORY,
            SPECIFIED_SUBBASINS
    )