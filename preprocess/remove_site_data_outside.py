import pandas as pd
import os


def filter_output_files(sites_whitelist_file: str, target_folder: str):
    """
    根据一个站点白名单文件，筛选并删除一个文件夹中不需要的站点CSV文件。

    Args:
        sites_whitelist_file (str): 包含需要保留的站点编号的CSV文件路径。
        target_folder (str): 存放了之前生成的站点CSV文件的文件夹路径。
    """
    print(f"--- 开始根据白名单 '{sites_whitelist_file}' 筛选文件夹 '{target_folder}' ---")

    # --- 1. 读取站点白名单 ---
    if not os.path.exists(sites_whitelist_file):
        print(f"错误: 站点白名单文件未找到 -> {sites_whitelist_file}")
        return

    try:
        sites_df = pd.read_csv(sites_whitelist_file, dtype=str)
        # 假设站点编号在 'MonitoringLocationIdentifier' 列中
        # 如果列名不同，请在此处修改
        site_id_column = 'MonitoringLocationIdentifier'
        if site_id_column not in sites_df.columns:
            # 尝试备用列名
            site_id_column = 'Monitoring Location ID'
            if site_id_column not in sites_df.columns:
                print(
                    f"错误: 在白名单文件中找不到站点编号列。请确保列名为 'MonitoringLocationIdentifier' 或 'Monitoring Location ID'。")
                return

        # 将所有需要保留的站点编号存入一个Set中，以便快速查找
        sites_to_keep = set(sites_df[site_id_column].unique())
        print(f"成功读取 {len(sites_to_keep)} 个需要保留的站点编号。")

    except Exception as e:
        print(f"错误: 读取白名单文件时失败 -> {e}")
        return

    # --- 2. 遍历目标文件夹并进行筛选 ---
    if not os.path.isdir(target_folder):
        print(f"错误: 目标文件夹不存在 -> {target_folder}")
        return

    files_in_folder = os.listdir(target_folder)
    deleted_count = 0
    kept_count = 0

    print("\n开始检查文件...")
    for filename in files_in_folder:
        # 只处理 .csv 文件
        if filename.endswith('.csv'):
            # 从文件名（例如 "USGS-123456.csv"）中提取站点编号
            site_id_from_filename = os.path.splitext(filename)[0]

            # 检查站点编号是否在白名单中
            if site_id_from_filename not in sites_to_keep:
                # 如果不在，则删除文件
                file_path_to_delete = os.path.join(target_folder, filename)
                try:
                    os.remove(file_path_to_delete)
                    print(f"  [-] 已删除: {filename} (站点不在白名单中)")
                    deleted_count += 1
                except OSError as e:
                    print(f"  [!] 删除文件失败: {filename}. 错误: {e}")
            else:
                # 如果在，则保留文件
                # print(f"  [+] 已保留: {filename}")
                kept_count += 1

    print("\n--- 筛选完成！ ---")
    print(f"总共保留了 {kept_count} 个文件。")
    print(f"总共删除了 {deleted_count} 个文件。")


# --- 如何使用这个脚本 ---
if __name__ == '__main__':
    # ========================== 用户配置 ==========================

    # 1. 包含站点“白名单”的文件
    whitelist_csv = r'C:\Users\ljzhu\Downloads\wqx_processed\sites_shp\sites_within_manitowoc.csv'

    # 2. 之前生成所有站点CSV文件的文件夹
    #    请确保这个路径与您上一个脚本的输出路径一致
    output_directory = r'C:\Users\ljzhu\Downloads\wqx_processed'

    # =============================================================

    # 调用筛选函数
    filter_output_files(whitelist_csv, output_directory)