# 我基于pymoo实现的nsga-II优化算法，对SWAT+模型的月径流模拟进行了自动率定，请你写出Python代码分析参与率定的参数在高NSE解集中的取值范围。
# 1. 自动率定的所有中间数据都存在multi_runs文件夹下，以gen_<i>命名的文件夹对应不同进化代数，目前i从0到30
# 2. gen_<i>文件夹内有OutletsResults_<j>文件夹，j从1到1000，存有1000个个体的SWAT+模型结果，主要包括model_performance.json, 数据结果为
#     {"363375_flo_out_mon_cali_NSE": 0.87,
#     "363375_flo_out_mon_cali_RSR": 0.36,
#     "363375_flo_out_mon_cali_PBIAS": 5.48,
#     "363375_flo_out_mon_cali_R_square": 0.87}
# 我已经将每个OutletsResults文件夹中的model_performance.json读取并保存为model_performances_all.csv了，每个文件夹（个体）对应1行
# 3. gen_<i>文件夹还用pickle打包了当前遗传算法的状态algorithm_state_gen_<i>.pkl、当前代数的个体population_gen_<i>.npy，当前代数Pareto解集final_pareto_solutions_F_<i>.npy和final_pareto_solutions_X_<i>.npy以及pareto_front_gen_<i>.json
# 4. 我想以NSE>0.8 （即363375_flo_out_mon_cali_NSE）和 PBIAS（363375_flo_out_mon_cali_PBIAS）的绝对值小于10%，筛选所有代数的个体，然后对其对应的率定参数的值进行分析
# 5. 可以给出每个率定参数的缩小的取值范围，也可以给出这些率定参数的聚类中心，如果有多个聚类中心也可以
import os
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import silhouette_score
from pymoo.core.problem import Problem
from utils.optimization import SWATPlusProblem, get_algorithm

BASE_DIR = r'C:\Users\ljzhu\Downloads\cali_all\multi_runs'
CSV_FILENAME = 'model_performances_all.csv'
COL_SIM_INDEX = 'sim_index'

# 定义筛选条件列表
# op 支持: '>', '>=', '<', '<=', 'abs<', 'abs<=', '==', '!='
FILTER_CONFIG = [
    {
        "col": "usgs04085427_flo_out_day_cali_NSE",
        "op": ">",
        "val": 0.58
    },
    {
        "col": "usgs04085427_flo_out_day_cali_PBIAS",
        "op": "abs<=",
        "val": 6.0
    },
    {
        "col": "usgs04085427_flo_out_mon_cali_NSE",
        "op": ">",
        "val": 0.85
    },
    {
        "col": "usgs04085427_flo_out_mon_cali_PBIAS",
        "op": "abs<=",
        "val": 6.0
    },
    {
        "col": "363375_flo_out_mon_cali_NSE",
        "op": ">",
        "val": 0.85
    },
    {
        "col": "363375_flo_out_mon_cali_PBIAS",
        "op": "abs<=",
        "val": 6.0
    },
    {
        "col": "10020782_flo_out_mon_cali_NSE",
        "op": ">",
        "val": 0.85
    },
    {
        "col": "10020782_flo_out_mon_cali_PBIAS",
        "op": "abs<=",
        "val": 6.0
    },
    {
        "col": "363313_flo_out_mon_cali_NSE",
        "op": ">",
        "val": 0.55
    },
    {
        "col": "363313_flo_out_mon_cali_PBIAS",
        "op": "abs<=",
        "val": 6.0
    },
]

# 结果中需要保留并输出显示的列 (除了参数外)
# 用于最后生成 CSV 时保留 NSE 和 PBIAS 的具体数值
KEEP_METRICS_COLS = [
    "usgs04085427_flo_out_day_cali_NSE",
    "usgs04085427_flo_out_day_cali_RSR",
    "usgs04085427_flo_out_day_cali_PBIAS",
    "usgs04085427_flo_out_day_cali_R_square",
    "usgs04085427_flo_out_day_vali_NSE",
    "usgs04085427_flo_out_day_vali_RSR",
    "usgs04085427_flo_out_day_vali_PBIAS",
    "usgs04085427_flo_out_day_vali_R_square",
    "usgs04085427_flo_out_mon_cali_NSE",
    "usgs04085427_flo_out_mon_cali_RSR",
    "usgs04085427_flo_out_mon_cali_PBIAS",
    "usgs04085427_flo_out_mon_cali_R_square",
    "usgs04085427_flo_out_mon_vali_NSE",
    "usgs04085427_flo_out_mon_vali_RSR",
    "usgs04085427_flo_out_mon_vali_PBIAS",
    "usgs04085427_flo_out_mon_vali_R_square",
    "363375_flo_out_mon_cali_NSE",
    "363375_flo_out_mon_cali_RSR",
    "363375_flo_out_mon_cali_PBIAS",
    "363375_flo_out_mon_cali_R_square",
    "10020782_flo_out_mon_cali_NSE",
    "10020782_flo_out_mon_cali_RSR",
    "10020782_flo_out_mon_cali_PBIAS",
    "10020782_flo_out_mon_cali_R_square",
    "363313_flo_out_mon_cali_NSE",
    "363313_flo_out_mon_cali_RSR",
    "363313_flo_out_mon_cali_PBIAS",
    "363313_flo_out_mon_cali_R_square"
]


def get_param_names_from_pickle(gen=0):
    """从第0代的 pickle 中自动读取参数名"""
    pkl_path = os.path.join(BASE_DIR, f'gen_{gen}', f'algorithm_state_gen_{gen}.pkl')

    if not os.path.exists(pkl_path):
        print(f"错误: 找不到 pickle 文件 {pkl_path}，无法自动获取参数名。")
        return None

    try:
        with open(pkl_path, 'rb') as f:
            algorithm = pickle.load(f)

        # 这里直接访问你在 __init__ 中定义的 self.param_names
        if hasattr(algorithm.problem, 'param_names'):
            return algorithm.problem.param_names
        else:
            print("警告: 在 problem 对象中未找到 param_names 属性。")
            return None

    except AttributeError as e:
        print(f"Pickle 加载错误: {e}")
        print("提示: 请确保 SWATPlusProblem 类定义存在于当前脚本中。")
        return None
    except Exception as e:
        print(f"其他错误: {e}")
        return None

def apply_dynamic_filters(df, config):
    """
    根据配置列表动态生成筛选掩码
    """
    # 初始化一个全 True 的 mask
    mask = pd.Series([True] * len(df), index=df.index)

    for rule in config:
        col = rule['col']
        op = rule['op']
        val = rule['val']

        if col not in df.columns:
            print(f"Warning: 列 {col} 不存在于数据中，跳过该筛选条件。")
            continue

        # 根据操作符更新 mask
        if op == '>':
            mask &= (df[col] > val)
        elif op == '>=':
            mask &= (df[col] >= val)
        elif op == '<':
            mask &= (df[col] < val)
        elif op == '<=':
            mask &= (df[col] <= val)
        elif op == 'abs<':
            mask &= (df[col].abs() < val)
        elif op == 'abs<=':
            mask &= (df[col].abs() <= val)
        elif op == '==':
            mask &= (df[col] == val)
        elif op == '!=':
            mask &= (df[col] != val)
        else:
            print(f"Error: 未知的操作符 {op}")

    return mask

def main():
    # 1. 自动获取参数名称
    print(">>> 正在从 Pickle 文件读取参数名称...")
    param_names = get_param_names_from_pickle(0)

    if param_names is None:
        print("无法获取参数名，程序终止。")
        return

    print(f"成功读取 {len(param_names)} 个参数: {param_names}")

    # 2. 遍历每一代提取数据
    print(f"\n>>> 开始遍历数据文件夹 {BASE_DIR} ...")
    all_good_data = []

    # 自动检测有多少代 (gen_0, gen_1...)
    gen_folders = [d for d in os.listdir(BASE_DIR) if
                   d.startswith('gen_') and os.path.isdir(os.path.join(BASE_DIR, d))]
    # 按数字排序
    gen_folders.sort(key=lambda x: int(x.split('_')[1]))

    for folder_name in gen_folders:
        gen_id = int(folder_name.split('_')[1])
        gen_dir = os.path.join(BASE_DIR, folder_name)

        csv_path = os.path.join(gen_dir, CSV_FILENAME)
        npy_path = os.path.join(gen_dir, f'population_gen_{gen_id}.npy')

        if not os.path.exists(csv_path) or not os.path.exists(npy_path):
            continue

        try:
            # 读取性能 CSV
            df_perf = pd.read_csv(csv_path)
            # 筛选
            mask = apply_dynamic_filters(df_perf, FILTER_CONFIG)
            good_rows = df_perf[mask]
            if good_rows.empty:
                continue

            # 读取参数 NPY
            pop_data = np.load(npy_path, allow_pickle=True)
            X = pop_data

            # 匹配参数
            for _, row in good_rows.iterrows():
                sim_idx = int(row[COL_SIM_INDEX])
                array_idx = sim_idx - 1  # 1-based 转 0-based

                if array_idx < 0 or array_idx >= len(X):
                    continue

                vals = X[array_idx]

                record = {
                    'gen': gen_id,
                    'sim_index': sim_idx,
                }

                for metric_col in KEEP_METRICS_COLS:
                    if metric_col in row:
                        record[metric_col] = row[metric_col]

                # 动态添加参数值
                for k, name in enumerate(param_names):
                    record[name] = vals[k]

                all_good_data.append(record)

        except Exception as e:
            print(f"处理 {folder_name} 时出错: {e}")

    # 转 DataFrame
    df_final = pd.DataFrame(all_good_data)

    if df_final.empty:
        print("未找到符合条件的优质解。")
        return

    print(f"\n>>> 提取完成。共找到 {len(df_final)} 个优质解")

    # ================= 3. 统计与聚类分析 =================

    # 仅分析参数列
    df_params = df_final[param_names]

    # --- A. 范围统计 ---
    print("\n[结果 1] 参数置信区间 (90% Bounds)")
    stats = df_params.describe(percentiles=[0.05, 0.5, 0.95]).T
    stats['width'] = stats['95%'] - stats['5%']
    print(stats[['5%', '95%', 'width', 'std']])
    stats.to_csv(BASE_DIR + os.sep + 'final_param_ranges.csv')

    # --- B. 聚类分析 ---
    print("\n[结果 2] 聚类分析 (寻找多解模式)")
    if len(df_final) > 15:
        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(df_params)

        # 寻找最佳 K
        best_k = 2
        best_score = -1
        for k in range(2, min(6, len(df_final) // 2)):
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(X_scaled)
            score = silhouette_score(X_scaled, labels)
            if score > best_score:
                best_score = score
                best_k = k

        print(f"最佳聚类数: {best_k} (Silhouette: {best_score:.3f})")

        # 最终聚类
        final_km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        df_final['Cluster'] = final_km.fit_predict(X_scaled)

        # 还原中心点
        centers = scaler.inverse_transform(final_km.cluster_centers_)
        center_df = pd.DataFrame(centers, columns=param_names)
        center_df.index.name = 'Cluster_ID'

        print(center_df)
        center_df.to_csv(BASE_DIR + os.sep + 'final_cluster_centers.csv')
        df_final.to_csv(BASE_DIR + os.sep + 'final_high_performance_solutions.csv', index=False)
    else:
        print("样本太少，跳过聚类。")

    # --- C. 绘图 ---
    print("\n>>> 正在绘制箱线图...")
    plt.figure(figsize=(14, 6))

    # 归一化画图
    df_norm = (df_params - df_params.min()) / (df_params.max() - df_params.min())
    df_melt = df_norm.melt(var_name='Parameter', value_name='Normalized Value')

    sns.boxplot(x='Parameter', y='Normalized Value', data=df_melt)
    plt.title("Normalized Parameter Ranges for High NSE Solutions")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(BASE_DIR + os.sep + 'final_param_boxplot.png', dpi=300)
    print("完成。图片已保存至 final_param_boxplot.png")


if __name__ == "__main__":
    main()