import json
import random
import os
import sys
import pathlib

# --- 1. 从您的示例中复制所有指标的键 ---
# (确保这个列表与您的 config.py 中定义的目标一致)
METRIC_KEYS = [
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


def generate_random_metrics(keys_list):
    """
    (这是您请求的核心函数)
    为给定的键列表生成一个包含随机指标的字典。
    """
    results = {}
    for key in keys_list:
        value = 0.0

        # 根据指标名称，分配“合理”的随机值范围
        if "NSE" in key:
            # NSE: 范围 -inf 到 1。较差的模型可能在 -2.0 到 0.5 之间
            value = random.uniform(-2.5, 0.95)
        elif "PBIAS" in key:
            # PBIAS: 百分比偏差，可正可负
            value = random.uniform(-100.0, 100.0)
        elif "RSR" in key:
            # RSR: 0 (完美) 到 +inf。1.0 以下通常被认为不错
            value = random.uniform(0.1, 2.5)
        elif "R_square" in key:
            # R²: 0 到 1
            value = random.uniform(0.0, 1.0)
        else:
            # 其他任何未识别的键
            value = random.uniform(0.0, 1.0)

        # 将结果四舍五入到2位小数，使其看起来更真实
        results[key] = round(value, 2)

    return results


def simulate_model_runs(n_pop):
    """
    模拟整个种群的运行。
    为 N_POP 个体创建 'sim_i/results.json'。
    """
    print(f"--- 模拟模型运行 ---")
    print(f"正在生成 {n_pop} 个模拟结果文件夹 ('sim_1' 到 'sim_{n_pop}')...")

    for i in range(n_pop):
        sim_id = i + 1
        sim_dir = f"sim_{sim_id}"
        output_path = os.path.join(sim_dir, "results.json")

        # 1. 确保目标文件夹存在
        os.makedirs(sim_dir, exist_ok=True)

        # 2. 生成随机指标数据
        mock_data = generate_random_metrics(METRIC_KEYS)

        # 3. 将数据写入 JSON 文件
        try:
            with open(output_path, 'w') as f:
                json.dump(mock_data, f, indent=4)
            print(f"-> 已创建: {output_path}")
        except IOError as e:
            print(f"!! 写入文件时出错 {output_path}: {e}")


# --- 主执行 ---
if __name__ == "__main__":
    # Calibration file
    cal_file = sys.argv[1]
    # Result folder for extracted simulation results and calculated model performances
    results_dir = sys.argv[2]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = pathlib.Path(results_dir).resolve()
    os.makedirs(results_dir, exist_ok=True)

    mock_data = generate_random_metrics(METRIC_KEYS)
    json_file = results_dir / 'model_performance.json'

    with open(json_file, 'w') as output_write:
        json.dump(mock_data, output_write, indent=4)
