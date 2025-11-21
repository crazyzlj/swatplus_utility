from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import pathlib
import numpy as np
import pickle
import json
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import pandas as pd

from pymoo.core.problem import Problem
from pymoo.core.population import Population
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.algorithms.moo.nsga3 import NSGA3
from pymoo.util.ref_dirs import get_reference_directions
from collections import Counter

from utils import RUNS_BASE_DIR
from utils.iterative_job import get_current_generation, check_continue, SIGNAL_FILE, update_generation

from config import N_POP, MAX_GENERATIONS, OBJECTIVES, WORKER_DAG_TEMPLATE, POP_FILE, GENSTATE_FILE
from config import MIN_NSE, MAX_PBIAS, WORST_VALUE, COMBINED_WEIGHTS, PENALTIES

def save_pareto_to_json(algorithm, obj_labels, output_filename):
    """
    将当前代的 Pareto (非支配) 解保存到 JSON 文件中。
    """

    # 1. 获取当前的 Result 对象
    try:
        current_result = algorithm.result()
    except Exception as e:
        print(f"WARNING: 无法获取算法结果，跳过保存 JSON. Error: {e}")
        return

    # 2. 从 Result 对象中提取 Pareto 解
    pareto_X = current_result.X  # 参数 (numpy 数组)
    pareto_F = current_result.F  # 目标 (numpy 数组)

    solutions_list = []

    # 3. 检查是否有解
    if pareto_X is None or pareto_F is None or len(pareto_X) == 0:
        print(f"在 {output_filename} 中未找到 Pareto 解。保存空列表。")
        with open(output_filename, 'w') as f:
            json.dump([], f, indent=4)
        return

    print(f"正在将 {len(pareto_X)} 个 Pareto 解保存到 {output_filename}...")

    # 4. 遍历每个非支配解
    for i in range(len(pareto_X)):
        solution_X = pareto_X[i]
        solution_F = pareto_F[i]

        # 5. (!! 关键修改 !!) 创建 "parameters" 字典 (使用序号)
        #    使用 "param_0", "param_1", ... 作为键
        params_dict = {f"param_{idx}": val.item() for idx, val in enumerate(solution_X)}

        # 6. 创建 "objectives" 字典 (不变)
        objs_dict = {label: val.item() for label, val in zip(obj_labels, solution_F)}

        # 7. 组合成一个条目
        solution_entry = {
            "individual_id": i,
            "parameters": params_dict,
            "objectives": objs_dict
        }
        solutions_list.append(solution_entry)

    # 8. 将所有解的列表写入 JSON 文件
    try:
        with open(output_filename, 'w') as f:
            json.dump(solutions_list, f, indent=4)
        print(f"成功保存 {output_filename}")
    except Exception as e:
        print(f"WARNING: 写入 Pareto JSON 文件时出错: {e}")

def combined_score(nse, pbias,
                   nse_min=-1.0,
                   pbias_max=30.0,
                   w_nse=0.6,
                   w_pbias=0.4,
                   worst_value=99.):
    """
    Combine NSE and PBIAS into one metric (higher is better).

    Parameters
    ----------
    nse : float
    pbias : float
    nse_min : float
        Minimum acceptable NSE. Below this returns a very bad score.
    pbias_max : float
        Maximum acceptable absolute PBIAS (%).
    w_nse : float
        Weight for NSE.
    w_pbias : float
        Weight for PBIAS.

    Returns
    -------
    score : float
        Larger values mean better performance.
    """

    # ---------- hard constraint ----------
    if nse < nse_min or abs(pbias) > pbias_max:
        return worst_value

    # ---------- normalize NSE ----------
    nse_norm = (nse - nse_min) / (1 - nse_min)
    nse_norm = max(0, min(1, nse_norm))

    # ---------- normalize PBIAS ----------
    pbias_norm = 1 - min(1.0, abs(pbias) / pbias_max)

    # ---------- combine ----------
    score = w_nse * nse_norm + w_pbias * pbias_norm

    return score

def read_simulation_results(gen_dir):
    """
    (重大修改)
    循环读取每个 sim_i 文件夹中的 results.json，
    并根据 config.OBJECTIVES 提取和转换目标函数值。
    """
    print(f"Reading simulation results based on config.OBJECTIVES...")

    all_objectives_list = []  # 存储所有个体的目标值
    all_penalties_list = []

    # 从配置中获取目标定义
    objectives_config = OBJECTIVES
    n_pop = N_POP

    data_rows = []

    for i in range(1, n_pop + 1):
        sim_dir = gen_dir / f'OutletsResults_{i}'
        result_file = sim_dir / 'model_performance.json'

        if not os.path.exists(result_file):
            print(f"FATAL ERROR: Result file not found for {sim_dir}")
            raise FileNotFoundError(f"Missing results file: {result_file}")

        with open(result_file, 'r') as f:
            try:
                sim_data = json.load(f)
            except json.JSONDecodeError:
                print(f"FATAL ERROR: Could not decode JSON in {result_file}")
                raise

        # Put the calculation of combined index here, may be moved elsewhere.
        station_prefix = []
        for name in sim_data.keys():
            curname = name[:name.rfind('_')]
            if curname not in station_prefix:
                station_prefix.append(curname)
        for sname in station_prefix:
            combine_name = f"{sname}_COMBINED"
            if combine_name in sim_data:
                continue
            nse_name = f"{sname}_NSE"
            pbias_name = f"{sname}_PBIAS"
            if nse_name not in sim_data:
                continue
            if pbias_name not in sim_data:
                continue
            nse_v = sim_data[nse_name]
            pbias_v = sim_data[pbias_name]
            comb_v = combined_score(nse_v, pbias_v, MIN_NSE, MAX_PBIAS,
                                    COMBINED_WEIGHTS["NSE"], COMBINED_WEIGHTS["PBIAS"],
                                    WORST_VALUE)
            sim_data[combine_name] = comb_v

        # end of calculation of combined index

        individual_objectives = []
        individual_penalties = []

        # 2. 遍历在 config.py 中定义的目标
        for metric_name, goal in objectives_config.items():
            if metric_name not in sim_data:
                print(f"FATAL ERROR: Metric '{metric_name}' not found in {result_file}")
                raise KeyError(f"Metric '{metric_name}' not in {result_file}")

            value = sim_data[metric_name]

            if "PBIAS" in metric_name and goal.upper() == "MIN":
                value = abs(value)

            if goal.upper() == "MAX":  # 最大化 NSE -> 最小化 -NSE
                objective_value = -value
            elif goal.upper() == "MIN":  # 最小化 abs(PBIAS) -> 目标值就是 abs(PBIAS)
                objective_value = value
            else:
                raise ValueError(f"Unknown goal '{goal}' for metric '{metric_name}'")

            individual_objectives.append(objective_value)

        all_objectives_list.append(individual_objectives)

        # penalty less than 0 means it is a feasible solution
        for metric_name, v in PENALTIES.items():
            value = sim_data[metric_name]
            if "PBIAS" in metric_name:
                value = abs(value)
                individual_penalties.append(value - v)
            elif "NSE" in metric_name:
                individual_penalties.append(v - value)
            elif "RSR" in metric_name:
                individual_penalties.append(value - v)
            else:
                individual_penalties.append(v - value)

        all_penalties_list.append(individual_penalties)

        sim_data['sim_index'] = i
        data_rows.append(sim_data)

    F = np.array(all_objectives_list)
    G = np.array(all_penalties_list)


    indicator_df_unordered = pd.DataFrame(data_rows)
    indicator_df = indicator_df_unordered.set_index('sim_index').sort_index()
    indicator_file = gen_dir / 'model_performances_all.csv'
    try:
        indicator_df.to_csv(indicator_file, index=True)
        print(f"--- Save all indicators of model performances to: {indicator_file} ---")
    except Exception as e:
        print(f"!! Error: cannot save indicator_df to CSV: {e} !!")

    if F.shape != (n_pop, len(objectives_config)):
        print(f"FATAL ERROR: Shape mismatch in objectives array. "
              f"Expected {(n_pop, len(objectives_config))}, got {F.shape}")
        raise ValueError("Shape mismatch in objectives array")

    print(f"Successfully loaded and processed results for {n_pop} individuals.")
    return F, G


def visualize_results(algorithm, iter_index, obj_labels, sim_dir):
    """
    (修正版)
    根据算法结果生成并保存 Pareto 前沿可视化图。
    """
    F = algorithm.result().F
    if F is None or len(F) == 0:
        print("Visualization skipped: No results found.")
        return

    n_obj = algorithm.problem.n_obj
    print(f"Visualizing Pareto front for {n_obj} objectives...")

    plt.figure(figsize=(10, 8))

    if n_obj == 2:
        plt.scatter(F[:, 0], F[:, 1], s=30, facecolors='none', edgecolors='blue')
        plt.title(f"Pareto Front (Generation {iter_index})")
        plt.xlabel(obj_labels[0])
        plt.ylabel(obj_labels[1])
        plt.grid(True)
    elif n_obj == 3:
        ax = plt.gcf().add_subplot(111, projection='3d')
        ax.scatter(F[:, 0], F[:, 1], F[:, 2], s=30, c='blue', marker='o')
        ax.set_title(f"Pareto Front (Generation {iter_index})")
        ax.set_xlabel(obj_labels[0])
        ax.set_ylabel(obj_labels[1])
        ax.set_zlabel(obj_labels[2])
        plt.grid(True)
    else:
        # --- 高维 (>3) 可视化 ---
        print(f"High-dimensional data ({n_obj} obj). "
              f"Using Scatter Matrix and Parallel Coordinates.")

        df = pd.DataFrame(F, columns=obj_labels)

        # 检查是否有 'std' (标准差) 为 0 的列
        if (df.std() == 0).any():
            print("--------------------------------------------------------------------")
            print("WARNING: At least one objective column has a standard deviation of 0 "
                  "(all values are identical).")
            print("The Parallel Coordinates plot will likely be empty or fail.")
            print(df.describe())
            print("--------------------------------------------------------------------")
        # 1. 散点图矩阵
        try:
            print("Attempting to generate Scatter Matrix plot...")
            pd.plotting.scatter_matrix(df, alpha=0.7, figsize=(12, 12), diagonal='kde')
            plt.suptitle(f"Scatter Matrix (Generation {iter_index})")
            plt.savefig(sim_dir / f"pareto_scatter_matrix_gen_{iter_index}.png")
            print(f"Saved pareto_scatter_matrix_gen_{iter_index}.png")
        except Exception as e:
            print(f"WARNING: Could not generate Scatter Matrix plot. Error: {e}")
        finally:
            plt.close()  # 确保关闭图像

        # 2. 平行坐标图
        print("Attempting to generate Parallel Coordinates plot...")
        try:
            plt.figure(figsize=(12, 6))  # <--- 必须创建一个新图像
            pd.plotting.parallel_coordinates(df, class_column=None, colormap='viridis', alpha=0.5)
            plt.title(f"Parallel Coordinates (Generation {iter_index})")
            plt.grid(True)
            plt.savefig(sim_dir / f"pareto_parallel_coords_gen_{iter_index}.png")
            print(f"Saved pareto_parallel_coords_gen_{iter_index}.png")
        except Exception as e:
            # 捕获所有错误，包括由 std=0 引起的内部错误
            print(f"WARNING: Could not generate Parallel Coordinates plot. Error: {e}")
            print("This is often due to all values in one column being identical (std=0).")
        finally:
            # 无论成功与否，都关闭图像，防止资源泄漏
            plt.close()

        return  # 从 'else' 块中返回

    output_filename = sim_dir / f"pareto_front_gen_{iter_index}.png"
    plt.savefig(output_filename)
    print(f"Visualization saved to {output_filename}")
    plt.close()


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = script_dir + '/../'
    base_dir = pathlib.Path(base_dir).resolve()
    current_gen = get_current_generation(base_dir)
    next_gen = current_gen + 1

    print(f"--- Running ctrl_calibration_generate.py for Generation {current_gen} ---")
    N_OBJ = len(OBJECTIVES)
    N_POP = N_POP
    sim_dir_name = RUNS_BASE_DIR
    sim_dir_path = script_dir + '/../' + sim_dir_name

    sim_dir = pathlib.Path(sim_dir_path).resolve()
    gen_dir = sim_dir / f'gen_{current_gen}'

    nextgen_dir = sim_dir / f'gen_{next_gen}'
    if not os.path.exists(nextgen_dir):
        os.makedirs(nextgen_dir, exist_ok=True)

    cur_pop_filename = POP_FILE.format(current_gen)
    cur_pop_fpath = gen_dir / cur_pop_filename
    cur_genstate_filename = GENSTATE_FILE.format(current_gen)
    cur_genstate_fpath = gen_dir / cur_genstate_filename

    next_pop_filename = POP_FILE.format(next_gen)
    next_pop_fpath = nextgen_dir / next_pop_filename
    next_genstate_filename = GENSTATE_FILE.format(next_gen)
    next_genstate_fpath = nextgen_dir / next_genstate_filename

    if not os.path.exists(cur_genstate_fpath):
        print(f"FATAL ERROR: State file '{cur_genstate_fpath}' not found.")
        exit(-1)

    # 1. 加载算法的 *当前* 状态 (Gen k)
    print(f"Loading algorithm state from {cur_genstate_fpath}...")
    with open(cur_genstate_fpath, 'rb') as f:
        algorithm = pickle.load(f)

    # 2. 读取模拟结果
    try:
        F, G = read_simulation_results(gen_dir)
    except Exception as e:
        print(f"Error during read_simulation_results: {e}")
        # 使作业失败
        raise
    # 3. 读取模拟参数 X (Gen k)
    print(f"Loading simulation parameters (X) from {cur_pop_fpath}...")
    if not os.path.exists(cur_pop_fpath):
        print(f"FATAL ERROR: Missing population file {cur_pop_fpath}")
        raise FileNotFoundError(f"Missing {cur_pop_fpath}")

    X = np.load(cur_pop_fpath)

    # (!! 重要 !!) 确保 X 是 2D 的 (处理 n_var=1 的情况)
    if X.ndim == 1:
        print("Detected 1D parameters array, reshaping to 2D.")
        X = X.reshape(-1, 1)

    # 4. 将 X 和 F 组合成 pymoo 的 Population 对象
    print("Re-creating Population object from X and F...")
    evaluated_population = Population.new("X", X, "F", F, "G", G)

    # 5. "tell" 算法 *已评估的种群*
    print("Telling algorithm the evaluated population...")
    # (!! 修正 !!) 不再是 algorithm.tell(F)
    algorithm.tell(infills=evaluated_population)

    # 保存最终的非支配解
    final_X = algorithm.result().X
    final_F = algorithm.result().F
    final_X_fpath = gen_dir / f"final_pareto_solutions_X_{current_gen}.npy"
    np.save(final_X_fpath, final_X)
    final_F_fpath = gen_dir / f"final_pareto_solutions_F_{current_gen}.npy"
    np.save(final_F_fpath, final_F)

    # 生成用于绘图的标签
    plot_labels = []
    for name, goal in OBJECTIVES.items():
        if "PBIAS" in name and goal.upper() == "MIN":
            plot_labels.append(f"Min Abs({name})")
        elif goal.upper() == "MAX":
            plot_labels.append(f"Min (-{name})")  # Pymoo 最小化 -Value
        else:
            plot_labels.append(f"Min ({name})")

    # 保存 Pareto 前沿到 JSON
    try:
        json_output_filename = gen_dir / f"pareto_front_gen_{current_gen}.json"
        save_pareto_to_json(algorithm, plot_labels, json_output_filename)

    except Exception as e:
        print(f"Warning: Saving Pareto JSON failed with error: {e}")

    # 5. 可视化
    try:
        visualize_results(algorithm, current_gen, plot_labels, gen_dir)
    except Exception as e:
        print(f"Warning: Visualization failed with error: {e}")

    # 6. 检查是否终止
    # Always create/touch the signal file first
    signal_fpath = base_dir / SIGNAL_FILE
    with open(signal_fpath, "w") as f:
        f.write("")  # Create an empty file initially

    if current_gen < MAX_GENERATIONS:
        print(f"Current generation {current_gen} is less than max {MAX_GENERATIONS}. Continuing.")
        # Overwrite the empty file with "continue"
        with open(signal_fpath, "w") as f:
            f.write("continue")
        print(f"Created signal file: {SIGNAL_FILE} with content.")
        # 7. "ask" 算法索要 *下一个* 种群 (Gen k+1)
        print("Asking algorithm for the next generation population...")
        X_next = algorithm.ask()
        X_next_np = X_next.get("X")  # population to numpy array

        # 8. 保存 *下一个* 种群的参数
        np.save(next_pop_fpath, X_next_np)
        print(f"Saved NEXT generation population to {next_pop_fpath}.")

        # 9. 保存算法的 *下一个* 状态 (Gen k+1)
        with open(next_genstate_fpath, 'wb') as f:
            pickle.dump(algorithm, f)
        print(f"Saved NEXT algorithm state to {next_genstate_fpath}.")

        print("\n--- Analysis Complete ---")
        print(f"Ready for Iteration {next_gen}.")
        update_generation(base_dir, next_gen)
    else:
        print(f"Reached max generation {current_gen}. Stopping.")
        print(f"Signal file: {SIGNAL_FILE} remains empty.")





