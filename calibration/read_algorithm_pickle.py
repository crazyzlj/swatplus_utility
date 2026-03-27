import pickle
import os
import sys

# === 这里的路径指向任意一代的 pickle 文件 ===
pkl_file = r'C:\Users\ljzhu\Downloads\cali_up1\multi_runs\gen_0\algorithm_state_gen_0.pkl'

# === 关键：确保 Python 能找到 SWATPlusProblem 类的定义 ===
# 如果 SWATPlusProblem 定义在当前脚本中，没问题。
# 如果定义在其他模块（比如 my_model.py），需要 import 它
# from my_model import SWATPlusProblem

try:
    with open(pkl_file, 'rb') as f:
        algorithm = pickle.load(f)

    problem = algorithm.problem
    print("成功加载 algorithm 对象！")
    print(f"Problem 类型: {type(problem)}")

    # === 尝试寻找参数名 ===
    param_names = None

    # 情况 A: 类里直接存了 names 属性 (这是最理想的)
    if hasattr(problem, 'param_names'):
        print("发现 .names 属性！")
        param_names = problem.name
        print(param_names)

    # 情况 B: 类里存了当初传入的 sobol_problem 字典
    # 你需要检查 problem 的属性，看看哪个属性长得像那个字典
    elif hasattr(problem, 'problem_dict') and 'names' in problem.problem_dict:
        print("发现 .problem_dict['names']！")
        param_names = problem.problem_dict['names']

    # 情况 C: 假如你不知道属性名叫什么，打印所有属性看一眼
    else:
        print("\n未直接找到 'names'。请查看以下属性列表，找到存储 'copy_problem' 字典的属性名：")
        print([d for d in dir(problem) if not d.startswith('__')])

        # 暴力搜索：查看所有属性，看哪个是包含 'names' 的字典
        for attr_name in dir(problem):
            if attr_name.startswith('__'): continue
            attr_val = getattr(problem, attr_name)
            if isinstance(attr_val, dict) and 'names' in attr_val:
                print(f"\n>>> 找到了！参数名在属性: self.{attr_name}['names']")
                param_names = attr_val['names']
                break

    if param_names:
        print("\n提取到的参数名：")
        print(param_names)
    else:
        print("\n无法自动找到参数名，请检查 SWATPlusProblem 类的 __init__ 代码。")

except ImportError as e:
    print(f"加载失败：{e}")
    print("提示：分析脚本必须能 import 定义 'SWATPlusProblem' 类的模块，否则 pickle 无法重建对象。")