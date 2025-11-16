from __future__ import absolute_import
import os
import sys

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import math
from pymoo.core.problem import Problem
from pymoo.core.duplicate import DuplicateElimination
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.algorithms.moo.nsga3 import NSGA3


class SWATPlusProblem(Problem):
    def __init__(self, pymoo_inputs, n_obj):
        self.param_names = pymoo_inputs['names']
        lowers = []
        uppers = []
        for rng in pymoo_inputs['bounds']:
            lowers.append(rng[0])
            uppers.append(rng[1])
        self.param_bounds_lower = lowers
        self.param_bounds_upper = uppers
        n_var = len(self.param_names)
        super().__init__(n_var=n_var, n_obj=n_obj, n_constr=0,
                         xl=self.param_bounds_lower, xu=self.param_bounds_upper)

    def _evaluate(self, x, out, *args, **kwargs): pass


class MyDuplicateElimination(DuplicateElimination):
    def is_equal(self, a, b):
        return (a.X == b.X).all()


def get_algorithm(n_obj, n_pop):
    dup_elim = MyDuplicateElimination()

    if n_obj == 2:
        print(f"Detected {n_obj} objectives. Using NSGA-II.")
        algorithm = NSGA2(
                pop_size=n_pop,
                eliminate_duplicates=dup_elim
        )
    elif n_obj >= 3:
        print(f"Detected {n_obj} objectives. Using NSGA-III.")
        print(f"Target N_POP is {n_pop} (pymoo will auto-select n_partitions)")

        # 不要手动创建 ref_dirs
        # 只需将 config.N_POP 传递给算法
        # pymoo 将自动计算最佳的 n_partitions 和最终的 N_POP

        algorithm = NSGA3(
                pop_size=n_pop,
                ref_dirs=None,
                eliminate_duplicates=dup_elim
        )

    else:
        raise ValueError(f"Number of objectives must be at least 2, but got {n_obj}")

    return algorithm


def calculate_n_pop(n_obj, p):
    """
    计算 NSGA-III 所需的 N_POP (种群大小)。

    参数:
    n_obj (int): 目标函数的数量 (e.g., 4)
    p (int):     分区数 (n_partitions, e.g., 7)

    返回:
    int: 理想的种群大小 (N_POP)
    """
    if n_obj <= 0 or p <= 0:
        return 0

    # 这是组合公式 H = C(p + n_obj - 1, p)
    try:
        n_pop = math.comb(p + n_obj - 1, p)
        return n_pop
    except ValueError as e:
        print(f"计算出错: {e}")
        return 0


# --- 在这里修改并运行来查询 ---
if __name__ == "__main__":
    # --- 您想查询的设置 ---
    N_OBJECTIVES = 4
    N_PARTITIONS = 10
    # -------------------------

    pop_size = calculate_n_pop(N_OBJECTIVES, N_PARTITIONS)

    print(f"当 n_obj = {N_OBJECTIVES} 且 n_partitions = {N_PARTITIONS} 时:")
    print(f"理想的 N_POP = {pop_size}")

    print("\n--- 示例：n_obj = 3 ---")
    print(f"p=12, n_obj=3 => N_POP = {calculate_n_pop(3, 12)}")