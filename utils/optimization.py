from __future__ import absolute_import
import os
import sys

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import math
from pymoo.core.problem import Problem
from pymoo.core.duplicate import DuplicateElimination
from pymoo.util.ref_dirs import get_reference_directions
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


def estimate_n_partitions(n_obj, target_pop_size):
    """
    估算最佳的 n_partitions (p)，
    使其产生的 N_POP (H) 尽可能接近 target_pop_size。
    """
    # 从 p=1 开始搜索
    p = 1
    while True:
        # H = C(p + n_obj - 1, n_obj - 1)
        current_pop_size = math.comb(p + n_obj - 1, n_obj - 1)

        # 如果当前 N_POP 已经超过了目标，我们需要做个决定
        if current_pop_size > target_pop_size:
            # 检查上一个 p (p-1) 是否更接近
            if p == 1:
                # p=1 是最小允许值
                return p

            prev_pop_size = math.comb((p - 1) + n_obj - 1, n_obj - 1)

            # 如果 (p) 产生的 N_POP 比 (p-1) 更接近目标，用 (p)
            if abs(current_pop_size - target_pop_size) < abs(prev_pop_size - target_pop_size):
                return p
            else:
                # 否则，(p-1) 是最好的选择
                return p - 1

        # 如果还没超过，继续增加 p
        p += 1

        # (安全退出，防止 p 过大导致组合数计算溢出或死循环)
        if p > 100:
            print(f"Warning: estimate_n_partitions 搜索超过 p=100。")
            return p - 1


class MyDuplicateElimination(DuplicateElimination):
    def is_equal(self, a, b):
        return (a.X == b.X).all()


def get_algorithm(n_obj, target_pop_size, change_popsize=False):
    """
    Gets the algorithm. For NSGA-III, it uses target_pop_size to estimate
    partitions, then creates ref_dirs and the *actual* pop_size.
    """

    dup_elim = MyDuplicateElimination()

    if n_obj <= 5:
        print(f"Detected {n_obj} objectives. Using NSGA-II.")
        algorithm = NSGA2(
                pop_size=target_pop_size,
                eliminate_duplicates=dup_elim
        )
    elif n_obj > 5:
        print(f"Detected {n_obj} objectives. Using NSGA-III.")
        print(f"Target N_POP is {target_pop_size}.")

        n_partitions = estimate_n_partitions(n_obj, target_pop_size)
        print(f"Estimated n_partitions = {n_partitions} to match target N_POP.")

        #  生成 'ref_dirs'
        ref_dirs = get_reference_directions("das-dennis", n_obj,
                                            n_partitions=n_partitions)

        # 获取 'ref_dirs' 产生的 *实际* N_POP
        actual_pop_size = ref_dirs.shape[0]

        if actual_pop_size != target_pop_size:
            print(f"Warning: The proper N_POP should be {actual_pop_size} "
                  f"to match reference directions.")
            if change_popsize:
                print(f"Warning: N_POP from config ({target_pop_size}) "
                      f"has been adjusted to {actual_pop_size} to match reference directions.")
                target_pop_size = actual_pop_size

        # 4. 将 *actual_pop_size* 和 *ref_dirs* 传递给构造函数
        algorithm = NSGA3(
                ref_dirs=ref_dirs,
                pop_size=target_pop_size,
                eliminate_duplicates=dup_elim
        )

    else:
        raise ValueError(f"Number of objectives must be at least 2, but got {n_obj}")

    return algorithm, target_pop_size


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

    pop_size = calculate_n_pop(N_OBJECTIVES, N_PARTITIONS)

    print(f"当 n_obj = {N_OBJECTIVES} 且 n_partitions = {N_PARTITIONS} 时:")
    print(f"理想的 N_POP = {pop_size}")

    print("\n--- 示例：n_obj = 3 ---")
    print(f"p=12, n_obj=3 => N_POP = {calculate_n_pop(3, 12)}")