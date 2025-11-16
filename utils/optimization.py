from __future__ import absolute_import
import os
import sys

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

from pymoo.core.problem import Problem
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


def get_algorithm(n_obj, n_pop):
    # ... (与 analyze.py 中的函数完全相同)
    if n_obj == 2:
        print(f"Detected {n_obj} objectives. Using NSGA-II.")
        algorithm = NSGA2(pop_size=n_pop)
    elif n_obj >= 3:
        print(f"Detected {n_obj} objectives. Using NSGA-III.")
        ref_dirs = get_reference_directions("das-dennis", n_obj, n_partitions=12)
        algorithm = NSGA3(pop_size=n_pop, ref_dirs=ref_dirs)
    else:
        raise ValueError(f"Number of objectives (N_OBJ) must be at least 2, but got {n_obj}")
    return algorithm