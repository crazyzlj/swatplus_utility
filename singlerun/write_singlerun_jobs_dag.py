from __future__ import absolute_import
import os
import sys
if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

from io import open

def write_singlerun_jobs_dag(dag_fpath, jobid_list, sim_dir_name):
    with open(dag_fpath, 'w') as dag_f:
        for i in jobid_list:
            param_file = f'{sim_dir_name}/sim_{i}.cal'
            dag_f.write(f"JOB run_{i} worker.sub\n")
            dag_f.write(f"VARS run_{i} ParamFile=\"{param_file}\"\n")
            dag_f.write(f"VARS run_{i} ResultDir=\"{sim_dir_name}/OutletsResults_{i}\"\n")
            dag_f.write("\n")

if __name__ == '__main__':
    DAG_FILE_NAME = "worker_jobs_unsolved.dag"
    jobids = [113, 437, 479, 712, 828, 892, 1488, 1512, 1642, 1814, 2055, 2691, 3656, 3662,
              4025, 4067, 4172, 4337, 5298, 5469, 5499, 5675, 5685, 6071, 6813, 7250, 7601,
              7766, 7772, 7892, 8057, 8126, 8405, 8528]
    write_singlerun_jobs_dag(DAG_FILE_NAME, jobids, 'multi_runs')