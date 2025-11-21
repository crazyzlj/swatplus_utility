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
    jobids_list_file = "unsolved.txt"
    jobids = []
    if jobids_list_file is not None:
        with open(jobids_list_file, 'r', encoding='utf-8') as f:
            for line in f:
                content = line.strip()
                if content:
                    jobids.append(int(content))

    write_singlerun_jobs_dag(DAG_FILE_NAME, jobids, 'multi_runs')