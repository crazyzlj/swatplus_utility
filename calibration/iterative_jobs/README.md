## File tree of the current workspace for auto-calibration

```
├── calibration
│        ├── config.py
│        ├── ctrl_calibration_analyze.py
│        ├── ctrl_calibration_generate.py
│        └── __init__.py
├── postprocess
│        ├── eval_model_performance_v2.py
│        ├── read_channel_sd_output.py
│        └── __init__.py
├── singlerun
│        ├── __init__.py
│        └── worker_runmodel.py
├── utils
│        ├── cal_param_def.py
│        ├── __init__.py
│        ├── iterative_job.py
│        └── optimization.py
├── __init__.py
├── resubmit_if_needed.sh
├── ctrl_calibration.dag
├── ctrl_calibration_generate_job.sub
├── ctrl_calibration_generate.sh
├── ctrl_calibration_analyze_job.sub
├── worker.sub
├── run_model_extract_results.sh
├── observed.tar.gz
├── TxtInOut.tar.gz
├── param_defs.txt
├── hru_combinations.json
├── channel_combinations.json
│
├── iteration.state
├── multi_runs
└─      └── gen_0

```

**注意：iteration.state文件是自己新建的，里面只有一个0，multi_runs/gen_0是自己新建的空文件夹，
目前没有太方便的办法，只能先这样手动创建文件、满足遗传算法初始运行的文件要求。**

```
condor_submit_dag -insert_env "iter_index=0" -insert_env "nextiter_index=1" ctrl_calibration.dag
```

submit generate or analyze job:
```
condor_submit ctrl_calibration_generate_job.sub iter_index=0
condor_submit ctrl_calibration_analyze_job.sub iter_index=0 nextiter_index=1
```
