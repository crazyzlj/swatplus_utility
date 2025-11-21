
# population size
N_POP = 5
# maximum generations
MAX_GENERATIONS = 3

# {
#     "usgs04085427_flo_out_day_cali_NSE": -0.39,
#     "usgs04085427_flo_out_day_cali_RSR": 1.18,
#     "usgs04085427_flo_out_day_cali_PBIAS": 87.18,
#     "usgs04085427_flo_out_day_cali_R_square": 0.16,
#     "usgs04085427_flo_out_day_vali_NSE": -0.19,
#     "usgs04085427_flo_out_day_vali_RSR": 1.09,
#     "usgs04085427_flo_out_day_vali_PBIAS": 87.36,
#     "usgs04085427_flo_out_day_vali_R_square": 0.18,
#     "usgs04085427_flo_out_mon_cali_NSE": -0.59,
#     "usgs04085427_flo_out_mon_cali_RSR": 1.26,
#     "usgs04085427_flo_out_mon_cali_PBIAS": 87.1,
#     "usgs04085427_flo_out_mon_cali_R_square": 0.49,
#     "usgs04085427_flo_out_mon_vali_NSE": -0.24,
#     "usgs04085427_flo_out_mon_vali_RSR": 1.11,
#     "usgs04085427_flo_out_mon_vali_PBIAS": 87.35,
#     "usgs04085427_flo_out_mon_vali_R_square": 0.77,
#     "363375_flo_out_mon_cali_NSE": -2.06,
#     "363375_flo_out_mon_cali_RSR": 1.75,
#     "363375_flo_out_mon_cali_PBIAS": 93.56,
#     "363375_flo_out_mon_cali_R_square": 0.28,
#     "10020782_flo_out_mon_cali_NSE": -1.09,
#     "10020782_flo_out_mon_cali_RSR": 1.45,
#     "10020782_flo_out_mon_cali_PBIAS": 94.85,
#     "10020782_flo_out_mon_cali_R_square": 0.68,
#     "363313_flo_out_mon_cali_NSE": -0.78,
#     "363313_flo_out_mon_cali_RSR": 1.33,
#     "363313_flo_out_mon_cali_PBIAS": 76.17,
#     "363313_flo_out_mon_cali_R_square": 0.19
# }
OBJECTIVES = {
    # Five or less objectives will use NSGA-II
    "usgs04085427_flo_out_day_cali_COMBINED": "max",
    "usgs04085427_flo_out_mon_cali_COMBINED": "max",
    "363375_flo_out_mon_cali_COMBINED": "max",
    "363313_flo_out_mon_cali_COMBINED": "max",
    "10020782_flo_out_mon_cali_COMBINED": "max"
}

MIN_NSE = -1
MAX_PBIAS = 90
WORST_VALUE = -99

COMBINED_WEIGHTS = {
    "NSE": 0.8,
    "PBIAS": 0.2
}

PENALTIES = {
    "usgs04085427_flo_out_day_cali_NSE": 0.01,
    "usgs04085427_flo_out_mon_cali_NSE": 0.01,
    "363375_flo_out_mon_cali_NSE": 0.01,
    "363313_flo_out_mon_cali_NSE": 0.01,
    "10020782_flo_out_mon_cali_NSE": 0.01
}

WORKER_DAG_TEMPLATE = 'worker_jobs_gen_{}.dag'
WORKER_DAG_CURRENT_SYMLINK = 'worker_jobs_current.dag'

POP_FILE = 'population_gen_{}.npy'
GENSTATE_FILE = 'algorithm_state_gen_{}.pkl'
