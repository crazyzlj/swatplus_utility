"""
Test to submit a job of parameter sensitivity analysis to CHTC
"""
import json
import os
import shutil
import time
import pathlib
import copy
import SALib
import numpy
import pandas

import pySWATPlus
import pySWATPlus.utils as utils
import pySWATPlus.validators as validators

from postprocess.read_channel_sd_output import process_swat_output_memory_efficient
from postprocess.eval_model_performance_v2 import evaluate_performance

def parse_parameter_file(filepath: str) -> list[dict]:
    """
    Reads a definition file and parses it into a list of dictionaries containing parameters to be considered.

    The file format is assumed to be: name,change_type,lower_bound,upper_bound
    - Lines starting with '#' (comments) are ignored.
    - Empty lines are ignored.

    Args:
        filepath (str): The path to the input text file.

    Returns:
        list[dict]: A list of dictionaries containing parameter information.
                   e.g.: [{'name': 'esco', 'change_type': 'absval',
                           'lower_bound': 0.0, 'upper_bound': 1.0}, ...]
    """
    parameters = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                # 1. Strip leading/trailing whitespace (like newlines)
                line = line.strip()

                # 2. Ignore empty lines or comment lines
                if not line or line.startswith('#'):
                    continue

                # 3. Split the data
                try:
                    parts = line.split(',')

                    # Ensure there are exactly 4 parts after splitting
                    if len(parts) == 4:
                        # 4. Create the dictionary and perform type conversion
                        param_dict = {
                            'name': parts[0].strip(),
                            'change_type': parts[1].strip(),
                            'lower_bound': float(parts[2].strip()),  # Convert to float
                            'upper_bound': float(parts[3].strip())  # Convert to float
                        }
                        parameters.append(param_dict)
                    else:
                        print(f"Warning: Skipping malformed line: {line}")

                except ValueError:
                    # Handle failures during float() conversion
                    print(f"Warning: Skipping line with data type error: {line}")
                except Exception as e:
                    print(f"Warning: Unknown error processing line '{line}': {e}")

    except FileNotFoundError:
        print(f"Error: File not found: {filepath}")
        return []  # Return an empty list
    except Exception as e:
        print(f"Error: An error occurred while reading the file: {e}")
        return []  # Return an empty list

    return parameters


# Sensitivity simulation
if __name__ == '__main__':
    # Text file to define multiple parameters to be considered
    #  the format of each parameter MUST be "name,chang_type,lower_bound,upper_bound".
    param_def_file = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\param_defs.txt'
    # TxtInOut folder
    tio_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    # Actual simulation folder for every model runs
    sim_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\testsensitivity'
    # Observation folder
    obs_dir = r'D:\data_m\manitowoc\observed'
    # sample_number: Generates an array of length `2^N * (D + 1)`, where `D` is the number of parameter changes
    #   and `N = sample_number + 1`. For example, when `sample_number` is 1 and `D` is 2, 12 samples will be generated.
    sample_number = 1
    # Result folder for extracted simulation results and calculated model performances
    results_dir = sim_dir

    CHANNEL_NUMBER = [68]
    SUFFIX = ['_usgs04085427']

    CHANNEL_NUMBERS = [68, 170, 157, 74]
    SUFFIXES = ['_usgs04085427', '_363375', '_10020782', '_363313']

    # Configuration for calculating model performances
    plot_stime = '2008/1/1'
    plot_etime = '2024/12/31'
    plot_flag = False
    # For flow only
    conf = {'usgs04085427': {'flo_out': {'day': {'ylabel': 'Q(m^3/s)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '2008/1/1',
                                                 'vali_etime': '2013/12/31'},
                                         'mon': {'ylabel': 'Q(m^3/s)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1',
                                                 'cali_etime': '2024/12',
                                                 'vali_stime': '2008/1',
                                                 'vali_etime': '2013/12'}}
                             },
            '363375': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/5',
                                           'vali_stime': '',
                                           'vali_etime': ''}}
                       },
            '10020782': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                             'plot_style': 'dotline',
                                             'cali_stime': '2017/7',
                                             'cali_etime': '2019/10',
                                             'vali_stime': '',
                                             'vali_etime': ''}}
                         },
            '363313': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/10',
                                           'vali_stime': '',
                                           'vali_etime': ''}}
                       }
            }

    # Start time
    start_time = time.time()
    tio_dir = pathlib.Path(tio_dir).resolve()
    sim_dir = pathlib.Path(sim_dir).resolve()
    obs_dir = pathlib.Path(obs_dir).resolve()

    # Steps for running the parameter analysis job on CHTC
    # 1. Submit job of reading parameters definitions, generating samples, and saving to separated files
    param_def = parse_parameter_file(param_def_file)

    # 2. Submit each single model job and receive results_dir
    # Initialize TxtinoutReader with the simulation directory
    txtinout_reader = pySWATPlus.TxtinoutReader(
        tio_dir=tio_dir

    )
    # List of BoundDict objects
    params_bounds = utils._parameters_bound_dict_list(
        parameters=param_def
    )
    # Validate configuration of simulation parameters
    # validators._simulation_preliminary_setup(
    #     sim_dir=sim_dir,
    #     tio_dir=tio_dir,
    #     parameters=params_bounds
    # )
    # Create an object of pySWATPlus.SensitivityAnalyzer()
    sensitivity_obj = pySWATPlus.SensitivityAnalyzer()
    # problem dictionary
    problem = sensitivity_obj._create_sobol_problem(
        params_bounds=params_bounds
    )
    copy_problem = copy.deepcopy(x=problem)

    # Generate sample array
    sample_array = SALib.sample.sobol.sample(
        problem=copy_problem,
        N=pow(2, sample_number),
        calc_second_order=True
    )

    # Unique array to avoid duplicate computations
    unique_array = numpy.unique(
        ar=sample_array,
        axis=0
    )

    # Number of unique simulations
    num_sim = len(unique_array)

    sample_out_file = sim_dir / 'sobol_samples.npz'
    numpy.savez_compressed(sample_out_file, samples=sample_array, uniques=unique_array)

    # Output sensitivity analysis data (without simulation results)
    required_time = time.time() - start_time
    time_stats = {
        'sample_length': len(sample_array),
        'time_sec': round(required_time),
        'time_per_sample_sec': round(required_time / len(sample_array), 1),
    }
    sim_dict = {}
    sensim_output = {
        'time': time_stats,
        'problem': problem,
        'sample': sample_array,
        'simulation': sim_dict
    }

    # Write output to the file 'sensitivity_simulation.json' in simulation folder
    sensitivity_obj._write_simulation_in_json(
            sensim_dir=sim_dir,
            sensim_output=sensim_output
    )

    # Write calibration.cal file
    for idx, arr in enumerate(unique_array, start=1):
        # Dictionary mapping for sensitivity simulation name and variable
        var_names = copy_problem['names']
        var_dict = {
            var_names[i]: float(arr[i]) for i in range(len(var_names))
        }
        # Create ParameterType dictionary to write calibration.cal file
        params_sim = []
        for i, param in enumerate(params_bounds):
            params_sim.append(
                    {
                        'name': param.name,
                        'change_type': param.change_type,
                        'value': var_dict[var_names[i]],
                        'units': param.units,
                        'conditions': param.conditions
                    }
            )
        # List of ModifyDict objects
        params = utils._parameters_modify_dict_list(
                parameters=params_sim,
        )
        txtinout_reader._write_calibration_file(
                parameters=params
        )
        # Remove and rename calibration.cal file to sim_<i>.cal
        shutil.move(tio_dir / 'calibration.cal', sim_dir / f'sim_{idx}.cal')

    # Run SWAT+ model and calculate model performances
    for idx, arr in enumerate(unique_array, start=1):
        # Display start of current simulation for tracking
        print(f'Started simulation: {idx}/{num_sim}', flush=True)

        # Create simulation directory
        cpu_dir = f'sim_{idx}'
        cpu_path = sim_dir / cpu_dir
        # cpu_path.mkdir()
        #
        # # Remove and rename sim_<i>.cal to cpu_path/calibration.cal
        # cali_file = cpu_path / 'calibration.cal'
        # if os.path.exists(cali_file):
        #     os.remove(cali_file)
        # shutil.move(sim_dir / f'sim_{idx}.cal', cali_file)

        # # Output simulation dictionary
        # cpu_output = {
        #     'dir': cpu_dir,
        #     'array': arr
        # }
        # # Copy required files to an empty simulation directory
        # cursim_dir = txtinout_reader.copy_required_files(
        #     sim_dir=cpu_path
        # )
        # # Initialize TxtinoutReader with the simulation directory
        # cursim_reader = pySWATPlus.TxtinoutReader(
        #     tio_dir=cursim_dir
        # )
        # # Run SWAT+ model in each directory
        # cursim_reader.run_swat(
        #     parameters=None,
        #     begin_date='01-Jan-2007',
        #     end_date='31-Dec-2008',
        #     warmup=1
        # )
        # Extract interested simulation results to the result folder
        INPUT_FILE = cpu_path / 'channel_sd_day.txt'
        MON_INPUT_FILE = cpu_path / 'channel_sd_mon.txt'
        CHANNEL_NUMBER = [68]
        SUFFIX = ['_usgs04085427']

        CHANNEL_NUMBERS = [68, 170, 157, 74]
        SUFFIXES = ['_usgs04085427', '_363375', '_10020782', '_363313']

        OUTPUT_DIRECTORY = sim_dir / f'OutletsResults_{idx}'

        # process_swat_output_memory_efficient(
        #         input_file_path=INPUT_FILE, skiplines=3,
        #         channel_id=CHANNEL_NUMBER,
        #         output_folder=OUTPUT_DIRECTORY,
        #         fname_suffix=SUFFIX
        # )
        #
        # process_swat_output_memory_efficient(
        #         input_file_path=MON_INPUT_FILE, skiplines=3,
        #         channel_id=CHANNEL_NUMBERS,
        #         output_folder=OUTPUT_DIRECTORY,
        #         fname_suffix=SUFFIXES, is_daily=False
        # )

        # Calculate model performance indices
        # evaluate_performance(conf, OUTPUT_DIRECTORY, obs_dir, OUTPUT_DIRECTORY, '',
        #                      plot_stime, plot_etime, plot_flag=plot_flag)



    # for idx, arr in enumerate(unique_array, start=1):
    #     # Display start of current simulation for tracking
    #     print( f'Started simulation: {idx}/{num_sim}', flush=True)
    #
    #     # Write calibration.cal to each folder of simulations
    #     # Dictionary mapping for sensitivity simulation name and variable
    #     var_names = copy_problem['names']
    #     var_dict = {
    #         var_names[i]: float(arr[i]) for i in range(len(var_names))
    #     }
    #
    #     # Create ParameterType dictionary to write calibration.cal file
    #     params_sim = []
    #     for i, param in enumerate(params_bounds):
    #         params_sim.append(
    #                 {
    #                     'name': param.name,
    #                     'change_type': param.change_type,
    #                     'value': var_dict[var_names[i]],
    #                     'units': param.units,
    #                     'conditions': param.conditions
    #                 }
    #         )
    #
    #     # Create simulation directory
    #     cpu_dir = f'sim_{idx}'
    #     cpu_path = sim_dir / cpu_dir
    #     cpu_path.mkdir()
    #
    #     # Output simulation dictionary
    #     cpu_output = {
    #         'dir': cpu_dir,
    #         'array': arr
    #     }
    #     # Copy required files to an empty simulation directory
    #     cursim_dir = txtinout_reader.copy_required_files(
    #         sim_dir=cpu_path
    #     )
    #     # Initialize TxtinoutReader with the simulation directory
    #     cursim_reader = pySWATPlus.TxtinoutReader(
    #         tio_dir=cursim_dir
    #     )
    #
    #     # Create calibration.cal file
    #     # List of ModifyDict objects
    #     params = utils._parameters_modify_dict_list(
    #             parameters=params_sim,
    #     )
    #     cursim_reader._write_calibration_file(
    #             parameters=params
    #     )
    #
    #     # Run SWAT+ model in each directory
    #     cursim_reader.run_swat(
    #         parameters=None,
    #         begin_date='01-Jan-2007',
    #         end_date='31-Dec-2008',
    #         warmup=1
    #     )
    #     # Extract interested simulation results to the result folder

    #
    # 3. Submit sensitivity analysis job
    # Load sensitivity simulation dictionary from JSON file
    sensim_file = sim_dir / 'sensitivity_simulation.json'

    with open(sensim_file, 'r') as input_sim:
        sensitivity_sim = json.load(input_sim)

    problem = sensitivity_sim['problem']
    samples = sensitivity_sim['sample']  # all generated samples, may include duplicates

    data = numpy.load(sample_out_file)
    loaded_sample_array = data['samples']
    loaded_unique_array = data['uniques']

    data_rows = []
    results_map = {}
    for idx, arr in enumerate(loaded_unique_array, start=1):
        sample_key = tuple(arr)
        cur_out_dir = sim_dir / f'OutletsResults_{idx}'
        cur_model_indicator_json = cur_out_dir / 'model_performance.json'
        with open(cur_model_indicator_json, 'r') as cur_ind:
            cur_model_indicators = json.load(cur_ind)
            results_map[sample_key] = cur_model_indicators
    for idx, arr in enumerate(loaded_sample_array, start=1):
        sample_key = tuple(arr)
        results_for_this_row = results_map[sample_key].copy()
        results_for_this_row['Scenario'] = idx
        data_rows.append(results_for_this_row)
    indicator_df = pandas.DataFrame(data_rows)
    indicator_df = indicator_df.set_index('Scenario')
    print(indicator_df)
    indicators = indicator_df.columns.tolist()

    # Check input variables type

    # validators._variable_origin_static_type(
    #         vars_types=typing.get_type_hints(
    #                 obj=self.parameter_sensitivity_indices
    #         ),
    #         vars_values=locals()
    # )
    #
    # # Problem and indicators
    # prob_ind = PerformanceMetrics().scenario_indicators(
    #         sensim_file=sensim_file,
    #         df_name=df_name,
    #         sim_col=sim_col,
    #         obs_file=obs_file,
    #         date_format=date_format,
    #         obs_col=obs_col,
    #         indicators=indicators
    # )
    # problem = prob_ind['problem']
    # indicator_df = prob_ind['indicator']
    #
    # Sensitivity indices
    sensitivity_indices = {}
    for indicator in indicators:
        # Indicator sensitivity indices
        indicator_sensitivity = SALib.analyze.sobol.analyze(
                problem=copy.deepcopy(problem),
                Y=indicator_df[indicator].values
        )
        print(indicator_df[indicator].values)
        sensitivity_indices[indicator] = indicator_sensitivity

    # Write the sensitivity indices
    json_file = sim_dir / 'sensitivity_result.json'
    json_file = pathlib.Path(json_file).resolve()
    validators._json_extension(
            json_file=json_file
    )
    sensitivity_obj._write_index_in_json(
            index_dict=sensitivity_indices,
            json_file=json_file
    )
    # Output dictionary
    output = {
        'problem': problem,
        'sensitivity_indices': sensitivity_indices
    }

    print(output)