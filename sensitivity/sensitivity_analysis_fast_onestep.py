"""
Test to submit a job of parameter sensitivity analysis to CHTC
"""
import json
import os
import shutil
import time
import pathlib
import copy
import numpy as np
import pandas as pd
from SALib.sample import fast_sampler
from SALib.analyze import fast
import matplotlib.pyplot as plt

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

def plot_sensitivity_indices(Si, problem, output_filepath, criteria='ST'):
    """
    Plots the S1 and ST sensitivity indices from a FAST or Sobol' analysis
    using Matplotlib and saves the plot to a file.

    Args:
    Si (dict): The results dictionary from SALib.analyze.fast.analyze()
               or SALib.analyze.sobol.analyze().
    problem (dict): The SALib problem definition (used for parameter names).
    output_filepath (str): The full path to save the image
                           (e.g., 'my_plots/fast_nse.jpg').
    criteria (str): 'ST' or 'S1', the index to use for sorting parameters.
    """

    # --- 1. Data Preparation ---
    param_names = problem['names']
    num_params = len(param_names)

    s1_values = Si['S1']
    st_values = Si['ST']
    s1_conf = Si.get('S1_conf', np.zeros(num_params))
    st_conf = Si.get('ST_conf', np.zeros(num_params))

    # --- 2. Sort by Importance (Recommended) ---
    if criteria == 'ST':
        indices = np.argsort(st_values)[::-1]
    else:
        indices = np.argsort(s1_values)[::-1]

    param_names = [param_names[i] for i in indices]
    s1_values = np.array(s1_values)[indices]
    st_values = np.array(st_values)[indices]
    s1_conf = np.array(s1_conf)[indices]
    st_conf = np.array(st_conf)[indices]

    # --- 3. Plotting ---
    # Create a figure object
    fig = plt.figure(figsize=(10, 6))

    bar_width = 0.35
    index = np.arange(num_params)

    # Plot S1 (First-order) bars
    plt.bar(index - bar_width / 2, s1_values, bar_width,
            yerr=s1_conf, capsize=5,
            label='S1 (First-order effect)', color='skyblue', ecolor='gray')

    # Plot ST (Total-order) bars
    plt.bar(index + bar_width / 2, st_values, bar_width,
            yerr=st_conf, capsize=5,
            label='ST (Total-order effect)', color='salmon', ecolor='gray')

    # --- 4. Chart Formatting ---
    plt.title(f'FAST Sensitivity Indices (Sorted by {criteria})')
    plt.ylabel('Sensitivity Index')
    plt.xlabel('Model Parameters')
    plt.xticks(index, param_names)
    plt.legend()
    plt.axhline(y=0, color='gray', linewidth=0.8)
    plt.tight_layout()

    # --- 5. Save, Don't Show, and Close ---

    # Ensure the output directory exists
    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):  # Check if output_dir is not empty
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created directory: {output_dir}")

    # Save the figure as a high-quality JPG
    # We use bbox_inches='tight' to ensure labels (like x-ticks) are not cut off
    plt.savefig(output_filepath, dpi=300, format='jpg', bbox_inches='tight')

    # plt.show() has been removed as requested.

    # Close the plot figure to free up memory
    plt.close(fig)

    print(f"\nPlot successfully saved to: {output_filepath}")


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
    # Total model runs should be N * D, M can be 4 (by default) or 8 and N > 4M^2 (N > 64)
    N_fast = 65
    M_fast = 4
    # Result folder for extracted simulation results and calculated model performances
    results_dir = sim_dir
    clean_simulation = True

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
    sample_array = fast_sampler.sample(copy_problem, N_fast, M=M_fast)

    # Number of unique simulations
    num_sim = sample_array.shape[0]

    sample_out_file = sim_dir / 'fast_samples.npz'
    np.savez_compressed(sample_out_file, samples=sample_array)

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
    #
    # # Write calibration.cal file
    # for idx, arr in enumerate(sample_array, start=1):
    #     # Dictionary mapping for sensitivity simulation name and variable
    #     var_names = copy_problem['names']
    #     var_dict = {
    #         var_names[i]: float(arr[i]) for i in range(len(var_names))
    #     }
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
    #     # List of ModifyDict objects
    #     params = utils._parameters_modify_dict_list(
    #             parameters=params_sim,
    #     )
    #     txtinout_reader._write_calibration_file(
    #             parameters=params
    #     )
    #     # Remove and rename calibration.cal file to sim_<i>.cal
    #     shutil.move(tio_dir / 'calibration.cal', sim_dir / f'sim_{idx}.cal')

    # # Run SWAT+ model and calculate model performances
    # for idx, arr in enumerate(sample_array, start=1):
    #     # Display start of current simulation for tracking
    #     print(f'Started simulation: {idx}/{num_sim}', flush=True)
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
    #     # Remove and rename sim_<i>.cal to cpu_path/calibration.cal
    #     cali_file = cpu_path / 'calibration.cal'
    #     if os.path.exists(cali_file):
    #         os.remove(cali_file)
    #     shutil.move(sim_dir / f'sim_{idx}.cal', cali_file)
    #
    #     # Run SWAT+ model in each directory
    #     cursim_reader.run_swat(
    #         parameters=None,
    #         begin_date='01-Jan-2007',
    #         end_date='31-Dec-2008',
    #         warmup=1
    #     )
    #     # Extract interested simulation results to the result folder
    #     INPUT_FILE = cpu_path / 'channel_sd_day.txt'
    #     MON_INPUT_FILE = cpu_path / 'channel_sd_mon.txt'
    #     CHANNEL_NUMBER = [68]
    #     SUFFIX = ['_usgs04085427']
    #
    #     CHANNEL_NUMBERS = [68, 170, 157, 74]
    #     SUFFIXES = ['_usgs04085427', '_363375', '_10020782', '_363313']
    #
    #     OUTPUT_DIRECTORY = sim_dir / f'OutletsResults_{idx}'
    #
    #     process_swat_output_memory_efficient(
    #             input_file_path=INPUT_FILE, skiplines=3,
    #             channel_id=CHANNEL_NUMBER,
    #             output_folder=OUTPUT_DIRECTORY,
    #             fname_suffix=SUFFIX
    #     )
    #
    #     process_swat_output_memory_efficient(
    #             input_file_path=MON_INPUT_FILE, skiplines=3,
    #             channel_id=CHANNEL_NUMBERS,
    #             output_folder=OUTPUT_DIRECTORY,
    #             fname_suffix=SUFFIXES, is_daily=False
    #     )
    #
    #     # Calculate model performance indices
    #     evaluate_performance(conf, OUTPUT_DIRECTORY, obs_dir, OUTPUT_DIRECTORY, '',
    #                          plot_stime, plot_etime, plot_flag=plot_flag)
    #
    #     # Remove simulation directory
    #     if clean_simulation:
    #         shutil.rmtree(cpu_path, ignore_errors=True)

    # 3. Submit sensitivity analysis job
    # Load sensitivity simulation dictionary from JSON file
    sensim_file = sim_dir / 'sensitivity_simulation.json'

    with open(sensim_file, 'r') as input_sim:
        sensitivity_sim = json.load(input_sim)

    problem = sensitivity_sim['problem']
    # samples = sensitivity_sim['sample']  # all generated samples, may include duplicates

    data = np.load(sample_out_file)
    loaded_sample_array = data['samples']

    data_rows = []
    for idx, arr in enumerate(loaded_sample_array, start=1):
        sample_key = tuple(arr)
        cur_out_dir = sim_dir / f'OutletsResults_{idx}'
        cur_model_indicator_json = cur_out_dir / 'model_performance.json'
        with open(cur_model_indicator_json, 'r') as cur_ind:
            cur_model_indicators = json.load(cur_ind)
            cur_model_indicators['Scenario'] = sample_key
            data_rows.append(cur_model_indicators)
    indicator_df = pd.DataFrame(data_rows)
    indicator_df = indicator_df.set_index('Scenario')
    print(indicator_df)
    indicators = indicator_df.columns.tolist()

    # Sensitivity indices
    sensitivity_indices = {}
    for indicator in indicators:
        # Indicator sensitivity indices
        indicator_sensitivity = fast.analyze(problem=copy_problem,
                                             Y=indicator_df[indicator].values,
                                             M=M_fast,
                                             print_to_console=True)
        # print(indicator_df[indicator].values)
        sensitivity_indices[indicator] = indicator_sensitivity

    # Write the sensitivity indices
    json_file = sim_dir / 'sensitivity_result.json'
    json_file = pathlib.Path(json_file).resolve()
    validators._json_extension(
            json_file=json_file
    )
    with open(json_file, 'w') as output_json:
        json.dump(sensitivity_indices, output_json, indent=4)

    # Output dictionary
    output = {
        'problem': problem,
        'sensitivity_indices': sensitivity_indices
    }

    print(output)

    # Specify the folder and the .jpg filename
    # Example: 'sensitivity_plots/fast_nse_analysis.jpg'
    save_path = 'sensitivity_plots/fast_nse_analysis.jpg'

    print("Generating sensitivity analysis plot...")
    for k, v in sensitivity_indices.items():
        save_path = sim_dir / f'S1sort-{k}.jpg'
        plot_sensitivity_indices(v, problem,
                                 output_filepath=save_path,
                                 criteria='S1')
        save_path = sim_dir / f'STsort-{k}.jpg'
        plot_sensitivity_indices(v, problem,
                                 output_filepath=save_path,
                                 criteria='ST')