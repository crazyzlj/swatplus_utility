import json
import os
import pathlib
import numpy as np
import pandas as pd
from SALib.sample import fast_sampler
from SALib.analyze import fast, morris
import matplotlib.pyplot as plt

import pySWATPlus
import pySWATPlus.validators as validators

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

def convert_to_json_serializable(item):
    """
    Recursively converts an item (dict, list, np.ndarray)
    into a JSON-serializable format (lists and basic types).
    """
    if isinstance(item, np.ndarray):
        return item.tolist()
    if isinstance(item, dict):
        return {key: convert_to_json_serializable(value) for key, value in item.items()}
    if isinstance(item, (list, tuple)):
        return [convert_to_json_serializable(x) for x in item]
    # handle NumPy scalar types, e.g., np.float64, np.int64
    if isinstance(item, (np.generic,)):
        return item.item()
    return item

def plot_morris_scatter(Si, problem, output_filepath, indicator_name=''):
    """
    Plots the mu_star vs. sigma scatter plot from a Morris analysis.

    Args:
    Si (dict): The results dictionary from SALib.analyze.morris.analyze().
    problem (dict): The SALib problem definition (used for parameter names).
    output_filepath (str): The full path to save the image.
    indicator_name (str): Name of the performance indicator (for title).
    """
    param_names = problem['names']
    mu_star = Si['mu_star']
    sigma = Si['sigma']
    mu_star_conf = Si.get('mu_star_conf', np.zeros(len(param_names)))

    fig = plt.figure(figsize=(10, 8))

    plt.errorbar(mu_star, sigma, xerr=mu_star_conf, fmt='o',
                 color='blue', ecolor='gray', capsize=5, elinewidth=1,
                 alpha=0.7)

    for i, txt in enumerate(param_names):
        plt.annotate(txt, (mu_star[i], sigma[i]),
                     xytext=(5, 5), textcoords='offset points')

    plt.axvline(x=np.mean(mu_star), linestyle='--', color='grey', lw=0.8)
    plt.axhline(y=np.mean(sigma), linestyle='--', color='grey', lw=0.8)

    # --- Formatting ---
    title = f'Morris Method: $\mu^*$ vs. $\sigma$ for {indicator_name}'
    plt.title(title)
    # Use LaTeX
    plt.xlabel(r'$\mu^*$ (Mean of elementary effects)')
    plt.ylabel(r'$\sigma$ (Std. dev. of elementary effects)')
    plt.grid(True, linestyle=':', alpha=0.5)
    plt.tight_layout()

    # --- 5. Save, Don't Show, and Close ---
    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created directory: {output_dir}")

    plt.savefig(output_filepath, dpi=300, format='jpg', bbox_inches='tight')
    plt.close(fig)
    print(f"\nMorris scatter plot saved to: {output_filepath}")

def plot_morris_barchart(Si, problem, output_filepath, indicator_name=''):
    """
    Plots a sorted bar chart of mu_star from a Morris analysis,
    similar to the FAST S1/ST plots for easy ranking.

    Args:
    Si (dict): The results dictionary from SALib.analyze.morris.analyze().
    problem (dict): The SALib problem definition.
    output_filepath (str): The full path to save the image.
    indicator_name (str): Name of the performance indicator (for title).
    """
    param_names = problem['names']
    mu_star = Si['mu_star']
    mu_star_conf = Si.get('mu_star_conf', np.zeros(len(param_names)))

    # --- 2. Sort by Importance (mu_star) ---
    indices = np.argsort(mu_star)[::-1]
    param_names = [param_names[i] for i in indices]
    mu_star = mu_star[indices]
    mu_star_conf = mu_star_conf[indices]

    # --- 3. Plotting ---
    fig = plt.figure(figsize=(10, 6))
    index = np.arange(len(param_names))
    bar_width = 0.7

    plt.bar(index, mu_star, bar_width,
            yerr=mu_star_conf, capsize=5,
            label=r'$\mu^*$ (Importance)', color='deepskyblue', ecolor='gray')

    # --- 4. Chart Formatting ---
    title = f'Morris Method: $\mu^*$ Ranking for {indicator_name}'
    plt.title(title)
    plt.ylabel(r'$\mu^*$ Index')
    plt.xlabel('Model Parameters')
    plt.xticks(index, param_names, rotation=45, ha="right")
    plt.legend()
    plt.axhline(y=0, color='gray', linewidth=0.8)
    plt.tight_layout()

    # --- 5. Save, Don't Show, and Close ---
    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created directory: {output_dir}")

    plt.savefig(output_filepath, dpi=300, format='jpg', bbox_inches='tight')
    plt.close(fig)
    print(f"\nMorris bar chart saved to: {output_filepath}")


# Sensitivity simulation
if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Actual simulation folder for every model runs
    # sim_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\testsensitivity'
    sim_dir = script_dir + '/../multi_runs'
    sim_dir = pathlib.Path(sim_dir).resolve()

    fast_sample_file = sim_dir / 'fast_samples.npz'
    morris_sample_file = sim_dir / 'morris_samples.npz'

    if fast_sample_file.exists():
        METHOD = 'FAST'
        sample_out_file = fast_sample_file
        M_fast = 4
        print("--- Detected FAST method ---")
    elif morris_sample_file.exists():
        METHOD = 'Morris'
        sample_out_file = morris_sample_file
        print("--- Detected Morris method ---")
    else:
        raise FileNotFoundError(
                f"No sample file found in {sim_dir}. "
                f"Expected 'fast_samples.npz' or 'morris_samples.npz'."
        )

    # Load sensitivity simulation dictionary from JSON file
    sensim_file = sim_dir / 'sensitivity_simulation.json'
    with open(sensim_file, 'r') as input_sim:
        sensitivity_sim = json.load(input_sim)
    problem = sensitivity_sim['problem']
    # samples = sensitivity_sim['sample']  # all generated samples, may include duplicates

    data = np.load(sample_out_file)
    loaded_sample_array = data['samples']
    num_sim = loaded_sample_array.shape[0]

    print(f"--- Collecting results from {num_sim} simulations... ---")
    data_rows = []
    for idx in range(1, num_sim + 1):
        cur_out_dir = sim_dir / f'OutletsResults_{idx}'
        cur_model_indicator_json = cur_out_dir / 'model_performance.json'

        if not cur_model_indicator_json.exists():
            print(f"Warning: Result file not found, skipping: {cur_model_indicator_json}")
            continue

        with open(cur_model_indicator_json, 'r') as cur_ind:
            cur_model_indicators = json.load(cur_ind)
            cur_model_indicators['sim_index'] = idx
            data_rows.append(cur_model_indicators)

    indicator_df_unordered = pd.DataFrame(data_rows)
    indicator_df = indicator_df_unordered.set_index('sim_index').sort_index()
    indicator_file = sim_dir / 'model_performances_all.csv'
    try:
        indicator_df.to_csv(indicator_file, index=True)
        print(f"--- Save all indicators of model performances to: {indicator_file} ---")
    except Exception as e:
        print(f"!! Error: cannot save indicator_df to CSV: {e} !!")

    model_outputs_Y = {}
    indicators = [col for col in indicator_df.columns]
    for indicator in indicators:
        model_outputs_Y[indicator] = indicator_df[indicator].values
    print(f"Successfully loaded {len(model_outputs_Y)} indicators for {len(indicator_df)} runs.")

    sensitivity_indices = {}
    print(f"--- Running {METHOD} analysis... ---")

    if METHOD == 'FAST':
        for indicator in indicators:
            print(f"Analyzing {indicator}...")
            Y = model_outputs_Y[indicator]

            # Execute FAST analyze
            indicator_sensitivity = fast.analyze(problem=problem,
                                                 Y=Y,
                                                 M=M_fast,
                                                 print_to_console=False)
            sensitivity_indices[indicator] = indicator_sensitivity

            # FAST figures
            # S1 sorting
            save_path_s1 = sim_dir / f'plot_fast_S1sort_{indicator}.jpg'
            plot_sensitivity_indices(indicator_sensitivity, problem,
                                     output_filepath=save_path_s1,
                                     criteria='S1')
            # ST sorting
            save_path_st = sim_dir / f'plot_fast_STsort_{indicator}.jpg'
            plot_sensitivity_indices(indicator_sensitivity, problem,
                                     output_filepath=save_path_st,
                                     criteria='ST')

    elif METHOD == 'Morris':
        for indicator in indicators:
            print(f"Analyzing {indicator}...")
            Y = model_outputs_Y[indicator]

            # Execute Morris analyze, attention: Morris requires both X and Y
            indicator_sensitivity = morris.analyze(problem=problem,
                                                   X=loaded_sample_array,
                                                   Y=Y,
                                                   conf_level=0.95,
                                                   print_to_console=False)
            sensitivity_indices[indicator] = indicator_sensitivity

            # Morris figures
            # 1. mu_star vs. sigma
            save_path_scatter = sim_dir / f'plot_morris_scatter_{indicator}.jpg'
            plot_morris_scatter(indicator_sensitivity, problem,
                                output_filepath=save_path_scatter,
                                indicator_name=indicator)

            # 2. mu_star
            save_path_bar = sim_dir / f'plot_morris_barchart_{indicator}.jpg'
            plot_morris_barchart(indicator_sensitivity, problem,
                                 output_filepath=save_path_bar,
                                 indicator_name=indicator)

    print("--- Saving sensitivity results to JSON ---")
    serializable_indices = convert_to_json_serializable(sensitivity_indices)
    json_file = sim_dir / 'sensitivity_result.json'
    json_file = pathlib.Path(json_file).resolve()
    validators._json_extension(
            json_file=json_file
    )
    with open(json_file, 'w') as output_json:
        json.dump(serializable_indices, output_json, indent=4)

    print(f"Analysis complete. Results saved to {json_file}")
