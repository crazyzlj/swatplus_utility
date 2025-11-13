import pandas as pd
import os


def extract_and_filter_columns(input_csv_path: str, columns_to_extract: list, output_csv_path: str):
    """
    Reads a processed site CSV file, extracts specific columns, and removes rows where
    all specified data columns are empty.
    """
    print(f"--- 1. Starting to process and filter file: {input_csv_path} ---")
    if not os.path.exists(input_csv_path):
        print(f"Error: Input file not found -> {input_csv_path}")
        return

    try:
        df = pd.read_csv(input_csv_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    final_columns_to_keep = ['DATETIME']
    existing_cols_to_check = []
    for col in columns_to_extract:
        if col in df.columns:
            final_columns_to_keep.append(col)
            existing_cols_to_check.append(col)
        else:
            print(f"Warning: Column '{col}' not found and will be ignored.")

    if not existing_cols_to_check:
        print("Error: None of the requested data columns were found. Aborting.")
        return

    subset_df = df[final_columns_to_keep].copy()
    filtered_df = subset_df.dropna(subset=existing_cols_to_check, how='all')
    print(f"Filtered data down to {len(filtered_df)} rows with valid entries.")

    filtered_df.to_csv(output_csv_path, index=False)
    print(f"Successfully saved filtered water quality data to: {output_csv_path}")


def merge_data_by_date(wq_csv_path: str, flow_csv_path: str, output_csv_path: str):
    """
    Merges water quality data with flow data by matching on the date part (YYYY-MM-DD),
    keeping only the records that have a match in both files.
    """
    print(f"\n--- 2. Starting to merge with flow data from: {flow_csv_path} ---")

    # --- Read Input Files ---
    if not os.path.exists(wq_csv_path) or not os.path.exists(flow_csv_path):
        print(f"Error: One of the input files was not found.")
        return

    wq_df = pd.read_csv(wq_csv_path)
    flow_df = pd.read_csv(flow_csv_path)
    print(f"Read {len(wq_df)} water quality records and {len(flow_df)} flow records.")

    # --- Prepare Date Columns for Merging ---
    wq_df['DATETIME'] = pd.to_datetime(wq_df['DATETIME'], errors='coerce')
    flow_df['Date'] = pd.to_datetime(flow_df['Date'], errors='coerce')

    wq_df['merge_date'] = wq_df['DATETIME'].dt.date
    flow_df['merge_date'] = flow_df['Date'].dt.date

    wq_df.dropna(subset=['DATETIME', 'merge_date'], inplace=True)
    flow_df.dropna(subset=['Date', 'merge_date'], inplace=True)

    # --- Perform the Merge (Changed to 'inner') ---
    flow_subset = flow_df[['merge_date', 'Flow']].rename(columns={'Flow': 'Flow_m3s'})

    # Use 'inner' merge to keep only rows where the date exists in BOTH files.
    merged_df = pd.merge(wq_df, flow_subset, on='merge_date', how='inner')

    # --- Format Date, Clean Up, and Save ---
    # Format the DATETIME column to the desired YYYY/MM/DD format
    merged_df['DATETIME'] = merged_df['DATETIME'].dt.strftime('%Y/%m/%d')

    # The 'merge_date' column is no longer needed
    merged_df.drop(columns=['merge_date'], inplace=True)

    # To match the flow data format, rename the final date column to 'Date'
    merged_df.rename(columns={'DATETIME': 'Date'}, inplace=True)

    print(
        f"Found {len(merged_df)} records with matching dates in both files. These records have been kept.")

    try:
        merged_df.to_csv(output_csv_path, index=False, float_format='%.3f')
        print(f"\n--- Success! ---")
        print(f"Final merged data has been saved to: {output_csv_path}")
    except Exception as e:
        print(f"Error saving the final merged file: {e}")


# --- How to Use This Script ---
if __name__ == '__main__':
    # ========================== User Configuration ==========================

    # 1. The initial, wide-format water quality file.
    WQ_INPUT_FILE = r'C:\Users\ljzhu\Downloads\wqx_processed\selected\USGS-04085427.csv'

    # 2. The flow data file. Assumes it has 'Date' (e.g., YYYY-MM-DD) and 'Flow' columns.
    FLOW_DATA_FILE = r'D:\data_m\manitowoc\observed\flow_cms_usgs04085427.csv'

    # 3. Names for the intermediate and final output files.
    FILTERED_WQ_FILE = r'C:\Users\ljzhu\Downloads\wqx_processed\selected\USGS-04085427-orgn.csv'
    FINAL_MERGED_FILE = r'C:\Users\ljzhu\Downloads\wqx_processed\selected\USGS-04085427-orgn-q.csv'

    # 4. A list of the parameter columns you want to extract from the file.
    #    Use the parameter codes, e.g., '00060' for Discharge, '00010' for Temperature.
    COLUMNS_TO_KEEP = [
        'Organic Nitrogen'
    ]
    # COLUMNS_TO_KEEP = [
    #     '62855',
    #     'Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)',
    #     '00631',
    #     '00613',
    #     'Nitrate',
    #     'Nitrite',
    #     'Organic Nitrogen',
    #     '00608',
    #     'Ammonia and ammonium'
    # ]
    # N:
    # nocode1,"Nitrate"
    # nocode2,"Nitrite"
    # nocode3,"Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"
    # nocode4,"Organic Nitrogen"
    # nocode5,"Ammonia and ammonium"
    # 00608,"Ammonia (NH3 + NH4+), water, filtered, milligrams per liter as nitrogen"
    # 00613,"Nitrite, water, filtered, milligrams per liter as nitrogen"
    # 00631,"Nitrate plus nitrite, water, filtered, milligrams per liter as nitrogen"
    # 62855,"Total nitrogen [nitrate + nitrite + ammonia + organic-N], water, unfiltered, analytically determined, milligrams per liter"

    # 62855 and nocode3 can be combined to be used as Total Nitrogen (TN).
    # nocode1, 00631, or 00631-00613 can be combined to be used as Nitrate (NO3).
    # nocode2 and 00613 can be combined to be used as Nitrite (NO2).
    # nocode5 and 00608 can be combined to be used as NH4.
    # nocode4 can be used for orgn

    # P:
    # COLUMNS_TO_KEEP = [
    #     '00665',
    #     '00671'
    # ]
    # 00665,"Phosphorus, water, unfiltered, milligrams per liter as phosphorus"
    # 71886,"Phosphorus, water, unfiltered, milligrams per liter as PO4"
    # 70507,"Orthophosphate, water, unfiltered, milligrams per liter as phosphorus"
    # 00666,"Phosphorus, water, filtered, milligrams per liter as phosphorus"
    # 00671,"Orthophosphate, water, filtered, milligrams per liter as phosphorus"

    # 00665 can be used for tp_out and 00671 for solp_out

    # Sed:
    # COLUMNS_TO_KEEP = [
    #     'Suspended Sediment Concentration (SSC)',
    #     'Suspended Sediment Discharge',
    #     '80154',
    #     '80155'
    # ]
    # nocode6,"Sediment"
    # nocode7,"Suspended Sediment Concentration (SSC)"
    # nocode8,"Suspended Sediment Discharge"
    # 80154,"Suspended sediment concentration, milligrams per liter"
    # 80155,"Suspended sediment discharge, short tons per day"
    # 70331,"Suspended sediment, sieve diameter, percent smaller than 0.0625 millimeters"
    # 91157,"Suspended sediment, sediment mass recovered from whole water sample, dry weight, grams"
    # 91158,"Suspended sediment, smaller than 0.063 mm sieve diameter, sediment mass recovered from whole water sample, dry weight, grams"
    # 91159,"Suspended sediment, larger than 0.063 mm sieve diameter, sediment mass recovered from whole water sample, dry weight, grams"


    # ========================================================================

    # --- Step 1: Filter the water quality data ---
    extract_and_filter_columns(
            input_csv_path=WQ_INPUT_FILE,
            columns_to_extract=COLUMNS_TO_KEEP,
            output_csv_path=FILTERED_WQ_FILE
    )

    # --- Step 2: Merge the filtered data with flow data ---
    # This step will only run if the first step was successful and created the file
    if os.path.exists(FILTERED_WQ_FILE):
        merge_data_by_date(
                wq_csv_path=FILTERED_WQ_FILE,
                flow_csv_path=FLOW_DATA_FILE,
                output_csv_path=FINAL_MERGED_FILE
        )