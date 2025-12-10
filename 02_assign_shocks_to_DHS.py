import os
import gc
import logging
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm  # for notebooks
from numba import njit

if __name__ == "__main__":
    tqdm.pandas()

    pd.options.mode.chained_assignment = None  # default='warn'
    logging.getLogger("distributed").setLevel(logging.WARNING)

    # Set global variables
    PROJECT = r"C:\Working Papers\Paper - Child Mortality and Climate Shocks"
    OUTPUTS = rf"{PROJECT}\Outputs"
    DATA = rf"{PROJECT}\Data"
    DATA_IN = rf"{DATA}\Data_in"
    DATA_PROC = rf"{DATA}\Data_proc"
    DATA_OUT = rf"{DATA}\Data_out"

    ### Load data #############
    print("Loading data...")

    ### CLIMATE DATA
    climate_data = xr.open_dataset(rf"{DATA_OUT}/Climate_shocks_v11.nc")
        
    ### DHS DATA
    full_dhs = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_07272025.dta")
    full_dhs["ID"] = full_dhs.index
    # Gen unique id from groups of lat, lon and from_date
    df = full_dhs.copy()
    print("Data loaded! Processing...")

    ###########################

    climate_variables = [
        "spi1",
        "stdm_t",
        "absdifm_t",
        "hd35", 
        "hd40", 
        "fd", 
        "id",
    ]
    climate_data = climate_data[climate_variables]
    ORDERED_CLIMATE_VARS = list(climate_data.data_vars)


    def round_to_nearest_quarter(number):
        """Rounds a number to the nearest 0.25 increment.

        >>> round_to_nearest_quarter(1.3)
        1.25
        >>> round_to_nearest_quarter(1.62)
        1.5
        >>> round_to_nearest_quarter(3.9)
        4.0
        >>> round_to_nearest_quarter(2.05)
        2.0
        >>> round_to_nearest_quarter(5.78)
        5.75
        >>> round_to_nearest_quarter(3)
        3.0
        """
        return round(number * 4) / 4
            
    # 1. Define timeframes as quarters. The value is the 0-based index of the *last month* of the quarter.
    TIMEFRAMES_QUARTERLY = {
        "inutero_1m3m": 2,
        "inutero_3m6m": 5,
        "inutero_6m9m": 8,
        "born_1m3m": 11,
        "born_3m6m": 14,
        "born_6m9m": 17,
        "born_9m12m": 20,
    }
    TIMEFRAMES_BIANNUALY = {
        "inutero": 8,
        "born_1m": 9,
        "born_1m6m": 14,
        "born_6m12m": 20,
        "born_12m18m": 26,
        "born_18m24m": 32,
        "born_24m30m": 38,
        "born_30m36m": 44,
    }
    TIMEFRAMES_IUFOCUS = {
        "inutero_1m3m": 2,
        "inutero_3m6m": 5,
        "inutero_6m9m": 8,
        "born_1m": 9,
        "born_2m3m": 11,
        "born_3m6m": 14,
    }
    TIMEFRAMES_MONTHS = {
        "inutero_1m": 0,
        "inutero_2m": 1,
        "inutero_3m": 2,
        "inutero_4m": 3,
        "inutero_5m": 4,
        "inutero_6m": 5,
        "inutero_7m": 6,
        "inutero_8m": 7,
        "inutero_9m": 8,
        "born_1m": 9,
        "born_2m": 10,
        "born_3m": 11,
        "born_4m": 12,
        "born_5m": 13,
        "born_6m": 14,
    }
    ALL_TIMEFRAMES = {
        "q": TIMEFRAMES_QUARTERLY,
        "b": TIMEFRAMES_BIANNUALY,
        "m": TIMEFRAMES_MONTHS,
        "iu": TIMEFRAMES_IUFOCUS,
    }

    AVG_WINDOWS = {
        "q": np.array([0], dtype=np.int32), # 0 means no window, -1 means max of the period, -2 means min of the period
        "b": np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=np.int32),
        "m": np.array([0], dtype=np.int32),
        "iu": np.array([0], dtype=np.int32),
    }    
    
    COLUMN_NAMES = {} # Dinamically populate this dict 
    for timeframe_name, timeframe in ALL_TIMEFRAMES.items():
        result_column_names = []
        avg_windows = AVG_WINDOWS[timeframe_name] 
        for timename in timeframe.keys():
            for var in ORDERED_CLIMATE_VARS:
                for window in avg_windows:
                    if window==-1:
                        stat="max"
                    elif window==-2:
                        stat="min"
                    elif window==0:
                        stat="avg"
                    else:
                        stat=f"w{window}"
                    col_name = f"{var}_{timename}_{timeframe_name}_{stat}"
                    result_column_names.append(col_name)
        COLUMN_NAMES[timeframe_name] = result_column_names

    @njit(parallel=True)
    def _compute_stats(data_array, time_indices, window_sizes, death_month_index):
        n_vars = data_array.shape[0]
        n_indices = len(time_indices)
        n_windows = len(window_sizes)
        n_timesteps = data_array.shape[1]
        results = np.empty((n_vars, n_indices, n_windows), dtype=np.float32)

        for time_pos in range(n_indices):
            end_idx = time_indices[time_pos]
            if death_month_index != -1:
                # Case 1: Death happened BEFORE this period even started.
                # The entire period has irrelevant data, assign nan.
                period_start = time_indices[time_pos-1] + 1 if time_pos > 0 else 0
                if death_month_index < period_start:
                    results[:, time_pos, :] = np.nan
                    continue
                
                # Case 2: Death happened during or after this period.
                # We must truncate the period's end to the death month.
                # The min() function handles both situations:
                # - If death is during the period (death_month_index < end_idx), end_idx becomes death_month_index.
                # - If death is after the period (death_month_index >= end_idx), end_idx remains unchanged.
                end_idx = min(end_idx, death_month_index)
            
            start_idx = time_indices[time_pos-1] + 1 if time_pos > 0 else 0

            for var_pos in range(n_vars):
                for window_pos in range(n_windows):
                    window = window_sizes[window_pos]
                    
                    if (window == 0) | (window == -1) | (window == -2):
                        # Window 0 is unbounded, -1 is max, -2 is min
                        avg_start_idx = start_idx
                    elif window > 0:
                        # Average the previous {windows} months
                        avg_start_idx = end_idx - window + 1
                    else:
                        raise ValueError(f"windows has to be 0 or positive!! window value: {window}")        
                
                    if avg_start_idx > end_idx or end_idx >= n_timesteps or avg_start_idx < 0:
                        #   avg_start_idx > end_idx: This should never happen but is a safety check
                        #   end_idx >= n_timesteps: This could happen if the data ingested is shorter that what is expected!
                        #   avg_start_idx < 0 is an error: requiring a window larger than loaded data
                        raise ValueError(f"There is some issue with the data ingested! debug: {avg_start_idx > end_idx}, {end_idx >= n_timesteps}, {avg_start_idx < 0} ")        

                    avg_slice = data_array[var_pos, avg_start_idx : end_idx + 1]
                    if avg_slice.size > 0:
                        if window==-1:
                            data_results = np.nanmax(avg_slice)
                        elif window==-2:
                            data_results = np.nanmin(avg_slice)
                        else:
                            data_results = np.nanmean(avg_slice)
                        results[var_pos, time_pos, window_pos] = data_results
                    else:
                        results[var_pos, time_pos, window_pos] = np.nan
        return results


    ### Process dataframe ####
    # Drop nans in date columns
    df = df.dropna(subset=["v008", "chb_year", "chb_month"], how="any")

    # Create Birth datetime object from year and month
    df["day"] = 1
    df["month"] = df["chb_month"].astype(int)
    df["year"] = df["chb_year"].astype(int)
    df["birth_date"] = pd.to_datetime(df[["year", "month", "day"]]).to_numpy()
    df = df.drop(columns=["day", "month", "year"])

    # Create Death datetime object from year and month
    deads = df["child_death_ind"].astype(bool)
    
    # Maximum range of dates
    df["from_date"] = df["birth_date"] + pd.DateOffset(
        months=-9
    )  # From in utero (9 months before birth)
    df["to_date"] = df["birth_date"] + pd.DateOffset(
        months=11+12*2
    )  # To the third year of life
    # df["to_date"] = df[["to_date", "death_date"]].min(axis=1)  # If the child died, compute stats until death

    # Filter children from_date greater than 1991 (we only have climate data from 1990)
    df = df[df["from_date"] > "1991-01-01"]

    # Filter children to_date smalle than 2021 (we only have climate data to 2020)
    df = df[df["to_date"] < "2021-01-01"]
    

    # Date of interview
    df["year"] = 1900 + (df["v008"] - 1) // 12
    df["month"] = df["v008"] - 12 * (df["year"] - 1900)
    df["day"] = 1
    df["interview_date"] = pd.to_datetime(df[["year", "month", "day"]], dayfirst=False)
    df["interview_year"] = df["year"]
    df["interview_month"] = df["month"]
    df = df.drop(columns=["year", "month", "day"])

    # Number of days from interview
    df["days_from_interview"] = df["interview_date"] - df["birth_date"]

    # excluir del análisis a aquellos niños que nacieron 12 meses alrededor de la fecha de la encuesta y no más allá de 10 y 15 años del momento de la encuesta.
    df["last_15_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
        df["days_from_interview"] < np.timedelta64(15 * 365, "D")
    )
    df["last_10_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
        df["days_from_interview"] < np.timedelta64(10 * 365, "D")
    )
    df["since_2003"] = df["interview_year"] >= 2003
    df = df[df["last_15_years"]]
       
    df["lat_round"] = df["LATNUM"].apply(lambda x: round_to_nearest_quarter(x))
    df["lon_round"] = df["LONGNUM"].apply(lambda x: round_to_nearest_quarter(x))
    df = df.sort_values(["lat_round", "lon_round", "from_date"]) 
    df = df.dropna(subset=["ID", "from_date", "to_date", "lat_round", "lon_round"])
    
    # Store in variable
    df["point_ID"] = (
        df["lat_round"].astype(str) + "_" + 
        df["lon_round"].astype(str) + "_" + 
        df["from_date"].dt.strftime(r"%Y-%m-%d") + "_"  
    )
    
    # count unique points
    print("Number of unique points:", df["point_ID"].nunique())
    full_dhs = df.copy()
    df = df.reset_index(drop=True)
    

    #### Run process ####
    # Chunking implementation: load a large array from the nc to keep the slicing fast! 

    ## Clean up old files ####
    output_dir = os.path.join(DATA_PROC, "DHS_Climate")
    # for file in os.listdir(output_dir):
    #     if file.startswith("births_climate_"):
    #         os.remove(os.path.join(output_dir, file))
    # print("Old intermediate files removed!")

    # --- TUNABLE PARAMETER --- Larger means more ram is used
    LAT_CHUNK_SIZE = 20
    
    # Get a sorted list of unique latitudes to iterate over
    unique_lats = np.sort(df["lat_round"].unique())
    lat_chunks = [unique_lats[i:i + LAT_CHUNK_SIZE] for i in range(0, len(unique_lats), LAT_CHUNK_SIZE)]

    for i, lat_slice in enumerate(lat_chunks):
        chunk_filename = os.path.join(output_dir, f"births_climate_{i}.parquet")
        # if os.path.exists(chunk_filename):
        #     continue

        print(f"\n--- Processing Latitude Chunk {i+1}/{len(lat_chunks)} (lats: {lat_slice[0]} to {lat_slice[-1]}) ---")
        
        # PRE-LOAD CHUNK INTO MEMORY
        climate_chunk = climate_data.sel(lat=lat_slice).load()
        df_subset = df[df["lat_round"].isin(lat_slice)]
        
        # Group by point ID, which includes time of death
        grouped = df_subset.groupby("point_ID")

        chunk_results = []
        for point_id, group in tqdm(grouped, desc=f"Processing groups in chunk {i+1}"):
            
            # All children in this group have the same climate data needs.
            # We only need to get the parameters from the first row.
            first_row = group.iloc[0]
            lat, lon = first_row['lat_round'], first_row['lon_round']
            from_date, to_date = first_row['from_date'], first_row['to_date']
            # death_idx = first_row['death_month_index']
            
            # 3. Select from the IN-MEMORY climate_chunk. This is very fast.
            point_data = climate_chunk.sel(
                time=slice(from_date, to_date),
                lat=lat,
                lon=lon,
            )
            data_np = point_data.to_array().values.astype(np.float32)

            # A list to hold the results from each timeframe configuration
            result_dict = {}

            # Loop over each timeframe configuration (quarterly, biannual, etc.)
            for timeframe_name, timeframe_dict in ALL_TIMEFRAMES.items():
                                    
                # 1. Dynamically create the time indices for the current configuration
                time_indices_np = np.array(list(timeframe_dict.values()), dtype=np.int32)
             
                # 2. Query the column names for the current configuration
                result_column_names = []
                avg_windows = AVG_WINDOWS[timeframe_name]
                result_column_names = COLUMN_NAMES[timeframe_name]

                # 3. Compute the stats for this configuration
                stats_np = _compute_stats(data_np, time_indices_np, avg_windows, -1) 
                #   I assign -1 because the anchored method was not doing ok.
                #   Comparing a 1-month avg vs a 3-month avg means we assign a higher
                #   weight on dead children because the variance of 1-month is much higher
                
                # 4. Directly update the main dictionary. This is much faster
                # than creating a Series and concatenating later.
                stats_np_flat = stats_np.transpose(1, 0, 2).ravel()
                result_dict.update(dict(zip(result_column_names, stats_np_flat)))

            # Combine the results from all timeframe configurations into a single Series
            result_dict['lat'] = lat
            result_dict['lon'] = lon
            result_dict['point_ID'] = point_id
            chunk_results.append(result_dict)
                       
        # Save intermediate file for this chunk
        if chunk_results:
            climate_results_chunk = pd.DataFrame(chunk_results)
            climate_results_chunk.to_parquet(chunk_filename)
            print(f"Chunk {i+1} saved to {chunk_filename}")

        # 4. FREE UP MEMORY before loading the next chunk
        del climate_chunk, df_subset, grouped, chunk_results, climate_results_chunk
        gc.collect()
        # print("Memory freed for next chunk.")
    
    print("\n--- All chunks processed. Consolidating results... ---")

    # Recontruct the chuncked dataframe    
    files = os.listdir(rf"{DATA_PROC}/DHS_Climate")
    files = [f for f in files if f.startswith("births_climate_")]
    data = []
    for file in tqdm(files):
        df_chunk = pd.read_parquet(rf"{DATA_PROC}/DHS_Climate/{file}")
        data += [df_chunk]
    df = pd.concat(data)
    data = None
    del data
    df.to_parquet(f"{DATA_PROC}/DHS_Climate/births_climate.parquet")
    print("File saved at", f"{DATA_PROC}/DHS_Climate/births_climate.parquet")
    climate_cols = df.columns
    df.to_parquet(rf"{DATA_PROC}/DHS_Climate_not_assigned.parquet")
    gc.collect()
    
    ####### Process data:
    df = full_dhs.merge(df, on="point_ID", how="inner")
    print("Number of observations merged with climate data:", df.shape[0])

    ####### Export data:
    df = df[
        climate_cols.to_list()
        + [
            "ID",
            "interview_year",
            "interview_month",
            "birth_date",
            "last_15_years",
            "last_10_years",
            "since_2003",
        ]
    ]

    # Cast everything in float64 to float32
    float64_cols = df.select_dtypes(include=["float64"]).columns
    if len(float64_cols) > 0:
        print(f"Converting float64 to float32: {float64_cols}")
        df[float64_cols] = df[float64_cols].astype("float32")

    # Drop nans in spi/temp values
    all_shock_cols = [
       col for sublist in COLUMN_NAMES.values() for col in sublist
    ]
    shock_cols = [col for col in all_shock_cols if col in df.columns]
    # df = df.dropna(subset=shock_cols, how="any")
    df.to_parquet(rf"{DATA_PROC}\ClimateShocks_assigned_v11_full.parquet")

    # float16_cols = df.select_dtypes(include=["float16"]).columns
    # if len(float16_cols) > 0:
    #     print(f"Converting float16 to float32: {float16_cols}")
    #     df[float16_cols] = df[float16_cols].astype("float32")
        
    # df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v11.dta")
    # print(f"Data ready! file saved at {DATA_PROC}/ClimateShocks_assigned_v11.dta")
