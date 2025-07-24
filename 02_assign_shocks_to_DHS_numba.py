import os
import logging
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm  # for notebooks
from numba import njit # MODIFICATION: Import njit

if __name__ == "__main__":
    tqdm.pandas()

    pd.options.mode.chained_assignment = None  # default='warn'
    logging.getLogger("distributed").setLevel(logging.WARNING)

    # Set global variables
    PROJECT = r"D:\World Bank\Paper - Child Mortality and Climate Shocks"
    OUTPUTS = rf"{PROJECT}\Outputs"
    DATA = rf"{PROJECT}\Data"
    DATA_IN = rf"{DATA}\Data_in"
    DATA_PROC = rf"{DATA}\Data_proc"
    DATA_OUT = rf"{DATA}\Data_out"

    ### Load data #############
    print("Loading data...")

    ### CLIMATE DATA
    climate_data = xr.open_dataset(rf"{DATA_PROC}/Climate_shocks_v9d.nc") # 9d includes hd35, hd40, id and fd
    
    ### DHS DATA
    full_dhs = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_11072024.dta")
    full_dhs["ID"] = full_dhs.index
    # Gen unique id from groups of lat, lon and from_date
    df = full_dhs.copy()
    print("Data loaded! Processing...")

    ###########################

    climate_variables = [
        "spi1",
        "stdm_t",
        # "absdif_t",
        # "hd35", 
        # "hd40", 
        # "fd", 
        # "id",
    ]
    # MODIFICATION START: Enforce a consistent order of variables for the numpy array conversion
    climate_data = climate_data[climate_variables]
    ORDERED_CLIMATE_VARS = list(climate_data.data_vars)
    # MODIFICATION END


    def round_off(number):
        """Round a number to .25 or .75."""
        return round((number - 0.25) * 2) / 2 + 0.25
        
    # Define timeframes outside the function for reuse
    TIMEFRAMES = {
        "inutero_1m": 0, "inutero_2m": 1, "inutero_3m": 2, "inutero_4m": 3,
        "inutero_5m": 4, "inutero_6m": 5, "inutero_7m": 6, "inutero_8m": 7,
        "inutero_9m": 8, "born_1m": 9, "born_2m": 10, "born_3m": 11,
        "born_4m": 12, "born_5m": 13, "born_6m": 14,
    }
    AVG_WINDOWS = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)

    # Create the numba-jitted function. It only works with numpy arrays.
    @njit
    def _compute_multi_window_stats_njit(data_array, time_indices, window_sizes):
        """
        Numba-accelerated function to compute multi-window moving averages for all specified windows.
        """
        n_indices = len(time_indices)
        n_vars = data_array.shape[0]
        n_windows = len(window_sizes)
        
        # Initialize a 3D result array to store results for every combination:
        # (timeframe, variable, window_size)
        results = np.empty((n_indices, n_vars, n_windows), dtype=data_array.dtype)
        
        # Loop over each requested timeframe (e.g., inutero_5m, born_1m)
        for i in range(n_indices):
            end_idx = time_indices[i]
            
            # Loop over each variable (e.g., spi1, std_t)
            for j in range(n_vars):
                
                # KEY LOOP: This loop iterates through EACH of the window sizes
                # in the `window_sizes` array (i.e., 1, 2, 3, 4, 5, 6).
                for k in range(n_windows):
                    window = window_sizes[k] # This will be 1, then 2, then 3, etc.
                    
                    # Handle edge case: start of slice cannot be less than 0
                    start_idx = max(0, end_idx - window + 1)
                    
                    # Take the slice of data for the current variable and calculated window
                    data_slice = data_array[j, start_idx : end_idx + 1]
                    
                    # Numba efficiently computes the mean and stores it
                    results[i, j, k] = np.mean(data_slice)
                    
        return results

    # 2. Pre-calculate the expanded column names for ALL combinations.
    # The order of these loops is critical and must match the 3D array structure above.
    TIME_INDICES_NP = np.array(list(TIMEFRAMES.values()), dtype=np.int32)

    RESULT_COLUMN_NAMES = []
    for timename in TIMEFRAMES.keys():              # Outermost loop -> matches n_indices
        for var in ORDERED_CLIMATE_VARS:            # Middle loop -> matches n_vars
            for window in AVG_WINDOWS:              # Innermost loop -> matches n_windows
                RESULT_COLUMN_NAMES.append(f"{var}_{timename}_{window}m_avg")

    # --- END OF MODIFICATIONS ---


    ### Process dataframe ####
    df = df.dropna(subset=["v008", "chb_year", "chb_month"], how="any")
    df["day"] = 1
    df["month"] = df["chb_month"].astype(int)
    df["year"] = df["chb_year"].astype(int)
    df["birth_date"] = pd.to_datetime(df[["year", "month", "day"]]).to_numpy()
    df = df.drop(columns=["day", "month", "year"])
    
    df["from_date"] = df["birth_date"] + pd.DateOffset(months=-9)
    df["to_date"] = df["birth_date"] + pd.DateOffset(months=24)

    df = df[df["from_date"] > "1991-01-01"]
    df = df[df["to_date"] < "2021-01-01"]
    
    df["year"] = 1900 + (df["v008"] - 1) // 12
    df["month"] = df["v008"] - 12 * (df["year"] - 1900)
    df["day"] = 1
    df["interview_date"] = pd.to_datetime(df[["year", "month", "day"]], dayfirst=False)
    df["interview_year"] = df["year"]
    df["interview_month"] = df["month"]
    df = df.drop(columns=["year", "month", "day"])

    df["days_from_interview"] = df["interview_date"] - df["birth_date"]
    
    df["last_15_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (df["days_from_interview"] < np.timedelta64(15 * 365, "D"))
    df["last_10_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (df["days_from_interview"] < np.timedelta64(10 * 365, "D"))
    df["since_2003"] = df["interview_year"] >= 2003
    df = df[df["last_15_years"] == True]
       
    df["lat_round"] = df["LATNUM"].apply(lambda x: round_off(x))
    df["lon_round"] = df["LONGNUM"].apply(lambda x: round_off(x))
    df = df.sort_values(["lat_round", "lon_round", "from_date"]) 
    df = df.dropna(subset=["ID", "from_date", "to_date", "lat_round", "lon_round"])
    
    df["point_ID"] = df["lat_round"].astype(str) + "_" + df["lon_round"].astype(str) + "_" + df["from_date"].dt.strftime(r"%Y-%m-%d")
    
    print("Number of unique points:", df["point_ID"].nunique())
    full_dhs = df.copy()
    df = df.reset_index(drop=True)
    
    ### Run process ####
    print(len(full_dhs))

    # files = os.listdir(rf"{DATA_PROC}/DHS_Climate")
    # for file in files:
    #     os.remove(rf"{DATA_PROC}/DHS_Climate/{file}")
    # print("Old files removed!")

    # grouped = df.groupby(["lat_round", "lon_round"])
    # results = []
    # for i, stuff in tqdm(enumerate(grouped), total=len(grouped)):
        
    #     (lat, lon), group = stuff
        
    #     earliest_from_date = group["from_date"].min()
    #     latest_to_date = group["to_date"].max()

    #     point_data = climate_data.sel(
    #         time=slice(earliest_from_date, latest_to_date),
    #         lat=lat,
    #         lon=lon,
    #     ).load()
        
    #     date_grouped = group.groupby(["from_date", "to_date"])
        
    #     for (from_date, to_date), date_group in tqdm(date_grouped, total=len(date_grouped), leave=False):

    #         data = point_data.sel(time=slice(from_date, to_date))

    #         # MODIFICATION START: Use the njit-accelerated path
    #         # 1. Convert the relevant xarray data to a numpy array
    #         data_np = data.to_array().values.astype(np.float32)

    #         # 2. Call the fast Numba function
    #         # The result is a numpy array of shape (n_timeframes, n_vars)
    #         stats_np = _compute_multi_window_stats_njit(data_np, TIME_INDICES_NP, AVG_WINDOWS)

    #         # 3. Create the pandas Series from the numpy result.
    #         # .ravel() flattens the array in the correct order (C-style, row-major)
    #         # to match the pre-generated column names.
    #         stats = pd.Series(stats_np.ravel(), index=RESULT_COLUMN_NAMES, dtype="float16")
    #         # MODIFICATION END

    #         stats["lat"] = lat
    #         stats["lon"] = lon

    #         stats_df = pd.DataFrame([stats])
    #         stats_df["point_ID"] = date_group["point_ID"].iloc[0]
    #         results.append(stats_df) # Use append for lists
            
    #     if ((i-1)%1000 == 0) | (i == len(grouped)-1):
    #         climate_results = pd.concat(results, ignore_index=True)
    #         climate_results.to_parquet(f"{DATA_PROC}/DHS_Climate/births_climate_{i}.parquet")
    #         print(f"File saved at {DATA_PROC}/DHS_Climate/births_climate_{i}.parquet")
    #         results = []
    #         climate_results = None
    
    # --- The rest of the script remains unchanged ---
                    
    files = os.listdir(rf"{DATA_PROC}/DHS_Climate")
    files = [f for f in files if f.startswith("births_climate_")]
    data = []
    for file in tqdm(files):
        df_chunk = pd.read_parquet(rf"{DATA_PROC}/DHS_Climate/{file}")
        data.append(df_chunk)
    df = pd.concat(data)
    df.to_parquet(f"{DATA_PROC}/DHS_Climate/births_climate.parquet")
    print("File saved at", f"{DATA_PROC}/DHS_Climate/births_climate.parquet")
    climate_cols = df.columns
    df.to_parquet(rf"{DATA_PROC}/DHS_Climate_not_assigned.parquet")

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

    float64_cols = df.select_dtypes(include=["float64"]).columns
    if len(float64_cols) > 0:
        print(f"Converting float64 to float32: {float64_cols}")
        df[float64_cols] = df[float64_cols].astype("float32")

    # The shock_cols variable was empty in the original script, this might need attention
    # If RESULT_COLUMN_NAMES is what you intended, you can use that.
    shock_cols = [col for col in RESULT_COLUMN_NAMES if col in df.columns]
    df = df.dropna(subset=shock_cols, how="any")
    df.to_parquet(rf"{DATA_PROC}\ClimateShocks_assigned_v9e_months_njit.parquet")

    float16_cols = df.select_dtypes(include=["float16"]).columns
    if len(float16_cols) > 0:
        print(f"Converting float16 to float32: {float16_cols}")
        df[float16_cols] = df[float16_cols].astype("float32")
        
    df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v9e_months_njit.dta")
    print(f"Data ready! file saved at {DATA_PROC}/ClimateShocks_assigned_v9e_months_njit_alt.dta")
