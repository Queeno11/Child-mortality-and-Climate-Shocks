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
        "absdifm_t",
        "hd35", 
        "hd40", 
        "fd", 
        "id",
    ]
    # MODIFICATION START: Enforce a consistent order of variables for the numpy array conversion
    climate_data = climate_data[climate_variables]
    ORDERED_CLIMATE_VARS = list(climate_data.data_vars)
    # MODIFICATION END


    def round_off(number):
        """Round a number to .25 or .75.
        >>> round_off(1.3)
        1.25
        >>> round_off(2.6)
        2.75
        >>> round_off(3)
        3.25
        """
        
        return round((number - 0.25) * 2) / 2 + 0.25
        
    # 1. Define timeframes as quarters. The value is the 0-based index of the *last month* of the quarter.
    TIMEFRAMES_QUARTERLY = {
        "inutero_1m3m": 2,
        "inutero_4m6m": 5,
        "inutero_6m9m": 8,
        "born_1m3m": 11,
        "born_3m6m": 14,
        "born_6m9m": 17,
    }
    
    # 2. Define the calculation windows. Window '2' is excluded as requested.
    AVG_WINDOWS = np.array([1, 3, 4, 5, 6, 9, 12], dtype=np.int32)
    
    # 3. Add an error check to prevent using window size 2.
    if 2 in AVG_WINDOWS:
        raise ValueError("Averaging window of 2 is not a supported calculation.")

    # 4. The njit function remains the same (calculates max for window=1, avg for others)
    @njit
    def _compute_quarterly_stats_njit(data_array, time_indices, window_sizes):
        """
        Numba-accelerated function to compute quarterly statistics with conditional logic.
        - For window=1: Calculates the MAX value over the 3-month quarter.
        - For window>=3: Calculates the trailing AVG over the specified window.
        All calculations are based on the end_idx of the quarter.
        """
        n_indices = len(time_indices)
        n_vars = data_array.shape[0]
        n_windows = len(window_sizes)
        
        results = np.empty((n_indices, n_vars, n_windows), dtype=data_array.dtype)
        
        for i in range(n_indices):
            end_idx = time_indices[i]
            
            for j in range(n_vars):
                for k in range(n_windows):
                    window = window_sizes[k]
                    
                    if window == 1:
                        quarter_start_idx = max(0, end_idx - 2)
                        quarter_slice = data_array[j, quarter_start_idx : end_idx + 1]
                        results[i, j, k] = np.max(quarter_slice)
                    else: # Handles windows 3, 4, 5, 6...
                        avg_start_idx = max(0, end_idx - window + 1)
                        avg_slice = data_array[j, avg_start_idx : end_idx + 1]
                        results[i, j, k] = np.mean(avg_slice)
                        
        return results

    # 5. REVERTED CHANGE: Pre-calculate column names to *always* use the "_avg" suffix.
    TIME_INDICES_NP = np.array(list(TIMEFRAMES_QUARTERLY.values()), dtype=np.int32)

    RESULT_COLUMN_NAMES = []
    for timename in TIMEFRAMES_QUARTERLY.keys():
        for var in ORDERED_CLIMATE_VARS:
            for window in AVG_WINDOWS:
                # This line now unconditionally uses "_avg" as requested.
                col_name = f"{var}_{timename}_{window}m_avg"
                RESULT_COLUMN_NAMES.append(col_name)

    ### Process dataframe ####
    # Drop nans in date columns
    df = df.dropna(subset=["v008", "chb_year", "chb_month"], how="any")

    # Create datetime object from year and month
    df["day"] = 1
    df["month"] = df["chb_month"].astype(int)
    df["year"] = df["chb_year"].astype(int)
    df["birth_date"] = pd.to_datetime(df[["year", "month", "day"]]).to_numpy()
    df = df.drop(columns=["day", "month", "year"])
    
    # Maximum range of dates
    # FIXME: CHequear que estén bien asignados estos valores
    df["from_date"] = df["birth_date"] + pd.DateOffset(
        months=-9
    )  # From in utero (9 months before birth)
    df["to_date"] = df["birth_date"] + pd.DateOffset(
        months=24
    )  # To the second year of life # FIXME: make it 12 months for faster run

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
    df = df[df["last_15_years"] == True]
       
    df["lat_round"] = df["LATNUM"].apply(lambda x: round_off(x))
    df["lon_round"] = df["LONGNUM"].apply(lambda x: round_off(x))
    df = df.sort_values(["lat_round", "lon_round", "from_date"]) 
    df = df.dropna(subset=["ID", "from_date", "to_date", "lat_round", "lon_round"])
    
    # Store in variable
    df["point_ID"] = df["lat_round"].astype(str) + "_" + df["lon_round"].astype(str) + "_" + df["from_date"].dt.strftime(r"%Y-%m-%d")
    # count unique points
    print("Number of unique points:", df["point_ID"].nunique())
    full_dhs = df.copy()
    df = df.reset_index(drop=True)
    
    ### Run process ####
    print(len(full_dhs))

    files = os.listdir(rf"{DATA_PROC}/DHS_Climate")
    for file in files:
        os.remove(rf"{DATA_PROC}/DHS_Climate/{file}")
    print("Old files removed!")

    grouped = df.groupby(["lat_round", "lon_round"])
    results = []
    for i, stuff in tqdm(enumerate(grouped), total=len(grouped)):
        
        (lat, lon), group = stuff
        
        earliest_from_date = group["from_date"].min()
        latest_to_date = group["to_date"].max()

        point_data = climate_data.sel(
            time=slice(earliest_from_date, latest_to_date),
            lat=lat,
            lon=lon,
        ).load()
        
        date_grouped = group.groupby(["from_date", "to_date"])
        
        for (from_date, to_date), date_group in date_grouped:

            data = point_data.sel(time=slice(from_date, to_date))

            # MODIFICATION START: Use the njit-accelerated path
            # 1. Convert the relevant xarray data to a numpy array
            data_np = data.to_array().values.astype(np.float32)

            # 2. Call the fast Numba function
            # The result is a numpy array of shape (n_timeframes, n_vars)
            stats_np = _compute_quarterly_stats_njit(data_np, TIME_INDICES_NP, AVG_WINDOWS)

            # 3. Create the pandas Series from the numpy result.
            # .ravel() flattens the array in the correct order (C-style, row-major)
            # to match the pre-generated column names.
            stats = pd.Series(stats_np.ravel(), index=RESULT_COLUMN_NAMES, dtype="float16")

            stats["lat"] = lat
            stats["lon"] = lon

            stats_df = pd.DataFrame([stats])
            stats_df["point_ID"] = date_group["point_ID"].iloc[0]
            results.append(stats_df) # Use append for lists
            
        if ((i-1)%1000 == 0) | (i == len(grouped)-1):
            climate_results = pd.concat(results, ignore_index=True)
            climate_results.to_parquet(f"{DATA_PROC}/DHS_Climate/births_climate_{i}.parquet")
            print(f"File saved at {DATA_PROC}/DHS_Climate/births_climate_{i}.parquet")
            results = []
            climate_results = None
    
    files = os.listdir(rf"{DATA_PROC}/DHS_Climate")
    files = [f for f in files if f.startswith("births_climate_")]
    data = []
    for file in tqdm(files):
        df_chunk = pd.read_parquet(rf"{DATA_PROC}/DHS_Climate/{file}")
        data += [df_chunk]
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

    # Cast everything in float64 to float32
    float64_cols = df.select_dtypes(include=["float64"]).columns
    if len(float64_cols) > 0:
        print(f"Converting float64 to float32: {float64_cols}")
        df[float64_cols] = df[float64_cols].astype("float32")

    # Drop nans in spi/temp values
    shock_cols = [col for col in RESULT_COLUMN_NAMES if col in df.columns]
    df = df.dropna(subset=shock_cols, how="any")
    df.to_parquet(rf"{DATA_PROC}\ClimateShocks_assigned_v11.parquet")

    float16_cols = df.select_dtypes(include=["float16"]).columns
    if len(float16_cols) > 0:
        print(f"Converting float16 to float32: {float16_cols}")
        df[float16_cols] = df[float16_cols].astype("float32")
        
    df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v11.dta")
    print(f"Data ready! file saved at {DATA_PROC}/ClimateShocks_assigned_v9e_quarterly.dta")
