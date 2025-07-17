import os
import logging
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm  # for notebooks

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
        "spi3",
        "spi6",
        "spi9",
        "spi12",
        "spi24",
        "spi48",
        "std_t",
        "stdm_t",
        "t",
        "absdif_t",
        "absdifm_t",
        "hd35", 
        "hd40", 
        "fd", 
        "id",
    ]


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
    
    def compute_stats(ds):
        # Initialize an empty dictionary to store results
        results = {}
        
        # Define the time slices
        timeframes = {
            "inutero_1m3m": slice(0, 3),
            "inutero_4m6m": slice(3, 6),
            "inutero_6m9m": slice(6, 9),
            "born_1m3m": slice(9, 12),
            "born_3m6m": slice(12, 15),
            "born_6m9m": slice(15, 18),
            "born_9m12m": slice(18, 21),
            "born_12m15m": slice(21, 24),
            "born_15m18m": slice(24, 27),
            "born_18m21m": slice(27, 30),
            "born_21m24m": slice(30, 33),
        }
        
        # # Process variables in ds_temp
        for timename, filter in timeframes.items():
            
            data = ds.isel(time=filter).mean()
            
            for var in ds.data_vars:

                results[f"{var}_{timename}_avg"] = data[var].item()
            
        # Convert results to pandas Series
        results_series = pd.Series(results, dtype="float16")

        return results_series


    def get_climate_shock(from_date, to_date, lat, lon):
        if pd.isna(from_date):
            return np.nan
        
        # Filter point and time
        point_data= climate_data.sel(
            time=slice(from_date, to_date), lat=lat, lon=lon
        )

        out = compute_stats(point_data)

        return out

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
    )  # To the second year of life

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
    # PREGUNTA PARA PAULA: ¿ella ya hizo el filtro de 15 años y 30 dias?
    df["last_15_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
        df["days_from_interview"] < np.timedelta64(15 * 365, "D")
    )
    df["last_10_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
        df["days_from_interview"] < np.timedelta64(10 * 365, "D")
    )
    df["since_2003"] = df["interview_year"] >= 2003
    df = df[df["last_15_years"] == True]
   
    
    # Lat/Lon coordinates    
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
    
    # Sort data to improve efficiency in querying xarray data (a lot faster!)
    ### Run process ####
    coords_cols = ["lat_climate", "lon_climate"]
    shock_cols = []
    print(len(full_dhs))
    # print("Assigning climate shocks to DHS data...")

    # Group by rounded latitude and longitude
    # grouped = df.groupby(["lat_round", "lon_round"])
    # results = []
    # for i, stuff in tqdm(enumerate(grouped), total=len(grouped)):
        
    #     (lat, lon), group = stuff
    #     # try:
        
    #     # Get the overall time range for this group
    #     earliest_from_date = group["from_date"].min()
    #     latest_to_date = group["to_date"].max()

    #     # Fetch the climate data once for this location and time range
    #     point_data = climate_data.sel(
    #         time=slice(earliest_from_date, latest_to_date),
    #         lat=lat,
    #         lon=lon,
    #     ).load()
        
    #     # Group observations by from_date and to_date
    #     date_grouped = group.groupby(["from_date", "to_date"])
        
    #     for (from_date, to_date), date_group in tqdm(date_grouped, total=len(date_grouped), leave=False):

    #         # Select the data for the specific time range
    #         data = point_data.sel(time=slice(from_date, to_date))
    #         # assert data.time.shape[0] == (9+12+12), f"Time length is not right (must be {9+12+12}): {len(point_data.time)})"

    #         # Compute statistics
    #         stats = compute_stats(data)

    #         stats["lat"] = lat
    #         stats["lon"] = lon

    #         # Assign the computed statistics to all observations in the date group
    #         stats_df = pd.DataFrame([stats])

    #         stats_df["point_ID"] = date_group["point_ID"].iloc[0]

    #         results += [stats_df]
            
            
    #     if ((i-1)%1000 == 0) | (i == len(grouped)-1): # Save every 1000 groups  (or if its the last one) 
    #         climate_results = pd.concat(results, ignore_index=True)
    #         climate_results.to_parquet(f"{DATA_PROC}/DHS_Climate/births_climate_{i}.parquet")
    #         print(f"File saved at {DATA_PROC}/DHS_Climate/births_climate_{i}.parquet")
    #         results = []
    #         climate_results = None
                    
    files = os.listdir(rf"{DATA_PROC}/DHS_Climate")
    files = [f for f in files if f.startswith("births_climate_")]
    data = []
    for file in tqdm(files):
        df = pd.read_parquet(rf"{DATA_PROC}/DHS_Climate/{file}")
        data += [df]
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
    df = df.dropna(subset=shock_cols, how="any")
    df.to_parquet(rf"{DATA_PROC}\ClimateShocks_assigned_v9d.parquet")

    float16_cols = df.select_dtypes(include=["float16"]).columns
    if len(float16_cols) > 0:
        print(f"Converting float16 to float32: {float16_cols}")
        df[float16_cols] = df[float16_cols].astype("float32")
        
    df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v9d.dta")
    print(f"Data ready! file saved at {DATA_PROC}/ClimateShocks_assigned_v9d.dta")
