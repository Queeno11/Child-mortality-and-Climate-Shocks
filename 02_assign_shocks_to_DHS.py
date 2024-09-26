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
    climate_data_temp = xr.open_dataset(rf"{DATA_PROC}/Climate_shocks_v6.nc", engine="h5netcdf", chunks={"lat": 1402, "lon":1802, "time":1224})
    climate_data_temp = climate_data_temp[["std_t"]]

    climate_data_spi = xr.load_dataset(rf"{DATA_PROC}/Climate_shocks_v8_spei.nc")

    ### DHS DATA
    full_dhs = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_05142024.dta")
    full_dhs["ID"] = full_dhs.index
    df = full_dhs.copy()
    print("Data loaded! Processing...")

    ###########################

    climate_variables = [
        "spei1",
        "spei3",
        "spei6",
        "spei9",
        "spei12",
        "spei24",
        "spei48",
        "std_t",
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
    
    def compute_stats(ds_temp, ds_spi):
        # Initialize an empty dictionary to store results
        results = {}
        
        # Define the time slices
        inutero_slice = slice(0, 9)
        born_1m_slice = slice(9, 11)
        born_2to12m_slice = slice(11, 22)
        
        # # Process variables in ds_temp
        for var in ds_temp.data_vars:
            data = ds_temp[var].load() # if already in memory, does nothing
            results[f"{var}_inutero_avg"] = data.isel(time=inutero_slice).mean().item()
            results[f"{var}_30d_avg"] = data.isel(time=born_1m_slice).mean().item()
            results[f"{var}_2m12m_avg"] = data.isel(time=born_2to12m_slice).mean().item()
        
        # Process variables in ds_spi
        for var in ds_spi.data_vars:
            data = ds_spi[var]
            results[f"{var}_inutero_avg"] = data.isel(time=inutero_slice).mean().item()
            results[f"{var}_30d_avg"] = data.isel(time=born_1m_slice).mean().item()
            results[f"{var}_2m12m_avg"] = data.isel(time=born_2to12m_slice).mean().item()
        
        # Convert results to pandas Series
        results_series = pd.Series(results)
        return results_series


    def get_climate_shock(from_date, to_date, lat, lon):
        if pd.isna(from_date):
            return np.nan

        # Filter point and time
        point_data_temp = climate_data_temp.sel(
            time=slice(from_date, to_date), lat=lat, lon=lon
        )

        point_data_spi = climate_data_spi.sel(
            time=slice(from_date, to_date), lat=round_off(lat), lon=round_off(lon)
        )

        out = compute_stats(point_data_temp, point_data_spi)

        return out

    ### Process dataframe ####
    # Create datetime object from year and month
    df["day"] = 1
    df["month"] = df["chb_month"].astype(float)
    df["year"] = df["chb_year"].astype(float)
    df["birthdate"] = pd.to_datetime(df[["year", "month", "day"]]).to_numpy()

    # Maximum range of dates
    df["from_date"] = df["birthdate"] + pd.DateOffset(
        months=-9
    )  # From in utero (9 months before birth)
    df["to_date"] = df["birthdate"] + pd.DateOffset(
        months=13
    )  # To the first year of life

    # Filter children from_date greater than 1990 (we only have climate data from 1990)
    df = df[df["from_date"] > "1990-01-01"]

    # Filter children to_date smalle than 2021 (we only have climate data to 2020)
    df = df[df["to_date"] < "2021-01-01"]

    df["lat_round"] = df["LATNUM"].apply(lambda x: np.round(x, decimals=1))
    df["lon_round"] = df["LONGNUM"].apply(lambda x: np.round(x, decimals=1))
    df = df.sort_values(["lat_round",	"lon_round", "from_date"]) 
    df = df.reset_index(drop=True)
    
    # Sort data to improve efficiency in querying xarray data (a lot faster!)
    ### Run process ####
    coords_cols = ["lat_climate", "lon_climate"]
    shock_cols = []

    print("Assigning climate shocks to DHS data...")
    chunk_size = 100_000
    for n in tqdm(range(0, df.index.max(), chunk_size)):

        file = rf"{DATA_PROC}/births_climate_{n}.parquet"
        if os.path.exists(file):
            print(f"{file} exists, moving to next iteration")
            continue

        chunk = df.loc[
            (df.index >= n) & (df.index < n + chunk_size),
            ["ID", "from_date", "to_date", "lat_round", "lon_round"],
        ].copy()

        climate_results = chunk.progress_apply(
            lambda s: get_climate_shock(
                s["from_date"],
                s["to_date"],
                s["lat_round"],
                s["lon_round"],
            ),
            axis=1,
        )

        # Save results into a df
        climate_results = climate_results.apply(pd.Series)
        climate_results["ID"] = chunk["ID"]
        climate_results["lat"] = chunk["lat_round"]
        climate_results["lon"] = chunk["lon_round"]
        climate_results.to_parquet(file, index=False)

    files = os.listdir(rf"{DATA_PROC}")
    files = [f for f in files if f.startswith("births_climate_")]
    data = []
    for file in tqdm(files):
        df = pd.read_parquet(rf"{DATA_PROC}/{file}")
        data += [df]
    df = pd.concat(data)
    climate_cols = df.columns

    ####### Process data:
    df = full_dhs.merge(df, on="ID", how="inner")
    print("Number of observations merged with climate data:", df.shape[0])
    df = df.dropna(subset=["v008", "chb_year", "chb_month"], how="any")

    # Date of interview
    df["year"] = 1900 + (df["v008"] - 1) // 12
    df["month"] = df["v008"] - 12 * (df["year"] - 1900)
    df["day"] = 1
    df["interview_date"] = pd.to_datetime(df[["year", "month", "day"]], dayfirst=False)
    df["interview_year"] = df["year"]
    df["interview_month"] = df["month"]
    df = df.drop(columns=["year", "month", "day"])

    # Date of birth
    df["year"] = df["chb_year"].astype(int)
    df["month"] = df["chb_month"].astype(int)
    df["day"] = 15
    df["birth_date"] = pd.to_datetime(df[["year", "month", "day"]], dayfirst=False)
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

    ####### Export data:
    # df.to_parquet(rf"{DATA_PROC}/ClimateShocks_assigned_v3.parquet")
    df = df[
        climate_cols.to_list()
        + [
            "interview_year",
            "interview_month",
            "birth_date",
            "last_15_years",
            "last_10_years",
            "since_2003",
        ]
    ]

    # Drop nans in spi/temp values
    df = df.dropna(subset=shock_cols, how="any")
    df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v8.dta")
    print(f"Data ready! file saved at {DATA_PROC}/ClimateShocks_assigned_v8.dta")
