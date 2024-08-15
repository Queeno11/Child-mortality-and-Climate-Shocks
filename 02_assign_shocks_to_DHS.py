import os
import logging
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm  # for notebooks
from dask.distributed import Client
import swifter

if __name__ == "__main__":

    swifter.set_defaults(
        force_parallel=True,
    )

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
    climate_data = xr.open_dataset(rf"{DATA_PROC}/Climate_shocks_v4.nc")
    dates = climate_data.time.values

    full_dhs = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_05142024.dta")
    full_dhs["ID"] = full_dhs.index
    df = full_dhs.copy()
    print("Data loaded! Processing...")
    ###########################

    climate_variables = [
        "spi1",
        "spi3",
        "spi6",
        "spi9",
        "spi12",
        "t",
        "std_t",
        "stdm_t",
    ]

    def get_climate_shock(from_date, to_date, lat, lon):
        if pd.isna(from_date):
            return np.nan

        # Filter point
        climate_data_vars = climate_data[climate_variables]
        point_data = climate_data_vars.sel(time=slice(from_date, to_date)).sel(
            lat=lat, lon=lon, method="nearest"
        )

        # Get position of original data
        lat = point_data.lat.item()
        lon = point_data.lon.item()

        # Filter by time
        ## 80% of the children are born between week 37 (~8.5 months) and 41 (~9.5 months) of gestation.
        ## https://commons.wikimedia.org/wiki/File:Distribution_of_gestational_age_at_childbirth.jpg
        ## Given we only have monthly data, we will consider the first 8 months of gestation as in utero (month 0 to 7)
        ## Because ~50% of the children are born before the 15th the initial month, on average, we will ensure that
        ## most of the kids' first month of life is included in the "1st month of life" category (month 8 to 10)
        inutero = point_data.isel(time=slice(0, 8))  # 8 not included
        born_1m = point_data.isel(time=slice(8, 10))
        born_2to12m = point_data.isel(time=slice(10, 21))

        out = [lat, lon]
        for ds_time in [inutero, born_1m, born_2to12m]:
            time_avg = ds_time.mean(dim="time").to_array().values
            time_min = ds_time.min(dim="time").to_array().values
            time_max = ds_time.max(dim="time").to_array().values
            out = np.concatenate([out, time_avg, time_min, time_max])

        return out.tolist()

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

    # # Construct deathdate variable
    # df["deathdate"] = df[df["child_agedeath"]<=12].progress_apply(lambda x: x["birthdate"] + pd.DateOffset(months=x["child_agedeath"]), axis=1)

    # # Replace to_date with deathdate if the child died
    # df["to_date"] = np.where((df["child_agedeath"]<=12) & (df["deathdate"]<df["to_date"]), df["deathdate"], df["to_date"])

    # Filter children to_date smalle than 2021 (we only have climate data to 2020)
    df = df[df["to_date"] < "2021-01-01"]

    ### Run process ####
    coords_cols = ["lat_climate", "lon_climate"]
    shock_cols = []
    for name in climate_variables:
        shock_cols += [
            f"{name}_inutero_avg",
            f"{name}_inutero_min",
            f"{name}_inutero_max",
            f"{name}_30d_mean",
            f"{name}_30d_min",
            f"{name}_30d_max",
            f"{name}_2m12m_mean",
            f"{name}_2m12m_min",
            f"{name}_2m12m_max",
        ]
    all_cols = coords_cols + shock_cols

    print("Assigning climate shocks to DHS data...")
    # Initialize Dask client
    client = Client()

    chunk_size = 1_000_000
    for n in tqdm(range(0, df.ID.max(), chunk_size)):
        if os.path.exists(rf"{DATA_PROC}/births_climate_{n}.csv"):
            print(f"births_climate_{n}.csv exists, moving to next iteration")
            continue
        chunk = df.loc[
            (df.ID >= n) & (df.ID < n + chunk_size),
            ["ID", "from_date", "to_date", "LATNUM", "LONGNUM"],
        ].copy()
        if chunk.shape[0] == 0:
            continue
        climate_results = chunk.swifter.apply(
            lambda s: get_climate_shock(
                s["from_date"], s["to_date"], s["LATNUM"], s["LONGNUM"]
            ),
            axis=1,
        )
        climate_results = climate_results.apply(pd.Series)
        climate_results.columns = all_cols
        climate_results["ID"] = chunk["ID"]
        climate_results.to_csv(rf"{DATA_PROC}/births_climate_{n}.csv", index=False)

    files = os.listdir(rf"{DATA_PROC}")
    files = [f for f in files if f.startswith("births_climate_")]
    data = []
    for file in tqdm(files):
        df = pd.read_csv(rf"{DATA_PROC}/{file}")
        data += [df]
    df = pd.concat(data)

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
        all_cols
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

    # Drop nans in spi/temp values
    df = df.dropna(subset=shock_cols, how="any")
    df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v4.dta")
    print(f"Data ready! file saved at {DATA_PROC}/ClimateShocks_assigned_v4.dta")
