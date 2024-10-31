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
    climate_data = xr.open_dataset(rf"{DATA_PROC}/Climate_shocks_v9.nc")
    climate_data = climate_data.drop_vars("month")
    
    ### DHS DATA
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
        "spi24",
        "spi48",
        "std_t",
        "stdm_t",
        "t",
        "absdif_t",
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
        inutero_slice = slice(0, 9)
        born_1m_slice = slice(9, 11)
        born_2to12m_slice = slice(11, 22)
        
        # # Process variables in ds_temp
        for var in ds.data_vars:

            data = ds[var]

            results[f"{var}_inutero_avg"] = data.isel(time=inutero_slice).mean().item()
            results[f"{var}_30d_avg"] = data.isel(time=born_1m_slice).mean().item()
            results[f"{var}_2m12m_avg"] = data.isel(time=born_2to12m_slice).mean().item()

            results[f"{var}_inutero_min"] = data.isel(time=inutero_slice).min().item()
            results[f"{var}_30d_min"] = data.isel(time=born_1m_slice).min().item()
            results[f"{var}_2m12m_min"] = data.isel(time=born_2to12m_slice).min().item()

            results[f"{var}_inutero_max"] = data.isel(time=inutero_slice).max().item()
            results[f"{var}_30d_max"] = data.isel(time=born_1m_slice).max().item()
            results[f"{var}_2m12m_max"] = data.isel(time=born_2to12m_slice).max().item()
        
        # Convert results to pandas Series
        results_series = pd.Series(results)
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
        months=12
    )  # To the first year of life

    # Filter children from_date greater than 1991 (we only have climate data from 1990)
    df = df[df["from_date"] > "1991-01-01"]

    # Filter children to_date smalle than 2021 (we only have climate data to 2020)
    df = df[df["to_date"] < "2021-01-01"]
    df = df[(df["year"] >= 2003)] # Since 2003 (coordinates are ok)
    
    df["lat_round"] = df["LATNUM"].apply(lambda x: round_off(x))
    df["lon_round"] = df["LONGNUM"].apply(lambda x: round_off(x))
    df = df.sort_values(["lat_round", "lon_round", "from_date"]) 
    df = df.dropna(subset=["ID", "from_date", "to_date", "lat_round", "lon_round"])
    df = df.reset_index(drop=True)
    
    # Sort data to improve efficiency in querying xarray data (a lot faster!)
    ### Run process ####
    coords_cols = ["lat_climate", "lon_climate"]
    shock_cols = []

    print("Assigning climate shocks to DHS data...")

    # Group by rounded latitude and longitude
    grouped = df.groupby(["lat_round", "lon_round"])
    results = []
    for i, stuff in tqdm(enumerate(grouped), total=len(grouped)):
        # if (i <= 6_500):
        #     continue
        i_100 = ((i-1) // 100 + 1) * 100  # Round to the nearest 100, but the first one is 0

        file = rf"{DATA_PROC}/DHS_climate/births_climate_{i_100}.parquet"         
        if os.path.exists(file):
            continue
        
        (lat, lon), group = stuff
        try:
            
            # Get the overall time range for this group
            earliest_from_date = group["from_date"].min()
            latest_to_date = group["to_date"].max()

            # Fetch the climate data once for this location and time range
            point_data = climate_data.sel(
                time=slice(earliest_from_date, latest_to_date),
                lat=lat,
                lon=lon,
            ).load()

            for idx, row in tqdm(group.iterrows(), total=len(group), leave=False):
                from_date = row["from_date"]
                to_date = row["to_date"]
               
                # Select the data for the specific time range
                data = point_data.sel(time=slice(from_date, to_date))
                assert data.time.shape[0] == 22, "Time length is not 22: " + str(len(point_data.time))

                # Compute statistics
                stats = compute_stats(data)

                stats["ID"] = row["ID"]
                stats["lat"] = lat
                stats["lon"] = lon

                results.append(stats)
               
            if (i%100 == 0) | (i == len(grouped)-1): # Save every 100 groups  (or if its the last one) 
                        
                climate_results = pd.DataFrame(results)
                climate_results.to_parquet(file, index=False)
                print("File saved at", file)
                results = []
                

        except Exception as e:
            print(e)
            print(f"Error with group: lat {lat}, lon {lon}")
            continue
        
        
    files = os.listdir(rf"{DATA_PROC}/DHS_Climate")
    files = [f for f in files if f.startswith("births_climate_")]
    data = []
    for file in tqdm(files):
        df = pd.read_parquet(rf"{DATA_PROC}/DHS_Climate/{file}")
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
    df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v9.dta")
    print(f"Data ready! file saved at {DATA_PROC}/ClimateShocks_assigned_v9.dta")
