import os
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm  # for notebooks

pd.options.mode.chained_assignment = None  # default='warn'
tqdm.pandas()

# Set global variables
PROJECT = r"Z:\Laboral\World Bank\Paper - Child mortality and Climate Shocks"
OUTPUTS = rf"{PROJECT}\Outputs"
DATA = rf"{PROJECT}\Data"
DATA_IN = rf"{DATA}\Data_in"
DATA_PROC = rf"{DATA}\Data_proc"
DATA_OUT = rf"{DATA}\Data_out"

### Load data #############
climate_data = xr.open_dataset(rf"{DATA_OUT}/Climate_shocks_v3_spi.nc")
dates = climate_data.time.values

full_dhs = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_04172024.dta")
full_dhs["ID"] = full_dhs.index
df = full_dhs.copy()
###########################


def get_climate_shock(from_date, to_date, lat, lon):
    if pd.isna(from_date):
        return np.nan

    # Filter point
    point_data = climate_data.sel(time=slice(from_date, to_date)).sel(
        lat=lat, lon=lon, method="nearest"
    )

    # Get position of original data
    lat = point_data.lat.item()
    lon = point_data.lon.item()

    # Filter by time
    inutero_q1 = point_data.isel(time=slice(0, 3))
    inutero_q2 = point_data.isel(time=slice(3, 6))
    inutero_q3 = point_data.isel(time=slice(6, 9))
    born_1m = point_data.isel(time=slice(9, 10))
    born_2to3m = point_data.isel(time=slice(10, 12))
    born_3to6m = point_data.isel(time=slice(12, 15))
    born_6to12m = point_data.isel(time=slice(15, 21))

    out_vars = [
        lat,
        lon,
    ]
    for spi in [
        "spi1",
        "spi3",
        "spi6",
        "spi9",
        "spi12",
    ]:
        # Compute mean values for SPI
        inutero_q1_mean = inutero_q1[spi].mean().item()
        inutero_q2_mean = inutero_q2[spi].mean().item()
        inutero_q3_mean = inutero_q3[spi].mean().item()
        born_1m_mean = born_1m[spi].mean().item()
        born_2to3m_mean = born_2to3m[spi].mean().item()
        born_3to6m_mean = born_3to6m[spi].mean().item()
        born_6to12m_mean = born_6to12m[spi].mean().item()

        out_vars_this_spi = [
            inutero_q1_mean,
            inutero_q2_mean,
            inutero_q3_mean,
            born_1m_mean,
            born_2to3m_mean,
            born_3to6m_mean,
            born_6to12m_mean,
        ]
        out_vars += out_vars_this_spi

    # Compute mean values for temperature
    inutero_q1_temp_mean = inutero_q1["t2m"].mean().item()
    inutero_q2_temp_mean = inutero_q2["t2m"].mean().item()
    inutero_q3_temp_mean = inutero_q3["t2m"].mean().item()
    born_1m_temp_mean = born_1m["t2m"].mean().item()
    born_2to3m_temp_mean = born_2to3m["t2m"].mean().item()
    born_3to6m_temp_mean = born_3to6m["t2m"].mean().item()
    born_6to12m_temp_mean = born_6to12m["t2m"].mean().item()

    out_vars_temp = [
        inutero_q1_temp_mean,
        inutero_q2_temp_mean,
        inutero_q3_temp_mean,
        born_1m_temp_mean,
        born_2to3m_temp_mean,
        born_3to6m_temp_mean,
        born_6to12m_temp_mean,
    ]
    out_vars += out_vars_temp

    return out_vars


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
df["to_date"] = df["birthdate"] + pd.DateOffset(years=1)  # To the first year of life

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
spi_cols = []
for i in [1, 3, 6, 9, 12]:
    spi_cols += [
        f"spi{i}_inutero_q1",
        f"spi{i}_inutero_q2",
        f"spi{i}_inutero_q3",
        f"spi{i}_born_1m",
        f"spi{i}_born_2to3m",
        f"spi{i}_born_3to6m",
        f"spi{i}_born_6to12m",
    ]
temp_cols = [
    "temp_inutero_q1",
    "temp_inutero_q2",
    "temp_inutero_q3",
    "temp_born_1m",
    "temp_born_2to3m",
    "temp_born_3to6m",
    "temp_born_6to12m",
]
all_cols = coords_cols + spi_cols + temp_cols

for n in tqdm(range(0, df.ID.max(), 10_000)):
    if os.path.exists(rf"{DATA_PROC}/births_climate_{n}.csv"):
        print(f"births_climate_{n}.csv exists, moving to next iteration")
        continue
    chunk = df.loc[
        (df.ID >= n) & (df.ID < n + 10_000),
        ["ID", "from_date", "to_date", "LATNUM", "LONGNUM"],
    ].copy()
    if chunk.shape[0] == 0:
        continue
    climate_results = chunk[["from_date", "to_date", "LATNUM", "LONGNUM"]].apply(
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
df = df.dropna(subset=spi_cols + temp_cols, how="any")
df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned_v3.dta")
