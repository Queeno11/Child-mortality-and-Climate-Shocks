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
climate_data = xr.open_dataset(rf"{DATA_OUT}/Climate_shocks_v2_previous_months.nc")
dates = climate_data.time.values

df = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_04172024.dta")
df["ID"] = df.index
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
    for prec in [
        "standarized_precipitation",
        "standarized_precipitation_3",
        "standarized_precipitation_6",
        "standarized_precipitation_12",
    ]:
        # Compute min and max values for both variables
        inutero_q1_mean = inutero_q1[prec].mean().item()
        inutero_q2_mean = inutero_q2[prec].mean().item()
        inutero_q3_mean = inutero_q3[prec].mean().item()
        born_1m_mean = born_1m[prec].mean().item()
        born_2to3m_mean = born_2to3m[prec].mean().item()
        born_3to6m_mean = born_3to6m[prec].mean().item()
        born_6to12m_mean = born_6to12m[prec].mean().item()

        out_vars_this_prec = [
            inutero_q1_mean,
            inutero_q2_mean,
            inutero_q3_mean,
            born_1m_mean,
            born_2to3m_mean,
            born_3to6m_mean,
            born_6to12m_mean,
        ]
        out_vars += out_vars_this_prec

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
prec_cols = [
    "prec_inutero_q1",
    "prec_inutero_q2",
    "prec_inutero_q3",
    "prec_born_1m",
    "prec_born_2to3m",
    "prec_born_3to6m",
    "prec_born_6to12m",
]
prec_3_cols = [
    "prec_3_inutero_q1",
    "prec_3_inutero_q2",
    "prec_3_inutero_q3",
    "prec_3_born_1m",
    "prec_3_born_2to3m",
    "prec_3_born_3to6m",
    "prec_3_born_6to12m",
]
prec_6_cols = [
    "prec_6_inutero_q1",
    "prec_6_inutero_q2",
    "prec_6_inutero_q3",
    "prec_6_born_1m",
    "prec_6_born_2to3m",
    "prec_6_born_3to6m",
    "prec_6_born_6to12m",
]
prec_12_cols = [
    "prec_12_inutero_q1",
    "prec_12_inutero_q2",
    "prec_12_inutero_q3",
    "prec_12_born_1m",
    "prec_12_born_2to3m",
    "prec_12_born_3to6m",
    "prec_12_born_6to12m",
]
all_cols = coords_cols + prec_cols + prec_3_cols + prec_6_cols + prec_12_cols

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

df = df.drop(columns="Unnamed: 0")
df.to_stata(rf"{DATA_PROC}\ClimateShocks_assigned.dta")
