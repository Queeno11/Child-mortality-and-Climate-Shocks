# ---------- 0.  Packages & paths ----------
import os  
import pandas as pd
import numpy as np
from tqdm import tqdm
import pyarrow.feather as feather

# Stata globals → Python Path objects
PROJECT = r"D:\World Bank\Paper - Child mortality and Climate Shocks"
OUTPUTS = rf"{PROJECT}\Outputs"
DATA = rf"{PROJECT}\Data"
DATA_IN = rf"{DATA}\Data_in"
DATA_PROC = rf"{DATA}\Data_proc"
DATA_OUT = rf"{DATA}\Data_out"

# Make sure output folders exist
# os.mkdir(parents=True, exist_ok=True)

# ---------- 1.  Country income-group lookup ----------
print("Loading and merging data...")
df_iso = pd.read_excel(
    r"D:\World Bank\Data-Portal-Brief-Generator\Data\Data_Raw\Country codes & metadata\country_classification.xlsx",
)
df_iso = df_iso.rename(columns={"wbcode": "code_iso3"})

# ---------- 2.  Read DHS births file & successive merges ----------
# 2.1 births + shocks ---------------------------------------------------------
births = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_11072024.dta")
births["ID"] = np.arange(len(births))

climate = pd.read_stata(rf"{DATA_PROC}/ClimateShocks_assigned_v9e_months.dta")
births = births.merge(climate, on="ID", how="inner")

# 2.2 add income group --------------------------------------------------------
births = births.merge(df_iso[["code_iso3", "wbincomegroup"]], on="code_iso3", how="inner")

# 2.3 add climate bands, south-hemisphere dummy and RWI ------------------------------
bands = pd.read_stata(
    rf"{DATA_PROC}/DHSBirthsGlobalAnalysis_11072024_climate_bands_assigned.dta"
)
births = births.merge(bands, on="ID_HH", how="inner")
print(f"Data loaded! Number of observations: {births.shape[0]}")

# ---------- 3.  Climate-shock feature engineering ----------
print("Creating variables...")
climate_list  = ["std_t", "stdm_t", "absdif_t", "absdifm_t", "spi1"]#, "spi3", "spi6", "spi9", "spi12", "spi24", "hd35", "hd40", "fd", "id"]
time_list = [
    "inutero_1m", "inutero_2m", "inutero_3m", 
    "inutero_4m", "inutero_5m", "inutero_6m", 
    "inutero_7m", "inutero_8m", "inutero_9m", 
    "born_1m", "born_2m", "born_3m", 
    "born_4m", "born_5m", "born_6m"
]

newcols = {}
for var in tqdm(climate_list):
    for t in tqdm(time_list, leave=False):
        base = f"{var}_{t}_avg"
        if base not in births.columns:
            print(f"Column {base} not in births columns, skipping...")
            continue

        s = births[base].copy()       
        # newcols[f'{base}_sq']  = s * s            # ^2
        newcols[f'{base}_pos'] = (s >= 0).astype(bool)
        newcols[f'{base}_neg'] = (s <= 0).astype(bool)
    
        mu, sigma = float(s.mean()), float(s.std())
        for k in (1, 2):
            thr_pos, thr_neg = mu + k*sigma, mu - k*sigma
            newcols[f'{base}_gt{k}']   = (s >=  thr_pos).astype(bool)
            newcols[f'{base}_bt0{k}']  = ((s <  thr_pos) & (s >= 0)).astype(bool)
            newcols[f'{base}_bt0m{k}'] = ((s >= thr_neg) & (s <  0)).astype(bool)
            newcols[f'{base}_ltm{k}']  = (s <  thr_neg).astype(bool)

# mother covariates
s = births['mother_ageb'].copy()
newcols['mother_ageb_squ'] = pd.to_numeric(s**2, downcast="float") # to_numeric compresses to the smallest possible dtype
newcols['mother_ageb_cub'] = pd.to_numeric(s**3, downcast="float")
s = births['mother_eduy'].copy()
newcols['mother_eduy_squ'] = pd.to_numeric(s**2, downcast="integer")
newcols['mother_eduy_cub'] = pd.to_numeric(s**3, downcast="integer")
s = None

newcols = pd.DataFrame(newcols, index=births.index)

births = pd.concat(
    [births, newcols],
    axis=1,
    copy=False                   # avoid an extra copy here
)

# # immediately defragment so later ops stay fast & small
# births = births.copy()

## Birth Order
# build a “months since year 0” key
months = births["chb_year"].astype(int) * 12 + births["chb_month"].astype(int)

# rank that key within each mother by ascending value
births["birth_order"] = (
    months
    .groupby(births["ID_R"])
    .rank(method="first", ascending=True)
    .astype("int16")
)

## Year sq and dummies
births["chb_year_sq"] = births["chb_year"] ** 2

# Correct categorical variables to dummy
for col in ["hhaircon", "hhfan"]:
    births[col] = births[col].map(lambda x: 1 if x == "Yes" else 0).astype(bool)

# Create relative wealth index (rwi) quintiles indicator
births["rwi_quintiles"] = pd.qcut(births["rwi"], 5, labels=False, duplicates="drop") + 1
births["rwi_deciles"] = pd.qcut(births["rwi"], 10, labels=False, duplicates="drop") + 1

# ---------- 4.  Child age-at-death dummies (per 1 000 births) ----------
labels = [
    "1m", "2m", "3m", "4m", "5m", "6m",
]

births.drop(columns=[f"child_agedeath_{lab}" for lab in labels], errors="ignore", inplace=True)
for month, lab in enumerate(labels):
    # Remove possibly pre-existing column
    births[f"child_agedeath_{lab}"] = ((births["child_agedeath"] == month) * 1_000).astype("int16")  # per 1 000 births

# ---------- 5.  Location & household controls ----------
print("Creating location and time fixed effects...")
## 0.1° (original), 0.25°, 0.5°, 1° and 2° aggregations
# For 0.1°, we use the coordinates straight from the ERA5 cell where we extract climate data
births["lat_climate_1"] = births["lat"]            # 0.1°
births["lon_climate_1"] = births["lon"]

# For 0.25° onwards, we group based on the DHS original coordinates (cell would work too...)
lat, lon = births["LATNUM"], births["LONGNUM"]

births["lat_climate_2"] = np.round(lat * 4) / 4    # 0.25°
births["lon_climate_2"] = np.round(lon * 4) / 4

births["lat_climate_3"] = np.round(lat * 2) / 2    # 0.5°
births["lon_climate_3"] = np.round(lon * 2) / 2

births["lat_climate_4"] = np.round(lat)            # 1°
births["lon_climate_4"] = np.round(lon)

# births["lat_climate_5"] = births["lat_climate_4"] - (births["lat_climate_4"] % 2)  # 2°
# births["lon_climate_5"] = births["lon_climate_4"] - (births["lon_climate_4"] % 2)

# Factorise to integer IDs
for j in range(1, 5):
    births[f"ID_cell{j}"] = births.groupby([f'lat_climate_{j}', f'lon_climate_{j}'], sort=False).ngroup()

# Country numeric code (Stata: encode code_iso3)
births["ID_country"] = births.groupby("code_iso3", sort=False).ngroup()


# Survey ID and time trend
births["IDsurvey_country"] = births.groupby("v000").ngroup()
births["time"]     = births["chb_year"] - 1989 # Start in 1990 = 1
births["time_sq"]  = births["time"] ** 2

# ---------- 6.  Set final dataset  ----------
print("Dropping variables...")
IDs = ["ID", "ID_R", "ID_CB", "ID_HH",]
climate_shocks = [
    col for col in births.columns if col.startswith(("t_", "std_t_", "stdm_t_", "absdif_t_", "absdifm_t_", "spi", "hd35", "hd40", "fd", "id",))
]
controls = [
    "child_fem", "child_mulbirth", "birth_order", "rural",
    "d_weatlh_ind_2", "d_weatlh_ind_3", "d_weatlh_ind_4", "d_weatlh_ind_5",
    "mother_ageb", "mother_ageb_squ", "mother_ageb_cub",
    "mother_eduy", "mother_eduy_squ", "mother_eduy_cub",
    "chb_month", "chb_year", "chb_year_sq", "rwi", 
]
death_vars = [
    col for col in births.columns if col.startswith("child_agedeath_")
]
fixed_effects = [
    col for col in births.columns if col.startswith("ID_cell")
]
mechanisms = [
    "pipedw", "helec", "href", "hhaircon", "hhfan", "hhelectemp", 
]
heterogeneities = [
    "climate_band_3", "climate_band_2", "climate_band_1", "southern", "wbincomegroup", "rwi_quintiles", "rwi_deciles",
]

keep_vars = IDs + climate_shocks + controls + death_vars + fixed_effects + mechanisms + heterogeneities
births = births[keep_vars]   # deduplicate & preserve order

# Drop obsolete child-death variants
births.drop(columns=[
    "child_agedeath_30d", "child_agedeath_30d3m",
    "child_agedeath_6m12m", "child_agedeath_12m"
], errors="ignore", inplace=True)

# ---------- 7.  Compress dataframe  ----------------------------------------
print("Recasting categoricals...")
for col in tqdm(heterogeneities):
    births[col] = pd.Categorical(births[col])

print("Recasting floats...")
float_cols = births.select_dtypes(include=["float64", "float32"]).columns
for col in tqdm(float_cols):
    births[col] = pd.to_numeric(births[col], downcast="float")
    if births[col].max() < np.finfo(np.float16).max:
        births[col] = births[col].astype("float16", errors="raise")
    elif births[col].max() < np.finfo(np.float32).max:
        births[col] = births[col].astype("float32", errors="raise")
    else:
        print(f"Column {col} too large for float32, skipping...")

print("Recasting ints...")
int_cols = births.select_dtypes(include=["int64", "int32"]).columns
for col in tqdm(int_cols):
    births[col] = pd.to_numeric(births[col], downcast="integer") # to_numeric compresses to the smallest possible dtype

# births = births.reset_index(drop=True)

# ---------- 8.  Save outputs (.dta 118 and .csv) -----------------------------
print("Writing files...")
out_feather   = rf"{DATA_OUT}/DHSBirthsGlobal&ClimateShocks_v9e_months.feather"

# `df` is your pandas DataFrame
feather.write_feather(
    births, 
    out_feather,    
    version=2,
    compression='zstd',
)

print("✓ Files written:",
      f"\n  • {out_feather}")
