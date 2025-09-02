# ---------- 0.  Packages & paths ----------
import os  
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import numpy as np
from tqdm import tqdm
import pyarrow.feather as feather
import pyarrow.parquet as pq

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
births = pd.read_stata(rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_07272025.dta")
births["ID"] = np.arange(len(births))
print(births.shape[0])

parquet_file = pq.ParquetFile(rf"{DATA_PROC}/ClimateShocks_assigned_v11.parquet")
cols = parquet_file.schema.names

# 2. Create the list of columns you want to read (all except the excluded one)
cols_to_exclude = []
for extremes in ["hd35", "hd40", "fd", "id"]:
    for window in ["b_w1", "b_w2", "b_w3", "b_w4", "b_w5", "b_w6", "b_w7", "b_w8", "b_w9"]:
        cols_to_exclude += [col for col in cols if (extremes in col and window in col)]
columns_to_read = [col for col in cols if col not in cols_to_exclude]# print(cols_to_exclude)

climate = pd.read_parquet(rf"{DATA_PROC}/ClimateShocks_assigned_v11.parquet", columns=columns_to_read).set_index("ID")
# Cast everything in float64 to float32
climate_shocks = [
    col for col in climate.columns if col.startswith(("t_", "std_t_", "stdm_t_", "absdif_t_", "absdifm_t_", "spi", "hd35", "hd40", "fd", "id",))
]
for col in tqdm(climate_shocks):
    climate[col] = pd.to_numeric(climate[col], downcast="float")
    if climate[col].max() < np.finfo(np.float16).max:
        climate[col] = climate[col].astype("float16", errors="raise")
    elif climate[col].max() < np.finfo(np.float32).max:
        climate[col] = climate[col].astype("float32", errors="raise")
    else:
        print(f"Column {col} too large for float32, skipping...")
        
births = climate.join(births, how="inner")
print(births.shape[0])

# 2.2 add income group --------------------------------------------------------
births = births.merge(df_iso[["code_iso3", "wbincomegroup"]], on="code_iso3", how="inner")
print(births.shape[0])

# 2.3 add climate bands, south-hemisphere dummy and RWI ------------------------------
bands = pd.read_stata(
    rf"{DATA_PROC}/DHSBirthsGlobalAnalysis_07272025_climate_bands_assigned.dta"
)
births = births.merge(bands, on="ID_HH", how="inner")
print(f"Data loaded! Number of observations: {births.shape[0]}")
print(births.shape[0])

# ---------- 3.  Climate-shock feature engineering ----------
print("Creating variables...")
climate_list  = ["absdifm_t", "stdm_t", "spi1", "hd35", "hd40", "fd", "id"]
time_list = [
    # TIMEFRAMES_QUARTERLY
    "inutero_1m3m", "inutero_3m6m", "inutero_6m9m",
    "born_1m3m"   , "born_3m6m"   , "born_6m9m", "born_9m12m", 
    # TIMEFRAMES_BIANNUALY
    "inutero", "born_1m6m", "born_6m12m", 
    "born_12m18m", "born_18m24m", "born_24m30m", "born_30m36m",     
    # TIMEFRAMES_IUFOCUS
    "born_1m", "born_2m3m",
    # TIMEFRAMES_MONTHLY
    "inutero_1m","inutero_2m","inutero_3m",
    "inutero_4m","inutero_5m","inutero_6m",
    "inutero_7m","inutero_8m","inutero_9m",
    "born_2m","born_3m","born_4m","born_5m","born_6m",
]
stats_list = [
    "q_min", "q_max", "q_avg", 
    "m_avg", 
    "iu_max", "iu_min", "iu_avg", 
    "b_max", "b_min", "b_avg", "b_w1", "b_w2", "b_w3", "b_w4", "b_w5", "b_w6", "b_w7", "b_w8", "b_w9"
]

# This will try all the combinations between stat and time_list and create the available vars
newcols = {}
for var in tqdm(climate_list):
    for t in tqdm(time_list, leave=False):
        for stat in stats_list:
            
            # if "_max" in stat:
            #     stat = "maxmin"
            #     base_pos = f"{var}_{t}_max"
            #     base_neg = f"{var}_{t}_min"
            # else:
            #     base_pos = f"{var}_{t}_{stat}"
            #     base_neg = f"{var}_{t}_{stat}"
                
            base = f"{var}_{t}_{stat}"
            
            if (base not in births.columns):
                print(f"Column {base} not in births columns, skipping...")
                continue
            
            s = births[base].copy()
            # newcols[f'{base}_sq']  = s * s            # ^2
            newcols[f'{base}_pos'] = (s >= 0).astype(bool)
            newcols[f'{base}_neg'] = (s <= 0).astype(bool)
            
        
            # mu, sigma = float(s.mean()), float(s.std())
            # for k in (1, 2):
            #     thr_pos, thr_neg = mu + k*sigma, mu - k*sigma
            #     newcols[f'{base}_gt{k}']   = (s >=  thr_pos).astype(bool)
            #     newcols[f'{base}_bt0{k}']  = ((s <  thr_pos) & (s >= 0)).astype(bool)
            #     newcols[f'{base}_bt0m{k}'] = ((s >= thr_neg) & (s <  0)).astype(bool)
            #     newcols[f'{base}_ltm{k}']  = (s <  thr_neg).astype(bool)

# mother covariates
s = births['mother_ageb'].copy()
newcols['mother_ageb_squ'] = pd.to_numeric(s**2, downcast="float") # to_numeric compresses to the smallest possible dtype
newcols['mother_ageb_cub'] = pd.to_numeric(s**3, downcast="float")
s = births['mother_eduy'].copy()
newcols['mother_eduy_squ'] = pd.to_numeric(s**2, downcast="integer")
newcols['mother_eduy_cub'] = pd.to_numeric(s**3, downcast="integer")
s = None
bins = [0, 7, 13, 35, np.inf]
names = ['6 years or less', '6-12 years', 'more than 12 years', 'No data']
births['mother_educ'] = pd.cut(births['mother_eduy'], bins, labels=names)

newcols = pd.DataFrame(newcols, index=births.index)

births = pd.concat(
    [births, newcols],
    axis=1,
    copy=False                   # avoid an extra copy here
)

# immediately defragment so later ops stay fast & small
births = births.copy()

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

# Create wealth index indicators
births["rwi_tertiles"] = pd.qcut(births["rwi"], 3, labels=False) + 1
births["rwi_quintiles"] = pd.qcut(births["rwi"], 5, labels=False) + 1

# Create house indicators: "housing_quality_index", "heat_protection_index", "cold_protection_index"
births["high_quality_housing"] = pd.qcut(births["housing_quality_index"], 2, labels=False)
births["high_heat_protection"] = pd.qcut(births["heat_protection_index"], 2, labels=False)
births["high_cold_protection"] = pd.qcut(births["cold_protection_index"], 2, labels=False)

# ---------- 4.  Child age-at-death dummies (per 1 000 births) ----------
bins_labels = {
    "quarterly": {
        "bins": [0, 3, 6, 9, 12, 15, np.inf],
        "labels": ["1m3m", "3m6m", "6m9m", "9m12m", "12m15m", "alive"]
    },
    "biannual": {
        "bins": [0, 6, 12, 18, 24, 30, 36,  np.inf],
        "labels": ["1m6m", "6m12m", "12m18m", "18m24m", "24m30m", "30m36m", "alive"],
    },
    "inutero": {
        "bins": [0, 1, 3, 7, np.inf],
        "labels": ["1m", "2m3m", "3m7m", "alive"],
    },    
    "months": {
        "bins": [0, 1, 2, 3, 4, 5, 6, np.inf],
        "labels": ["1m", "2m", "3m", "4m", "5m", "6m", "alive"],
    }    

}
births["child_agedeath"] = births["child_agedeath"].fillna(1000) # 1000 so it neves gets in any condition
for bin_name, data in bins_labels.items():

    bins, labels = data["bins"], data["labels"] 
    
    # Remove possibly pre-existing column
    births.drop(columns=[f"child_agedeath_{lab}" for lab in labels], errors="ignore", inplace=True)

    cat = pd.cut(births["child_agedeath"], bins=bins, labels=labels, right=False)
    print(cat.value_counts())
    for lab in labels:
        births[f"child_agedeath_{lab}"] = ((cat == lab) * 1_000).astype("int16")  # per 1 000 births
        assert births[f"child_agedeath_{lab}"].mean()>0, lab

# ---------- 5.  Location & household controls ----------
print("Creating location and time fixed effects...")
## 0.25° (original), 0.5° and 1° aggregations
# For 0.25°, we use the coordinates straight from the ERA5 cell where we extract climate data
births["lat_climate_1"] = births["lat"]            # 0.1°
births["lon_climate_1"] = births["lon"]

# For 0.5° onwards, we group based on the DHS original coordinates (cell would work too...)
lat, lon = births["LATNUM"], births["LONGNUM"]

births["lat_climate_2"] = np.round(lat * 2) / 2    # 0.5°
births["lon_climate_2"] = np.round(lon * 2) / 2

births["lat_climate_3"] = np.round(lat)            # 1°
births["lon_climate_3"] = np.round(lon)

# births["lat_climate_5"] = births["lat_climate_4"] - (births["lat_climate_4"] % 2)  # 2°
# births["lon_climate_5"] = births["lon_climate_4"] - (births["lon_climate_4"] % 2)

# Factorise to integer IDs
for j in range(1, 4):
    births[f"ID_cell{j}"] = births.groupby([f'lat_climate_{j}', f'lon_climate_{j}'], sort=False).ngroup()

# Country numeric code (Stata: encode code_iso3)
births["ID_country"] = births.groupby("code_iso3", sort=False).ngroup()

# Survey ID and time trend
births["IDsurvey_country"] = births.groupby("v000").ngroup()
births["time"]     = births["chb_year"] - 1989 # Start in 1990 = 1
births["time_sq"]  = births["time"] ** 2

# ---------- 6.  Set final dataset  ----------
print("Dropping variables...")
IDs = ["ID", "ID_R", "ID_CB", "ID_HH", "lat", "lon", "child_agedeath"]
climate_shocks = [
    col for col in births.columns if col.startswith(("t_", "std_t_", "stdm_t_", "absdif_t_", "absdifm_t_", "spi", "hd35", "hd40", "fd", "id",))
]
controls = [
    "child_fem", "child_mulbirth", "birth_order", "rural", "poor",
    "weatlh_ind", "d_weatlh_ind_1", "d_weatlh_ind_2", "d_weatlh_ind_3", "d_weatlh_ind_4", "d_weatlh_ind_5",
    "mother_ageb", "mother_ageb_squ", "mother_ageb_cub",
    "mother_eduy", "mother_eduy_squ", "mother_eduy_cub", "mother_educ",
    "chb_month", "chb_year", "chb_year_sq", "rwi", 
]
death_vars = [
    col for col in births.columns if col.startswith("child_agedeath_")
]
fixed_effects = [
    col for col in births.columns if col.startswith("ID_cell")
]
mechanisms = [
    "pipedw", "refrigerator", "electricity", "hhaircon", "hhfan", 
    "housing_quality_index", "heat_protection_index", "cold_protection_index",  
    "high_quality_housing", "high_heat_protection", "high_cold_protection",
]
heterogeneities = [
    "climate_band_3", "climate_band_2", "climate_band_1", "southern", "wbincomegroup", "rwi_tertiles", "rwi_quintiles",
]

keep_vars = IDs + climate_shocks + controls + death_vars + fixed_effects + mechanisms + heterogeneities
births = births[keep_vars]   # deduplicate & preserve order

# Drop obsolete child-death variants
births.drop(columns=[
    "child_agedeath_30d", "child_agedeath_30d3m", "child_agedeath_12m"
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
out_feather   = rf"{DATA_OUT}/DHSBirthsGlobal&ClimateShocks_v11_nanmean.feather"

# `df` is your pandas DataFrame
feather.write_feather(
    births, 
    out_feather,    
    version=2,
    compression='zstd',
)

print("✓ Files written:",
      f"\n  • {out_feather}")
