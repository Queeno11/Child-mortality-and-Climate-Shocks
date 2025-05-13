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

climate = pd.read_stata(rf"{DATA_PROC}/ClimateShocks_assigned_v9d.dta")
births = births.merge(climate, on="ID", how="inner")

# 2.2 add income group --------------------------------------------------------
births = births.merge(df_iso[["code_iso3", "wbincomegroup"]], on="code_iso3", how="inner")

# 2.3 add climate bands & south-hemisphere dummy ------------------------------
bands = pd.read_stata(
    rf"{DATA_PROC}/DHSBirthsGlobalAnalysis_11072024_climate_bands_assigned.dta"
)
births = births.merge(bands, on="ID_HH", how="inner")
print(f"Data loaded! Number of observations: {births.shape[0]}")

# ---------- 3.  Climate-shock feature engineering ----------
print("Creating variables...")
climate_list  = ["std_t", "stdm_t", "spi1", "spi3", "spi6", "spi9", "spi12", "spi24", "hd35", "hd40", "fd", "id"]
time_list = [
    "inutero_1m3m", "inutero_4m6m", "inutero_6m9m",
    "born_1m3m",    "born_3m6m",    "born_6m9m", "born_9m12m",
    "born_12m15m",  "born_15m18m",  "born_18m21m", "born_21m24m"
]

newcols = {}
for var in tqdm(climate_list):
    for t in tqdm(time_list, leave=False):
        base = f"{var}_{t}_avg"
        if base not in births.columns:
            print(f"Column {base} not in births columns, skipping...")
            continue

        s = births[base].copy()       
        newcols[f'{base}_sq']  = s * s            # ^2
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
for v in ('mother_ageb', 'mother_eduy'):
    s = pd.Series(births[v], dtype="UInt32").copy()
    newcols[f'{v}_squ'] = s ** 2
    newcols[f'{v}_cub'] = s ** 3

newcols = pd.DataFrame(newcols, index=births.index)

births = pd.concat(
    [births, newcols],
    axis=1,
    copy=False                   # avoid an extra copy here
)

# immediately defragment so later ops stay fast & small
births = births.copy()

# Birth order within mother (Stata: by ID_R: gen birth_order = _n)
# use NumPy’s lightweight lexsort to get the permutation
order = np.lexsort((
    births['chb_month'].values,   # 3rd key (minor)
    births['chb_year'].values,    # 2nd
    births['ID_R'].values         # 1st (major)
))

# reorder the frame in-place (take() is a *view*, not a copy)
births = births.take(order)

# now rows are already grouped + sorted, so cumcount is O(n) & cheap
births['birth_order'] = births.groupby('ID_R', sort=False).cumcount().astype('int16') + 1

births["chb_year_sq"] = births["chb_year"] ** 2


# ---------- 4.  Child age-at-death dummies (per 1 000 births) ----------
bins   = [0, 3, 6, 9, 12, 15, 18, 21, 24]          # right-open intervals
labels = [
    "1m3m", "3m6m", "6m9m", "9m12m",
    "12m15m", "15m18m", "18m21m", "21m24m"
]
agecol = "child_agedeath"                          # assumes months

# Remove possibly pre-existing column
births.drop(columns=[f"child_agedeath_{lab}" for lab in labels], errors="ignore", inplace=True)

cat = pd.cut(births[agecol], bins=bins, labels=labels, right=False)
for lab in labels:
    births[f"child_agedeath_{lab}"] = ((cat == lab) * 1_000).astype("int16")  # per 1 000 births

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

# ---------- 6.  Keep/Drop variables exactly as in Stata ----------
print("Dropping variables...")
IDs = ["ID", "ID_R", "ID_CB", "ID_HH",]
climate_shocks = [
    col for col in births.columns if col.startswith(("t_", "std_t_", "stdm_t_", "absdif_t_", "absdifm_t_", "spi", "hd35", "hd40", "fd", "id",))
]
controls = [
    "child_fem", "child_mulbirth", "birth_order", "rural",
    "d_weatlh_ind_2", "d_weatlh_ind_3", "d_weatlh_ind_4", "d_weatlh_ind_5",
    "mother_age", "mother_ageb", "mother_ageb_squ", "mother_ageb_cub",
    "mother_eduy", "mother_eduy_squ", "mother_eduy_cub",
    "chb_month", "chb_year", "chb_year_sq",
]
death_vars = [
    col for col in births.columns if col.startswith("child_agedeath_")
]
fixed_effects = [
    col for col in births.columns if col.startswith("ID_cell")
]
mechanisms = [
    "pipedw", "helec", "href", "hhaircon", "hhfan", "hhelectemp", "wbincomegroup",
]
heterogeneities = [
    "climate_band_3", "climate_band_2", "climate_band_1", "southern"
]

keep_vars = IDs + climate_shocks + controls + death_vars + fixed_effects + mechanisms + heterogeneities
births = births[keep_vars]   # deduplicate & preserve order

# Drop obsolete child-death variants
births.drop(columns=[
    "child_agedeath_30d", "child_agedeath_30d3m",
    "child_agedeath_6m12m", "child_agedeath_12m"
], errors="ignore", inplace=True)

# ---------- 7.  Cast floats to float32 to mimic Stata's 'recast float' -------
print("Recasting categoricals...")
for col in tqdm(fixed_effects+mechanisms+heterogeneities):
    births[col] = pd.Categorical(births[col])

print("Recasting floats...")
float_cols = births.select_dtypes(include=["float64"]).columns
for col in tqdm(float_cols):
    
    if births[col].max() < np.finfo(np.float16).max:
        births[col] = births[col].astype("float16", errors="raise")
    elif births[col].max() < np.finfo(np.float32).max:
        births[col] = births[col].astype("float32", errors="raise")
    else:
        print(f"Column {col} too large for float32, skipping...")
        
# births = births.reset_index(drop=True)

# ---------- 8.  Save outputs (.dta 118 and .csv) -----------------------------
print("Writing files...")
out_feather   = rf"{DATA_OUT}/DHSBirthsGlobal&ClimateShocks_v9d.feather"

# `df` is your pandas DataFrame
feather.write_feather(
    births, 
    out_feather,    
    version=2,
    compression='zstd',
)

print("✓ Files written:",
      f"\n  • {out_feather}")
