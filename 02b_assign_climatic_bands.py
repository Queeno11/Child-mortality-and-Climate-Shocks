import os
import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd
from geocube.vector import vectorize
import matplotlib.pyplot as plt

print("Cargando y procesando bases...")
##### CLIMATIC BANDS #####
da = xr.open_dataset(r"D:\Datasets\Köppen-Geiger Climate Classification\KG_1986-2010.grd", engine="rasterio").band_data.sel(band=1)

# To geopandas
gdf = vectorize(da)

## Set legends
# Legends can be found in the R file provided by the official distro: https://koeppen-geiger.vu-wien.ac.at/present.htm
# They are in alphabetical order, so for example Af is band 1 and As band 3. Band 32 is the ocean
# Drop ocean
gdf = gdf[gdf.band_data != 32]

# Assign categories
categories_3 = [
    'Af', 'Am', 'As', 'Aw', 
    'BSh', 'BSk', 'BWh', 'BWk', 
    'Cfa', 'Cfb','Cfc', 'Csa', 'Csb', 'Csc', 'Cwa','Cwb', 'Cwc', 
    'Dfa', 'Dfb', 'Dfc','Dfd', 'Dsa', 'Dsb', 'Dsc', 'Dsd','Dwa', 'Dwb', 'Dwc', 'Dwd', 
    'EF','ET', 
]
labels_3 = dict(zip(range(1, 32), categories_3))
labels_2 = {
    "Af":"Tropical (Rainforest)", 
    "Am":"Tropical (Monsoon)", 
    "As":"Tropical (Savanna, dry winter)", 
    "Aw":"Tropical (Savanna, dry summer)", 
    "BS":"Arid desert", 
    "BW":"Semi-Arid steppe", 
    "Cf":"Temperate (No dry season)", 
    "Cs":"Temperate (Dry summer)", 
    "Cw":"Temperate (Dry winter)", 
    "Df":"Continental (No dry season)", 
    "Ds":"Continental (Dry summer)", 
    "Dw":"Continental (Dry winter)", 
    "EF":"Polar (Tundra)",
    "ET":"Polar (Ice cap)",
}
labels_1 = {"A":"Tropical", "B":"Arid", "C":"Temperate", "D":"Continental", "E":"Polar"}


## Replace values with legends
gdf['climate_band_3'] = gdf['band_data'].map(labels_3)
gdf['climate_band_2'] = gdf['climate_band_3'].str[0:2].map(labels_2)
gdf['climate_band_1'] = gdf['climate_band_3'].str[0].map(labels_1)

# Show
for band in ["climate_band_1", "climate_band_2", "climate_band_3"]:
    f = gdf.plot(column=band, legend=True)
    plt.savefig(fr"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_out\{band}.png", dpi=300)
    print(f"Se creó la figura Data_out\{band}")

##### META SPATIAL RELATIVE WEALTH INDEX #####

path = r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_in\relative-wealth-index-april-2021"
files = os.listdir(path)

dfs = []
for file in files:
    if file.endswith(".csv"):
        file_path = os.path.join(path, file)
        df = pd.read_csv(file_path)
    dfs += [df]
df = pd.concat(dfs, ignore_index=True)

gdf_rwi = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["longitude"], df["latitude"])).drop(columns=["longitude", "latitude"])
gdf_rwi = gdf_rwi.set_crs("EPSG:4326")

dfs = None
df = None


##### LOAD DHS DATA #####
    
print("Procesando base de DHS... Esto puede tardar unos minutos")
df = pd.read_stata(r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_in\DHS\DHSBirthsGlobalAnalysis_11072024.dta")
gdf_dhs = df[["ID_HH","LATNUM","LONGNUM"]].drop_duplicates(subset="ID_HH")
gdf_dhs = gpd.GeoDataFrame(gdf_dhs, geometry=gpd.points_from_xy(gdf_dhs["LONGNUM"], gdf_dhs["LATNUM"]))


##### SPATIAL MERGES #####
print("Realizando merges espaciales...")

# Merge DHS and climate bands
gdf_dhs = gdf_dhs.set_crs("epsg:4326", allow_override=True)
gdf = gdf.set_crs("epsg:4326", allow_override=True)
gdf_dhs = gdf_dhs.sjoin(gdf)
gdf_dhs = gdf_dhs.drop(columns="index_right")

# Merge DHS and RWI
gdf_dhs = gdf_dhs.sjoin_nearest(gdf_rwi, how="left", max_distance=.1, distance_col="distance")
gdf_dhs = gdf_dhs.rename(columns={"distance":"rwi_distance"})

## Create southern hemisphere dummy 
gdf_dhs["southern"] = (gdf_dhs["LATNUM"]<0)

##### EXPORT #####
print("Exportando archivo...")
outpath = r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_proc\DHSBirthsGlobalAnalysis_11072024_climate_bands_assigned.dta"
gdf_dhs = gdf_dhs[["ID_HH", "climate_band_3", "climate_band_2", "climate_band_1", "southern", "rwi", "rwi_distance"]].drop_duplicates("ID_HH")
gdf_dhs.to_stata(outpath)
print(f"Se creó el archivo {outpath}")