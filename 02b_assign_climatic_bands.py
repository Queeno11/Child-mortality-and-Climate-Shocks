import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd
from geocube.vector import vectorize
import matplotlib.pyplot as plt

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
    
## Load DHS data
df = pd.read_stata(r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_in\DHS\DHSBirthsGlobalAnalysis_05142024.dta")
gdf_dhs = df[["ID_HH","LATNUM","LONGNUM"]].drop_duplicates(subset="ID_HH")
gdf_dhs = gpd.GeoDataFrame(gdf_dhs, geometry=gpd.points_from_xy(gdf_dhs["LONGNUM"], gdf_dhs["LATNUM"]))

# Merge DHS and climate bands
gdf_dhs = gdf_dhs.set_crs("epsg:4326", allow_override=True)
gdf = gdf.set_crs("epsg:4326", allow_override=True)
gdf_dhs_climatebands = gdf_dhs.sjoin(gdf)

# Export
outpath = r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_proc\DHSBirthsGlobalAnalysis_05142024_climate_bands_assigned.dta"
gdf_dhs_climatebands = gdf_dhs_climatebands[["ID_HH", "climate_band_3", "climate_band_2", "climate_band_1"]].drop_duplicates("ID_HH")
gdf_dhs_climatebands.to_stata(outpath)
print(f"Se creó el archivo {outpath}")