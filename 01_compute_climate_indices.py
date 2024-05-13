import os
import xarray as xr
from tqdm import tqdm
from climate_indices import indices, compute

# Set global variables
PROJECT = r"Z:\Laboral\World Bank\Paper - Child mortality and Climate Shocks"
OUTPUTS = rf"{PROJECT}\Outputs"
DATA = rf"{PROJECT}\Data"
DATA_IN = rf"{DATA}\Data_in"
DATA_PROC = rf"{DATA}\Data_proc"
DATA_OUT = rf"{DATA}\Data_out"
ERA5_DATA = rf"Z:\WB Data\ERA5 Reanalysis\monthly"

#######################

########################
####  Open datasets ####
########################

print("Loading ERA5 raw data...")
files = os.listdir(ERA5_DATA)
datasets = []
for file in tqdm(files):
    ds = xr.open_dataset(os.path.join(ERA5_DATA, file))
    datasets += [ds]
precipitation = xr.concat(datasets, dim="time")
precipitation.to_netcdf(os.path.join(DATA_OUT, "ERA5_monthly_1970-2021.nc"))

precipitation = xr.open_dataset(os.path.join(DATA_OUT, "ERA5_monthly_1970-2021.nc"))

########################
####  Process data  ####
########################

print("Data Loaded! Processing...")


## Longitude is in range 0-360, with 0 at Greenwich.
#   We need to transform it to -180 to 180
def transform_longitude(longitude):
    if longitude > 180:
        return longitude - 360
    else:
        return longitude


precipitation["longitude"] = (
    precipitation["longitude"].to_series().apply(transform_longitude).values
)
precipitation = precipitation.sortby("longitude").sortby("latitude")  # Reorder
precipitation = precipitation.rename({"longitude": "lon", "latitude": "lat"})  # Rename

## Mask values on the sea, as we only need country data.
# Data from countries comes from non-nan values in the precipitation_cckp dataset
countries = xr.open_dataset(
    rf"{DATA_IN}\Climate Data\timeseries-pr-monthly-mean_cru_monthly_cru-ts4.06-timeseries_mean_1901-2021.nc"
)
mask = countries["timeseries-pr-monthly-mean"].isel(time=0).notnull()
# Interpolate mask to ERA5 resolution
mask = (
    mask.astype(int)
    .interp(lat=precipitation.lat, lon=precipitation.lon, method="nearest")
    .astype(bool)
)

# Mask data
precipitation = precipitation.where(mask)


########################
####   Compute SPI  ####
########################
print("Data Ready! Computing SPI. This will take at least a few hours...")

### Running this takes... A lot. Aprox. 90m for each SPI, so ~7.5h for all SPIs.

## Script based on: https://github.com/monocongo/climate_indices/issues/326
## Original paper: https://www.droughtmanagement.info/literature/AMS_Relationship_Drought_Frequency_Duration_Time_Scales_1993.pdf
## User guide to SPI: https://digitalcommons.unl.edu/cgi/viewcontent.cgi?article=1208&context=droughtfacpub
#   It is recommended to use SPI-9 or SPI-12 to compute droughts.
#   "SPI values below -1.5 for these timescales (SPI-9) are usually a good indication that dryness is having a significant impact on
#    agriculture and may be affecting other sectors as well."
## More here: https://www.researchgate.net/profile/Sorin-Cheval/publication/264467702_Spatiotemporal_variability_of_the_meteorological_drought_in_Romania_using_the_Standardized_Precipitation_Index_SPI/links/5842d18a08ae2d21756372f8/Spatiotemporal-variability-of-the-meteorological-drought-in-Romania-using-the-Standardized-Precipitation-Index-SPI.pdf
## Ignore negative values, they are normal: https://confluence.ecmwf.int/display/UDOC/Why+are+there+sometimes+small+negative+precipitation+accumulations+-+ecCodes+GRIB+FAQ

# Parameters
distribution = indices.Distribution.gamma
data_start_year = 1970
calibration_year_initial = 1970
calibration_year_final = 2020
periodicity = compute.Periodicity.monthly

da_precip_groupby = precipitation["tp"].stack(point=("lat", "lon")).groupby("point")

# apply SPI to each `point`
spis = []
for i in [1, 3, 6, 9, 12]:
    print(f"Computing SPI-{i}")
    spi_path = os.path.join(DATA_OUT, f"ERA5_monthly_1970-2021_SPI{i}.nc")
    if os.path.exists(spi_path):
        da_spi = xr.open_dataset(spi_path)
        print(f"SPI-{i} already computed. Skipping...")
    else:
        da_spi = xr.apply_ufunc(
            indices.spi,
            da_precip_groupby,
            i,
            distribution,
            data_start_year,
            calibration_year_initial,
            calibration_year_final,
            periodicity,
        )
        da_spi = da_spi.unstack("point").rename(f"spi{i}")
        da_spi.to_netcdf(spi_path)
    spis += [da_spi]

climate_data = xr.combine_by_coords(spis + [precipitation["t2m"]])

########################
####   Export data  ####
########################

climate_data.to_netcdf(rf"{DATA_OUT}/Climate_shocks_v3_spi.nc")
print(f"Data ready! file saved at {DATA_OUT}/Climate_shocks_v3_spi.nc")
