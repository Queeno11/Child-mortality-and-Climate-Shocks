if __name__ == "__main__":

    import os
    import warnings
    import xarray as xr
    from tqdm import tqdm
    from climate_indices import indices, compute
    from dask.diagnostics import ProgressBar
    from dask.distributed import Client

    # Set global variables
    PROJECT = r"D:\World Bank\Paper - Child mortality and Climate Shocks"
    OUTPUTS = rf"{PROJECT}\Outputs"
    DATA = rf"{PROJECT}\Data"
    DATA_IN = rf"{DATA}\Data_in"
    DATA_PROC = rf"{DATA}\Data_proc"
    DATA_OUT = rf"{DATA}\Data_out"
    ERA5_DATA = rf"D:\Datasets\ERA5 Reanalysis\monthly-land"

    #######################
    # Filter runtime warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    client = Client()  # start distributed scheduler locally.
    print(client)

    ########################
    ####  Process data ####
    ########################

    era5_path = os.path.join(DATA_PROC, "ERA5-Land_monthly_1970-2021.nc")
    if os.path.exists(era5_path):
        print("ERA5 already processed. Loading...")
    else:
        ########################
        ####  Load Data    ####
        print("Loading ERA5 raw data...")
        files = os.listdir(ERA5_DATA)
        datasets = []
        for file in files:
            ds = xr.open_dataset(
                os.path.join(ERA5_DATA, file),
                chunks="auto",
            )
            datasets += [ds]
        precipitation = xr.concat(datasets, dim="time")
        # precipitation = precipitation.chunk({"time": 15})

        print("Raw Data Loaded! Processing...")

        ########################
        ####  Process Data  ####
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
        precipitation = precipitation.rename({"longitude": "lon", "latitude": "lat"})

        ## Temperature is in Kelvin, we need it in Celsius
        precipitation["t2m"] = precipitation["t2m"] - 273.15

        with ProgressBar():
            encoding = {
                var: {"zlib": True, "complevel": 5} for var in precipitation.data_vars
            }
            precipitation.to_netcdf(
                era5_path,
                encoding=encoding,
            )

    precipitation = xr.open_dataset(
        era5_path, chunks={"latitude": 100, "longitude": 100}
    )

    ########################
    ####   Compute SPI  ####
    ########################

    ### Running this takes... A lot. Aprox. 90m for each SPI, so ~7.5h for all SPIs.

    ## Script based on: https://github.com/monocongo/climate_indices/issues/326
    ## Original paper: https://www.droughtmanagement.info/literature/AMS_Relationship_Drought_Frequency_Duration_Time_Scales_1993.pdf
    ## User guide to SPI: https://digitalcommons.unl.edu/cgi/viewcontent.cgi?article=1208&context=droughtfacpub
    #   It is recommended to use SPI-9 or SPI-12 to compute droughts.
    #   "SPI values below -1.5 for these timescales (SPI-9) are usually a good indication that dryness is having a significant impact on
    #    agriculture and may be affecting other sectors as well."
    ## More here: https://www.researchgate.net/profile/Sorin-Cheval/publication/264467702_Spatiotemporal_variability_of_the_meteorological_drought_in_Romania_using_the_Standardized_Precipitation_Index_SPI/links/5842d18a08ae2d21756372f8/Spatiotemporal-variability-of-the-meteorological-drought-in-Romania-using-the-Standardized-Precipitation-Index-SPI.pdf
    ## Ignore negative values, they are normal: https://confluence.ecmwf.int/display/UDOC/Why+are+there+sometimes+small+negative+precipitation+accumulations+-+ecCodes+GRIB+FAQ

    print("Data Ready! Computing SPI. This will take at least a few hours...")

    # Mask data
    print(precipitation)
    da_precip_groupby = precipitation["tp"].stack(point=("lat", "lon")).groupby("point")

    # Parameters
    distribution = indices.Distribution.gamma
    data_start_year = 1970
    calibration_year_initial = 1970
    calibration_year_final = 2020
    periodicity = compute.Periodicity.monthly

    # apply SPI to each `point`
    spis = []
    for i in [1, 3, 6, 9, 12]:
        print(f"Computing SPI-{i}")
        spi_path = os.path.join(DATA_PROC, f"ERA5-Land_monthly_1970-2021_SPI{i}.nc")
        if os.path.exists(spi_path):
            da_spi = xr.open_dataset(
                spi_path, chunks={"latitude": 100, "longitude": 100}
            )
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
                dask="parallelized",
            )
            da_spi = da_spi.unstack("point").rename(f"spi{i}")
            with ProgressBar():
                encoding = {
                    var: {"zlib": True, "complevel": 5} for var in da_spi.data_vars
                }
                da_spi.to_netcdf(
                    spi_path,
                    encoding=encoding,
                )
        spis += [da_spi]

    #########################
    ####   Compute Temp  ####
    #########################

    # Standardize temperature over 30-year average

    stdtemp_path = os.path.join(DATA_PROC, "ERA5-Land_monthly_1970-2021_stdtemp.nc")
    if os.path.exists(stdtemp_path):
        print("Standardized temperature already computed. Skipping...")
    else:
        temperature = xr.open_dataset(era5_path, chunks={"time": 12})
        climatology_mean = temperature["t2m"].mean(dim="time")
        climatology_std = temperature["t2m"].std(dim="time")
        stand_temp = xr.apply_ufunc(
            lambda x, m, s: (x - m) / s,
            temperature["t2m"],
            climatology_mean,
            climatology_std,
            dask="parallelized",
        )

        with ProgressBar():
            encoding = {
                var: {"zlib": True, "complevel": 5} for var in stand_temp.data_vars
            }
            stand_temp.to_netcdf(
                stdtemp_path,
                encoding=encoding,
            )
    stand_temp = xr.open_dataset(stdtemp_path, chunks={"time": 12})
    stand_temp = stand_temp.rename({"t2m": "std_t"})

    # Standardize temperature over 30-year monthly average
    stdmtemp_path = os.path.join(DATA_PROC, "ERA5-Land_monthly_1970-2021_stdmtemp.nc")
    if os.path.exists(stdmtemp_path):
        print("Standardized temperature monthly already computed. Skipping...")
    else:
        temperature = xr.open_dataset(era5_path, chunks={"time": 12})
        climatology_mean_m = temperature["t2m"].groupby("time.month").mean("time")
        climatology_std_m = temperature["t2m"].groupby("time.month").std("time")
        stand_anomalies = xr.apply_ufunc(
            lambda x, m, s: (x - m) / s,
            temperature["t2m"].groupby("time.month"),
            climatology_mean_m,
            climatology_std_m,
            dask="parallelized",
        )
        with ProgressBar():
            encoding = {
                var: {"zlib": True, "complevel": 5} for var in stand_anomalies.data_vars
            }
            stand_anomalies.to_netcdf(
                stdmtemp_path,
                encoding=encoding,
            )

    stand_mtemp = xr.open_dataset(stdmtemp_path, chunks={"time": 12})
    print(stand_mtemp)
    stand_mtemp = stand_mtemp.rename({"t2m": "stdm_t"})

    # List of temperature variables
    temperature = xr.open_dataset(era5_path, chunks={"time": 12})
    temperature = temperature.rename({"t2m": "t"})
    temps = [temperature["t"], stand_temp["std_t"], stand_mtemp["stdm_t"]]

    ########################
    ####   Export data  ####
    ########################

    climate_data = xr.combine_by_coords(spis + temps)
    with ProgressBar():
        out = rf"{DATA_PROC}/Climate_shocks_v6.nc"
        print(f"Data ready! file saved at {out}")
        encoding = {
            var: {"zlib": True, "complevel": 5} for var in climate_data.data_vars
        }
        climate_data.to_netcdf(
            out,
            encoding=encoding,
        )
