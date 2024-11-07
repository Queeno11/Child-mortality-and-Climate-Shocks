if __name__ == "__main__":
    print("Para Nico: correr desde Anaconda Bash!")
    import os
    import logging
    import numpy as np
    import xarray as xr
    from climate_indices import indices, compute, utils
    from dask.diagnostics import ProgressBar
    from dask.distributed import Client

    # Set global variables
    PROJECT = r"D:\World Bank\Paper - Child mortality and Climate Shocks"
    OUTPUTS = rf"{PROJECT}\Outputs"
    DATA = rf"{PROJECT}\Data"
    DATA_IN = rf"{DATA}\Data_in"
    DATA_PROC = rf"{DATA}\Data_proc"
    DATA_OUT = rf"{DATA}\Data_out"
    ERA5_DATA = r"D:\Datasets\ERA5 Reanalysis\monthly-single-levels"

    #######################
    #### Filter warnings (disable if debugging)
    logging.disable(logging.CRITICAL)

    def drop_duplicate_dims(ds):
        dims = list(ds.dims)
        for dim in dims:
            _, unique_indices = np.unique(ds[dim], return_index=True)

            # Select only the unique values along the x dimension
            ds = ds.isel({dim:unique_indices})
        return ds

    ########################
    ####  Process data ####
    ########################
    print("Warning: Running this scripts takes about a few days and requires ~600GB to store all the required data. Ensure you have such space available...")
    era5_path = os.path.join(DATA_PROC, "ERA5_monthly_1970-2021.nc")
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
        era5_path#, chunks={"latitude": 10, "longitude": 10, "time": -1}
    )

    # # Select south america: -71.894531,-29.228890,-43.593750,-3.337954
    # precipitation = precipitation.sel(
    #     lat=slice(-20, -10),
    #     lon=slice(-60, -50),
    # )
    
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

    print("Data Ready!")
    spi_out = rf"{DATA_PROC}\ERA5_monthly_1991-2021_spi.nc"
    if os.path.exists(spi_out):
        print("SPI already computed!")
    else:
        print("Computing SPI. This will take at least a few hours...")

        def compute_spi_series(precip_series, scale, distribution, data_start_year, calibration_year_initial, calibration_year_final, periodicity):
            # Ensure the array is writable
            precip = np.array(precip_series)
            precip = precip.copy()
            return indices.spi(
                precip,
                scale,
                distribution,
                data_start_year,
                calibration_year_initial,
                calibration_year_final,
                periodicity,
            )
        
        # Stack 'lat' and 'lon' into a 'point' dimension
        da_precip = precipitation['tp'].stack(point=('lat', 'lon'))
        da_precip = da_precip.chunk({'time': -1, 'point': 100000})
        print(da_precip.chunks)
        # Parameters
        distribution = indices.Distribution.gamma
        data_start_year = 1986
        calibration_year_initial = 1991
        calibration_year_final = 2020
        periodicity = compute.Periodicity.monthly

        # apply SPI to each `point`
        spis = []
        for i in [1, 3, 6, 9, 12, 24, 48]:
            print(f"Computing SPI-{i}")
            spi_path = os.path.join(DATA_PROC, f"ERA5_monthly_1991-2021_SPI{i}.nc")
            if os.path.exists(spi_path):
                da_spi = xr.open_dataset(
                    spi_path, chunks={"time": 12, "latitude": 500, "longitude": 500}
                )
                print(f"SPI-{i} already computed. Skipping...")
            else:
                da_spi_stacked = xr.apply_ufunc(
                    compute_spi_series,
                    da_precip,
                    i,
                    distribution,
                    data_start_year,
                    calibration_year_initial,
                    calibration_year_final,
                    periodicity,
                    input_core_dims=[["time"], [], [], [], [], [], []],
                    output_core_dims=[["time"]],
                    output_dtypes=[np.float32],
                    vectorize=True,
                    dask="parallelized",
                )                
                da_spi = da_spi_stacked.unstack('point').rename(f'spi{i}')
                # da_spi = da_spi.sel(time=slice("1991", "2021") ) # Only last 30 years
                encoding = {da_spi.name: {"zlib": True, "complevel": 6}}
                with ProgressBar():
                    da_spi.to_netcdf(spi_path, encoding=encoding)
                    
            spis += [da_spi]

        spis = xr.combine_by_coords(spis)
        encoding = {name: {"zlib": True, "complevel": 5} for name in spis.data_vars}
        with ProgressBar():
            spis.to_netcdf(spi_out)

    #########################
    ####   Compute Temp  ####
    #########################

    # Standardize temperature over 30-year average

    stdtemp_path = os.path.join(DATA_PROC, "ERA5_monthly_1991-2021_stdtemp.nc")
    if os.path.exists(stdtemp_path):
        print("Standardized temperature already computed. Skipping...")

    else:
        print("Computing standardized temperature...")
        temperature = xr.open_dataset(era5_path, chunks={"time": 12})
        temperature = temperature.sel(time=slice("1991", "2021")) # Only last 30 years
        climatology_mean = temperature["t2m"].mean(dim="time")
        climatology_std = temperature["t2m"].std(dim="time")
        stand_temp = xr.apply_ufunc(
            lambda x, m, s: (x - m) / s,
            temperature["t2m"],
            climatology_mean,
            climatology_std,
            dask="parallelized",
        )

        encoding = {stand_temp.name: {"zlib": True, "complevel": 5}}
        with ProgressBar():
            stand_temp.to_netcdf(
                stdtemp_path,
                encoding=encoding,
            )
            

    absdiff_path = os.path.join(DATA_PROC, "ERA5_monthly_1991-2021_absdifftemp.nc")
    if os.path.exists(absdiff_path):
        print("Abs diff temperature already computed. Skipping...")

    else:
        print("Computing Abs diff temperature...")
        temperature = xr.open_dataset(era5_path, chunks={"time": 12})
        temperature = temperature.sel(time=slice("1991", "2021")) # Only last 30 years
        climatology_mean = temperature["t2m"].mean(dim="time")
        climatology_std = temperature["t2m"].std(dim="time")
        absdiff_temp = xr.apply_ufunc(
            lambda x, m: (x - m),
            temperature["t2m"],
            climatology_mean,
            dask="parallelized",
        )

        encoding = {absdiff_temp.name: {"zlib": True, "complevel": 5}}
        with ProgressBar():
            absdiff_temp.to_netcdf(
                absdiff_path,
                encoding=encoding,
            )

    # Standardize temperature over 30-year monthly average
    stdmtemp_path = os.path.join(DATA_PROC, "ERA5_monthly_1991-2021_stdmtemp.nc")
    if os.path.exists(stdmtemp_path):
        print("Standardized temperature monthly already computed. Skipping...")
    else:
        print("Computing temperature anomalies...")
        temperature = xr.open_dataset(era5_path, chunks={"time": -1, "lat": 500, "lon": 500})
        temperature = temperature.sel(time=slice("1991", "2021")) # Only last 30 years
        climatology_mean_m = temperature["t2m"].groupby("time.month").mean("time")
        climatology_std_m = temperature["t2m"].groupby("time.month").std("time")
        stand_anomalies = xr.apply_ufunc(
            lambda x, m, s: (x - m) / s,
            temperature["t2m"].groupby("time.month"),
            climatology_mean_m,
            climatology_std_m,
            dask="parallelized",
        )
        encoding = {stand_anomalies.name: {"zlib": True, "complevel": 5}}
        with ProgressBar():
            stand_anomalies.to_netcdf(
                stdmtemp_path,
                encoding=encoding,
            )

    absdiffm_path = os.path.join(DATA_PROC, "ERA5_monthly_1991-2021_absdiffmtemp.nc")
    if os.path.exists(absdiffm_path):
        print("Abs diff monthly temperature already computed. Skipping...")

    else:
        print("Computing monthly temperature anomalies...")
        temperature = xr.open_dataset(era5_path, chunks={"time": -1, "lat": 500, "lon": 500})
        temperature = temperature.sel(time=slice("1991", "2021")) # Only last 30 years
        climatology_mean_m = temperature["t2m"].groupby("time.month").mean("time")
        climatology_std_m = temperature["t2m"].groupby("time.month").std("time")
        stand_anomalies = xr.apply_ufunc(
            lambda x, m: (x - m),
            temperature["t2m"].groupby("time.month"),
            climatology_mean_m,
            dask="parallelized",
        )
        encoding = {stand_anomalies.name: {"zlib": True, "complevel": 5}}
        with ProgressBar():
            stand_anomalies.to_netcdf(
                absdiffm_path,
                encoding=encoding,
            )

    stand_temp = xr.open_dataset(stdtemp_path, chunks={"lat": 700, "lon": 700, "time": 120})
    stand_temp = stand_temp.rename({"t2m": "std_t"})

    absdiff_temp = xr.open_dataset(absdiff_path, chunks={"lat": 700, "lon": 700, "time": 120})
    absdiff_temp = absdiff_temp.rename({"t2m": "absdif_t"})

    absdiffm_temp = xr.open_dataset(absdiffm_path, chunks={"lat": 700, "lon": 700, "time": 120})
    absdiffm_temp = absdiffm_temp.rename({"t2m": "absdifm_t"})

    stand_mtemp = xr.open_dataset(stdmtemp_path, chunks={"lat": 700, "lon": 700, "time": 120})
    stand_mtemp = stand_mtemp.rename({"t2m": "stdm_t"})

    temperature = xr.open_dataset(era5_path, chunks={"lat": 700, "lon": 700, "time": 120}).sel(time=slice("1991", "2021"))
    temperature = temperature.rename({"t2m": "t"})

    spis = xr.open_dataset(spi_out, chunks={"lat": 700, "lon": 700, "time": 120}).sel(time=slice("1991", "2021"))

    data_arrays = [spis, temperature["t"], stand_temp["std_t"], stand_mtemp["stdm_t"], absdiff_temp["absdif_t"], absdiffm_temp["absdifm_t"]]


    ########################
    ####   Export data  ####
    ########################

    climate_data = xr.combine_by_coords(data_arrays)

    out = rf"{DATA_PROC}/Climate_shocks_v9.nc"
    encoding = {
        var: {"zlib": True, "complevel": 6} for var in climate_data.data_vars
    }
    with ProgressBar():
        climate_data.to_netcdf(
            out,
            encoding=encoding,
        )
    print(f"Data ready! file saved at {out}")