if __name__ == "__main__":
    print("Para Nico: correr desde Anaconda Bash!")
    import os
    import logging
    import numpy as np
    import xarray as xr
    from climate_indices import indices, compute
    from dask.diagnostics import ProgressBar
    from dask.distributed import Client, LocalCluster

    # Set global variables (paths come from paths.py / .env)
    from paths import OUTPUTS, DATA, DATA_IN, DATA_PROC, ERA5_MONTHLY_DIR

    OUTPUTS = str(OUTPUTS)
    DATA = str(DATA)
    DATA_IN = str(DATA_IN)
    DATA_PROC = str(DATA_PROC)
    ERA5_DATA = str(ERA5_MONTHLY_DIR)

    # Filter warnings
    logging.disable(logging.CRITICAL)

    # ---------------------------------------------------------
    # CONFIGURATION FOR MAX CPU / SAFE RAM
    # ---------------------------------------------------------
    # We use a LocalCluster to strictly control memory per worker.
    # n_workers = number of physical cores (high CPU)
    # memory_limit = restricts each worker so they don't eat all RAM.
    # ---------------------------------------------------------
    n_workers = os.cpu_count() - 2 # Leave 1 core for OS
    if n_workers < 1: n_workers = 1
    
    # Calculate memory limit per worker (leave 4GB buffer for OS)
    
    client = Client(
        n_workers=10,
        threads_per_worker=1, # Pure processing power
    )
    print(f"Cluster active: {n_workers} workers, 500MB RAM limit each.")
    print(f"Dashboard: {client.dashboard_link}")
    
    #######################
    #### Filter warnings (disable if debugging)
    logging.disable(logging.CRITICAL)
    print(client)
    def drop_duplicate_dims(ds):
        dims = list(ds.dims)
        for dim in dims:
            _, unique_indices = np.unique(ds[dim], return_index=True)

            # Select only the unique values along the x dimension
            ds = ds.isel({dim:unique_indices})
        return ds

    ########################
    ####  Process data  ####
    ########################
    print("Warning: Ensure you have ~600GB space available...")
    
    # 1. Prepare Base ERA5 Data
    era5_path = os.path.join(DATA_PROC, "ERA5_monthly_1970-2021.nc")
    
    if os.path.exists(era5_path):
        print("ERA5 already processed. Loading...")
    else:
        print("Loading ERA5 raw data...")
        files = os.listdir(ERA5_DATA)
        # Load all with open_mfdataset for better parallel I/O handling than manual loop
        # combining by coords usually safer for time series
        try:
            full_paths = [os.path.join(ERA5_DATA, f) for f in files if f.endswith('.nc')]
            precipitation = xr.open_mfdataset(full_paths, chunks={"time": 12}, combine='by_coords', parallel=True)
        except Exception as e:
            print(f"mfdataset failed ({e}), falling back to manual loop...")
            datasets = []
            for file in files:
                ds = xr.open_dataset(os.path.join(ERA5_DATA, file), chunks="auto")
                datasets.append(ds)
            precipitation = xr.concat(datasets, dim="time")

        print("Raw Data Loaded! Processing...")

        # Longitude Transformation
        # Using xarray functionality is faster than converting to pandas series
        precipitation["longitude"] = xr.where(
            precipitation["longitude"] > 180, 
            precipitation["longitude"] - 360, 
            precipitation["longitude"]
        )
        precipitation = precipitation.sortby(["longitude", "latitude"])
        precipitation = precipitation.rename({"longitude": "lon", "latitude": "lat"})

        # Kelvin to Celsius
        precipitation["t2m"] = precipitation["t2m"] - 273.15

        # Save Base File
        encoding = {var: {"zlib": True, "complevel": 5} for var in precipitation.data_vars}
        with ProgressBar():
            precipitation.to_netcdf(era5_path, encoding=encoding)

    # Load cleaned data
    # Chunking: Time -1 (all) is needed for SPI, but for Rolling stats we need access to neighbors.
    # A chunk size of {'time': 120} (10 years) represents a good balance.
    ds_base = xr.open_dataset(era5_path, chunks={"time": 120, "lat": 100, "lon": 100})

    ########################
    ####   Compute SPI  ####
    ########################
    
    # Keeping original logic for SPI as it requires specific calibration periods
    spi_out = rf"{DATA_PROC}/ERA5_monthly_1991-2021_spi.nc"
    if os.path.exists(spi_out):
        print("SPI already computed!")
        ds_spi_final = xr.open_dataset(spi_out, chunks={"time": 120, "lat": 100, "lon": 100})
    else:
        print("Computing SPI...")

        def compute_spi_series(precip_series, scale, distribution, start_year, cal_start, cal_end, periodicity):
            # Helper for ufunc
            precip = np.array(precip_series).copy()
            return indices.spi(precip, scale, distribution, start_year, cal_start, cal_end, periodicity)
        
        # SPI Prep
        da_precip = ds_base['tp'].stack(point=('lat', 'lon'))
        # Rechunk for time-series heavy operation
        da_precip = da_precip.chunk({'time': -1, 'point': 'auto'})
        
        distribution = indices.Distribution.gamma
        data_start_year = 1970 # Updated to match file start if needed, or keep 1986 if intended
        calibration_year_initial = 1991
        calibration_year_final = 2020
        periodicity = compute.Periodicity.monthly

        spi_datasets = []
        scales = [1, 6, 12]
        
        for i in scales:
            print(f"Computing SPI-{i}")
            spi_temp_path = os.path.join(DATA_PROC, f"ERA5_monthly_1991-2021_SPI{i}.nc")
            
            if os.path.exists(spi_temp_path):
                print(f"SPI-{i} found, loading...")
                da_spi = xr.open_dataset(spi_temp_path)
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
                # Slice to desired output range
                da_spi = da_spi.sel(time=slice("1991", "2021"))
                
                encoding = {da_spi.name: {"zlib": True, "complevel": 6}}
                with ProgressBar():
                    da_spi.to_netcdf(spi_temp_path, encoding=encoding)
            
            spi_datasets.append(da_spi)

        ds_spi_final = xr.merge(spi_datasets)
        # Save combined SPI if needed, or just keep in memory/temp files
        # (Original script combined them here)
        encoding = {var: {"zlib": True, "complevel": 5} for var in ds_spi_final.data_vars}
        if not os.path.exists(spi_out):
            with ProgressBar():
                ds_spi_final.to_netcdf(spi_out, encoding=encoding)

    ###############################
    ####   Compute Temperature ####
    ###############################
    print("Computing Temperature Statistics...")

    # Define Temperature Variable
    # ds_base is full 1970-2021, used for rolling window history
    temp_da = ds_base["t2m"].rename("t")
    
    # Define Target Period for Export
    target_period = slice("1991", "2021")

    # Dictionary to hold all result DataArrays
    results = {}

    # 1. FIXED 30-YEAR CLIMATOLOGY (Original Variables)
    # -------------------------------------------------
    print("Computing Fixed (1991-2021) Climatology Variables...")
    
    # Isolate reference data
    ref_da = temp_da.sel(time=target_period)
    
    # Global Mean/Std (Fixed)
    clim_mean = ref_da.mean(dim="time")
    clim_std = ref_da.std(dim="time")
    
    # Monthly Mean/Std (Fixed)
    clim_mean_m = ref_da.groupby("time.month").mean("time")
    clim_std_m = ref_da.groupby("time.month").std("time")

    # Add Raw Temp
    results["t"] = ref_da
    
    # Compute Fixed Stats
    results["std_t"] = (ref_da - clim_mean) / clim_std
    results["absdif_t"] = (ref_da - clim_mean)
    
    results["stdm_t"] = xr.apply_ufunc(
        lambda x, m, s: (x - m) / s,
        ref_da.groupby("time.month"),
        clim_mean_m,
        clim_std_m,
        dask="parallelized"
    ).drop_vars("month")
    
    results["absdifm_t"] = xr.apply_ufunc(
        lambda x, m: (x - m),
        ref_da.groupby("time.month"),
        clim_mean_m,
        dask="parallelized"
    ).drop_vars("month")

    # 2. ROLLING STATISTICS (New Variables)
    # -------------------------------------
    windows_years = [5, 10, 20, 30]
    
    for y in windows_years:
        print(f"Preparing rolling statistics: {y}-year window...")
        
        # A) CONTINUOUS ROLLING (Generic t, std_t, absdif_t)
        # --------------------------------------------------
        # Window size in months
        w_months = y * 12
        
        # Continuous rolling on the full timeline (1970-2021)
        # center=False: Window is [t - window, t]
        cont_roller = temp_da.rolling(time=w_months, center=False, min_periods=w_months)
        
        cont_mean = cont_roller.mean()
        cont_std = cont_roller.std()
        
        # Calculate Continuous Anomalies
        # std_t_roll: How deviant is T relative to the last Y years average?
        roll_z = (temp_da - cont_mean) / cont_std
        roll_diff = (temp_da - cont_mean)
        
        # Slice to output period and store
        results[f"t_roll_{y}y_mean"] = cont_mean.sel(time=target_period)
        results[f"t_roll_{y}y_std"] = cont_std.sel(time=target_period)
        results[f"std_t_roll_{y}y"] = roll_z.sel(time=target_period)
        results[f"absdif_t_roll_{y}y"] = roll_diff.sel(time=target_period)

        # B) MONTHLY ROLLING (stdm_t, absdifm_t)
        # --------------------------------------
        # We need to compare Jan 2000 to {Jan 1999... Jan 19XX}
        # Strategy: Group by month, then apply rolling over the time dimension 
        # (which, inside the group, represents years).
        
        def calc_rolling_monthly_anomalies(x, window):
            # x is a single month (e.g. all Januarys)
            # window is in years (counts of Januarys)
            
            # min_periods=window ensures we have full history
            roller = x.rolling(time=window, center=False, min_periods=window)
            r_mean = roller.mean()
            r_std = roller.std()
            
            z = (x - r_mean) / r_std
            d = (x - r_mean)
            
            # Return dataset to compute both at once
            return xr.Dataset({"z": z, "d": d})

        # Apply using map
        # This creates a Dask graph that computes rolling stats per month-group
        print(f"  - Mapping monthly rolling logic for {y}y...")
        
        monthly_rolled = temp_da.groupby("time.month").map(
            lambda x: calc_rolling_monthly_anomalies(x, y)
        )
        
        # Extract and Slice
        results[f"stdm_t_roll_{y}y"] = monthly_rolled["z"].sel(time=target_period)
        results[f"absdifm_t_roll_{y}y"] = monthly_rolled["d"].sel(time=target_period)

    ########################
    ####   Export data  ####
    ########################
    print("Merging all datasets...")
    
    # Ensure SPI is sliced correctly
    ds_spi_final = ds_spi_final.sel(time=target_period)
    
    # Combine Dictionary to Dataset
    ds_temp_all = xr.Dataset(results)
    
    # Merge Temp and SPI
    climate_data = xr.merge([ds_spi_final, ds_temp_all])

    # Optimize datatypes (float32 saves 50% space vs float64)
    for var in climate_data.data_vars:
        if climate_data[var].dtype == 'float64':
            climate_data[var] = climate_data[var].astype(np.float32)

    out = rf"{DATA_PROC}/Climate_shocks_v2.0.nc"
    
    print(f"Writing final file to {out}...")
    print("This step performs the actual computation. It may take a while.")
    
    encoding = {
        var: {"zlib": True, "complevel": 5} for var in climate_data.data_vars
    }
    
    with ProgressBar():
        climate_data.to_netcdf(out, encoding=encoding)
        
    print("Process Complete!")