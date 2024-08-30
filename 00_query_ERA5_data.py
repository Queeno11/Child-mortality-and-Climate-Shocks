import os
import cdsapi
from tqdm import tqdm

folder = "/mnt/d/Datasets/ERA5 Reanalysis/monthly-single-levels"
downloaded = os.listdir(folder)
try:
    downloaded = [int(x.split("_")[1].split(".")[0]) for x in downloaded]
    last_year = max(downloaded) if downloaded else (2020 - 51)
except:
    last_year = 2020 - 51

c = cdsapi.Client()
for year in tqdm(range(last_year + 1, 2021)):
    print("Descargando año: ", year)
    c.retrieve(
        "reanalysis-era5-single-levels-monthly-means",
        {
            "format": "netcdf",
            "product_type": "monthly_averaged_reanalysis",
            "variable": [
                "2m_dewpoint_temperature",
                "2m_temperature",
                "surface_pressure",
                "total_precipitation",
            ],
            "year": [f"{year}"],
            "month": [
                "01",
                "02",
                "03",
                "04",
                "05",
                "06",
                "07",
                "08",
                "09",
                "10",
                "11",
                "12",
            ],
            "time": "00:00",
        },
        f"{folder}/data_{year}.zip",
    )
    print(f"Se descargó data_{year}.zip")
