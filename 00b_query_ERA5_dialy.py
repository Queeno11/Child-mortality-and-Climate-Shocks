import os
import cdsapi
from tqdm import tqdm

folder = "/mnt/d/Datasets/ERA5 Reanalysis/dialy-single-levels"
downloaded = os.listdir(folder)
try:
    downloaded = [int(x.split("_")[1].split(".")[0]) for x in downloaded]
    last_year = max(downloaded) if downloaded else (2020 - 51)
except:
    last_year = 2020 - 51

c = cdsapi.Client()
for year in tqdm(range(last_year + 1, 2021)):

    dataset = "derived-era5-single-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        # "format": "netcdf",
        "variable": ["2m_temperature"],
        "year": f"{year}",
        "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ],
        "day": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12",
            "13", "14", "15",
            "16", "17", "18",
            "19", "20", "21",
            "22", "23", "24",
            "25", "26", "27",
            "28", "29", "30",
            "31"
        ],
        "daily_statistic": "daily_mean",
        "time_zone": "utc+00:00",
        "frequency": "1_hourly"
    }

    client = cdsapi.Client()
    client.retrieve(dataset, request, f"{folder}/data_{year}.zip")
# "/mnt/d/World Bank/Paper - Child Mortality and Climate Shocks/00b_query_ERA5_dialy.py"