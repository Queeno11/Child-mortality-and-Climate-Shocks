import os
import argparse
import plot_tools
import numpy as np
import pandas as pd
import geopandas as gpd

# --- 1. Argument Parsing and Configuration Selection ---
parser = argparse.ArgumentParser(description="Plot regression results from LaTeX output.")
parser.add_argument("--spi", type=str, default="spi1", help="SPI variable name (default: spi1)")
parser.add_argument("--temp", type=str, default="stdm_t", help="Temperature variable name (default: stdm_t)")
parser.add_argument("--stat", type=str, default="avg", help="Statistic to use (e.g., avg, m_w0, q_w0)")
parser.add_argument("--timeframe", type=str, default="quarterly", choices=['quarterly', 'monthly', 'semester'], help="Timeframe configuration (default: quarterly)")
args = parser.parse_args()

spi = args.spi
temp = args.temp
stat = args.stat
timeframe_name = args.timeframe

# Select the appropriate configuration dictionary from plot_tools
if timeframe_name == "quarterly":
    main_config = plot_tools.QUARTERLY_CONFIG
elif timeframe_name == "monthly":
    main_config = plot_tools.MONTHLY_CONFIG
elif timeframe_name == "semester":
    main_config = plot_tools.SEMESTER_CONFIG
else:
    raise ValueError(f"Unknown timeframe: {timeframe_name}")

print(f"--- Running plots with '{timeframe_name}' configuration ---")

DATA_OUT = r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Data\Data_out"
OUTPUTS = r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Outputs"
OUT_FIGS = rf"{OUTPUTS}\Figures\{spi} {temp} {stat}"
os.makedirs(rf"{OUT_FIGS}", exist_ok=True)

# ###### Figure 1: Histograms
# # cols = [
# #     "stdm_t_inutero_avg",
# #     "spi1_inutero_avg",
# #     "stdm_t_30d_avg",
# #     "spi1_30d_avg",
# #     "stdm_t_2m12m_avg",
# #     "spi1_2m12m_avg",
# # ]
# # print("Loading DHS-Climate data...")
# # df = pd.read_csv(rf"{DATA_OUT}\DHSBirthsGlobal&ClimateShocks_v9.csv", usecols=cols)
# # print("Data loaded!")
# # outpath = rf"{OUT_FIGS}\histograms.png"
# # plot_tools.plot_shocks_histogram(df, cols, outpath=outpath)

###### Figure 2: Main coefficients dummies true
file_path = rf"{OUTPUTS}\linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name} standard_fe standard_sym.tex"  # Replace with the actual path to your LaTeX file.
outdata = plot_tools.extract_coefficients_and_CI_latex(file_path)
plot_tools.plot_regression_coefficients(
    data=outdata, shock="temp", spi=spi, temp=temp, stat=stat,
    margin=0.25, colors=["#3e9fe1", "#ff5100"], labels=["Low temperature shocks", "High temperature shocks"],
    outpath=OUT_FIGS, add_line=True, start="main - ", ylim=(-0.6, 2.7), **main_config)
plot_tools.plot_regression_coefficients(
    data=outdata, shock="spi", spi=spi, temp=temp, stat=stat,
    margin=0.25, colors=["#ff5100", "#3e9fe1"], labels=["Low precipitation shocks", "High precipitation shocks"],
    outpath=OUT_FIGS, add_line=True, start="main - ", ylim=(-0.6, 2.7), **main_config)

# ### Figure 2b: Extreme temperatures
# labels = {
#     "fd": "# of days Tmin < 0°C",
#     "id": "# of days Tmax < 0°C",
#     35: "# of days Tmax ≥ 35°C",
#     40: "# of days Tmax ≥ 40°C"
# }
# for hot in [35, 40]:
#     for cold in ["fd", "id"]:
#         file_path = rf"{OUTPUTS}\linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name} standard_fe hd{hot}{cold}_sym.tex"  # Replace with the actual path to your LaTeX file.
#         outdata = plot_tools.extract_coefficients_and_CI_latex(file_path)
        
#         plot_tools.plot_regression_coefficients(
#             data=outdata, 
#             shock="temp",
#             spi=spi,
#             temp=temp,
#             stat=stat,
#             margin=0.25,
#             colors=["#3e9fe1", "#ff5100"], 
#             labels=[labels[cold], labels[hot]], 
#             outpath=rf"{OUT_FIGS}",
#             add_line=True,
#             start="extremes - ",
#             extra=f" - hd{hot}{cold}",
#             ylim=(-0.6, 2.7), 
#             **main_config
#         )
#         plot_tools.plot_regression_coefficients(
#             data=outdata, 
#             shock="spi",
#             spi=spi,
#             temp=temp,
#             stat=stat,
#             margin=0.25,
#             colors=["#ff5100", "#3e9fe1"], 
#             labels=["Low precipitation shocks", "High precipitation shocks"], 
#             outpath=rf"{OUT_FIGS}",
#             add_line=True,
#             start="extremes - ",
#             extra=f" - hd{hot}{cold}",
#             ylim=(-0.6, 2.7), 
#             **main_config
#         )

# ### Figure 2c: Horserace
# for hot in [35, 40]:
#     for cold in ["fd", "id"]:
#         if cold=="fd":
#             coldstat = "TMin"
#         else: # cold=="id"
#             coldstat = "TMax" 
#         file_path = rf"{OUTPUTS}\linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name} standard_fe horserace_hd{hot}{cold}_sym.tex"  # Replace with the actual path to your LaTeX file.
#         outdata = plot_tools.extract_coefficients_and_CI_latex_horserace(file_path)
        
#         plot_tools.plot_horserace_temp(
#             data=outdata, 
#             spi=spi,
#             temp=temp,
#             stat=stat,
#             colors=["#ff5100",  "#fdbb84","#3e9fe1",  "#87ceeb",],
#             labels=["High Temprature Anomalies", f"N° of Days with TMax>{hot}°", "Low Temprature Anomalies", f"N° of Days with {coldstat}<0°"],
#             outpath=rf"{OUT_FIGS}",
#             extra=f" - hd{hot}{cold}{temp}",
#             ylim=(-0.6, 2.7), 
#             **main_config
#         )
#         plot_tools.plot_regression_coefficients(
#             data=outdata["standard"],  # Or extreme, they are the same!
#             shock="spi",
#             spi=spi,
#             temp=temp,
#             stat=stat,
#             margin=0.25,
#             colors=["#ff5100", "#3e9fe1"], 
#             labels=["Low precipitation shocks", "High precipitation shocks"], 
#             outpath=rf"{OUT_FIGS}",
#             add_line=True,
#             start="horserace - ",
#             extra=f" - hd{hot}{cold}{temp}_spi",
#             ylim=(-0.6, 2.7), 
#             **main_config
#         )

# # Horserace by climate band
# for hot in [35, 40]:
#     for cold in ["fd", "id"]:
#         if cold=="fd":
#             coldstat = "TMin"
#         else: # cold=="id"
#             coldstat = "TMax" 

#         for band in ["Arid","Temperate", "Tropical"]:
#             file_path = rf"{OUTPUTS}\heterogeneity\climate_band_1\linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name} - {band} standard_fe horserace_hd{hot}{cold}_sym.tex"  # Replace with the actual path to your LaTeX file.
#             outdata = plot_tools.extract_coefficients_and_CI_latex_horserace(file_path)
            
#             plot_tools.plot_horserace_temp(
#                 data=outdata, 
#                 spi=spi,
#                 temp=temp,
#                 stat=stat,
#                 colors=["#ff5100",  "#fdbb84","#3e9fe1",  "#87ceeb",],
#                 labels=["High Temprature Anomalies", f"N° of Days with TMax>{hot}°", "Low Temprature Anomalies", f"N° of Days with {coldstat}<0°"],
#                 outpath=rf"{OUT_FIGS}",
#                 extra=f" - {band} hd{hot}{cold}{temp}",
#                 ylim=(-0.6, 2.7), 
#                 **main_config,
#             )
#             plot_tools.plot_regression_coefficients(
#                 data=outdata["standard"],  # Or extreme, they are the same!
#                 shock="spi",
#                 spi=spi,
#                 temp=temp,
#                 stat=stat,
#                 margin=0.25,
#                 colors=["#ff5100", "#3e9fe1"], 
#                 labels=["Low precipitation shocks", "High precipitation shocks"], 
#                 outpath=rf"{OUT_FIGS}",
#                 add_line=True,
#                 start="horserace - ",
#                 extra=f" - {band} hd{hot}{cold}{temp}_spi",
#                 ylim=(-0.6, 2.7), 
#                 **main_config,
#             )


### WINDOWS 1m figures:

# FIXME: this function is an actual disaster, but it works...
fd = plot_tools.plot_windows(                
    spi=spi,
    temp=temp,
    stat="b_w", 
    labels=["1m", "2m", "3m", "4m", "5m", "6m", "7m", "8m", "9m"],
    outpath=rf"{OUT_FIGS}",    time_frames=["born_1m"],
    title_labels= {'born_1m': 'First month of birth'},
    x_tick_labels=["1m", "2m", "3m", "4m", "5m", "6m", "7m", "8m", "9m"],
    xlim= (-0.5, 0.5), ylim=(-1, 2.1),
    legend_pos= {
        'loc': 'lower center',
        'bbox_to_anchor': (0.5, -0.1),
        'ncol': 2,
    }
)

# ### Figure 3: Main coefficients Spline
# file_path = rf"{OUTPUTS}\spline_dummies_false_{spi}_{stat}_{temp}  - spthreshold1 standard_fe standard_sym.tex"  # Replace with the actual path to your LaTeX file.
# outdata = plot_tools.extract_coefficients_and_CI_latex(file_path)

# plot_tools.plot_spline_coefficients(
#     data=outdata, 
#     shock="spi",
#     spi=spi,
#     temp=temp,
#     stat=stat,
#     margin=0.15,
#     colors = [
#         "#ff5100",  # Very high temperature
#         "#ff9a40",  # High temperature
#         "#76b7e5",  # Low temperature
#         "#3e9fe1",   # Very low temperature
#     ],
#     labels=[
#         "Very high precipitation shocks", 
#         "High precipitation shocks",
#         "Low precipitation shocks", 
#         "Very low precipitation shocks",
#     ],
#     outpath=rf"{OUT_FIGS}"
# )

# plot_tools.plot_spline_coefficients(
#     data=outdata, 
#     shock="temp",
#     spi=spi,
#     temp=temp,
#     stat=stat,
#     margin=0.15,
#     colors = [
#         "#3e9fe1",   # Very low temperature
#         "#76b7e5",  # Low temperature
#         "#ff9a40",  # High temperature
#         "#ff5100",  # Very high temperature
#     ],
#     labels=[
#         "Very low temperature shocks",
#         "Low temperature shocks", 
#         "High temperature shocks",
#         "Very high temperature shocks", 
#     ],
#     outpath=rf"{OUT_FIGS}"
# )


### Figure 4a: RWI heterogeneity
colors=["#fe3500", "#79c78d"] 
labels=["Affected by extreme temperatures in-utero", "Not affected by extreme temperatures in-utero"]

plot_tools.plot_heterogeneity(
    "shock_analysis_p25p0_p75p0",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    ylim=(-0.6, 2.7), 
    **main_config,
)    

### Figure 4a: RWI heterogeneity
colors=["#fe3500", "#ffd220", "#79c78d"] 
labels=["Low Income","Middle Income","High Income"]
        
plot_tools.plot_heterogeneity(
    "rwi_tertiles",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    ylim=(-0.6, 2.7), 
    **main_config,
)    

### Figure 4b: DHS poor indicator
colors=["#fe3500", "#79c78d"] 
labels=["Poor Household","Non Poor Household"]
        
plot_tools.plot_heterogeneity(
    "poor",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    ylim=(-0.6, 2.7), 
    **main_config,
)    

### Figure 4c: DHS Quintiles
colors=["#fe3500", "#fea500", "#ffd220", "#9ad42d", "#09cf6c"] 
labels=["1","2","3","4","5"]
        
plot_tools.plot_heterogeneity(
    "weatlh_ind",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)    

### Figure 4: Climate bands 1 heterogeneity
colors=["#fe3500", "#ffd220", "#79c78d", ] 
labels=[ "Tropical", "Arid","Temperate",]
        
plot_tools.plot_heterogeneity(
    "climate_band_1",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    ylim=(-1, 3.5), 
    **main_config,
)    

# ### FIgure 4.5: Climate bands 2 heterogeneity
# colors = [
#     "#EDC9AF",  # Arid desert (Desert Sand)
#     "#C2B280",  # Semi-Arid steppe (Light Khaki)
#     "#87CEFA",  # Temperate (Dry summer) (Light Sky Blue)
#     "#4682B4",  # Temperate (Dry winter) (Steel Blue)
#     "#9ACD32",  # Temperate (No dry season) (YellowGreen)
#     "#32CD32",  # Tropical (Monsoon) (Lime Green)
#     "#006400",  # Tropical (Rainforest) (Dark Green)
#     "#DAA520"   # Tropical Savanna (Goldenrod)
# ]
# labels = [
#     "Arid desert",
#     "Semi-Arid steppe",
#     "Temp. Dry summer",
#     "Temp. Dry winter",
#     "Temp. No dry season",
#     "Trop. Monsoon",
#     "Trop. Rainforest",
#     "Trop. Savanna"
# ]
        
# plot_tools.plot_heterogeneity(
#     "climate_band_2",
#     spi=spi,
#     temp=temp,
#     stat=stat,
#     colors=colors, 
#     labels=labels,
#     outpath=OUT_FIGS, 
#     **main_config,
# )        
    
### Figure 5: Income groups heterogeneity

f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}"
colors=["#fe3500", "#ffd220", "#79c78d"]
labels=["Low income","Lower middle income","Upper middle income"]
plot_tools.plot_heterogeneity(
    "wbincomegroup",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)    

# ### Figure 6: Northern/southern heterogeneity
# f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}
# colors = [
#     "#1f77b4",  # Northern Hemisphere (Bold Blue)
#     "#ff7f0e",   # Southern Hemisphere (Vivid Orange)
# ]
# labels=["Northern Hemisphere","Southern Hemisphere"]
# plot_tools.plot_heterogeneity(
#     "southern",
#     spi=spi,
#     temp=temp,
#     stat=stat,
#     colors=colors, 
#     labels=labels,
#     outpath=OUT_FIGS, 
#     **main_config,
# )    

# ### Figure 7: Rural/Urban heterogeneity
# f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}
# colors = [
#     "#808080",  # Urban (Gray)
#     "#228B22"   # Rural (Forest Green)
# ]
# labels=["Urban","Rural"]
# plot_tools.plot_heterogeneity(
#     "rural",
#     spi=spi,
#     temp=temp,
#     stat=stat,
#     colors=colors, 
#     labels=labels,
#     outpath=OUT_FIGS, 
#     **main_config,
# )    

### Figure 8: Mother education
f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}"
colors=["#fe3500", "#ffd220", "#79c78d"]
labels = ['6 years or less', '6-12 years', 'more than 12 years', 'No data']
plot_tools.plot_heterogeneity(
    "mother_educ",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)    

### Figure 8: Piped water heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No piped water acces", "Piped water access",]
plot_tools.plot_heterogeneity(
    "pipedw",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)    

### Figure 8: Refrigerator heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No refrigerator acces", "Refrigerator access",]
plot_tools.plot_heterogeneity(
    "refrigerator",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)    

### Figure 9: House Indexes
f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["Index below median", "Index above median",]
for index in ["high_quality_housing", "high_heat_protection", "high_cold_protection"]:
    plot_tools.plot_heterogeneity(
        index,
        spi=spi,
        temp=temp,
        stat=stat,
        timeframe=timeframe_name,
        colors=colors, 
        labels=labels,
        outpath=OUT_FIGS, 
        **main_config,
    )    

### Figure 10: Air conditioning heterogeneity
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No air conditioning", "Has air conditioning",]
plot_tools.plot_heterogeneity(
    "hhaircon",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)

### Figure 11: Fan heterogeneity
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No fan", "Has fan",]
plot_tools.plot_heterogeneity(
    "hhfan",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)

### Figure 12: Electricity heterogeneity
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No electricity", "Has electricity",]
plot_tools.plot_heterogeneity(
    "electricity",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    ylim=(-1.5, 3.5), 
    **main_config,
)


### Figure 9: Gender heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe_name}"
colors = [
    "#b25dfe",  
    "#fdb714",  
]
labels=["Male", "Female",]
plot_tools.plot_heterogeneity(
    "child_fem",
    spi=spi,
    temp=temp,
    stat=stat,
    timeframe=timeframe_name,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
    **main_config,
)    

################## Descriptive statistcs
####### Plot DHS sample:

# df = pd.read_stata(r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Data\Data_in\DHS\DHSBirthsGlobalAnalysis_05142024.dta")

# df = df.dropna(subset=["v008", "chb_year", "chb_month"], how="any")

# # Create datetime object from year and month
# df["day"] = 1
# df["month"] = df["chb_month"].astype(int)
# df["year"] = df["chb_year"].astype(int)
# df["birth_date"] = pd.to_datetime(df[["year", "month", "day"]]).to_numpy()
# df = df.drop(columns=["day", "month", "year"])

# # Maximum range of dates
# df["from_date"] = df["birth_date"] + pd.DateOffset(
#     months=-9
# )  # From in utero (9 months before birth)
# df["to_date"] = df["birth_date"] + pd.DateOffset(
#     months=12
# )  # To the first year of life

# # Filter children from_date greater than 1991 (we only have climate data from 1990)
# df = df[df["from_date"] > "1991-01-01"]

# # Filter children to_date smalle than 2021 (we only have climate data to 2020)
# df = df[df["to_date"] < "2021-01-01"]


# # Date of interview
# df["year"] = 1900 + (df["v008"] - 1) // 12
# df["month"] = df["v008"] - 12 * (df["year"] - 1900)
# df["day"] = 1
# df["interview_date"] = pd.to_datetime(df[["year", "month", "day"]], dayfirst=False)
# df["interview_year"] = df["year"]
# df["interview_month"] = df["month"]
# df = df.drop(columns=["year", "month", "day"])

# # Number of days from interview
# df["days_from_interview"] = df["interview_date"] - df["birth_date"]

# # excluir del análisis a aquellos niños que nacieron 12 meses alrededor de la fecha de la encuesta y no más allá de 10 y 15 años del momento de la encuesta.
# # PREGUNTA PARA PAULA: ¿ella ya hizo el filtro de 15 años y 30 dias?
# df["last_15_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
#     df["days_from_interview"] < np.timedelta64(15 * 365, "D")
# )
# df["last_10_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
#     df["days_from_interview"] < np.timedelta64(10 * 365, "D")
# )
# df["since_2003"] = df["interview_year"] >= 2003
# df = df[df["last_15_years"] == True]

# gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LONGNUM, df.LATNUM))

# world_bounds = gpd.read_file(r"C:\Datasets\World Bank Official Boundaries\WB_countries_Admin0_10m\WB_countries_Admin0_10m.shp")

# # plot world without fill and with black borders and thin lines
# ax = world_bounds.simplify(0.1).plot(edgecolor='black', facecolor='none', linewidth=0.4, figsize=(20, 10))

# # Remove axis
# ax.axis('off')
# ax.set_xlim(-180, 180)
# ax.set_ylim(-70, 85)

# gdf.plot(ax=ax, markersize=.05)
stop

### Mapas
import xarray as xr
import matplotlib.pyplot as plt

ds = xr.open_dataset(r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Data\Data_proc\Climate_shocks_v9d.nc")
da = ds.stdm_t
da = da.rolling(dim={"time": 3}, center="left").mean()
landside = da.isel(time=-5).drop("time").notnull()

for t in 1.5, 2.5:
    ndays = (da > t).sum(dim="time") / ((2021-1991)*12)
    ndays = ndays.where(landside, drop=True) # Mask null values
    ndays.plot(figsize=(10, 5), cmap="Spectral_r")
    plt.title(f"Share of months with Monthly Temperature Anomalies >{t} SD (1991-2021)")
    plt.savefig(rf"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Outputs\Figures\stdm_t_{t}.png", bbox_inches="tight", dpi=450)
    
    ndays = (da < -t).sum(dim="time") / ((2021-1991)*12)
    ndays = ndays.where(landside, drop=True) # Mask null values
    ndays.plot(figsize=(10, 5), cmap="Spectral_r")
    plt.title(f"Share of months with Monthly Temperature Anomalies <-{t} SD (1991-2021)")
    plt.savefig(rf"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Outputs\Figures\stdm_t_-{t}.png", bbox_inches="tight", dpi=450)
    
### Distribuciones
# import seaborn as sns
# import matplotlib.pyplot as plt
# import pandas as pd
# labels = {
#     "stdm_t_inutero_1m3m_avg": "In-utero - 1st Quarter",
#     "stdm_t_inutero_4m6m_avg": "In-utero - 2nd Quarter",
#     "stdm_t_inutero_6m9m_avg": "In-utero - 3rd Quarter",
#     "stdm_t_born_1m3m_avg": "Born - 1st Quarter",
#     "stdm_t_born_3m6m_avg": "Born - 2nd Quarter",
#     "stdm_t_born_6m9m_avg": "Born - 3rd Quarter",
#     "stdm_t_born_9m12m_avg": "Born - 4th Quarter",
# }

# fig, axs = plt.subplots(2, 4, figsize=(20, 8))

# for i, ax in enumerate(axs.flatten()):
#     if i==0:
#         continue
#     var = list(labels.keys())[i-1]
#     data = pd.read_feather(
#         r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Data\Data_out\DHSBirthsGlobal&ClimateShocks_v10b.feather", columns=[var]
#     )[var]
#     ax = data.plot(kind="kde", ax=ax, color="black")
#     ax.set_title(labels[var])
#     ax.set_xlim(-3,3)
#     sns.despine()

# axs[0][0].spines['top'].set_visible(False)
# axs[0][0].spines['bottom'].set_visible(False)
# axs[0][0].spines['right'].set_visible(False)
# axs[0][0].spines['left'].set_visible(False)
# axs[0][0].set_xticks([])
# axs[0][0].set_yticks([])

# plt.savefig(r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Outputs\Figures\shock_distribution_in_DHS.png", dpi=600)