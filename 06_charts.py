import os
import argparse
import plot_tools
import numpy as np
import pandas as pd
import geopandas as gpd

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Plot regression results from LaTeX output.")
parser.add_argument("--spi", type=str, default="spi1", help="SPI variable name (default: spi1)")
parser.add_argument("--temp", type=str, default="stdm_t", help="Temperature variable name (default: stdm_t)")
parser.add_argument("--stat", type=str, default="avg", help="Statistic to use (default: avg)")
args = parser.parse_args()

# Assign variables from command-line arguments
spi  = args.spi
temp = args.temp
stat = args.stat

DATA_OUT = r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_out"
OUTPUTS = r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Outputs"
OUT_FIGS = rf"{OUTPUTS}\Figures\{spi} {temp} {stat}"
os.makedirs(rf"{OUT_FIGS}", exist_ok=True)

###### Figure 1: Histograms
# cols = [
#     "stdm_t_inutero_avg",
#     "spi1_inutero_avg",
#     "stdm_t_30d_avg",
#     "spi1_30d_avg",
#     "stdm_t_2m12m_avg",
#     "spi1_2m12m_avg",
# ]
# print("Loading DHS-Climate data...")
# df = pd.read_csv(rf"{DATA_OUT}\DHSBirthsGlobal&ClimateShocks_v9.csv", usecols=cols)
# print("Data loaded!")
# outpath = rf"{OUT_FIGS}\histograms.png"
# plot_tools.plot_shocks_histogram(df, cols, outpath=outpath)

###### Figure 2: Main coefficients dummies true
file_path = rf"{OUTPUTS}\linear_dummies_true_{spi}_{stat}_{temp}  standard_fe standard_sym.tex"  # Replace with the actual path to your LaTeX file.
outdata = plot_tools.extract_coefficients_and_CI_latex(file_path)

plot_tools.plot_regression_coefficients(
    data=outdata, 
    shock="temp",
    spi=spi,
    temp=temp,
    stat=stat,
    margin=0.25,
    colors=["#3e9fe1", "#ff5100"], 
    labels=["Low temperature shocks", "High temperature shocks"],  
    outpath=rf"{OUT_FIGS}",
    add_line=True,
)

plot_tools.plot_regression_coefficients(
    data=outdata, 
    shock="spi",
    spi=spi,
    temp=temp,
    stat=stat,
    margin=0.25,
    colors=["#ff5100", "#3e9fe1"], 
    labels=["Low precipitation shocks", "High precipitation shocks"], 
    outpath=rf"{OUT_FIGS}",
    add_line=True,
)

### Figure 3: Main coefficients Spline
file_path = rf"{OUTPUTS}\spline_dummies_false_{spi}_{stat}_{temp}  - spthreshold1 standard_fe standard_sym.tex"  # Replace with the actual path to your LaTeX file.
outdata = plot_tools.extract_coefficients_and_CI_latex(file_path)

plot_tools.plot_spline_coefficients(
    data=outdata, 
    shock="spi",
    spi=spi,
    temp=temp,
    stat=stat,
    margin=0.15,
    colors = [
        "#ff5100",  # Very high temperature
        "#ff9a40",  # High temperature
        "#76b7e5",  # Low temperature
        "#3e9fe1",   # Very low temperature
    ],
    labels=[
        "Very high precipitation shocks", 
        "High precipitation shocks",
        "Low precipitation shocks", 
        "Very low precipitation shocks",
    ],
    outpath=rf"{OUT_FIGS}"
)

plot_tools.plot_spline_coefficients(
    data=outdata, 
    shock="temp",
    spi=spi,
    temp=temp,
    stat=stat,
    margin=0.15,
    colors = [
        "#3e9fe1",   # Very low temperature
        "#76b7e5",  # Low temperature
        "#ff9a40",  # High temperature
        "#ff5100",  # Very high temperature
    ],
    labels=[
        "Very low temperature shocks",
        "Low temperature shocks", 
        "High temperature shocks",
        "Very high temperature shocks", 
    ],
    outpath=rf"{OUT_FIGS}"
)
    
    
### Figure 4: Climate bands 1 heterogeneity
colors=["#ffd220", "#79c78d", "#fe3500"] 
labels=["Arid","Temperate", "Tropical"]
        
plot_tools.plot_heterogeneity(
    "climate_band_1",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)    

### FIgure 4.5: Climate bands 2 heterogeneity
colors = [
    "#EDC9AF",  # Arid desert (Desert Sand)
    "#C2B280",  # Semi-Arid steppe (Light Khaki)
    "#87CEFA",  # Temperate (Dry summer) (Light Sky Blue)
    "#4682B4",  # Temperate (Dry winter) (Steel Blue)
    "#9ACD32",  # Temperate (No dry season) (YellowGreen)
    "#32CD32",  # Tropical (Monsoon) (Lime Green)
    "#006400",  # Tropical (Rainforest) (Dark Green)
    "#DAA520"   # Tropical Savanna (Goldenrod)
]
labels = [
    "Arid desert",
    "Semi-Arid steppe",
    "Temp. Dry summer",
    "Temp. Dry winter",
    "Temp. No dry season",
    "Trop. Monsoon",
    "Trop. Rainforest",
    "Trop. Savanna"
]
        
plot_tools.plot_heterogeneity(
    "climate_band_2",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)        
    
### Figure 5: Income groups heterogeneity

f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors=["#fe3500", "#ffd220", "#79c78d"]
labels=["Low income","Lower middle income","Upper middle income"]
plot_tools.plot_heterogeneity(
    "wbincomegroup",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)    

### Figure 6: Northern/southern heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#1f77b4",  # Northern Hemisphere (Bold Blue)
    "#ff7f0e",   # Southern Hemisphere (Vivid Orange)
]
labels=["Northern Hemisphere","Southern Hemisphere"]
plot_tools.plot_heterogeneity(
    "southern",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)    

### Figure 7: Rural/Urban heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#808080",  # Urban (Gray)
    "#228B22"   # Rural (Forest Green)
]
labels=["Urban","Rural"]
plot_tools.plot_heterogeneity(
    "rural",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)    

### Figure 8: Piped water heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
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
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)    

### Figure 8: Refrigerator heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No refrigerator acces", "Refrigerator access",]
plot_tools.plot_heterogeneity(
    "href",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)    

### Figure 9: Electrical temperature regulator heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No temperature regulator", "Has temperature regulator",]
plot_tools.plot_heterogeneity(
    "hhelectemp",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
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
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
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
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)

### Figure 12: Electricity heterogeneity
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No electricity", "Has electricity",]
plot_tools.plot_heterogeneity(
    "helec",
    spi=spi,
    temp=temp,
    stat=stat,
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)


### Figure 9: Gender heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
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
    colors=colors, 
    labels=labels,
    outpath=OUT_FIGS, 
)    

################## Descriptive statistcs
####### Plot DHS sample:

df = pd.read_stata(r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Data\Data_in\DHS\DHSBirthsGlobalAnalysis_05142024.dta")

df = df.dropna(subset=["v008", "chb_year", "chb_month"], how="any")

# Create datetime object from year and month
df["day"] = 1
df["month"] = df["chb_month"].astype(int)
df["year"] = df["chb_year"].astype(int)
df["birth_date"] = pd.to_datetime(df[["year", "month", "day"]]).to_numpy()
df = df.drop(columns=["day", "month", "year"])

# Maximum range of dates
df["from_date"] = df["birth_date"] + pd.DateOffset(
    months=-9
)  # From in utero (9 months before birth)
df["to_date"] = df["birth_date"] + pd.DateOffset(
    months=12
)  # To the first year of life

# Filter children from_date greater than 1991 (we only have climate data from 1990)
df = df[df["from_date"] > "1991-01-01"]

# Filter children to_date smalle than 2021 (we only have climate data to 2020)
df = df[df["to_date"] < "2021-01-01"]


# Date of interview
df["year"] = 1900 + (df["v008"] - 1) // 12
df["month"] = df["v008"] - 12 * (df["year"] - 1900)
df["day"] = 1
df["interview_date"] = pd.to_datetime(df[["year", "month", "day"]], dayfirst=False)
df["interview_year"] = df["year"]
df["interview_month"] = df["month"]
df = df.drop(columns=["year", "month", "day"])

# Number of days from interview
df["days_from_interview"] = df["interview_date"] - df["birth_date"]

# excluir del análisis a aquellos niños que nacieron 12 meses alrededor de la fecha de la encuesta y no más allá de 10 y 15 años del momento de la encuesta.
# PREGUNTA PARA PAULA: ¿ella ya hizo el filtro de 15 años y 30 dias?
df["last_15_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
    df["days_from_interview"] < np.timedelta64(15 * 365, "D")
)
df["last_10_years"] = (df["days_from_interview"] > np.timedelta64(30, "D")) & (
    df["days_from_interview"] < np.timedelta64(10 * 365, "D")
)
df["since_2003"] = df["interview_year"] >= 2003
df = df[df["last_15_years"] == True]

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LONGNUM, df.LATNUM))

world_bounds = gpd.read_file(r"D:\Datasets\World Bank Official Boundaries\WB_countries_Admin0_10m\WB_countries_Admin0_10m.shp")

# plot world without fill and with black borders and thin lines
ax = world_bounds.simplify(0.1).plot(edgecolor='black', facecolor='none', linewidth=0.4, figsize=(20, 10))

# Remove axis
ax.axis('off')
ax.set_xlim(-180, 180)
ax.set_ylim(-70, 85)

gdf.plot(ax=ax, markersize=.05)


