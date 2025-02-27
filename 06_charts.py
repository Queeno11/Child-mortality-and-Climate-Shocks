import os
import argparse
import plot_tools
import numpy as np
import pandas as pd

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
cols = [
    "stdm_t_inutero_avg",
    "spi1_inutero_avg",
    "stdm_t_30d_avg",
    "spi1_30d_avg",
    "stdm_t_2m12m_avg",
    "spi1_2m12m_avg",
]
print("Loading DHS-Climate data...")
df = pd.read_csv(rf"{DATA_OUT}\DHSBirthsGlobal&ClimateShocks_v9.csv", usecols=cols)
print("Data loaded!")
outpath = rf"{OUT_FIGS}\histograms.png"
plot_tools.plot_shocks_histogram(df, cols, outpath=outpath)
exit()

###### Figure 2: Main coefficients dummies true
file_path = rf"{OUTPUTS}\linear_dummies_true_{spi}_{stat}_{temp}  standard_fe.tex"  # Replace with the actual path to your LaTeX file.
outdata = plot_tools.extract_coefficients_and_CI_latex(file_path)

values_sfe = (outdata["temp_pos"]["coefs"] + outdata["spi_pos"]["coefs"], outdata["temp_neg"]["coefs"] + outdata["spi_neg"]["coefs"])
lower_sfe  = (outdata["temp_pos"]["lower"] + outdata["spi_pos"]["lower"], outdata["temp_neg"]["lower"] + outdata["spi_neg"]["lower"])
upper_sfe  = (outdata["temp_pos"]["upper"] + outdata["spi_pos"]["upper"], outdata["temp_neg"]["upper"] + outdata["spi_neg"]["upper"])

plot_tools.plot_regression_coefficients(
    values_sfe, 
    upper_sfe,
    lower_sfe,
    margin=0.14,
    colors=["#ff5100", "#3e9fe1"], 
    labels=["High temperature shocks","Low temperature shocks"], 
    plot="only_temp", 
    outpath=rf"{OUT_FIGS}\coefplot_temp.png"
)

plot_tools.plot_regression_coefficients(
    values_sfe, 
    upper_sfe,
    lower_sfe,
    margin=0.14,
    colors=["#3e9fe1", "#ff5100"], 
    labels=["High precipitation shocks","Low precipitation shocks"], 
    plot="only_spi", 
    outpath=rf"{OUT_FIGS}\coefplot_spi.png"
)

###### Figure 2.5: Fixed effects comparison
file_path = rf"{OUTPUTS}\linear_dummies_true_{spi}_{stat}_{temp}  quadratic_time_fe.tex"  # Replace with the actual path to your LaTeX file.
outdata = plot_tools.extract_coefficients_and_CI_latex(file_path)

values_tfe = (outdata["temp_pos"]["coefs"] + outdata["spi_pos"]["coefs"], outdata["temp_neg"]["coefs"] + outdata["spi_neg"]["coefs"])
lower_tfe  = (outdata["temp_pos"]["lower"] + outdata["spi_pos"]["lower"], outdata["temp_neg"]["lower"] + outdata["spi_neg"]["lower"])
upper_tfe  = (outdata["temp_pos"]["upper"] + outdata["spi_pos"]["upper"], outdata["temp_neg"]["upper"] + outdata["spi_neg"]["upper"])

values = [values_sfe[0], values_tfe[0], values_sfe[1], values_tfe[1]]
lower  = [lower_sfe[0], lower_tfe[0], lower_sfe[1], lower_tfe[1]]
upper =  [upper_sfe[0], upper_tfe[0], upper_sfe[1], upper_tfe[1]]

plot_tools.plot_regression_coefficients(
    values, 
    upper, 
    lower, 
    margin=0.1, 
    colors=["#ff5100", "#7c0f06", "#3e9fe1", "#1a4461", ], 
    labels=["Standard FE (+)","Quadratic Time FE (+)", "Standard FE (-)","Quadratic Time FE (-)"], 
    plot="only_temp", 
    outpath=rf"{OUT_FIGS}\coefplot_fe_temp.png"
)
plot_tools.plot_regression_coefficients(
    values, 
    upper, 
    lower, 
    margin=0.1, 
    colors=["#3e9fe1", "#1a4461", "#ff5100", "#7c0f06"], 
    labels=["Standard FE (+)","Quadratic Time FE (+)", "Standard FE (-)","Quadratic Time FE (-)"], 
    plot="only_spi", 
    outpath=rf"{OUT_FIGS}\coefplot_fe_spi.png"
)


### Figure 3: Main coefficients Spline
std = 1
file_path = rf"{OUTPUTS}\spline_dummies_false_{spi}_{stat}_{temp}  - spthreshold{std} standard_fe.tex"  # Replace with the actual path to your LaTeX file.
outdata = plot_tools.extract_coefficients_and_CI_latex(file_path, file_type="spline")

# Shock labels
names = {
    0: {"shock": "inutero", "death": "30d"},
    1: {"shock": "inutero", "death": "1m-12m"},
    2: {"shock": "30d", "death": "30d"},
    3: {"shock": "30d", "death": "1m-12m"},
    4: {"shock": "1m-12m", "death": "1m-12m"},
}

for var in ["temp", "spi"]:
    # Extract coefficients and ci
    data = {"coefs": [], "upper": [], "lower": []}
    for i in range(5):
        shock = names[i]["shock"]
        for j in ["coefs", "upper", "lower"]:
            ltm1 = outdata[var]["ltm1"][j][i]
            bt0m1 = outdata[var]["bt0m1"][j][i]
            bt01 = outdata[var]["bt01"][j][i]
            gt1 = outdata[var]["gt1"][j][i]
            data[j] += [[ltm1, bt0m1, bt01, gt1]]
            
        if names[i]["death"] == "1m-12m":
            if i!=5:
                labels = [names[i-1]["death"], names[i]["death"]]
            else:
                labels = [names[i-1]["death"]]
            outname = rf"{OUT_FIGS}\coefplot_spline_{var}_{shock}_{std}.png"
            plot_tools.plot_spline_coefficients(
                data["coefs"], 
                data["upper"], 
                data["lower"], 
                mfcs=["black", "white"], 
                labels=labels,
                margin=0.1, 
                outpath=outname
            )
            data = {"coefs": [], "upper": [], "lower": []}
    
    
### Figure 4: Climate bands 1 heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors=["#ffd220", "#79c78d", "#fe3500"] 
labels=["Arid","Temperate", "Tropical"]
plot_tools.plot_heterogeneity(f_name, "climate_band_1", folder=OUT_FIGS, colors=colors, labels=labels)    

### FIgure 4.5: Climate bands 2 heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"

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
plot_tools.plot_heterogeneity(f_name, "climate_band_2", folder=OUT_FIGS, colors=colors, labels=labels)    
    
### Figure 5: Income groups heterogeneity

f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors=["#fe3500", "#ffd220", "#79c78d"]
labels=["Low income","Lower middle income","Upper middle income"]
plot_tools.plot_heterogeneity(f_name, "wbincomegroup", folder=OUT_FIGS, colors=colors, labels=labels)    


### Figure 6: Northern/southern heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#1f77b4",  # Northern Hemisphere (Bold Blue)
    "#ff7f0e",   # Southern Hemisphere (Vivid Orange)
]
labels=["Northern Hemisphere","Southern Hemisphere"]
plot_tools.plot_heterogeneity(f_name, "southern", folder=OUT_FIGS, colors=colors, labels=labels)    

### Figure 7: Rural/Urban heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#808080",  # Urban (Gray)
    "#228B22"   # Rural (Forest Green)
]
labels=["Urban","Rural"]
plot_tools.plot_heterogeneity(f_name, "rural", folder=OUT_FIGS, colors=colors, labels=labels)    

### Figure 8: Piped water heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No piped water acces", "Piped water access",]
plot_tools.plot_heterogeneity(f_name, "pipedw", folder=OUT_FIGS, colors=colors, labels=labels)    

### Figure 8: Refrigerator heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No refrigerator acces", "Refrigerator access",]
plot_tools.plot_heterogeneity(f_name, "href", folder=OUT_FIGS, colors=colors, labels=labels)    

### Figure 9: Electrical temperature regulator heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#d62728",   # No Piped Water Access (Red)
    "#1f77b4",  # Piped Water Access (Blue)
]
labels=["No temperature regulator", "Has temperature regulator",]
plot_tools.plot_heterogeneity(f_name, "hhelectemp", folder=OUT_FIGS, colors=colors, labels=labels)    


### Figure 9: Gender heterogeneity
f_name = f"linear_dummies_true_{spi}_{stat}_{temp}"
colors = [
    "#b25dfe",  
    "#fdb714",  
]
labels=["Male", "Female",]
plot_tools.plot_heterogeneity(f_name, "child_fem", folder=OUT_FIGS, colors=colors, labels=labels)    

