import re
import os
import numpy as np
import matplotlib.pyplot as plt

OUTPUTS = r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Outputs"


def remove_words_from_string(long_string, words):
    for word in words:
        long_string = long_string.replace(word, "")
    return long_string.strip()

def contains_any_string(main_string, strings_list):
    return any(sub in main_string for sub in strings_list)

def contained_string(main_string, strings_list):
    contained =  [sub for sub in strings_list if sub in main_string]
    assert len(contained) == 1, f"More than one string from the list is contained in the main string: {main_string} contains {contained}"
    return contained[0]

def to_float(s):
    if len(s)>0:
        return float(s)
    else:
        return np.nan

def compute_ci(coefs, ses):
    lower = []
    upper = []
    for coef, se in zip(coefs, ses):
        if coef is not None and se is not None:
            lower.append(coef - 2.042 * se) # 95% confidence interval with t(30df)
            upper.append(coef + 2.042 * se)
        else:
            lower.append(None)
            upper.append(None)
    return lower, upper

def highlight_significant_points(ax, xvalues, coefs, lower, marker='o', color='red', s=80, **kwargs):
    """
    Highlights points on the axis where the lower bound of the CI is above zero.
    
    Parameters:
      ax : matplotlib.axes.Axes
          The axis on which to plot.
      xvalues : array-like
          The x-axis positions corresponding to the coefficient estimates.
      coefs : array-like
          The coefficient estimates.
      lower : array-like
          The lower bounds of the confidence intervals.
      marker : str, optional
          The marker style for highlighted points (default is 's' for square).
      color : str, optional
          The color for the highlighted markers (default is 'red').
      s : int, optional
          The size of the highlighted markers.
      **kwargs : additional keyword arguments passed to ax.scatter.
    """
    xvalues = np.array(xvalues)
    coefs = np.array(coefs)
    lower = np.array(lower)
    
    # Create a Boolean mask of points where the lower bound is above zero.
    significant = lower > 0
    if np.any(significant):
        ax.scatter(xvalues[significant],
                   coefs[significant],
                   marker=marker,
                   color=color,
                   s=s,
                   edgecolor='k',
                   linewidth=1.5,
                   zorder=3,
                   **kwargs)

def extract_coefficients_and_CI_latex(file_path):
    """
    Extracts coefficients and their 95% CI bounds from a LaTeX table.
    
    For default files (file_type="default"):
      - Expects variable names ending with either "avg_neg" or "avg_pos".
      - Ordering: for rows labeled "inutero" and "30d": use col1 then col4;
        for rows labeled "2m12m": use only col4.
      Returns a dictionary with keys "spi_neg", "spi_pos", "temp_neg", and "temp_pos".
    
    For spline files (file_type="spline"):
      - Expects variable names ending with one of four categories: "ltm1", "bt0m1", "bt01", or "gt1".
      - For each category, the ordering is similar:
           For "inutero" and "30d": use col1 then col4;
           For "2m12m": use only col4.
      Returns a dictionary with keys "spi" and "temp", each mapping to a dictionary with
      keys for each category.
    
    In both cases, the row names are assumed to begin with a valid SPI prefix (e.g., "spi1_", "spi3_", etc.)
    or a valid temperature prefix (e.g., "stdm_t_", "absdifm_t_", etc.). The function removes these prefixes 
    to derive a key.
    """
    # Set dictionary to export results
    results = {}
    
    # Define valid prefixes for each group.
    valid_temps = ("stdm_t_", "absdifm_t_", "absdif_t_", "std_t_", "t_")
    valid_spis = ("spi1_", "spi3_", "spi6_", "spi9_", "spi12_", "spi24_", "spi48_")
    valid_timeframes = [
        "inutero_1m3m", "inutero_4m6m", "inutero_6m9m", 
        "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m", 
        "born_12m15m", "born_15m18m", "born_18m21m", "born_21m24m", 
    ]
    spi_data = {"cell1": {}, "cell2": {}, "cell3": {}}
    temp_data = {"cell1": {}, "cell2": {}, "cell3": {}}

    # Read file lines.
    with open(file_path, "r") as file:
        lines = file.readlines()

    i = 0
    len_tokens = 0
    for i, line in enumerate(lines):
        line = line.strip()

        # Replace LaTeX escapes.
        line = line.replace(r"\\", "").replace(r"\_", "_")

        # Process lines that start with any valid spi or temp prefix.
        if not (line.startswith(valid_spis) or line.startswith(valid_temps)):
            continue

        # print(line)
        # The first token holds the variable name.
        tokens = line.split()  # splitting by whitespace
        err_line = lines[i + 1].strip()
        full_key = tokens[0]  # e.g., "spi_inutero_avg_neg" or "spi_inutero_avg_ltm1"

        # Remove the valid prefixes to obtain the key.
        key = remove_words_from_string(full_key, valid_spis)
        key = remove_words_from_string(key, valid_temps)
        if key and key[0] == "_":
            key = key[1:]
        
        # Split the row by ampersand to extract coefficient tokens.
        coeff_tokens = [t.replace("\\", "").strip() for t in line.split("&")]
        err_tokens = [t.replace("\\", "").strip() for t in err_line.split("&")]
        
        if len_tokens==0:
            len_tokens = len(coeff_tokens)
        assert len_tokens == len(coeff_tokens), f"Length mismatch: {len_tokens} vs {len(coeff_tokens)}"
        
        if contains_any_string(full_key, valid_timeframes):
           
            # Select the coefficients from the corresponding cell FE and remove the stars 
            cell1 = [to_float(c.replace("*", "")) for c in coeff_tokens[1::3]]
            cell2 = [to_float(c.replace("*", "")) for c in coeff_tokens[2::3]]
            cell3 = [to_float(c.replace("*", "")) for c in coeff_tokens[3::3]]
            
            # Select the standard errors from the corresponding cell FE and remove the stars
            err_cell1 = [to_float(c.replace("(", "").replace(")", "")) for c in err_tokens[1::3]]
            err_cell2 = [to_float(c.replace("(", "").replace(")", "")) for c in err_tokens[2::3]]
            err_cell3 = [to_float(c.replace("(", "").replace(")", "")) for c in err_tokens[3::3]]

            # Compute the confidence intervals
            cilower_cell1, ciupper_cell1 = compute_ci(cell1, err_cell1)
            cilower_cell2, ciupper_cell2 = compute_ci(cell2, err_cell2)
            cilower_cell3, ciupper_cell3 = compute_ci(cell3, err_cell3)
            
            if contains_any_string(full_key, valid_spis):
                spi_data["cell1"][key] = {"coef": cell1, "se": err_cell1, "lower": cilower_cell1, "upper": ciupper_cell1}
                spi_data["cell2"][key] = {"coef": cell2, "se": err_cell2, "lower": cilower_cell2, "upper": ciupper_cell2}
                spi_data["cell3"][key] = {"coef": cell3, "se": err_cell3, "lower": cilower_cell3, "upper": ciupper_cell3}
                
            elif contains_any_string(full_key, valid_temps):
                temp_data["cell1"][key] = {"coef": cell1, "se": err_cell1, "lower": cilower_cell1, "upper": ciupper_cell1}
                temp_data["cell2"][key] = {"coef": cell2, "se": err_cell2, "lower": cilower_cell2, "upper": ciupper_cell2}
                temp_data["cell3"][key] = {"coef": cell3, "se": err_cell3, "lower": cilower_cell3, "upper": ciupper_cell3}
                        
    results["spi"] = spi_data
    results["temp"] = temp_data
    
    return results

def extract_coefficients_and_CI_latex_heterogeneity(heterogeneity, shock, spi, temp, stat):
    """
    Extracts coefficients and confidence intervals from a LaTeX file.
    
    Parameters:
      file_path : str
          Path to the LaTeX file containing the regression results.
          
    Returns:
      dict : A dictionary containing the extracted coefficients and confidence intervals.
    """
    f_name = f"linear_dummies_true_{spi}_{stat}_{temp}  -"
    files = os.listdir(rf"{OUTPUTS}\heterogeneity\{heterogeneity}")
    files = [f for f in files if f_name in f]
    files = [f for f in files if "standard_fe.tex" in f]
    bands = [f.replace(f"linear_dummies_true_{spi}_{stat}_{temp}  - ", "").replace(" standard_fe.tex", "") for f in files] 

    plotdata = {}
    for i, band in enumerate(bands):

        file_path = rf"{OUTPUTS}\heterogeneity\\{heterogeneity}\{files[i]}"
        n = extract_sample_size(file_path)

        if n < 100_000:
            continue
        
        outdata = extract_coefficients_and_CI_latex(file_path)

        # Gather all the keys:
        keys = list(outdata[shock]["cell1"].keys())

        for key in keys:
            if key not in plotdata:
                plotdata[key] = {}
            plotdata[key][band] = outdata[shock]["cell1"][key]
    
    return plotdata
    
def distribute_x_values(x_values, n, margin=0.1):
    ''' Given a set of values for x, distribute them evenly across n groups. 
    
    This function is useful for creating a plot with multiple x series.
    '''

    n = n + 1 # Add one to account for the fact that the first value is not used
    offset = (1-2*margin)/n

    out_values = []
    for i in range(n):
        if i==0:
            continue
        actual_offset = offset*i - (0.5 - margin)

        out_values += [list(np.array(x_values) + actual_offset)]

    return out_values

def plot_regression_coefficients(
        data, 
        shock,
        spi, 
        temp, 
        stat,
        margin=0.2,
        colors=["#ff5100", "#3e9fe1"], 
        labels=["High temperature shocks","Low temperature shocks"],
        outpath=None,
    ):
    
    import os
    import seaborn as sns

    title_labels = {
        "inutero_1m3m_avg_pos": "1st In-Utero Quarter",
        "inutero_4m6m_avg_pos": "2nd In-Utero Quarter",
        "inutero_6m9m_avg_pos": "3rd In-Utero Quarter",
        "born_1m3m_avg_pos": "1st Born Quarter",
        "born_3m6m_avg_pos": "2nd Born Quarter",
        "born_6m9m_avg_pos": "3rd Born Quarter",
        "born_9m12m_avg_pos": "4th Born Quarter",
        "born_12m15m_avg_pos": "5th Born Quarter",
        "born_15m18m_avg_pos": "6th Born Quarter",
    }
    
    data = data[shock]["cell1"]

    fig, axs = plt.subplots(2, 4, figsize=(20, 6))

    xvalues_clean = [0,1,2,3,4]
    for i, key in enumerate(data.keys()):

        if i/2==len(axs.flatten()):
            break
        i_round = i // 2
        pos = int((i/2 - i_round )*2)
        
        plotdata = data[key]

        coefs = np.array(plotdata["coef"][:5])
        lower = np.array(plotdata["lower"][:5])
        upper = np.array(plotdata["upper"][:5])
        
        is_neg = "_neg" in key
        
        if is_neg:
            coefs = coefs*-1
            old_upper = upper
            upper = lower*-1
            lower = old_upper*-1
        yerr = [
            list(np.subtract(coefs, lower)), # 'down' error
            list(np.subtract(upper, coefs))
        ]  # 'up' error
        
        # Get the color from the cycle
        color = colors[0] if is_neg else colors[1]
        label = labels[0] if is_neg else labels[1]
        
        ax = axs.flatten()[i_round]

        xvalues = distribute_x_values(xvalues_clean, 2, margin=margin)[pos]
        if i_round != i/2:
            ax.set_title(title_labels[key])
        
        ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=label, color=color)
        ax.plot(xvalues, coefs, color=color)

        # Now call our helper function to highlight points with a lower CI bound > 0.
        highlight_significant_points(ax, xvalues, coefs, lower, color=color)

        ax.axhline(y=0, color="black", linewidth=1)
        # sns.lineplot(data["inutero_1m3m_avg_pos"]["coef"], ax=ax)
        # sns.despine()
        ax.spines['top'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xticks(xvalues_clean, labels=["1st Q", "2nd Q", "3rd Q", "4th Q", "5th Q"])#, "6th Q"])
        ax.set_xlim(-0.3, 4.6)
        if i<4*2: # Only the first row of plots
            ax.set_ylim(-0.8, 1.5)
        else:
            ax.set_ylim(-0.6, 0.8)
            
    fig.tight_layout()
    plt.legend(loc='lower center', bbox_to_anchor=(-1.2, -0.35), ncol=2, frameon=False)
    
    os.makedirs(outpath, exist_ok=True)
    filename = fr"{outpath}\{shock}_coefficients_{spi}_{stat}_{temp}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("Se creó la figura ", filename)
        
def plot_spline_coefficients(all_values, all_ci_top, all_ci_bot, margin, mfcs=[], labels=[], plot="both", outpath=None):
    
    def add_whitespace_to_axis(ax, x):
            ax.set_xticks(x)
            ax.set_xticklabels("")
            ax.tick_params(axis="x", length=10, width=10, color="white", direction="inout", zorder=100)  # Remove tick marks
            ax.spines["bottom"].set_visible(False)  # Move third x-axis further down
            ax.set_xticks(x, minor=True)

            return ax
        
    # Check if trying to plot single series or multiple:
    is_list_of_lists = isinstance(all_values[0], list)
    
    if not is_list_of_lists:
        all_values = [all_values]
        all_ci_top = [all_ci_top]
        all_ci_bot = [all_ci_bot]
        
    x = range(1, len(all_values[0])+1)
    values_x = distribute_x_values(x, len(all_values), margin) 

    fig, ax = plt.subplots(1, 1, figsize=(6, 4))
    for data_set in range(len(all_values)):
        values = all_values[data_set]
        x = values_x[data_set]
        ci_top = all_ci_top[data_set]
        ci_bot = all_ci_bot[data_set]
        mfc = mfcs[data_set] if len(mfcs) > 0 else "black"
        label = labels[data_set] if len(labels) > 0 else None
        
        yerr = [list(np.array(values) - np.array(ci_bot)), # 'down' error
                list(np.array(ci_top) - np.array(values))]  # 'up' error

        # Plot error bars
        ax.errorbar(x, values, yerr=yerr, capsize=3, fmt="o", mfc=mfc, color="black", label=label)

    if len(labels) > 0:
        ax.legend(bbox_to_anchor=(0.47, -0.15), frameon=False, ncols=4, loc="upper center")
    
    ax.axhline(y=0, color="black", linestyle="--", dashes=(7, 7), linewidth =1)
    ax.set_xlim(0.5, len(all_values[0])+.5)
    
    # Set second level of labels (1 month and 2-12 months)
    ax = add_whitespace_to_axis(ax, [1.5, 2.5, 3.5])
    
    ax2 = ax.secondary_xaxis('bottom')
    ax2.set_xticklabels([])
    ax2.tick_params(axis="x", length=0)  # Remove tick marks
    
    ax3 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
    
    ax3.set_xticks([1, 2, 3, 4],)
    ax3.set_xticklabels(["<-1 Std", "Between\n-1 and 0 Std", "Between\n0 and +1 Std", ">1 Std"])
    ax3.tick_params(axis="x", length=0)  # Remove tick marks
    
    fig.savefig(outpath, bbox_inches='tight')

def extract_sample_size(filepath):
    import re

    with open(filepath, "r") as file:
        lines = file.readlines()

    for line in lines:

        is_sample_size_row = re.search(r'\$N\$\s*&', line)
        
        if is_sample_size_row:

            n = re.search(r'(\d+,\d+,\d+)', line)
            if n is None:
                n = re.search(r'(\d+,\d+)', line)
                if n is None:
                    print(n)
                    n = re.search(r'(\d+)', line).group()
                else:
                    n = n.group().replace(",","")
            else:        
                n = n.group().replace(",","")
            n = int(n)

            return n 
        
    return None

def plot_heterogeneity(
        heterogeneity,
        spi, 
        temp, 
        stat, 
        colors=None,
        labels=None,    
        outpath=None,
    ):
    '''
        Plot the coefficients of the heterogeneity analysis.
        Parameters:
            full_data: dict
                The data containing the coefficients and confidence intervals.
            heterogeneity: str
                The type of heterogeneity to plot. Must have a folder in the 
                outputs directory with the same name. 
            shock: str
                The type of shock to plot. Can be "temp" or "spi".
            spi: str
                The type of SPI to plot. Can be "spi1", "spi3", "spi6", "spi9", 
                "spi12", "spi24", "spi48".   
            temp: str
                The type of temperature to plot.
            stat: str
                The type of statistic to plot.  
            colors: list
                The colors to use for the different cases.
            labels: list
                The labels to use for the different cases.
            outpath: str
                The path to save the plot.
    '''         
    for shock in ["temp", "spi"]:
        full_data = extract_coefficients_and_CI_latex_heterogeneity(
            heterogeneity, shock, spi, temp, stat
        )            

        for sign in ["_neg", "_pos"]:
            
            # Keep only keys that contain the specified sign
            data = {k: v for k, v in full_data.items() if sign in k}
            n_heterogeneity = len(data[f"inutero_1m3m_avg{sign}"].keys())

            title_labels = {
                f"inutero_1m3m_avg{sign}": "1st In-Utero Quarter",
                f"inutero_4m6m_avg{sign}": "2nd In-Utero Quarter",
                f"inutero_6m9m_avg{sign}": "3rd In-Utero Quarter",
                f"born_1m3m_avg{sign}": "1st Born Quarter",
                f"born_3m6m_avg{sign}": "2nd Born Quarter",
                f"born_6m9m_avg{sign}": "3rd Born Quarter",
                f"born_9m12m_avg{sign}": "4th Born Quarter",
                f"born_12m15m_avg{sign}": "5th Born Quarter",
                f"born_15m18m_avg{sign}": "6th Born Quarter",
            }
            fig, axs = plt.subplots(2, 4, figsize=(20, 6))
            xvalues_clean = [0,1,2,3,4]
            for i, key in enumerate(data.keys()):
                if "born_15m18m_avg" in key:
                    break
                heterogeneity_data = data[key]
                for j, case in enumerate(heterogeneity_data.keys()):

                    coefs = np.array(heterogeneity_data[case]["coef"][:5])
                    lower = np.array(heterogeneity_data[case]["lower"][:5])
                    upper = np.array(heterogeneity_data[case]["upper"][:5])
                    
                    is_neg = "_neg" in key
                    sign = "_neg" if is_neg else "_pos"
                    
                    if is_neg:
                        coefs = coefs*-1
                        old_upper = upper
                        upper = lower*-1
                        lower = old_upper*-1
                    yerr = [
                        list(np.subtract(coefs, lower)), # 'down' error
                        list(np.subtract(upper, coefs))
                    ]  # 'up' error
                    
                    # Get the color from the cycle
                    color = colors[j]
                    label = labels[j]
                    if (i==0) & (shock=="spi") & (sign=="_neg"): # Only print this for this first iteration
                        print(f"case: {case} -> label: {label}")
                    
                    ax = axs.flatten()[i]

                    xvalues = distribute_x_values(xvalues_clean, n_heterogeneity, margin=0.15)[j]
                    
                    ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=label, color=color)
                    ax.plot(xvalues, coefs, color=color)

                    # Now call our helper function to highlight points with a lower CI bound > 0.
                    highlight_significant_points(ax, xvalues, coefs, lower, color=color)

                ax.set_title(title_labels[key])
                ax.axhline(y=0, color="black", linewidth=1)
                ax.spines['top'].set_visible(False)
                ax.spines['bottom'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.set_xticks(xvalues_clean, labels=["1st Q", "2nd Q", "3rd Q", "4th Q", "5th Q"])#, "6th Q"])
                ax.set_xlim(-0.3, 4.6)

            fig.tight_layout()
            plt.legend(loc='lower center', bbox_to_anchor=(-1.2, -0.35), ncol=6, frameon=False)

            filename = fr"{outpath}\heterogeneity {heterogeneity} - {shock}{sign}_coefficients_{spi}_{stat}_{temp}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print("Se creó la figura ", filename)
    
def plot_shocks_histogram(df, cols, outpath):
    
    import seaborn as sns
    
    fig, axs = plt.subplots(3, 2, figsize=(8, 6), sharey=True)

    for i, ax in enumerate(axs.flatten()):
        col = cols[i]
        # if "spi1" not in col:
        #     continue
        
        s = df[col]
        
        # Compute thresholds
        std = s.std()
        mean = s.mean()
        positive = mean + std
        negative = mean - std
        
        s.plot(kind="kde", color="black", label=col, ax=ax)
        ax.axvline(positive, color="black", linestyle="--")
        ax.axvline(negative, color="black", linestyle="--")
        ax.axvline(0, color="black", linestyle="-")

        ax.set_xlim(-2.5, 2.5)
        ax.set_ylim(-0.1, 1.2)
        ax.set_ylabel("")
        
        
    fig.tight_layout(pad=2)

    plt.text(x=-7.33, y=5, s=f"Standardized Temperature Anomaly")
    plt.text(x=-1.54, y=5, s=f"Standardized Precipitation Index")

    plt.text(x=-2.73, y=3.16, s=f"In-utero", ha="center")
    plt.text(x=-2.73, y=1.36, s=f"First 30-days", ha="center")
    plt.text(x=-2.73, y=-0.45, s=f"Between month 1 and 12", ha="center")
    sns.despine()
    
    fig.savefig(outpath, dpi=300, bbox_inches='tight', pad_inches=0.2)

