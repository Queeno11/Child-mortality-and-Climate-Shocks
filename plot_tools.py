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
        add_line=False,
    ):
    
    import os

    title_labels = {
        "inutero_1m3m_avg_pos_int": "1st In-Utero Quarter",
        "inutero_4m6m_avg_pos_int": "2nd In-Utero Quarter",
        "inutero_6m9m_avg_pos_int": "3rd In-Utero Quarter",
        "born_1m3m_avg_pos_int": "1st Born Quarter",
        "born_3m6m_avg_pos_int": "2nd Born Quarter",
        "born_6m9m_avg_pos_int": "3rd Born Quarter",
        "born_9m12m_avg_pos_int": "4th Born Quarter",
        "born_12m15m_avg_pos_int": "5th Born Quarter",
        "born_15m18m_avg_pos_int": "6th Born Quarter",
        "born_18m21m_avg_pos_int": "7th Born Quarter",
        "born_21m24m_avg_pos_int": "8th Born Quarter",
    }
    
    data = data[shock]["cell1"]

    fig, axs = plt.subplots(3, 4, figsize=(20, 12))
    axs[0][0].spines['top'].set_visible(False)
    axs[0][0].spines['bottom'].set_visible(False)
    axs[0][0].spines['right'].set_visible(False)
    axs[0][0].spines['left'].set_visible(False)
    axs[0][0].set_xticks([])
    axs[0][0].set_yticks([])
    
    xvalues_clean = [0,1,2,3,4,5,6,7]
    for i, key in enumerate(data.keys()):

        if i/2==len(axs.flatten()):
            break
        i_round = i // 2
        pos = int((i/2 - i_round)*2)
        plotdata = data[key]

        coefs = np.array(plotdata["coef"][:8])
        lower = np.array(plotdata["lower"][:8])
        upper = np.array(plotdata["upper"][:8])
        
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
        
        ax = axs.flatten()[i_round+1]

        xvalues = distribute_x_values(xvalues_clean, 2, margin=margin)[pos]
        if i_round != i/2:
            ax.set_title(title_labels[key])
        
        ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=label, color=color)
        if add_line:
            ax.plot(xvalues, coefs, color=color)

        # Now call our helper function to highlight points with a lower CI bound > 0.
        highlight_significant_points(ax, xvalues, coefs, lower, color=color)

        ax.axhline(y=0, color="black", linewidth=1)
        # sns.lineplot(data["inutero_1m3m_avg_pos"]["coef"], ax=ax)
        # sns.despine()
        ax.spines['top'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xticks(xvalues_clean, labels=["1st Q", "2nd Q", "3rd Q", "4th Q", "5th Q", "6th Q", "7th Q", "8th Q"])
        ax.set_xlim(-0.3, 7.6)
        if i<4*2: # Only the first row of plots
            ax.set_ylim(-0.8, 1.5)
        else:
            ax.set_ylim(-0.6, 0.8)
            
    fig.tight_layout()
    plt.legend(loc='lower center', bbox_to_anchor=(-1.35, -0.2), ncol=2, frameon=False)
    
    os.makedirs(outpath, exist_ok=True)
    filename = fr"{outpath}\{shock}_coefficients_{spi}_{stat}_{temp}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("Se creó la figura ", filename)
        
def plot_spline_coefficients(
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

    title_labels = {
        "inutero_1m3m": "1st In-Utero Quarter",
        "inutero_4m6m": "2nd In-Utero Quarter",
        "inutero_6m9m": "3rd In-Utero Quarter",
        "born_1m3m": "1st Born Quarter",
        "born_3m6m": "2nd Born Quarter",
        "born_6m9m": "3rd Born Quarter",
        "born_9m12m": "4th Born Quarter",
        "born_12m15m": "5th Born Quarter",
        "born_15m18m": "6th Born Quarter",
        "born_18m21m": "7th Born Quarter",
        "born_21m24m": "8th Born Quarter",
    }

    data = data[shock]["cell1"]

    fig, axs = plt.subplots(3, 4, figsize=(20, 12))
    axs[0][0].spines['top'].set_visible(False)
    axs[0][0].spines['bottom'].set_visible(False)
    axs[0][0].spines['right'].set_visible(False)
    axs[0][0].spines['left'].set_visible(False)
    axs[0][0].set_xticks([])
    axs[0][0].set_yticks([])

    xvalues_clean = [0,1,2,3,4,5,6,7]
    line_values = []
    for i, key in enumerate(data.keys()):
        if i/4==len(axs.flatten()):
            break
        i_round = i // 4
        pos = int((i/4 - i_round )*4)
        
        plotdata = data[key]

        coefs = np.array(plotdata["coef"][:8])
        lower = np.array(plotdata["lower"][:8])
        upper = np.array(plotdata["upper"][:8])
        
        is_neg = ("ltm1" or "bt0m1") in key
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
        color = colors[pos]
        label = labels[pos]
        
        ax = axs.flatten()[i_round+1]

        xvalues = distribute_x_values(xvalues_clean, 4, margin=margin)[pos]
        if pos == 3:
            ax.set_title(title_labels[key.split(f"_{stat}")[0]])
        
        print(xvalues)
        print(coefs)
        print(yerr)
        ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=label, color=color)

        # Now call our helper function to highlight points with a lower CI bound > 0.
        highlight_significant_points(ax, xvalues, coefs, lower, color=color)

        ax.axhline(y=0, color="black", linewidth=1)
        # sns.lineplot(data["inutero_1m3m_avg_pos"]["coef"], ax=ax)
        # sns.despine()
        ax.spines['top'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xticks(xvalues_clean, labels=["1st Q", "2nd Q", "3rd Q", "4th Q", "5th Q", "6th Q", "7th Q", "8th Q"])
        ax.set_xlim(-0.3, 7.6)
        line_values += [coefs] 
        
            
    fig.tight_layout()
    plt.legend(loc='lower center', bbox_to_anchor=(-1.35, -0.25), ncol=2, frameon=False)

    os.makedirs(outpath, exist_ok=True)
    filename = fr"{outpath}\{shock}_spline_coefficients_{spi}_{stat}_{temp}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("Se creó la figura ", filename)

    return

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
        add_line=False,
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
                f"inutero_1m3m_avg{sign}_int": "1st In-Utero Quarter",
                f"inutero_4m6m_avg{sign}_int": "2nd In-Utero Quarter",
                f"inutero_6m9m_avg{sign}_int": "3rd In-Utero Quarter",
                f"born_1m3m_avg{sign}_int": "1st Born Quarter",
                f"born_3m6m_avg{sign}_int": "2nd Born Quarter",
                f"born_6m9m_avg{sign}_int": "3rd Born Quarter",
                f"born_9m12m_avg{sign}_int": "4th Born Quarter",
                f"born_12m15m_avg{sign}_int": "5th Born Quarter",
                f"born_15m18m_avg{sign}_int": "6th Born Quarter",
                f"born_18m21m_avg{sign}_int": "7th Born Quarter",
                f"born_21m24m_avg{sign}_int": "8th Born Quarter",
            }
            fig, axs = plt.subplots(3, 4, figsize=(20, 12))
            xvalues_clean = [0,1,2,3,4,5,6,7]
            axs[0][0].spines['top'].set_visible(False)
            axs[0][0].spines['bottom'].set_visible(False)
            axs[0][0].spines['right'].set_visible(False)
            axs[0][0].spines['left'].set_visible(False)
            axs[0][0].set_xticks([])
            axs[0][0].set_yticks([])

            for i, key in enumerate(data.keys()):
                if i==len(axs.flatten()):
                    break
                heterogeneity_data = data[key]
                for j, case in enumerate(heterogeneity_data.keys()):

                    coefs = np.array(heterogeneity_data[case]["coef"][:8])
                    lower = np.array(heterogeneity_data[case]["lower"][:8])
                    upper = np.array(heterogeneity_data[case]["upper"][:8])
                    
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
                    
                    ax = axs.flatten()[i+1]

                    xvalues = distribute_x_values(xvalues_clean, n_heterogeneity, margin=0.15)[j]
                    
                    ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=label, color=color)
                    if add_line:
                        ax.plot(xvalues, coefs, color=color)

                    # Now call our helper function to highlight points with a lower CI bound > 0.
                    highlight_significant_points(ax, xvalues, coefs, lower, color=color)

                ax.set_title(title_labels[key])
                ax.axhline(y=0, color="black", linewidth=1)
                ax.spines['top'].set_visible(False)
                ax.spines['bottom'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.set_xticks(xvalues_clean, labels=["1st Q", "2nd Q", "3rd Q", "4th Q", "5th Q", "6th Q", "7th Q", "8th Q"])
                ax.set_xlim(-0.3, 7.6)

            fig.tight_layout()
            plt.legend(loc='lower center', bbox_to_anchor=(-1.35, -0.25), ncol=2, frameon=False)

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

