import re
import os
import numpy as np
import matplotlib.pyplot as plt

OUTPUTS = r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Outputs"

# --- Derived Configurations for Specific Plots ---
SEMESTER_CONFIG = {
    "canvas_size": (2, 3),
    "time_frames": ["inutero", "born_1m6m", "born_6m12m", "born_12m18m", "born_18m24m", "born_24m30m"],
    "title_labels": {
        "inutero": "In-Utero",
        "born_1m6m": "1st Semester (months 0-6)", "born_6m12m": "2nd Semester (months 6-12)",
        "born_12m18m": "3rd Semester (months 12-18)", "born_18m24m": "4th Semester (months 18-24)", "born_24m30m": "5th Semester (months 24-30)",
    },
    "x_tick_labels": ["0-6", "6-12", "12-18", "18-24", "24-30",], # Example x-axis labels
    "xlim": (-0.5, 4.5), 
    "legend_pos": {"loc": 'lower center', "bbox_to_anchor": (0.5, -0.4), "ncol": 2}
}
# -- Base Configuration for Quarterly analysis (7 periods) --
QUARTERLY_CONFIG = {
    "canvas_size": (2, 4),
    "time_frames": [None, "inutero_1m3m", "inutero_3m6m", "inutero_6m9m", "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m"],
    "title_labels": {
        "inutero_1m3m": "1st In-Utero Quarter", "inutero_3m6m": "2nd In-Utero Quarter", "inutero_6m9m": "3rd In-Utero Quarter",
        "born_1m3m": "1st Born Quarter", "born_3m6m": "2nd Born Quarter", "born_6m9m": "3rd Born Quarter", "born_9m12m": "4th Born Quarter",
    },
    "x_tick_labels": ["1st Q", "2nd Q", "3rd Q", "4th Q"],
    "xlim": (-0.5, 3.5), "ylim": (-0.8, 1.5),
    "legend_pos": {"loc": 'lower center', "bbox_to_anchor": (-1.35, -0.3), "ncol": 2}
}

# -- Base Configuration for Monthly analysis (example, 12 periods) --
MONTHLY_CONFIG = {
    "canvas_size": (3, 4),
    "time_frames": [f"month_{i}" for i in range(1, 13)],
    "title_labels": {f"month_{i}": f"Month {i}" for i in range(1, 13)},
    "x_tick_labels": ["Jan", "Feb", "Mar"],
    "xlim": (-0.5, 2.5), "ylim": (-0.5, 0.5),
    "legend_pos": {"loc": 'lower center', "bbox_to_anchor": (0.5, -0.3), "ncol": 2}
}

# Horserace plots might need different y-axis limits or legend placement
HORSERACE_CONFIG = QUARTERLY_CONFIG.copy()
HORSERACE_CONFIG["ylim"] = (-0.6, 0.8)
HORSERACE_CONFIG["legend_pos"] = {"loc": 'lower center', "bbox_to_anchor": (-1.35, -0.35), "ncol": 2}

# Heterogeneity plots often need more space for the legend
HETEROGENEITY_CONFIG = QUARTERLY_CONFIG.copy()
HETEROGENEITY_CONFIG["legend_pos"] = {"loc": 'lower center', "bbox_to_anchor": (-1.35, -0.25), "ncol": 6}

# Spline plots have 4 series, so the legend needs more columns
SPLINE_CONFIG = QUARTERLY_CONFIG.copy()
SPLINE_CONFIG["legend_pos"] = {"loc": 'lower center', "bbox_to_anchor": (-1.35, -0.25), "ncol": 2}

# Window comparison plots have a different layout and number of subplots
WINDOWS_CONFIG = {
    "canvas_size": (2, 3),
    "time_frames": ["inutero_1m3m", "inutero_3m6m", "inutero_6m9m", "born_1m3m", "born_3m6m", "born_6m9m"],
    "title_labels": {
        "inutero_1m3m": "1st In-Utero Quarter", "inutero_3m6m": "2nd In-Utero Quarter", "inutero_6m9m": "3rd In-Utero Quarter",
        "born_1m3m": "1st Born Quarter", "born_3m6m": "2nd Born Quarter", "born_6m9m": "3rd Born Quarter",
    },
    "x_tick_labels": ["1st Q", "2nd Q", "3rd Q"],
    "xlim": (-0.3, 2.6), "ylim": None, # Let matplotlib decide ylim
    "legend_pos": {"loc": 'lower center', "bbox_to_anchor": (-0.64, -0.5), "ncol": 4}
}

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

def order_files_naturally(file_list):
    """
    Sorts a list of filenames in a "natural" or "alphanumeric" order.
    
    This function ensures that numbers within the filenames are treated as
    numerical values for sorting, so 'file2.txt' comes before 'file10.txt'.

    Args:
        file_list (list): A list of strings (filenames) to be sorted.

    Returns:
        list: A new list containing the filenames sorted in natural order.
        
    Example:
        >>> files = ['z1.txt', 'z10.txt', 'z12.txt', 'z2.txt', 'file_1_a.log']
        >>> order_files_naturally(files)
        ['file_1_a.log', 'z1.txt', 'z2.txt', 'z10.txt', 'z12.txt']
    """
    
    # This helper function will be used as the 'key' for sorting.
    # It converts a filename string into a list of strings and numbers.
    def natural_sort_key(s):
        # re.split finds all sequences of digits (\d+) and splits the string
        # by them. The parentheses ensure the digits themselves are kept in the list.
        # Example: 'v12.z3' -> ['', '12', '.z', '3', '']
        parts = re.split(r'(\d+)', s)
        
        # Now, we convert the string-based numbers to actual integers.
        # We also filter out any empty strings that re.split might create.
        return [int(text) if text.isdigit() else text.lower() for text in parts if text]

    # The sorted() function uses our custom key to perform the natural sort.
    return sorted(file_list, key=natural_sort_key)


def fix_extreme_temperatures_strings(s):
    ''' Convert the original hd35_inutero_6m9m_avg strings to t_inutero_6m9m_avg_pos. '''
    if "hd" in s:
        s = s + "_pos_int"
    elif "fd" in s or "id" in s:
        s = s + "_neg_int"
    for prefix in ["hd35_", "hd40_", "fd_", "id_"]:
        s = s.replace(prefix, "t_")
    return s

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

def extract_coefficients_and_CI_latex(file_path, horserace: None | str = None):
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
    valid_standard_temps = ("stdm_t_", "absdifm_t_", "absdif_t_", "std_t_", "t_")
    valid_extreme_temps = ("hd35_", "hd40_", "fd_", "id_")
    all_temps = valid_standard_temps + valid_extreme_temps
    if horserace is None:
        valid_temps = valid_standard_temps + valid_extreme_temps
    elif horserace == "extremes":
        valid_temps = valid_extreme_temps
    elif horserace == "standard":
        valid_temps = valid_standard_temps
    else:
        raise ValueError(f"Invalid horserace value: {horserace}. Must be 'extremes', 'standard', or None.")
        
    valid_spis = ("spi1_", "spi3_", "spi6_", "spi9_", "spi12_", "spi24_", "spi48_")
    all_spis = valid_spis
    valid_timeframes = [
        "born_1m",
        "inutero_1m3m", "inutero_3m6m", "inutero_6m9m", 
        "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m", 
        "born_12m15m", "born_15m18m", "born_18m21m",# "born_21m24m", 
        "inutero", "born_1m6m", "born_6m12m", 
        "born_12m18m", "born_18m24m",#, "born_24m30m", "born_30m36m", 
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

        # The first token holds the variable name.
        tokens = line.split()  # splitting by whitespace
        err_line = lines[i + 1].strip()
        full_key = tokens[0]  # e.g., "spi_inutero_avg_neg" or "spi_inutero_avg_ltm1"
        
        # Remove the valid prefixes to obtain the key.
        full_key = fix_extreme_temperatures_strings(full_key)
        key = remove_words_from_string(full_key, all_spis)
        key = remove_words_from_string(key, all_temps)

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
            cell1 = [to_float(c.replace("*", "")) for c in coeff_tokens[1::3]][:-2]
            cell2 = [to_float(c.replace("*", "")) for c in coeff_tokens[2::3]][:-2]
            cell3 = [to_float(c.replace("*", "")) for c in coeff_tokens[3::3]][:-2]

            # Select the standard errors from the corresponding cell FE and remove the stars
            err_cell1 = [to_float(c.replace("(", "").replace(")", "")) for c in err_tokens[1::3]][:-2]
            err_cell2 = [to_float(c.replace("(", "").replace(")", "")) for c in err_tokens[2::3]][:-2]
            err_cell3 = [to_float(c.replace("(", "").replace(")", "")) for c in err_tokens[3::3]][:-2]

            # Compute the confidence intervals
            cilower_cell1, ciupper_cell1 = compute_ci(cell1, err_cell1)
            cilower_cell2, ciupper_cell2 = compute_ci(cell2, err_cell2)
            cilower_cell3, ciupper_cell3 = compute_ci(cell3, err_cell3)
            
            if contains_any_string(full_key, all_spis):
                spi_data["cell1"][key] = {"coef": cell1, "se": err_cell1, "lower": cilower_cell1, "upper": ciupper_cell1}
                spi_data["cell2"][key] = {"coef": cell2, "se": err_cell2, "lower": cilower_cell2, "upper": ciupper_cell2}
                spi_data["cell3"][key] = {"coef": cell3, "se": err_cell3, "lower": cilower_cell3, "upper": ciupper_cell3}
            
            elif contains_any_string(full_key, all_temps):
                temp_data["cell1"][key] = {"coef": cell1, "se": err_cell1, "lower": cilower_cell1, "upper": ciupper_cell1}
                temp_data["cell2"][key] = {"coef": cell2, "se": err_cell2, "lower": cilower_cell2, "upper": ciupper_cell2}
                temp_data["cell3"][key] = {"coef": cell3, "se": err_cell3, "lower": cilower_cell3, "upper": ciupper_cell3}
    
    results["spi"] = spi_data
    results["temp"] = temp_data

    return results

def extract_coefficients_and_CI_latex_horserace(file_path):
    """
    Extracts coefficients and their 95% CI bounds from a LaTeX table for horserace analysis.
    
    Parameters:
      file_path : str
          Path to the LaTeX file containing the regression results.
      horserace : str
          The type of horserace analysis, either "standard" or "extreme".
          
    Returns:
      dict : A dictionary containing the extracted coefficients and confidence intervals.
    """
    standards = extract_coefficients_and_CI_latex(file_path, horserace="standard")
    extremes = extract_coefficients_and_CI_latex(file_path, horserace="extremes")
    return {"standard": standards, "extreme": extremes}

def extract_coefficients_and_CI_latex_heterogeneity(heterogeneity, shock, spi, temp, stat, timeframe):
    """
    Extracts coefficients and confidence intervals from a LaTeX file.
    
    Parameters:
      file_path : str
          Path to the LaTeX file containing the regression results.
          
    Returns:
      dict : A dictionary containing the extracted coefficients and confidence intervals.
    """
    f_name = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe} -"
    folder = rf"{OUTPUTS}\heterogeneity\{heterogeneity}"
    assert os.path.exists(folder), f"{folder} does not exist!"
    files = os.listdir(folder)
    assert len(files)>0, f"No files in folder! {folder}"
    files = [f for f in files if f_name in f]
    assert len(files)>0, f"f_name does not exist! {f_name}"
    files = [f for f in files if "standard_fe standard_sym.tex" in f]
    bands = [f.replace(f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe} - ", "").replace(" standard_fe standard_sym.tex", "") for f in files] 
    assert len(bands)>0, f"There is an issue with filenames! {bands}"
    plotdata = {}
    for i, band in enumerate(bands):

        file_path = rf"{OUTPUTS}\heterogeneity\\{heterogeneity}\{files[i]}"
        assert os.path.exists(file_path), f"{file_path} does not exist!"
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

def extract_coefficients_and_CI_latex_stat_windows(shock, spi, temp, stat):
    """
    Extracts coefficients and confidence intervals from a LaTeX file.
    
    Parameters:
      file_path : str
          Path to the LaTeX file containing the regression results.
          
    Returns:
      dict : A dictionary containing the extracted coefficients and confidence intervals.
    """
    f_name = f"linear_dummies_true_{spi}_{stat}"
    files = os.listdir(OUTPUTS)
    assert len(files)>0, "No files in OUTPUS!"
    files = [f for f in files if f_name in f]
    assert len(files)>0, f"No files with {f_name}!"
    files = [f for f in files if "standard_fe standard_sym.tex" in f]
    assert len(files)>0, f"No files with standard_fe standard_sym.tex!"
    files = order_files_naturally(files)
    
    windows = [f.replace(f"linear_dummies_true_{spi}_", "").replace("_stdm_t 1m windows standard_fe standard_sym.tex", "") for f in files] # 

    plotdata = {}
    for i, window in enumerate(windows):

        file_path = rf"{OUTPUTS}\{files[i]}"
        assert os.path.exists(file_path), f"{file_path} does not exist!"
        outdata = extract_coefficients_and_CI_latex(file_path)

        # Gather all the keys:
        keys = list(outdata[shock]["cell1"].keys())

        for key in keys:
            original_key = key
            key = key.replace(f"_{window}", "") 
            if key not in plotdata:
                plotdata[key] = {}
            plotdata[key][window] = outdata[shock]["cell1"][original_key]
            
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
        data, shock, spi, temp, stat,
        canvas_size, time_frames, title_labels, x_tick_labels, xlim, ylim=(-0.8, 1.5), legend_pos=(-0.5,2),
        margin=0.2, colors=["#ff5100", "#3e9fe1"],
        labels=["Low temp shocks (inverted)", "High temp shocks"],
        outpath=None, add_line=False, start="", extra="",
    ):
    data_to_plot = data.get(shock, {}).get("cell1", {})
    if not data_to_plot:
        print(f"Warning: No data found for shock '{shock}'. Skipping plot.")
        return

    n_rows, n_cols = canvas_size
    fig, axs = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3.5 * n_rows), squeeze=False)
    flat_axs = axs.flatten()
    xvalues_clean = np.arange(len(x_tick_labels))

    for i, ax in enumerate(flat_axs):
        if i >= len(time_frames) or time_frames[i] is None:
            ax.axis('off')
            continue

        timeframe_key_base = time_frames[i]
        
        for series_idx, sign in enumerate(["_neg", "_pos"]):
            full_data_key = f"{timeframe_key_base}_{stat}{sign}_int"
            
            if full_data_key not in data_to_plot:
                continue

            plotdata = data_to_plot[full_data_key]
            coefs = np.array(plotdata["coef"][:len(x_tick_labels)])
            lower = np.array(plotdata["lower"][:len(x_tick_labels)])
            upper = np.array(plotdata["upper"][:len(x_tick_labels)])
            
            if sign == "_neg":
                coefs *= -1
                lower, upper = -upper, -lower
            yerr = [coefs - lower, upper - coefs]

            xvalues = distribute_x_values(xvalues_clean, 2, margin=margin)[series_idx]
            ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=labels[series_idx], color=colors[series_idx])
            if add_line:
                ax.plot(xvalues, coefs, color=colors[series_idx])
            highlight_significant_points(ax, xvalues, coefs, lower, color=colors[series_idx])

        ax.set_title(title_labels.get(timeframe_key_base, timeframe_key_base.replace("_", " ")))
        ax.axhline(y=0, color="black", linewidth=1)
        ax.spines[['top', 'bottom', 'right']].set_visible(False)
        ax.set_xticks(xvalues_clean, labels=x_tick_labels)
        ax.set_xlim(xlim)
        if ylim: ax.set_ylim(ylim)

    fig.tight_layout(rect=[0, 0.08, 1, 1])
    plt.legend(**legend_pos, frameon=False)
    os.makedirs(outpath, exist_ok=True)
    filename = fr"{outpath}\{start}{shock}_coefficients_{spi}_{stat}_{temp}{extra}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("Se creó la figura ", filename)
    plt.close()


def plot_horserace_temp(
        data, spi, temp, stat,
        canvas_size, time_frames, title_labels, x_tick_labels, xlim, ylim=(-0.8, 1.5), legend_pos=(-0.5,2),
        colors=["#3e9fe1", "#ff5100", "#6baed6", "#fd8d3c"],
        labels=["Standard (+)", "Extreme (+)", "Standard (-) (inverted)", "Extreme (-)"],
        outpath=None, add_line=False, start="", extra="",
    ):
    # Data restructuring...
    plotdata = {}
    standard_temp_data = data.get('standard', {}).get('temp', {}).get('cell1', {})
    extreme_temp_data = data.get('extreme', {}).get('temp', {}).get('cell1', {})
    if not standard_temp_data and not extreme_temp_data:
        print("Warning: No horserace data found. Skipping plot.")
        return
        
    all_raw_keys = list(standard_temp_data.keys()) + list(extreme_temp_data.keys())
    base_keys = sorted(list(set([k.replace("_pos_int", "").replace("_neg_int", "") for k in all_raw_keys])))
    
    for base_key in base_keys:
        plotdata[base_key] = {}
        for sign in ["pos", "neg"]:
            key = f"{base_key}_{sign}_int"
            if key in standard_temp_data: plotdata[base_key][f'standard_{sign}'] = standard_temp_data[key]
            if key in extreme_temp_data: plotdata[base_key][f'extreme_{sign}'] = extreme_temp_data[key]
            
    cases = ['standard_pos', 'extreme_pos', 'standard_neg', 'extreme_neg']
    n_cases = len(cases)

    # Plotting setup...
    n_rows, n_cols = canvas_size
    fig, axs = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 4 * n_rows), squeeze=False)
    flat_axs = axs.flatten()
    xvalues_clean = np.arange(len(x_tick_labels))

    for i, ax in enumerate(flat_axs):
        if i >= len(time_frames) or time_frames[i] is None:
            ax.axis('off')
            continue

        timeframe_key_base = f"{time_frames[i]}_{stat}"
        horserace_data_for_key = plotdata.get(timeframe_key_base, {})

        for j, case_name in enumerate(cases):
            plotdata_case = horserace_data_for_key.get(case_name)
            if not plotdata_case: continue
                
            coefs = np.array(plotdata_case["coef"][:len(x_tick_labels)])
            lower = np.array(plotdata_case["lower"][:len(x_tick_labels)])
            upper = np.array(plotdata_case["upper"][:len(x_tick_labels)])

            if case_name == 'standard_neg':
                coefs *= -1
                lower, upper = -upper, -lower
                
            yerr = [coefs - lower, upper - coefs]
            xvalues = distribute_x_values(xvalues_clean, n_cases, margin=0.1)[j]
            ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=labels[j], color=colors[j])
            if add_line: ax.plot(xvalues, coefs, color=colors[j], alpha=0.7)
            highlight_significant_points(ax, xvalues, coefs, lower, color=colors[j])

        ax.set_title(title_labels.get(time_frames[i], ""))
        ax.axhline(y=0, color="black", linewidth=1)
        ax.spines[['top', 'right']].set_visible(False)
        ax.set_xticks(xvalues_clean, labels=x_tick_labels)
        ax.set_xlim(xlim)
        if ylim: ax.set_ylim(ylim)

    fig.tight_layout(rect=[0, 0.08, 1, 1])
    plt.legend(**legend_pos, frameon=False)
    filename = fr"{outpath}\horserace - {start}temp_coefficients_{spi}_{stat}_{temp}{extra}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("Se creó la figura ", filename)
    plt.close()


def plot_spline_coefficients(
        data, shock, spi, temp, stat,
        canvas_size, time_frames, title_labels, x_tick_labels, xlim, ylim=(-0.8, 1.5), legend_pos=(-0.5,2),
        margin=0.2,
        colors=["#ff5100", "#3e9fe1"], 
        labels=["High temp shocks","Low temp shocks"],
        outpath=None,
    ):
    data_to_plot = data.get(shock, {}).get("cell1", {})
    if not data_to_plot:
        print(f"Warning: No spline data found for shock '{shock}'. Skipping plot.")
        return

    n_rows, n_cols = canvas_size
    fig, axs = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3.5 * n_rows), squeeze=False)
    flat_axs = axs.flatten()
    xvalues_clean = np.arange(len(x_tick_labels))
    
    spline_cats = ['gt1_int', 'bt01_int', 'bt0m1_int', 'ltm1_int'] # From high to low

    for i, ax in enumerate(flat_axs):
        if i >= len(time_frames) or time_frames[i] is None:
            ax.axis('off')
            continue

        timeframe_key_base = time_frames[i]

        for j, cat_suffix in enumerate(spline_cats):
            full_data_key = f"{timeframe_key_base}_{stat}_{cat_suffix}"
            plotdata = data_to_plot.get(full_data_key)
            if not plotdata: continue

            coefs = np.array(plotdata["coef"][:len(x_tick_labels)])
            lower = np.array(plotdata["lower"][:len(x_tick_labels)])
            upper = np.array(plotdata["upper"][:len(x_tick_labels)])

            if "ltm1" in cat_suffix or "bt0m1" in cat_suffix:
                coefs *= -1
                lower, upper = -upper, -lower
                
            yerr = [coefs - lower, upper - coefs]
            
            xvalues = distribute_x_values(xvalues_clean, len(spline_cats), margin=margin)[j]
            ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=labels[j], color=colors[j])
            highlight_significant_points(ax, xvalues, coefs, lower, color=colors[j])

        ax.set_title(title_labels.get(timeframe_key_base, ""))
        ax.axhline(y=0, color="black", linewidth=1)
        ax.spines[['top', 'bottom', 'right']].set_visible(False)
        ax.set_xticks(xvalues_clean, labels=x_tick_labels)
        ax.set_xlim(xlim)
        if ylim: ax.set_ylim(ylim)

    fig.tight_layout(rect=[0, 0.1, 1, 1])
    plt.legend(**legend_pos, frameon=False)
    filename = fr"{outpath}\{shock}_spline_coefficients_{spi}_{stat}_{temp}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("Se creó la figura ", filename)
    plt.close()


def plot_heterogeneity(
        heterogeneity, spi, temp, stat, timeframe, 
        canvas_size, time_frames, title_labels, x_tick_labels, xlim, ylim=(-0.8, 1.5), legend_pos=(-0.5,2),
        colors=None, labels=None, outpath=None, add_line=False,
    ):
    for shock in ["temp", "spi"]:
        full_data = extract_coefficients_and_CI_latex_heterogeneity(
            heterogeneity, shock, spi, temp, stat, timeframe,
        )            
        if not full_data: 
            continue

        for sign in ["_neg", "_pos"]:
            data = {k: v for k, v in full_data.items() if sign in k}
            if not data: 
                continue

            n_heterogeneity = len(list(data.values())[0].keys())
            
            n_rows, n_cols = canvas_size
            fig, axs = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3.5 * n_rows), squeeze=False)
            flat_axs = axs.flatten()
            xvalues_clean = np.arange(len(x_tick_labels))

            for i, ax in enumerate(flat_axs):
                if i >= len(time_frames) or time_frames[i] is None:
                    ax.axis('off')
                    continue

                timeframe_key_base = time_frames[i]
                full_data_key = f"{timeframe_key_base}_{stat}{sign}_int"
                heterogeneity_data = data.get(full_data_key)
                if not heterogeneity_data: 
                    continue

                for j, case in enumerate(sorted(heterogeneity_data.keys())): # Sort keys for consistent order
                    print(case, colors[j])
                    plotdata_case = heterogeneity_data[case]
                    coefs = np.array(plotdata_case["coef"][:len(x_tick_labels)])
                    lower = np.array(plotdata_case["lower"][:len(x_tick_labels)])
                    upper = np.array(plotdata_case["upper"][:len(x_tick_labels)])
                    
                    if sign == "_neg":
                        coefs *= -1
                        lower, upper = -upper, -lower
                    yerr = [coefs - lower, upper - coefs]
                    
                    xvalues = distribute_x_values(xvalues_clean, n_heterogeneity, margin=0.15)[j]
                    ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=labels[j], color=colors[j])
                    if add_line: 
                        ax.plot(xvalues, coefs, color=colors[j])
                    highlight_significant_points(ax, xvalues, coefs, lower, color=colors[j])

                ax.set_title(title_labels.get(timeframe_key_base, ""))
                ax.axhline(y=0, color="black", linewidth=1)
                ax.spines[['top', 'bottom', 'right']].set_visible(False)
                ax.set_xticks(xvalues_clean, labels=x_tick_labels)
                ax.set_xlim(xlim)
                if ylim: 
                    ax.set_ylim(ylim)

            fig.tight_layout(rect=[0, 0.1, 1, 1])
            plt.legend(**legend_pos, frameon=False)
            filename = fr"{outpath}\heterogeneity {heterogeneity} - {shock}{sign}_coefficients_{spi}_{stat}_{temp}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print("Se creó la figura ", filename)
    plt.close()


def plot_windows(
        spi, temp, stat,
        time_frames, title_labels, x_tick_labels, xlim, ylim=(-0.8, 1.5), legend_pos=(-0.5,2),
        colors=None, labels=None, outpath=None, add_line=False,
    ):
    fig, axs = plt.subplots(2, 2, figsize=(12, 8), sharey=True)
    titles = [
        ["High temperature shocks", "Low temperature shocks"],
        ["High precipitation shocks", "Low precipitation shocks"],
    ]
    for shock_pos, shock in enumerate(["temp", "spi"]):
        full_data = extract_coefficients_and_CI_latex_stat_windows(
            shock, spi, temp, stat,
        )       
        if not full_data: continue
     
        for sign_pos, sign in enumerate(["_pos", "_neg"]):
            
            ax = axs[shock_pos][sign_pos]
            title = titles[shock_pos][sign_pos]
            
            data = {k: v for k, v in full_data.items() if sign in k}

            if not data: continue

            n_windows = len(x_tick_labels)

            xvalues = np.array(distribute_x_values([0], n_windows, margin=0.05)).flatten()

                
            timeframe_key_base = time_frames[0]
            full_data_key = f"{timeframe_key_base}{sign}_int"

            window_data = data.get(full_data_key)

            if not window_data: continue

            coefs = np.array([window_data[w]["coef"][0] for w in window_data.keys()])
            lower = np.array([window_data[w]["lower"][0] for w in window_data.keys()])
            upper = np.array([window_data[w]["upper"][0] for w in window_data.keys()])

            if sign == "_neg":
                coefs *= -1
                lower, upper = -upper, -lower
            yerr = [coefs - lower, upper - coefs]

            ax.errorbar(xvalues, coefs, yerr=yerr, capsize=3, fmt="o", label=labels, color=colors)
            if add_line: ax.plot(xvalues, coefs, color=colors)
            highlight_significant_points(ax, xvalues, coefs, lower, color=colors)

            ax.axhline(y=0, color="black", linewidth=1)
            ax.spines[['top', 'bottom', 'right']].set_visible(False)
            ax.set_xticks(xvalues, labels=x_tick_labels)
            ax.set_xlim(xlim)
            ax.set_title(title)
            if ylim: ax.set_ylim(ylim)

    fig.tight_layout()
    filename = fr"{outpath}\windows - 1m coefficients_{spi}_{stat}_{temp}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("Se creó la figura ", filename)
    plt.close()

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
                    n = re.search(r'(\d+)', line).group()
                else:
                    n = n.group().replace(",","")
            else:        
                n = n.group().replace(",","")
            n = int(n)

            return n 
        
    return None

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
    plt.close()
