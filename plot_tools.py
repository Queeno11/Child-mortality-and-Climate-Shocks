import re
import os
import numpy as np
import matplotlib.pyplot as plt

OUTPUTS = r"D:\World Bank\Paper - Child Mortality and Climate Shocks\Outputs"


def remove_words_from_string(long_string, words):
    for word in words:
        long_string = long_string.replace(word, "")
    return long_string.strip()

def compute_ci(coefs, ses):
    lower = []
    upper = []
    for coef, se in zip(coefs, ses):
        if coef is not None and se is not None:
            lower.append(coef - 2.0796 * se)
            upper.append(coef + 2.0796 * se)
        else:
            lower.append(None)
            upper.append(None)
    return lower, upper

def extract_coefficients_and_CI_latex(file_path, file_type="default"):
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
    # Define valid prefixes for each group.
    valid_temps = ("stdm_t_", "absdifm_t_", "absdif_t_", "std_t_", "t_")
    valid_spis = ("spi1_", "spi3_", "spi6_", "spi9_", "spi2_", "sp24_", "spi48_")
    
    spi_data = {}
    temp_data = {}
    
    # Read file lines.
    assert os.path.exists(file_path), f"File not found: {file_path}"
    with open(file_path, "r") as file:
        lines = file.readlines()
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        # Replace LaTeX escapes.
        line = line.replace(r"\\", "").replace(r"\_", "_")
        
        # Process lines that start with any valid spi or temp prefix.
        if line.startswith(valid_spis) or line.startswith(valid_temps):
            # The first token holds the variable name.
            tokens = line.split()  # splitting by whitespace
            first_token = tokens[0]  # e.g., "spi_inutero_avg_neg" or "spi_inutero_avg_ltm1"
            
            # Remove the valid prefixes to obtain the key.
            key = remove_words_from_string(first_token, valid_spis)
            key = remove_words_from_string(key, valid_temps)
            if key and key[0] == "_":
                key = key[1:]
            
            # Split the row by ampersand to extract coefficient tokens.
            coeff_tokens = [t.replace("\\", "").strip() for t in line.split("&")]
            
            # For rows containing "2m12m", assume col1 is blank so extract only col4.
            if "2m12m" in key:
                col1_val = None
                col4_token = coeff_tokens[4] if len(coeff_tokens) > 4 and coeff_tokens[4] != "" else None
                if col4_token:
                    col4_token = col4_token.replace("*", "")
                try:
                    col4_val = float(col4_token) if col4_token is not None else None
                except ValueError:
                    col4_val = None
            else:
                # Otherwise extract both column (1) and column (4).
                col1_token = coeff_tokens[1] if len(coeff_tokens) > 1 and coeff_tokens[1] != "" else None
                col4_token = coeff_tokens[4] if len(coeff_tokens) > 4 and coeff_tokens[4] != "" else None
                if col1_token:
                    col1_token = col1_token.replace("*", "")
                if col4_token:
                    col4_token = col4_token.replace("*", "")
                try:
                    col1_val = float(col1_token) if col1_token is not None else None
                except ValueError:
                    col1_val = None
                try:
                    col4_val = float(col4_token) if col4_token is not None else None
                except ValueError:
                    col4_val = None
            
            # Get the standard error row (assumed to be the very next line).
            std_err_val1 = None
            std_err_val4 = None
            if i + 1 < len(lines):
                err_line = lines[i + 1].strip()
                err_tokens = [t.replace("\\\\", "").strip() for t in err_line.split("&")]
                if "2m12m" in key:
                    if len(err_tokens) > 4 and err_tokens[4] != "":
                        err_token4 = err_tokens[4].replace("(", "").replace(")", "").replace("*", "").strip()
                    else:
                        err_token4 = None
                    try:
                        std_err_val4 = float(err_token4) if err_token4 is not None else None
                    except ValueError:
                        std_err_val4 = None
                else:
                    if len(err_tokens) > 1 and err_tokens[1] != "":
                        err_token1 = err_tokens[1].replace("(", "").replace(")", "").replace("*", "").strip()
                    else:
                        err_token1 = None
                    if len(err_tokens) > 4 and err_tokens[4] != "":
                        err_token4 = err_tokens[4].replace("(", "").replace(")", "").replace("*", "").strip()
                    else:
                        err_token4 = None
                    try:
                        std_err_val1 = float(err_token1) if err_token1 is not None else None
                    except ValueError:
                        std_err_val1 = None
                    try:
                        std_err_val4 = float(err_token4) if err_token4 is not None else None
                    except ValueError:
                        std_err_val4 = None
                i += 1  # Skip the error row.
            
            # Save into the appropriate dictionary.
            if line.startswith(valid_spis):
                spi_data[key] = {"coef": (col1_val, col4_val), "se": (std_err_val1, std_err_val4)}
            elif line.startswith(valid_temps):
                temp_data[key] = {"coef": (col1_val, col4_val), "se": (std_err_val1, std_err_val4)}
        i += 1

    # ----------------------------------------------------------------
    # Now reorder and compute CIs according to the file_type.
    if file_type == "default":
        # In default mode we expect keys like "..._avg_neg" or "..._avg_pos"
        def order_data(data, sign="neg"):
            ordered_coefs = []
            ordered_ses = []
            # For "inutero" and "30d": take col1 then col4.
            for key in data:
                if key.startswith(f"inutero_avg_{sign}") or key.startswith(f"30d_avg_{sign}"):
                    coef = data[key]["coef"]
                    se = data[key]["se"]
                    ordered_coefs.append(coef[0])
                    ordered_coefs.append(coef[1])
                    ordered_ses.append(se[0])
                    ordered_ses.append(se[1])
            # For "2m12m": take only the col4 value.
            key_def = f"2m12m_avg_{sign}"
            if key_def in data:
                coef = data[key_def]["coef"]
                se = data[key_def]["se"]
                ordered_coefs.append(coef[1])
                ordered_ses.append(se[1])
                
            # Reorder coefs
            ordered_coefs = [ordered_coefs[0], ordered_coefs[2], ordered_coefs[1], ordered_coefs[3], ordered_coefs[4]]
            ordered_ses = [ordered_ses[0], ordered_ses[2], ordered_ses[1], ordered_ses[3], ordered_ses[4]]

            return ordered_coefs, ordered_ses
        
        spi_neg_coefs, spi_neg_ses = order_data(spi_data, "neg")
        spi_pos_coefs, spi_pos_ses = order_data(spi_data, "pos")
        temp_neg_coefs, temp_neg_ses = order_data(temp_data, "neg")
        temp_pos_coefs, temp_pos_ses = order_data(temp_data, "pos")
        
        spi_neg_ci_lower, spi_neg_ci_upper = compute_ci(spi_neg_coefs, spi_neg_ses)
        spi_pos_ci_lower, spi_pos_ci_upper = compute_ci(spi_pos_coefs, spi_pos_ses)
        temp_neg_ci_lower, temp_neg_ci_upper = compute_ci(temp_neg_coefs, temp_neg_ses)
        temp_pos_ci_lower, temp_pos_ci_upper = compute_ci(temp_pos_coefs, temp_pos_ses)
        
        return {
            "spi_neg": {"coefs": spi_neg_coefs, "lower": spi_neg_ci_lower, "upper": spi_neg_ci_upper},
            "spi_pos": {"coefs": spi_pos_coefs, "lower": spi_pos_ci_lower, "upper": spi_pos_ci_upper},
            "temp_neg": {"coefs": temp_neg_coefs, "lower": temp_neg_ci_lower, "upper": temp_neg_ci_upper},
            "temp_pos": {"coefs": temp_pos_coefs, "lower": temp_pos_ci_lower, "upper": temp_pos_ci_upper}
        }
    
    elif file_type == "spline":
        # In spline mode we expect keys ending in one of four categories.
        categories = ["ltm1", "bt0m1", "bt01", "gt1"]
        
        def order_data_spline(data, cat):
            ordered_coefs = []
            ordered_ses = []
            # For "inutero" and "30d": take col1 then col4.
            for period in ["inutero", "30d"]:
                key = f"{period}_avg_{cat}"
                if key in data:
                    coef = data[key]["coef"]
                    se = data[key]["se"]
                    ordered_coefs.append(coef[0])
                    ordered_coefs.append(coef[1])
                    ordered_ses.append(se[0])
                    ordered_ses.append(se[1])
            # For "2m12m": take only col4.
            key = f"2m12m_avg_{cat}"
            if key in data:
                coef = data[key]["coef"]
                se = data[key]["se"]
                ordered_coefs.append(coef[1])
                ordered_ses.append(se[1])
            return ordered_coefs, ordered_ses
        
        spi_result = {}
        temp_result = {}
        for cat in categories:
            spi_coefs, spi_ses = order_data_spline(spi_data, cat)
            temp_coefs, temp_ses = order_data_spline(temp_data, cat)
            spi_ci_lower, spi_ci_upper = compute_ci(spi_coefs, spi_ses)
            temp_ci_lower, temp_ci_upper = compute_ci(temp_coefs, temp_ses)
            spi_result[cat] = {"coefs": spi_coefs, "lower": spi_ci_lower, "upper": spi_ci_upper}
            temp_result[cat] = {"coefs": temp_coefs, "lower": temp_ci_lower, "upper": temp_ci_upper}
        
        return {"spi": spi_result, "temp": temp_result}
    
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

def plot_regression_coefficients(all_values, all_ci_top, all_ci_bot, margin, colors=[], labels=[], plot="both", outpath=None, legend_cols=4):
    
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
    
    if plot == "only_spi":
        all_values = [x[5:] for x in all_values]
        all_ci_top = [x[5:] for x in all_ci_top]
        all_ci_bot = [x[5:] for x in all_ci_bot]
        
    elif plot == "only_temp":
        all_values = [x[:5] for x in all_values]
        all_ci_top = [x[:5] for x in all_ci_top]
        all_ci_bot = [x[:5] for x in all_ci_bot]    
    
    elif plot != "both":
        raise ValueError("Invalid value for 'plot'. Must be 'both', 'only_spi', or 'only_temp'.")

    x = range(1, len(all_values[0])+1)
    values_x = distribute_x_values(x, len(all_values), margin) 

    fig, ax = plt.subplots(1, 1, figsize=(6, 4))
    for data_set in range(len(all_values)):
        values = all_values[data_set]
        x = values_x[data_set]
        ci_top = all_ci_top[data_set]
        ci_bot = all_ci_bot[data_set]
        color = colors[data_set] if len(colors) > 0 else "black"
        label = labels[data_set] if len(labels) > 0 else None
        
        yerr = [list(np.array(values) - np.array(ci_bot)), # 'down' error
                list(np.array(ci_top) - np.array(values))]  # 'up' error

        # Plot error bars
        ax.errorbar(x, values, yerr=yerr, capsize=3, fmt="o", color=color, label=label)

    if len(labels) > 0:
        ax.legend(bbox_to_anchor=(0.47, -0.2), frameon=False, ncols=legend_cols, loc="upper center")
    
    ax.axhline(y=0, color="black", linestyle="--", dashes=(7, 7), linewidth =1)
    ax.set_xlim(0.5, len(all_values[0])+.5)
    
    # Set second level of labels (1 month and 2-12 months)
    if plot == "both":
        ax = add_whitespace_to_axis(ax, [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5])

        x_labels = ["1m", "2-12m", "1m", "2-12m", "2-12m", "1m", "2-12m", "1m", "2-12m", "2-12m",]
        ax2 = ax.secondary_xaxis('bottom')
        ax2.set_xticks(range(1, len(all_values[0])+1))
        ax2.set_xticklabels(x_labels)
        ax2.tick_params(axis="x", length=0)  # Remove tick marks
        ax3 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        
        ax3.spines["bottom"].set_position(("outward", 25))  # Move third x-axis further down
        ax3.set_xticks([1.5, 3.5, 5, 6.5, 8.5, 10],)
        ax3.set_xticklabels(["In-Utero", "1m", "2-12m", "In-Utero", "1m", "2-12m"])
        ax3.tick_params(axis="x", length=0)  # Remove tick marks

        ax4 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax4.spines["bottom"].set_position(("outward", 25))  # Move third x-axis further down
        ax4 = add_whitespace_to_axis(ax4, [2.5, 4.5, 5.5, 7.5, 9.5])

        # Set third level of labels (1 month and 2-12 months)
        ax5 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax5.spines["bottom"].set_position(("outward", 50))  # Move third x-axis further down
        ax5.set_xticks([3, 8],)
        ax5.set_xticklabels(["Temperature", "Precipitation"])
        ax5.tick_params(axis="x", length=0)  # Remove tick marks

        ax6 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax6.spines["bottom"].set_position(("outward", 50))  # Move third x-axis further down
        ax6 = add_whitespace_to_axis(ax6, [5.5])
        
    else:
        ax = add_whitespace_to_axis(ax, [1.5, 2.5, 3.5, 4.5])
        
        ax2 = ax.secondary_xaxis('bottom')
        ax2.set_xticklabels([])
        ax2.tick_params(axis="x", length=0)  # Remove tick marks
        
        ax3 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        
        ax3.set_xticks([1, 2, 3, 4, 5],)
        ax3.set_xticklabels(["In-Utero", "1m", "In-Utero", "1m", "2-12m"])
        ax3.tick_params(axis="x", length=0)  # Remove tick marks
        
        # Set second level of labels (1 month and 2-12 months)
        ax4 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax4.spines["bottom"].set_position(("outward", 25))  # Move third x-axis further down
        ax4.set_xticks([1.5, 4],)
        ax4.set_xticklabels(["1 month", "2-12 months"])
        ax4.tick_params(axis="x", length=0)  # Remove tick marks

        ax5 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax5.spines["bottom"].set_position(("outward", 25))  # Move third x-axis further down
        ax5 = add_whitespace_to_axis(ax5, [2.5])

    fig.savefig(outpath, bbox_inches='tight')

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

def plot_heterogeneity(f_name, var, folder, colors=[], labels=[]):
    
    files = os.listdir(rf"{OUTPUTS}\heterogeneity\{var}")
    files = [f for f in files if f_name in f]
    files = [f for f in files if "standard_fe.tex" in f]
    bands = [f.replace(f"{f_name}  - ", "").replace(" standard_fe.tex", "") for f in files] 
    
    signs = ["pos", "neg"]

    for sign in signs:
        valid_bands = []
        values = []
        lower = []
        upper = []

        for i, band in enumerate(bands):

            file_path = rf"{OUTPUTS}\heterogeneity\\{var}\{files[i]}"
            n = extract_sample_size(file_path)

            if n < 100_000:
                continue
            print(band)
            
            outdata = extract_coefficients_and_CI_latex(file_path)

            values += [outdata[f"temp_{sign}"]["coefs"] + outdata[f"spi_{sign}"]["coefs"]]
            lower  += [outdata[f"temp_{sign}"]["lower"] + outdata[f"spi_{sign}"]["lower"]]
            upper  += [outdata[f"temp_{sign}"]["upper"] + outdata[f"spi_{sign}"]["upper"]]
            valid_bands += [band]

        if len(colors)>0:
            if (len(colors)!=len(valid_bands)) or (len(labels)!=len(valid_bands)):
                raise ValueError(f"Number of colors and labels must match the number of valid_bands: {colors} {valid_bands}")
        
        plot_regression_coefficients(
            values, 
            upper,
            lower,
            margin=0.1,
            colors=colors, 
            labels=labels, 
            plot="only_temp", 
            outpath=rf"{folder}\heterogeneity - {var} - temp {f_name} {sign}.png"
        )

        plot_regression_coefficients(
            values, 
            upper,
            lower,
            margin=0.1,
            colors=colors, 
            labels=labels, 
            plot="only_spi", 
            outpath=rf"{folder}\heterogeneity - {var} - spi {f_name} {sign}.png"
        )        

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

