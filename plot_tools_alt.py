import re
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

OUTPUTS = r"C:\Working Papers\Paper - Child Mortality and Climate Shocks\Outputs"

# =============================================================================
# --- CONFIGURATIONS FOR "BY MODEL" PLOTTING (Unchanged) ---
# =============================================================================

# Each subplot is a MODEL (dependent variable). The x-axis shows the lags (independent variables).
SEMESTER_CONFIG = {
    "canvas_size": (2, 3),
    # The dependent variables that define each subplot
    "dep_vars": ["born_1m6m", "born_6m12m", "born_12m18m", "born_18m24m", "born_24m30m", "born_30m36m"],
    # The independent variables (lags) that will appear on the x-axis
    "indep_vars": ["inutero", "born_1m6m", "born_6m12m", "born_12m18m", "born_18m24m", "born_24m30m", "born_30m36m"],
    # Labels for the subplots
    "title_labels": {
        "born_1m6m": "Dep. Var: Mort. 0-6m", "born_6m12m": "Dep. Var: Mort. 6-12m",
        "born_12m18m": "Dep. Var: Mort. 12-18m", "born_18m24m": "Dep. Var: Mort. 18-24m",
        "born_24m30m": "Dep. Var: Mort. 24-30m", "born_30m36m": "Dep. Var: Mort. 30-36m"
    },
    # Labels for the x-axis ticks
    "x_tick_labels": ["In-Utero", "0-6m", "6-12m", "12-18m", "18-24m", "24-30m", "30-36m"],
    "ylim": (-1.5, 3.0),
    "legend_pos": {"loc": 'lower center', "bbox_to_anchor": (0.5, -0.4), "ncol": 2}
}

QUARTERLY_CONFIG = {
    "canvas_size": (2, 4),
    "dep_vars": ["inutero_1m3m", "inutero_3m6m", "inutero_6m9m", "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m"],
    "indep_vars": ["inutero_1m3m", "inutero_3m6m", "inutero_6m9m", "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m"],
    "title_labels": {
        "inutero_1m3m": "1st IU Qtr", "inutero_3m6m": "2nd IU Qtr", "inutero_6m9m": "3rd IU Qtr",
        "born_1m3m": "Mort. 0-3m", "born_3m6m": "Mort. 3-6m", "born_6m9m": "Mort. 6-9m", "born_9m12m": "Mort. 9-12m",
    },
    "x_tick_labels": ["IU Q1", "IU Q2", "IU Q3", "0-3m", "3-6m", "6-9m", "9-12m"],
    "ylim": (-0.8, 1.5),
    "legend_pos": {"loc": 'lower center', "bbox_to_anchor": (-1.35, -0.3), "ncol": 2}
}

HORSERACE_CONFIG = QUARTERLY_CONFIG.copy()
HORSERACE_CONFIG["ylim"] = (-0.6, 0.8)
HORSERACE_CONFIG["legend_pos"] = {"loc": 'lower center', "bbox_to_anchor": (-1.35, -0.35), "ncol": 2}

HETEROGENEITY_CONFIG = QUARTERLY_CONFIG.copy()
HETEROGENEITY_CONFIG["legend_pos"] = {"loc": 'lower center', "bbox_to_anchor": (-1.35, -0.25), "ncol": 6}


# =============================================================================
# --- DATA EXTRACTION & HELPER FUNCTIONS (Unchanged) ---
# =============================================================================

def remove_words_from_string(long_string, words):
    for word in words:
        long_string = long_string.replace(word, "")
    return long_string.strip()

def contains_any_string(main_string, strings_list):
    return any(sub in main_string for sub in strings_list)

def to_float(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return np.nan

def compute_ci(coefs, ses):
    lower = coefs - 2.042 * ses # 95% confidence interval with t(30df)
    upper = coefs + 2.042 * ses
    return lower, upper

def order_files_naturally(file_list):
    def natural_sort_key(s):
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s) if text]
    return sorted(file_list, key=natural_sort_key)

def fix_extreme_temperatures_strings(s):
    if "hd" in s:
        s = s + "_pos_int"
    elif "fd" in s or "id" in s:
        s = s + "_neg_int"
    for prefix in ["hd35_", "hd40_", "fd_", "id_"]:
        s = s.replace(prefix, "t_")
    return s

def highlight_significant_points(ax, xvalues, coefs, lower, upper, **kwargs):
    significant = (lower > 0) | (upper < 0)
    if np.any(significant):
        ax.scatter(xvalues[significant], coefs[significant],
                   marker='o', edgecolor='k', linewidth=1.5, zorder=5, **kwargs)

def distribute_x_values(x_center, n_series, width=0.8):
    offsets = np.linspace(-width / 2, width / 2, n_series)
    # If only one series, place it at the center
    if n_series == 1:
        offsets = [0]
    return [x_center + offset for offset in offsets]

def extract_coefficients_and_CI_latex(file_path, horserace=None):
    results = {}
    valid_standard_temps = ("stdm_t_", "absdifm_t_", "absdif_t_", "std_t_", "t_")
    valid_extreme_temps = ("hd35_", "hd40_", "fd_", "id_")
    
    if horserace == "extremes":
        valid_temps = valid_extreme_temps
    elif horserace == "standard":
        valid_temps = valid_standard_temps
    else:
        valid_temps = valid_standard_temps + valid_extreme_temps

    all_temps = valid_standard_temps + valid_extreme_temps
    valid_spis = tuple([f"spi{m}_" for m in [1, 3, 6, 9, 12, 24, 48]])
    
    spi_data = {"cell1": {}, "cell2": {}, "cell3": {}}
    temp_data = {"cell1": {}, "cell2": {}, "cell3": {}}

    with open(file_path, "r", encoding='utf-8') as file:
        lines = file.readlines()

    for i, line in enumerate(lines):
        line = line.strip().replace(r"\\", "").replace(r"\_", "_")
        
        if not (line.startswith(valid_spis) or line.startswith(valid_temps)):
            continue

        tokens = line.split("&")
        err_tokens = lines[i + 1].strip().replace("(", "").replace(")", "").split("&")
        
        full_key = tokens[0].strip()
        full_key = fix_extreme_temperatures_strings(full_key)
        
        key = remove_words_from_string(full_key, valid_spis)
        key = remove_words_from_string(key, all_temps).lstrip('_')

        for cell_idx, cell_name in enumerate(["cell1", "cell2", "cell3"]):
            coefs = np.array([to_float(c.replace("*", "")) for c in tokens[cell_idx + 1::3]])
            ses = np.array([to_float(e) for e in err_tokens[cell_idx + 1::3]])
            
            lower, upper = compute_ci(coefs, ses)
            
            data_dict = {"coef": coefs, "se": ses, "lower": lower, "upper": upper}

            if contains_any_string(full_key, valid_spis):
                spi_data[cell_name][key] = data_dict
            elif contains_any_string(full_key, all_temps):
                temp_data[cell_name][key] = data_dict

    results["spi"] = spi_data
    results["temp"] = temp_data
    return results

def extract_coefficients_and_CI_latex_horserace(file_path):
    standards = extract_coefficients_and_CI_latex(file_path, horserace="standard")
    extremes = extract_coefficients_and_CI_latex(file_path, horserace="extremes")
    return {"standard": standards, "extreme": extremes}

def extract_coefficients_and_CI_latex_heterogeneity(heterogeneity, shock, spi, temp, stat, timeframe):
    folder = os.path.join(OUTPUTS, "heterogeneity", heterogeneity)
    if not os.path.exists(folder): return {}
    
    f_name_part1 = f"linear_dummies_true_{spi}_{stat}_{temp} {timeframe} - "
    f_name_part2 = " standard_fe standard_sym.tex" # Added space at start
    
    plotdata = {}
    for f in os.listdir(folder):
        if f.startswith(f_name_part1) and f.endswith(f_name_part2):
            band = f.replace(f_name_part1, "").replace(f_name_part2, "").strip() # Use strip
            file_path = os.path.join(folder, f)
            outdata = extract_coefficients_and_CI_latex(file_path)
            
            for key, values in outdata.get(shock, {}).get("cell1", {}).items():
                if key not in plotdata:
                    plotdata[key] = {}
                plotdata[key][band] = values
    return plotdata


# =============================================================================
# --- CORRECTED PLOTTING FUNCTIONS ---
# =============================================================================

def plot_coefficients_by_model(
        data, shock, spi, temp, stat,
        dep_vars, indep_vars, title_labels, x_tick_labels, canvas_size,
        ylim=(-1.5, 3.0), legend_pos=None,
        colors=None, labels=None, outpath=None, start="", extra="",
    ):
    """
    Plots coefficients where each subplot represents a single regression model.
    The x-axis in each subplot shows the coefficients of all lagged variables.
    """
    data_to_plot = data.get(shock, {}).get("cell1", {})
    if not data_to_plot:
        print(f"Warning: No data found for shock '{shock}'. Skipping plot.")
        return

    labels = labels or [f"Low {shock} shocks (inverted)", f"High {shock} shocks"]
    colors = colors or ["#ff5100", "#3e9fe1"] # Red/Orange for Low, Blue for High
    
    n_rows, n_cols = canvas_size
    fig, axs = plt.subplots(n_rows, n_cols, figsize=(5 * n_cols, 4 * n_rows), squeeze=False, sharey=True, sharex=True)
    flat_axs = axs.flatten()

    num_models = len(dep_vars)

    for model_idx, ax in enumerate(flat_axs):
        if model_idx >= num_models:
            ax.axis('off')
            continue

        # --- THIS IS THE FIX ---
        # For the i-th regression, include shocks from period 0 up to and including the contemporaneous period.
        # The slice indep_vars[:model_idx + 2] correctly includes the contemporaneous shock.
        vars_in_this_regression = indep_vars[:model_idx + 2]
        # --- END FIX ---
        
        coefs_pos, lower_pos, upper_pos = [], [], []
        coefs_neg, lower_neg, upper_neg = [], [], []

        for var_key in vars_in_this_regression:
            data_pos = data_to_plot.get(f"{var_key}_{stat}_pos_int")
            coefs_pos.append(data_pos['coef'][model_idx] if data_pos is not None and model_idx < len(data_pos['coef']) else np.nan)
            lower_pos.append(data_pos['lower'][model_idx] if data_pos is not None and model_idx < len(data_pos['lower']) else np.nan)
            upper_pos.append(data_pos['upper'][model_idx] if data_pos is not None and model_idx < len(data_pos['upper']) else np.nan)

            data_neg = data_to_plot.get(f"{var_key}_{stat}_neg_int")
            coefs_neg.append(-1 * data_neg['coef'][model_idx] if data_neg is not None and model_idx < len(data_neg['coef']) else np.nan)
            lower_neg.append(-1 * data_neg['upper'][model_idx] if data_neg is not None and model_idx < len(data_neg['upper']) else np.nan)
            upper_neg.append(-1 * data_neg['lower'][model_idx] if data_neg is not None and model_idx < len(data_neg['lower']) else np.nan)
        
        x_base = np.arange(len(vars_in_this_regression))
        x_neg_vals, x_pos_vals = distribute_x_values(x_base, 2, width=0.6)

        yerr_neg = [np.array(coefs_neg) - np.array(lower_neg), np.array(upper_neg) - np.array(coefs_neg)]
        ax.errorbar(x_neg_vals, coefs_neg, yerr=yerr_neg, capsize=3, fmt="o", color=colors[0], label=labels[0])
        highlight_significant_points(ax, np.array(x_neg_vals), np.array(coefs_neg), np.array(lower_neg), np.array(upper_neg), s=80, color=colors[0])

        yerr_pos = [np.array(coefs_pos) - np.array(lower_pos), np.array(upper_pos) - np.array(coefs_pos)]
        ax.errorbar(x_pos_vals, coefs_pos, yerr=yerr_pos, capsize=3, fmt="o", color=colors[1], label=labels[1])
        highlight_significant_points(ax, np.array(x_pos_vals), np.array(coefs_pos), np.array(lower_pos), np.array(upper_pos), s=80, color=colors[1])
        
        ax.axhline(0, color='black', linestyle='--', linewidth=0.8)
        ax.set_title(title_labels.get(dep_vars[model_idx], dep_vars[model_idx]))
        ax.set_xticks(x_base, labels=x_tick_labels[:len(vars_in_this_regression)], rotation=30, ha='right')
        ax.spines[['top', 'right']].set_visible(False)
        ax.set_xlim(-0.5, len(vars_in_this_regression) - 0.5)
        if ylim: ax.set_ylim(ylim)

    fig.supxlabel("Shock Timing (Lag)", y=0.01, fontsize=12)
    fig.supylabel("Coefficient Estimate", x=0.01, fontsize=12)
    fig.tight_layout(rect=[0.03, 0.03, 1, 1])
    
    handles, plot_labels = ax.get_legend_handles_labels()
    fig.legend(handles, plot_labels, **legend_pos, frameon=False)
    
    os.makedirs(outpath, exist_ok=True)
    filename = os.path.join(outpath, f"{start}{shock}_coefficients_by_model_{spi}_{stat}_{temp}{extra}.png")
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Figure saved: {filename}")
    plt.close()


def plot_heterogeneity_by_model(
        heterogeneity, spi, temp, stat, timeframe,
        dep_vars, indep_vars, title_labels, x_tick_labels, canvas_size,
        ylim=(-1.5, 3.0), legend_pos=None,
        colors=None, outpath=None, start="", extra=""
    ):
    """
    Plots heterogeneity results where each subplot is a regression model.
    """
    for shock in ["temp", "spi"]:
        for sign in ["_pos", "_neg"]:
            full_data = extract_coefficients_and_CI_latex_heterogeneity(
                heterogeneity, shock, spi, temp, stat, timeframe
            )
            if not full_data: continue

            data_sign_filtered = {k: v for k, v in full_data.items() if sign in k}
            if not data_sign_filtered: continue

            groups = sorted(list(list(data_sign_filtered.values())[0].keys()))
            n_groups = len(groups)
            group_colors = colors or plt.cm.viridis(np.linspace(0, 1, n_groups))

            n_rows, n_cols = canvas_size
            fig, axs = plt.subplots(n_rows, n_cols, figsize=(5 * n_cols, 4 * n_rows), squeeze=False, sharey=True)
            flat_axs = axs.flatten()
            num_models = len(dep_vars)

            for model_idx, ax in enumerate(flat_axs):
                if model_idx >= num_models:
                    ax.axis('off')
                    continue

                # --- THIS IS THE FIX ---
                vars_in_this_regression = indep_vars[:model_idx + 2]
                # --- END FIX ---
                
                x_base = np.arange(len(vars_in_this_regression))

                for j, var_key in enumerate(vars_in_this_regression):
                    data_key = f"{var_key}_{stat}{sign}_int"
                    if data_key in data_sign_filtered:
                        x_group_vals = distribute_x_values(x_base[j], n_groups, width=0.8)
                        
                        for group_idx, group_name in enumerate(groups):
                            group_data = data_sign_filtered[data_key].get(group_name)
                            if group_data and model_idx < len(group_data['coef']):
                                coef = group_data['coef'][model_idx]
                                lower, upper = group_data['lower'][model_idx], group_data['upper'][model_idx]
                                
                                if sign == "_neg":
                                    coef, lower, upper = -coef, -upper, -lower

                                yerr = [[coef - lower], [upper - coef]]
                                ax.errorbar(x_group_vals[group_idx], coef, yerr=yerr, capsize=3, fmt="o",
                                            color=group_colors[group_idx], label=group_name if j == 0 and model_idx == 0 else "")

                ax.axhline(0, color='black', linestyle='--', linewidth=0.8)
                ax.set_title(title_labels.get(dep_vars[model_idx], dep_vars[model_idx]))
                ax.set_xticks(x_base, labels=x_tick_labels[:len(vars_in_this_regression)], rotation=30, ha='right')
                ax.spines[['top', 'right']].set_visible(False)
                ax.set_xlim(-0.5, len(vars_in_this_regression) - 0.5)
                if ylim: ax.set_ylim(ylim)

            fig.supxlabel("Shock Timing (Lag)", y=0.01, fontsize=12)
            fig.supylabel("Coefficient Estimate", x=0.01, fontsize=12)
            fig.suptitle(f"Heterogeneity by '{heterogeneity}' for {shock}{sign} shocks", fontsize=14)
            fig.tight_layout(rect=[0.03, 0.03, 1, 0.95])
            
            handles, plot_labels = fig.get_legend_handles_labels() # Use fig. instead of ax.
            fig.legend(handles, plot_labels, **legend_pos, frameon=False)
            
            os.makedirs(outpath, exist_ok=True)
            filename = os.path.join(outpath, f"heterogeneity_{heterogeneity}_{shock}{sign}_{spi}_{stat}{extra}.png")
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"Figure saved: {filename}")
            plt.close()

# =============================================================================
# --- EXAMPLE USAGE (Unchanged) ---
# =============================================================================

if __name__ == '__main__':
    # This block demonstrates how to call the corrected functions.
    
    # --- 1. Standard Plot ---
    print("Generating standard plot by model...")
    file_path_standard = os.path.join(OUTPUTS, "linear_dummies_true_spi1_b_avg_stdm_t semester standard_fe standard_sym.tex")
    if os.path.exists(file_path_standard):
        data_standard = extract_coefficients_and_CI_latex(file_path_standard)
        
        plot_coefficients_by_model(
            data=data_standard, shock="temp", spi="spi1", temp="stdm_t", stat="b_avg",
            outpath=os.path.join(OUTPUTS, "new_plots"), **SEMESTER_CONFIG
        )
        plot_coefficients_by_model(
            data=data_standard, shock="spi", spi="spi1", temp="stdm_t", stat="b_avg",
            outpath=os.path.join(OUTPUTS, "new_plots"), **SEMESTER_CONFIG
        )
    else:
        print(f"Standard file not found: {file_path_standard}")

    # --- 2. Heterogeneity Plot ---
    print("\nGenerating heterogeneity plot by model...")
    plot_heterogeneity_by_model(
        heterogeneity='climate_band_1', spi='spi1', temp='stdm_t', stat='q_avg', timeframe='quarterly',
        outpath=os.path.join(OUTPUTS, "new_plots", "heterogeneity"),
        colors=["#2ca02c", "#d62728", "#1f77b4"], **HETEROGENEITY_CONFIG
    )
    plot_heterogeneity_by_model(
        heterogeneity='rural', spi='spi1', temp='stdm_t', stat='q_avg', timeframe='quarterly',
        outpath=os.path.join(OUTPUTS, "new_plots", "heterogeneity"),
        colors=["#8c564b", "#9467bd"], **HETEROGENEITY_CONFIG
    )