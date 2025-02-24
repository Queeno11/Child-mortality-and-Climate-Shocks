import matplotlib.pyplot as plt
import numpy as np

def extract_coefficients_and_CI_latex(file_path):
    """
    Opens a LaTeX file containing a tabular environment, extracts rows for variables 
    starting with "spi1" or "stdm_t", and collects:
      - Coefficients from column (1) (token index 1) and column (4) (token index 4)
      - Standard errors from the row immediately following each coefficient row,
        from the corresponding columns.
      
    The function then computes 95% confidence intervals (lower and upper bounds) 
    using the formula:
          lower = coefficient - 1.96 * standard_error
          upper = coefficient + 1.96 * standard_error
    This is typical for fixed effects models.

    The ordering for each group is as follows: first, all the column (1) values are listed,
    then all the column (4) values.

    Returns:
        spi1_ordered_coeffs: list of coefficients for "spi1", first all from col1 then col4.
        spi1_CI_lower: list of lower bounds for the 95% CI (same order as above).
        spi1_CI_upper: list of upper bounds for the 95% CI (same order as above).
        stdm_t_ordered_coeffs: list of coefficients for "stdm_t", first all from col1 then col4.
        stdm_t_CI_lower: list of lower bounds for the 95% CI (same order as above).
        stdm_t_CI_upper: list of upper bounds for the 95% CI (same order as above).
    """
    # Lists to store tuples (col1, col4) for coefficients and standard errors respectively.
    spi1_coeffs = []
    spi1_errors = []
    stdm_t_coeffs = []
    stdm_t_errors = []
    
    with open(file_path, "r") as file:
        lines = file.readlines()
    
    # Iterate over lines by index so we can access the standard error row following a coefficient row.
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Skip empty lines or lines that don't look like data rows
        if not line or line.startswith("&"):
            i += 1
            continue
        
        # Check if this line is a coefficient row (starts with "spi1" or "stdm")
        if line.startswith("spi1") or line.startswith("stdm"):
            # Split the coefficient row by the ampersand and clean tokens.
            coeff_tokens = [t.replace("\\\\", "").strip() for t in line.split("&")]
            # Extract coefficient tokens from column (1) (index 1) and column (4) (index 4)
            col1_token = coeff_tokens[1] if len(coeff_tokens) > 1 and coeff_tokens[1] != "" else None
            col4_token = coeff_tokens[4] if len(coeff_tokens) > 4 and coeff_tokens[4] != "" else None
            
            # Remove any asterisks (e.g., significance markers)
            if col1_token:
                col1_token = col1_token.replace("*", "")
            if col4_token:
                col4_token = col4_token.replace("*", "")
            
            # Convert tokens to floats if possible.
            try:
                col1_val = float(col1_token) if col1_token is not None else None
            except ValueError:
                col1_val = None
            try:
                col4_val = float(col4_token) if col4_token is not None else None
            except ValueError:
                col4_val = None

            # Next, get the standard error row (assumed to be the next line).
            std_err_val1 = None
            std_err_val4 = None
            if i + 1 < len(lines):
                err_line = lines[i + 1].strip()
                err_tokens = [t.replace("\\\\", "").strip() for t in err_line.split("&")]
                # Standard errors are typically enclosed in parentheses, so remove them.
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
                # Skip the error row.
                i += 1

            # Append the extracted coefficient and error values as tuples.
            if line.startswith("spi1"):
                spi1_coeffs.append((col1_val, col4_val))
                spi1_errors.append((std_err_val1, std_err_val4))
            elif line.startswith("stdm"):
                stdm_t_coeffs.append((col1_val, col4_val))
                stdm_t_errors.append((std_err_val1, std_err_val4))
        
        i += 1

    # Order the coefficients: first all the column (1) values, then all the column (4) values.
    spi1_col1 = [t[0] for t in spi1_coeffs]
    spi1_col4 = [t[1] for t in spi1_coeffs]
    spi1_ordered_coeffs = spi1_col1 + spi1_col4

    stdm_t_col1 = [t[0] for t in stdm_t_coeffs]
    stdm_t_col4 = [t[1] for t in stdm_t_coeffs]
    stdm_t_ordered_coeffs = stdm_t_col1 + stdm_t_col4

    # Order the standard errors in the same way.
    spi1_err_col1 = [t[0] for t in spi1_errors]
    spi1_err_col4 = [t[1] for t in spi1_errors]
    spi1_ordered_errors = spi1_err_col1 + spi1_err_col4

    stdm_t_err_col1 = [t[0] for t in stdm_t_errors]
    stdm_t_err_col4 = [t[1] for t in stdm_t_errors]
    stdm_t_ordered_errors = stdm_t_err_col1 + stdm_t_err_col4

    # Compute the 95% CI lower and upper bounds using a multiplier of 1.96.
    # (If coefficient or error is None, then the CI bound will be None.)
    def compute_ci(coefs, errors):
        lower_bounds = []
        upper_bounds = []
        for coef, err in zip(coefs, errors):
            if coef is not None and err is not None:
                lower_bounds.append(coef - 1.96 * err)
                upper_bounds.append(coef + 1.96 * err)
            else:
                lower_bounds.append(None)
                upper_bounds.append(None)
        return lower_bounds, upper_bounds

    spi1_CI_lower, spi1_CI_upper = compute_ci(spi1_ordered_coeffs, spi1_ordered_errors)
    stdm_t_CI_lower, stdm_t_CI_upper = compute_ci(stdm_t_ordered_coeffs, stdm_t_ordered_errors)
    
    return (spi1_ordered_coeffs, spi1_CI_lower, spi1_CI_upper,
            stdm_t_ordered_coeffs, stdm_t_CI_lower, stdm_t_CI_upper)


def split_negative_and_positive_values(shock_values):   
    ''' Splits the series of shock values into two lists: one for negative values and one for positive values.
    
    Negative values are stored in odd indices, while positive values are stored in even indices.
    '''
    
    negative_values = shock_values[1::2]
    positive_values = shock_values[::2]
    
    negative_values = [x for x in negative_values if x is not None]
    positive_values = [x for x in positive_values if x is not None]
    
    return negative_values, positive_values

def interleave_temp_and_spi(temp_values, spi_values):
    ''' Interleaves the temperature and SPI values into a single list.
    
    The list is ordered as follows: temp, SPI, temp, SPI, ...
    '''
    
    interleaved_values = [None] * (len(temp_values) + len(spi_values))
    interleaved_values[::2] = temp_values
    interleaved_values[1::2] = spi_values
    
    return interleaved_values


def prepare_data_for_plot(spi, temp):
    spi_neg, spi_pos = split_negative_and_positive_values(spi)
    temp_neg, temp_pos = split_negative_and_positive_values(temp)
    data_neg = interleave_temp_and_spi(temp_neg, spi_neg)
    data_pos = interleave_temp_and_spi(temp_pos, spi_pos)

    return data_neg, data_pos

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

def plot_regression_coefficients(all_values, all_ci_top, all_ci_bot, margin, colors=[], labels=[], plot="both", outpath=None):
    
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
        all_values = [x[1::2] for x in all_values]
        all_ci_top = [x[1::2] for x in all_ci_top]
        all_ci_bot = [x[1::2] for x in all_ci_bot]    
        
    elif plot == "only_temp":
        all_values = [x[::2] for x in all_values]
        all_ci_top = [x[::2] for x in all_ci_top]
        all_ci_bot = [x[::2] for x in all_ci_bot]
    
    elif plot != "both":
        raise ValueError("Invalid value for 'plot'. Must be 'both', 'only_spi', or 'only_temp'.")

    x = range(1, len(all_values[0])+1)
    values_x = distribute_x_values(x, len(all_values), margin) 

    fig, ax = plt.subplots(figsize=(8, 6))
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
        ax.legend(bbox_to_anchor=(1.01, 0.5), frameon=False)

    
    ax.axhline(y=0, color="black", linestyle="--", dashes=(7, 7), linewidth =1)
    ax.set_xlim(0.5, len(all_values[0])+.5)
    
    # Set second level of labels (1 month and 2-12 months)
    if plot == "both":
        ax = add_whitespace_to_axis(ax, [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5])

        x_labels = ["Temp", "SPI", "Temp", "SPI", "Temp", "SPI", "Temp", "SPI", "Temp", "SPI"]
        ax2 = ax.secondary_xaxis('bottom')
        ax2.set_xticks(range(1, len(all_values[0])+1))
        ax2.set_xticklabels(x_labels)
        ax2.tick_params(axis="x", length=0)  # Remove tick marks
        ax3 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        
        ax3.spines["bottom"].set_position(("outward", 25))  # Move third x-axis further down
        ax3.set_xticks([1.5, 3.5, 5.5, 7.5, 9.5],)
        ax3.set_xticklabels(["In-Utero", "1m", "In-Utero", "1m", "2m-12m"])
        ax3.tick_params(axis="x", length=0)  # Remove tick marks

        ax4 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax4.spines["bottom"].set_position(("outward", 25))  # Move third x-axis further down
        ax4 = add_whitespace_to_axis(ax4, [2.5, 4.5, 6.5, 8.5])

        # Set third level of labels (1 month and 2-12 months)
        ax5 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax5.spines["bottom"].set_position(("outward", 50))  # Move third x-axis further down
        ax5.set_xticks([2.5, 7.5],)
        ax5.set_xticklabels(["1 month", "2-12 months"])
        ax5.tick_params(axis="x", length=0)  # Remove tick marks

        ax6 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax6.spines["bottom"].set_position(("outward", 50))  # Move third x-axis further down
        ax6 = add_whitespace_to_axis(ax6, [4.5])
    else:
        ax = add_whitespace_to_axis(ax, [1.5, 2.5, 3.5, 4.5])
        
        ax2 = ax.secondary_xaxis('bottom')
        ax2.set_xticklabels([])
        ax2.tick_params(axis="x", length=0)  # Remove tick marks
        
        ax3 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        
        ax3.set_xticks([1, 2, 3, 4, 5],)
        ax3.set_xticklabels(["In-Utero", "1m", "In-Utero", "1m", "2m-12m"])
        ax3.tick_params(axis="x", length=0)  # Remove tick marks
        
        # Set second level of labels (1 month and 2-12 months)
        ax4 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax4.spines["bottom"].set_position(("outward", 50))  # Move third x-axis further down
        ax4.set_xticks([1.5, 4],)
        ax4.set_xticklabels(["1 month", "2-12 months"])
        ax4.tick_params(axis="x", length=0)  # Remove tick marks

        ax5 = ax.secondary_xaxis('bottom')  # Add another secondary x-axis
        ax5.spines["bottom"].set_position(("outward", 50))  # Move third x-axis further down
        ax5 = add_whitespace_to_axis(ax5, [2.5])

    fig.savefig(outpath, bbox_inches='tight', pad_inches=1)


