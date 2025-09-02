module CustomModels

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, Arrow, Tables
    using StatsModels: termvars

    
    """
        get_required_vars(tbl, temp, drought, stat, controls)

    Identifies all column names required for a specific model configuration.

    The function scans the column names in the source table `tbl` and selects those
    that contain both the `temp` and `stat` substrings, and those that contain
    both the `drought` and `stat` substrings. It then combines these with the
    column names from the `controls` vector.

    # Arguments
    - `tbl`: The source data table (e.g., an `Arrow.Table` or `DataFrame`).
    - `temp::AbstractString`: The substring to identify temperature variables (e.g., "stdm_t").
    - `drought::AbstractString`: The substring to identify drought variables (e.g., "spi").
    - `stat::AbstractString`: The substring to identify the statistic (e.g., "avg", "inutero").
    - `controls::Vector{Term}`: A vector of `Term` objects representing the control variables.

    # Returns
    - `Vector{Symbol}`: A vector of unique `Symbol`s representing all required column names.
    """
    function get_required_vars(df,
                            temp::AbstractString,
                            drought::AbstractString,
                            stat::AbstractString,
                            controls::Vector{Term})

        #     fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
        # elseif fixed_effects == "quadratic_time"
        #     fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & term(:chb_year_sq) + fe(Symbol("ID_cell$i")) & fe(:chb_month)


        # 1. Convert the input `controls` from Vector{Term} to Vector{Symbol}.
        ctrl_syms = getproperty.(controls, :sym)

        # 2. Pull all column names as Symbols from the source table.
        all_cols = Tables.columnnames(df)

        # 3. Helper to test if a name contains both substrings.
        function has_both(a::AbstractString, b::AbstractString, c::AbstractString)
            return occursin(a, c) && occursin(b, c)
        end

        # 4. Find matching column names (they are already Symbols).
        temp_stat_cols    = filter(c -> has_both("$(temp)_",     "$(stat)",  String(c)), all_cols)
        temphd_stat_cols  = filter(c -> has_both("hd",           "$(stat)",  String(c)), all_cols)
        tempfd_stat_cols  = filter(c -> has_both("fd",           "$(stat)",  String(c)), all_cols)
        tempid_stat_cols  = filter(c -> has_both("id",           "$(stat)",  String(c)), all_cols)
        drought_stat_cols = filter(c -> has_both("$(drought)_",  "$(stat)",  String(c)), all_cols)
        cell_cols         = filter(c -> occursin("ID_cell",                  String(c)), all_cols)
        chb_cols          = filter(c -> occursin("chb_",                     String(c)), all_cols)
        agedeath_cols     = filter(c -> occursin("child_agedeath",           String(c)), all_cols)

        # 5. Merge the lists of Symbols and return the unique set.
        return unique(vcat(ctrl_syms, temp_stat_cols, temphd_stat_cols, tempfd_stat_cols, tempid_stat_cols, 
                       drought_stat_cols, cell_cols, chb_cols, agedeath_cols)) 
    end

    function load_dataset(df_lazy, temp, drought, stat, controls, name; verbose=false, filter_on::Union{Nothing, Pair{Symbol, T}}=nothing) where T
        println("Loading dataset... ")

        variables = get_required_vars(df_lazy, temp, drought, stat, controls)
        
        # Ensure the filter column is included if not already present
        if !isnothing(filter_on) && !in(filter_on.first, variables)
            push!(variables, filter_on.first)
        end

        # Select the required columns from the DataFrame
        df_selected = select(df_lazy, variables)
        
        # Apply the row filter if specified.
        if !isnothing(filter_on)
            println("Applying filter: $(filter_on.first) == $(filter_on.second)")
            # Use the non-copying filter! for efficiency before the final copy
            filter!(row -> !ismissing(row[filter_on.first]) && row[filter_on.first] == filter_on.second, df_selected)
        end

        # Filter based on names
        if name == "6m windows"
            # Keep only childs alive at 6th month
            df_selected = filter(row -> ismissing(row.child_agedeath) || row.child_agedeath >= 5, df_selected)
        end
        if name == "12m windows"
            # Keep only childs alive at 6th month
            df_selected = filter(row -> ismissing(row.child_agedeath) || row.child_agedeath >= 11, df_selected)
        end

        df = copy(df_selected)
        print("Dataset cargado!")
        if verbose
            println("Columns successfully loaded into DataFrame:")
            println(names(df))
        end

        return df
    end

    """
        run_regression(df, controls, times, folder, extra; model_type="linear", with_dummies=false)
    
    Runs the specified regression models on the provided DataFrame `df` for different time periods. 
    This function supports multiple model types, including linear, quadratic, and quadratic with dummy variables.
    
    Results in stata cannot be directly replicated because reghdfe creates one variable for each ID_cell, which makes it
    imposible to fit in a RAM. Nevertheless, this function replicates a model such as:
        
        reghdfe child_agedeath_30d spi1_inutero_avg_neg spi1_inutero_avg_pos spi1_30d_avg_neg spi1_30d_avg_pos 
                                   stdm_t_inutero_avg_neg stdm_t_inutero_avg_pos stdm_t_30d_avg_neg stdm_t_30d_avg_pos 
                                   {controls} i.ID_cell1#c.chb_year, 
                                   absorb(ID_cell1#chb_month ID_cell1) vce(cluster ID_cell1) nocons

    the term i.ID_cell1#c.chb_year adds a linear trend on every fixed effect, and the term absorb(ID_cell1#chb_month ID_cell1) 
    adds the fixed effects for each cell and cell-month interaction. 

    # Arguments
    - `df`: DataFrame containing the data.
    - `controls`: Array of control variables for the regression.
    - `times`: Array of time periods for the regression.
    - `folder`: Output folder path to save the regression results.
    - `extra`: Additional string to append to the output filenames.
    - `model_type`: Type of regression model to run. Options are `"linear"` (default) or `"quadratic"`.
    - `with_dummies`: Boolean indicating whether to include dummy variables for positive and negative deviations (default is `false`).
    
    # Output
    Saves regression results in both ASCII and LaTeX formats to the specified folder.
    """
    function stepped_regression(df, temp, drought_ind, controls,
        times, stat, sp_threshold, folder, extra;
        model_type      = "linear",
        with_dummies    = false,
        fixed_effects   = "standard",
        symbols         = "standard",
        cells           = [1,2,3], 
        binned          = false,)

        println("\rRunning Model: $(model_type) $(stat) $(temp) $(drought_ind) with dummies=$(with_dummies), symbols=$(symbols) ($(drought_ind)), fe=$(fixed_effects) \r")

        outpath = "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)"
        outtxt = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)_$(stat)_$(temp) $(extra) $(fixed_effects)_fe $(symbols)_sym.txt" 
        outtex = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)_$(stat)_$(temp) $(extra) $(fixed_effects)_fe $(symbols)_sym.tex"
        mkpath(outpath)

        # Set the symbols that will be used in the regression. If use_hd_symbols is true, use the HD symbols, else use the standard ones.
        if symbols == "standard"
            get_symbols = get_symbols_standard

        elseif startswith(symbols, "hd")
            # 1. Dynamically determine the parameters by parsing the string
            hot_prefix  = symbols[1:end-2]         # e.g., "hd35" from "hd35fd"
            cold_prefix = symbols[end-1:end]       # e.g., "fd" from "hd35fd"

            # 2. Use the parsed parameters to configure the function call
            get_symbols = (args...) -> get_symbols_temp_extremes(
                args...; 
                hot_prefix=hot_prefix, 
                cold_prefix=cold_prefix, 
                binned=binned
            )

        elseif startswith(symbols, "horserace")
            # extract suffixes: horserace_hd35fd -> hd35fd
            hot_prefix  = symbols[11:end-2]         # e.g., "hd35" from "horserace_hd35fd"
            cold_prefix = symbols[end-1:end]       # e.g., "fd" from "horserace_hd35fd"

            get_symbols = (args...) -> get_symbols_horserace(
                args...; 
                hot_prefix=hot_prefix, 
                cold_prefix=cold_prefix, 
                binned=binned
            )

        else
            throw("Invalid symbols option: $(symbols).")
        end        
        # if isfile(outtxt) && isfile(outtex)
        #     println("File exists, moving to next iteration.")
        #     return
        # end
        
        spi_previous = [] # This list is for adding the previous SPI variables to the regression
        temp_previous = []  # This list is for adding the previous temperature variables to the regression
        order_spi = [] # This list is for saving the regression tables in the right order
        order_temp = [] # This list is for saving the regression tables in the right order
        regs = [] # regs stores the outputs of the regression models

        # Compute the number of time periods including the word "inutero"
        inutero_periods_number = sum([occursin("inutero", time) for time in times])
        for time in inutero_periods_number:(length(times)-1)
            if time>0
                # This case happens when we do not include in utero in the regression
                time0 = times[time] # Previous period
                agedeath0 = replace(time0, "born_" => "child_agedeath_")
            end
            time1 = times[time + 1] # Contemporary period
            agedeath1 = replace(time1, "born_" => "child_agedeath_")

            # Get SPI and temperature symbols based on the model type and dummies
            spi_actual, temp_actual = get_symbols(df, temp, drought_ind, time1, stat, sp_threshold, model_type, with_dummies)
            if time == inutero_periods_number
                # Get the SPI and temperature symbols for the first three periods (inutero_1m3m, inutero_4m6m, inutero_6m9m)
                for i in 1:inutero_periods_number
                    # If no in-utero periods are included, this goes from 1:0, and in Julia this is skipped
                    spi_start, temp_start = get_symbols(df, temp, drought_ind, times[i], stat, sp_threshold, model_type, with_dummies)
                    append!(order_spi, spi_start)
                    append!(order_temp, temp_start)
                    append!(spi_previous, spi_start)
                    append!(temp_previous, temp_start)
                end
            else
                # Filter out children that did not survive the previous time period
                mask = df[!, Symbol(agedeath0)] .== 0
                df = @view df[mask, :]
            end

            all_spis = append!(spi_previous, spi_actual)
            all_temp = append!(temp_previous, temp_actual)

            for i in cells

                if fixed_effects == "standard"
                    fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
                elseif fixed_effects == "quadratic_time"
                    fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & term(:chb_year_sq) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
                end

                # validate_regression_inputs(
                #     df,
                #     Symbol(agedeath1),
                #     vcat(spi_actual, temp_actual),
                #     getproperty.(controls, :sym),
                #     fixed_effects_term;
                #     context_info="Time Period: $(agedeath1), Cell ID: ID_cell$i"
                # )

                reg_model = reg(
                    df,   
                    term(Symbol(agedeath1))  ~ sum(term.(all_spis)) + sum(term.(all_temp)) + sum(term.(controls)) + fixed_effects_term, 
                    Vcov.cluster(Symbol("ID_cell$i")), 
                    method=:CUDA
                )
                println(reg_model)
                push!(regs, reg_model)
            
            end

            append!(spi_previous, spi_actual)
            append!(temp_previous, temp_actual)
            append!(order_spi, spi_actual)
            append!(order_temp, temp_actual)
        end

        # Generate regression table
        order = vcat(order_spi, order_temp)
        order = [string(sym) for sym in order]

        regtable(
            regs...; 
            render = AsciiTable(), 
            file=outtxt,
            order=order,
        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file=outtex,
            order=order,
        )
        # catch e
        #     println("Error with ", outtex, e)
        # end
        
        # Garbage collection
        GC.gc()
    end

    function get_symbols_standard(df, temp, drought_ind,
        time, stat, sp_threshold, model_type, with_dummies)
        """
            get_symbols_standard!(df, temp, drought_ind, time, stat, sp_threshold,
                        model_type::AbstractString, with_dummies::Bool)
    
        Return the vectors of Symbols that should go into the regression **and**
        (when `with_dummies == true`) make sure the corresponding interaction
        columns exist in `df`.  The routine is idempotent, so it is safe to call
        it many times inside a loop.
        """

        # Containers we will return
        spi_syms  = Symbol[]
        temp_syms = Symbol[]

        spi_base  = Symbol("$(drought_ind)_$(time)_$(stat)")
        t_base    = Symbol("$(temp)_$(time)_$(stat)")

        
        for (ind, syms) in ((spi_base, spi_syms), (t_base, temp_syms))
            if model_type == "linear" && !with_dummies
                push!(syms,  ind)
            
            elseif model_type == "quadratic" && !with_dummies
                push!(syms,  ind, Symbol("$(ind)_sq"))

            elseif model_type == "linear" && with_dummies
                # 1.  Dummy indicators
                ind_neg_d = Symbol("$(ind)_neg")
                ind_pos_d = Symbol("$(ind)_pos")

                # 2.  Build the interaction columns
                ind_neg_x = Symbol("$(ind)_neg_int")   # continuous × neg-dummy
                ind_pos_x = Symbol("$(ind)_pos_int")   # continuous × pos-dummy

                df[!,  ind_neg_x] = passmissing(Float16).(df[!, ind] .* df[!, ind_neg_d])
                df[!,  ind_pos_x] = passmissing(Float16).(df[!, ind] .* df[!, ind_pos_d])

                append!(syms,  (ind_neg_x,  ind_pos_x))

            elseif model_type == "quadratic" && with_dummies
                # 1.  Dummy indicators & squared indicators
                ind_neg_d = Symbol("$(ind)_neg")
                ind_pos_d = Symbol("$(ind)_pos")
                ind_sq = Symbol("$(ind)_sq")

                # 2.  Build the interaction columns
                ind_neg_x = Symbol("$(ind)_neg_int")   # continuous × neg-dummy
                ind_pos_x = Symbol("$(ind)_pos_int")   # continuous × pos-dummy
                ind_neg_x_sq = Symbol("$(ind)_sq_neg_int")   # continuous × neg-dummy
                ind_pos_x_sq = Symbol("$(ind)_sq_pos_int")   # continuous × pos-dummy

                df[!,  ind_neg_x] = passmissing(Float16).(df[!, ind] .* df[!, ind_neg_d])
                df[!,  ind_pos_x] = passmissing(Float16).(df[!, ind] .* df[!, ind_pos_d])
                df[!,  ind_neg_x_sq] = passmissing(Float16).(df[!, ind_sq] .* df[!, ind_neg_d])
                df[!,  ind_pos_x_sq] = passmissing(Float16).(df[!, ind_sq] .* df[!, ind_pos_d])

                append!(syms,  (ind_neg_x,  ind_pos_x, ind_neg_x_sq, ind_pos_x_sq))

            elseif model_type == "spline"
                # 1.  Dummy indicators
                ind_gtk = Symbol("$(ind)_gt$(sp_threshold)")
                ind_bt0k = Symbol("$(ind)_bt0$(sp_threshold)")
                ind_bt0mk = Symbol("$(ind)_bt0m$(sp_threshold)")
                ind_ltk = Symbol("$(ind)_ltm$(sp_threshold)")

                # 2.  Build the interaction columns
                ind_gtk_x = Symbol("$(ind)_gt$(sp_threshold)_int")   # interaction
                ind_bt0k_x = Symbol("$(ind)_bt0$(sp_threshold)_int")   
                ind_bt0mk_x = Symbol("$(ind)_bt0m$(sp_threshold)_int")   
                ind_ltk_x = Symbol("$(ind)_ltm$(sp_threshold)_int")   
                
                df[!,  ind_gtk_x  ] = passmissing(Float16).(df[!, ind] .* df[!, ind_gtk  ])
                df[!,  ind_bt0k_x ] = passmissing(Float16).(df[!, ind] .* df[!, ind_bt0k ])
                df[!,  ind_bt0mk_x] = passmissing(Float16).(df[!, ind] .* df[!, ind_bt0mk])
                df[!,  ind_ltk_x  ] = passmissing(Float16).(df[!, ind] .* df[!, ind_ltk  ])
                
                append!(syms,  (ind_gtk_x,  ind_bt0k_x, ind_bt0mk_x, ind_ltk_x))
                
            else
                error("Combination (model_type=$(model_type), with_dummies=$(with_dummies)) not yet implemented.")
            end
        end
        return spi_syms, temp_syms
    end
    
    function get_symbols_temp_extremes(df, temp, drought_ind, time1, stat, sp_threshold, model_type, with_dummies;
        hot_prefix::String, cold_prefix::String, binned::Bool, create_spi_cols::Bool=true)
    
        """
            get_symbols_temp_extremes(...)
    
        Generates regression symbols for temperature extremes and SPI interactions.
        This version uses keyword arguments for clarity.
    
        # Arguments
        - `df, drought_ind, time, stat, with_dummies`: Standard arguments.
        - `hot_prefix::String`: (Keyword) The prefix for the hot day variable (e.g., "hd35").
        - `cold_prefix::String`: (Keyword) The prefix for the cold/frost day variable (e.g., "fd").
        - `binned::Bool`: (Keyword) If `true`, creates binned dummies for temperature variables.
        - `create_spi_cols::Bool`: (Keyword) If `true`, creates SPI interaction columns.
        """

        # Containers we will return
        spi_syms  = Symbol[] 
        temp_syms = Symbol[]

        if create_spi_cols
            # --- 1. SPI Interaction Logic (Consistent across all models) ---
            spi_base  = Symbol("$(drought_ind)_$(time1)_$(stat)")
            spi_neg_x = Symbol("$(spi_base)_neg_int")
            spi_pos_x = Symbol("$(spi_base)_pos_int")

            spi_neg_d = Symbol("$(spi_base)_neg")
            spi_pos_d = Symbol("$(spi_base)_pos")
            df[!,  spi_neg_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_neg_d])
            df[!,  spi_pos_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_pos_d])

            append!(spi_syms, (spi_neg_x, spi_pos_x))
        end

        # --- 2. Temperature Extremes Logic (Parameterized) ---
        hot_col  = Symbol("$(hot_prefix)_$(time1)_$(stat)")
        cold_col = Symbol("$(cold_prefix)_$(time1)_$(stat)")

        if binned
            # This branch replicates the behavior of the `...fd` functions.
            function add_binned_dummies!(df, src::Symbol; prefix::String)
                bins = ((1,10), (10,20), (20,30))
                syms = Symbol[]
                for (lo, hi) in bins
                    new_sym = Symbol("$(prefix)_bin$(hi)")
                    if with_dummies
                        df[!, new_sym] .= passmissing(x -> (lo ≤ x < hi) ? 1 : 0).(df[!, src])
                    end
                    push!(syms, new_sym)
                end
                return syms
            end
        
            append!(temp_syms, add_binned_dummies!(df, hot_col;  prefix=string(hot_col)))
            append!(temp_syms, add_binned_dummies!(df, cold_col; prefix=string(cold_col)))
            
        else
            # This branch replicates the behavior of the `...id` functions.
            append!(temp_syms, (hot_col, cold_col))
        end

        return spi_syms, temp_syms
    end
    
    function get_symbols_horserace(df, temp, drought_ind, time1, stat, sp_threshold, model_type, with_dummies;
                                    hot_prefix::String, cold_prefix::String, binned::Bool)
        """
            get_symbols_horserace(...)

        Generates regression symbols for a "horse race" model, combining standard 
        temperature/precipitation measures with extreme temperature day counts.

        This function leverages the logic from `get_symbols_standard` to create symbols for
        average temperature and SPI (handling linear, quadratic, and dummy interactions).
        It then appends symbols for extreme temperature days (e.g., hot days, frost days),
        allowing the regression model to assess the relative importance of average vs.
        extreme temperature metrics.

        # Arguments
        - `df, temp, drought_ind, ...`: Standard arguments for symbol generation.
        - `hot_prefix::String`: (Keyword) The prefix for the hot day variable (e.g., "hd35").
        - `cold_prefix::String`: (Keyword) The prefix for the cold/frost day variable (e.g., "fd").
        - `binned::Bool`: (Keyword) If `true`, creates binned dummies for extreme temperature variables.

        # Returns
        - `spi_syms::Vector{Symbol}`: A vector of symbols for the drought/precipitation variables.
        - `temp_syms::Vector{Symbol}`: A vector containing symbols for BOTH standard and extreme temperature variables.
        """
        
        # 1. Get the standard set of symbols for precipitation and average temperature.
        # This function correctly handles all model_type and with_dummies variations.
        spi_syms, standard_temp_syms = get_symbols_standard(df, temp, drought_ind,
            time1, stat, sp_threshold, model_type, with_dummies)

            
        _, extreme_temp_syms = get_symbols_temp_extremes(
            df, temp, drought_ind, time1, stat, sp_threshold, model_type, with_dummies;
            hot_prefix=hot_prefix, cold_prefix=cold_prefix, binned=binned, create_spi_cols=false
            )
            
        # Append both sets of temperature symbols.
        temp_syms = vcat(standard_temp_syms, extreme_temp_syms)

        return spi_syms, temp_syms
    end
    
    function run_models(df_lazy, controls, folder, extra, months; models::Vector{}=["linear"], filter_on::Union{Nothing, Pair{Symbol, T}}=nothing) where T
        """
            run_models(df_lazy, controls, folder, extra, months; only_linear=false, filter_on::Union{Nothing, Pair{Symbol, T}}=nothing) where T

            Runs the standard models by iterating through multiple SPI time windows and time periods.
            It calls the `run_regression` function for each combination of parameters.
            
            # Arguments
            - `df`: DataFrame containing the data.
            - `controls`: Array of control variables for the regression.
            - `folder`: Output folder path to save the regression results.
            - `extra`: Additional string to append to the output filenames.
            
            # Output
            Saves regression results for each combination of parameters in both ASCII and LaTeX formats.
        """
        
        println("\rRunning Standard Models for $(folder)\r")

        for month in months
            sp_threshold = 0.5 # Set default value to avoid breaking the function when this parameter is not used
            for (name, times, stats) in (
                ("semester", ["inutero", "born_1m6m", "born_6m12m", "born_12m18m", "born_18m24m", "born_24m30m", "born_30m36m"], ["b_avg",]),
                # ("quarterly", ["inutero_1m3m", "inutero_3m6m", "inutero_6m9m", "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m"], ["q_avg",]),
                # ("monthly", ["inutero_1m","inutero_2m","inutero_3m","inutero_4m","inutero_5m","inutero_6m","inutero_7m","inutero_8m","inutero_9m","born_1m","born_2m","born_3m","born_4m","born_5m","born_6m",], ["m_avg",]),
                # ("1m windows", ["born_1m",], ["b_w1", "b_w1", "b_w2", "b_w3", "b_w4", "b_w5", "b_w6", "b_w7", "b_w8", "b_w9"]),
                # ("6m windows", ["born_6m",], ["b_w1", "b_w1", "b_w2", "b_w3", "b_w4", "b_w5", "b_w6", "b_w7", "b_w8", "b_w9"]),
                # ("12m windows", ["born_12m",], ["b_w1", "b_w1", "b_w2", "b_w3", "b_w4", "b_w5", "b_w6", "b_w7", "b_w8", "b_w9"]),
                # ("iufocus", ["inutero_1m3m", "inutero_3m6m", "inutero_6m9m", "born_1m", "born_2m3m", "born_3m6m"], ["iu_avg", ]),
             )
                name = name * "$(extra)"
                for stat in stats
                    for temp in ["stdm_t", ]# "absdifm_t", "std_t", "absdif_t"]#,  "t"]
                        for drought in ["spi"]#, "spei"]        
                            # Add month to variable
                            drought = "$(drought)$(month)"

                            # Load dataset
                            df = load_dataset(df_lazy, temp, drought, stat, controls, name; verbose=false, filter_on=filter_on)
                            
                            # Linear models - all cases
                            if ("linear" in models)
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, cells=[1,2,3])
                            end
                            if (name=="1m windows") || (name=="6m windows") || (name=="12m windows") || (name=="iufocus") || (name=="monthly")
                                continue
                            end

                            # # HD/FD models
                            if "extremes" in models
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="hd35fd", cells=[1,2,3])
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="hd35id", cells=[1,2,3])
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="hd40fd", cells=[1,2,3])
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="hd40id", cells=[1,2,3])
                            end
                                
                            # horserace models
                            if "horserace" in models
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="horserace_hd35fd", cells=[1,2,3])
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="horserace_hd35id", cells=[1,2,3])
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="horserace_hd40fd", cells=[1,2,3])
                                stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name, model_type="linear", with_dummies=true, symbols="horserace_hd40id", cells=[1,2,3])
                            end

                            # Spline models - only for standardized variables (std_t, stdm_t):
                            if "spline" in models
                                for sp_threshold in ["1", "2"]
                                    name_with_threshold = name * " - spthreshold$(sp_threshold)"
                                    stepped_regression(df, temp, drought, controls, times, stat, sp_threshold, folder, name_with_threshold, model_type="spline")
                                end
                            end
                        end
                    end
                end
            end
        end
    end


    """
        run_heterogeneity(df_lazy, controls, heterogeneity_var, months; models=["linear"])

    Runs a heterogeneity analysis by splitting the dataset based on the unique values 
    in a specified column and running models on each subset.

    This function is highly efficient as it avoids loading the full dataset for each subgroup.
    Instead, it passes a filter condition down to the `load_dataset` function, which
    selects and filters the data in a single, optimized step.

    It works for both dummy variables (e.g., `0`/`1`) and categorical variables with
    multiple groups (e.g., country codes, wealth quintiles).

    # Arguments
    - `df_lazy`: A lazy table object (e.g., `Arrow.Table`) containing the full dataset.
    - `controls`: Array of control variable terms for the regression.
    - `heterogeneity_var::Symbol`: The symbol of the column to group the analysis by.
    - `months`: Array of months to iterate through for the models.
    - `only_linear`: Boolean to run only linear models, typically for faster heterogeneity checks.
    """
    function run_heterogeneity(df_lazy, controls, heterogeneity_var::Symbol, months; extra::String="", models::Vector{}=["linear"])

        # 1. Efficiently get the single column of interest from the lazy table.
        println("Finding unique groups in column: $(heterogeneity_var)...")
        column_data = Tables.getcolumn(df_lazy, heterogeneity_var)
        
        # 2. Find all unique, non-missing values to iterate over.
        groups = unique(filter(!ismissing, column_data))
        println("Found $(length(groups)) groups to analyze: $(groups)")

        # 3. Create a progress bar for monitoring the analysis.
        prog = Progress(length(groups), 1, "Running heterogeneity for '$(heterogeneity_var)': ")
        
        # 4. Loop over each unique group.
        for group in groups
            # Update progress bar
            next!(prog) 
            
            try
                # Define folder and file suffix for this specific group.
                folder = "heterogeneity\\$(heterogeneity_var)"
                suffix = " - $(group)$(extra)"

                # Define the filter instruction for this group.
                # This `Pair` will be passed down to `load_dataset`.
                filter_instruction = heterogeneity_var => group

                println("\n" * "="^80)
                println("Starting analysis for group: $(heterogeneity_var) == $(group)")
                println("="^80 * "\n")

                # Call the main model runner with the specific filter for this subgroup.
                # No data has been loaded or filtered yet. That happens inside run_models.
                CustomModels.run_models(df_lazy, controls, folder, suffix, months;
                                        models=models,
                                        filter_on=filter_instruction,
                )
            catch e
                println("ERROR processing group: $(group). Skipping.")
                println(e)
                # Optionally, print the full error for debugging:
                #showerror(stdout, e, catch_backtrace())
            end
        end
        println("Heterogeneity analysis for '$(heterogeneity_var)' complete.")
    end

    # function validate_regression_inputs(
    #     df::DataFrame,
    #     dependent_var::Symbol,
    #     regressor_vars::Vector{Symbol},
    #     control_vars::Vector{Symbol},
    #     fe_term;
    #     context_info::String=""
    #     )
    #     """
    #         validate_regression_inputs(df, dependent_var, regressor_vars, control_vars, fe_term; context_info="")
    
    #     Performs a series of pre-regression checks and throws an `ArgumentError` if any check fails.
    
    #     This function verifies that:
    #     1.  All specified variable columns exist in the DataFrame.
    #     2.  The DataFrame is not empty.
    
    #     If any check fails, it prints a detailed error message and throws an exception.
    #     If all checks pass, it prints a descriptive summary of the variables and returns `nothing`.
    
    #     # Arguments
    #     - `df::DataFrame`: The DataFrame to be checked.
    #     - `dependent_var::Symbol`: The symbol for the dependent variable.
    #     - `regressor_vars::Vector{Symbol}`: A vector of symbols for the main regressors.
    #     - `control_vars::Vector{Symbol}`: A vector of symbols for the control variables.
    #     - `fe_term`: The fixed effects term from `FixedEffectModels`.
    #     - `context_info::String`: Optional string to print context (e.g., model type, cell ID).
    
    #     # Throws
    #     - `ArgumentError`: If the DataFrame is empty or if any required columns are missing.
    #     """
    #     println("\n\n" * "="^20 * " VALIDATING REGRESSION INPUTS " * "="^20)
    #     if !isempty(context_info)
    #         println(context_info)
    #     end

    #     # --- Check 1: DataFrame should not be empty ---
    #     if isempty(df)
    #         msg = "Validation failed: The input DataFrame is empty. Cannot proceed."
    #         println("ERROR: " * msg)
    #         println("="^62 * "\n\n")
    #         throw(ArgumentError(msg))
    #     end
    #     println("DataFrame size: $(size(df))")

    #     # --- Check 2: All required columns must exist ---
    #     fixed_effect_vars = termvars(fe_term)
    #     all_needed_vars = unique(vcat(dependent_var, regressor_vars, control_vars, fixed_effect_vars))

    #     missing_cols = [var for var in all_needed_vars if !hasproperty(df, var)]

    #     if !isempty(missing_cols)
    #         # Construct a detailed error message
    #         msg = """
    #         Validation failed: The following required columns are MISSING from the DataFrame:
    #         $(join(["  - :$col" for col in missing_cols], "\n"))
    #         This is the likely cause of NaN results. Check that 'get_required_vars' is loading all necessary base columns for interactions.
    #         """
    #         println("ERROR: " * msg)
    #         println("="^62 * "\n\n")
    #         throw(ArgumentError(msg))
    #     else
    #         println("\nSUCCESS: All required columns are present in the DataFrame.")
    #     end

    #     # --- If all checks pass, print a descriptive summary ---
    #     println("\n--- Data Description of Model Variables ---")
    #     try
    #         println(describe(df, cols=all_needed_vars))
    #     catch e
    #         msg = "Validation failed: The `describe()` function threw an error. This may indicate corrupted or unexpected data."
    #         println("\nERROR: " * msg)
    #         showerror(stdout, e) # Show the original error from describe()
    #         println("="^62 * "\n\n")
    #         throw(ArgumentError(msg))
    #     end

    #     println("="^62 * "\n\n")
    #     return # Implicitly returns nothing
    # end

end # module SteppedRegression