module CustomModels

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

    function stepped_regression(df, months, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra; model_type="linear", with_dummies=false, fixed_effects="standard")
        """
            run_regression(df, months, controls, times, folder, extra; model_type="linear", with_dummies=false)
        
        Runs the specified regression models on the provided DataFrame `df` for different time periods. 
        This function supports multiple model types, including linear, quadratic, and quadratic with dummy variables.
        
        Results in stata cannot be directly replicated because reghdfe creates one variable for each ID_cell, which makes it
        imposible to fit in a RAM. Nevertheless, this function replicates a model such as:
            
            reghdfe child_agedeath_30d spi1_inutero_avg_neg spi1_inutero_avg_pos spi1_30d_avg_neg spi1_30d_avg_pos 
                                       stdm_t_inutero_avg_neg stdm_t_inutero_avg_pos stdm_t_30d_avg_neg stdm_t_30d_avg_pos 
                                       ${controls} i.ID_cell1#c.chb_year, 
                                       absorb(ID_cell1#chb_month ID_cell1) vce(cluster ID_cell1) nocons

        the term i.ID_cell1#c.chb_year adds a linear trend on every fixed effect, and the term absorb(ID_cell1#chb_month ID_cell1) 
        adds the fixed effects for each cell and cell-month interaction. 

        # Arguments
        - `df`: DataFrame containing the data.
        - `months`: String specifying the SPI time window (e.g., "1", "3", "6", "9", "12").
        - `controls`: Array of control variables for the regression.
        - `times`: Array of time periods for the regression.
        - `folder`: Output folder path to save the regression results.
        - `extra`: Additional string to append to the output filenames.
        - `model_type`: Type of regression model to run. Options are `"linear"` (default) or `"quadratic"`.
        - `with_dummies`: Boolean indicating whether to include dummy variables for positive and negative deviations (default is `false`).
        
        # Output
        Saves regression results in both ASCII and LaTeX formats to the specified folder.
        """
        println("\rRunning Model: $(model_type) with dummies=$(with_dummies) - $(drought_ind)$(months)\r")

        outpath = "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)"
        mkpath(outpath)
        outtxt = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)$(months)_$(stat)_$(temp) $(extra) $(fixed_effects)_fe.txt" 
        outtex = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)$(months)_$(stat)_$(temp) $(extra) $(fixed_effects)_fe.tex"

        if isfile(outtxt) && isfile(outtex)
            println("File exists, moving to next iteration.")
            return
        end
        
        spi_previous = [] # This list is for adding the previous SPI variables to the regression
        temp_previous = []  # This list is for adding the previous temperature variables to the regression
        order_spi = [] # This list is for saving the regression tables in the right order
        order_temp = [] # This list is for saving the regression tables in the right order
        regs = [] # regs stores the outputs of the regression models

        # try
        for time in 1:(length(times)-1)
            time1 = times[time]
            time2 = times[time + 1]

            # Get SPI and temperature symbols based on the model type and dummies
            spi_actual, temp_actual = get_symbols(months, temp, drought_ind, time2, stat, sp_threshold, model_type, with_dummies)
            if time == 1
                spi_start, temp_start = get_symbols(months, temp, drought_ind, time1, stat, sp_threshold, model_type, with_dummies)
                append!(order_spi, spi_start)
                append!(order_temp, temp_start)
                append!(spi_previous, spi_start)
                append!(temp_previous, temp_start)
            else
                # Filter out children that did not survive the previous time period
                df = filter(row -> row[Symbol("child_agedeath_$(time1)")] == 0, df)
            end

            for i in 1:3
                if fixed_effects == "standard"
                    fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
                elseif fixed_effects == "quadratic_time"
                    fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & term(:chb_year_sq) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
                end

                reg_model = reg(
                    df, 
                    term(Symbol("child_agedeath_$(time2)")) ~ sum(term.(spi_previous)) + sum(term.(spi_actual))  + sum(term.(temp_previous)) + sum(term.(temp_actual)) + sum(term.(controls)) + fixed_effects_term, 
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

    function get_symbols(months, temp, drought_ind, time, stat, sp_threshold, model_type, with_dummies)
        """
            get_symbols(months, temp, drought_ind, time, stat, model_type, with_dummies)
        
        Generates the appropriate SPI and temperature symbols for the regression based on the model type and the presence of dummy variables.
        
        # Arguments
        - `months`: String specifying the SPI time window (e.g., "1", "3", "6", "9", "12").
        - `temp`: String specifying the temperature variable.
        - `drought_ind`: String specifying the drought indicator variable
        - `time`: Time period for which the symbols are generated.
        - `stat`: Aggregation stat type for SPI and temperature (e.g., "avg" or "min").
        - `sp_threshold`: Threshold for defining the points to divede the data for the spline model.
        - `model_type`: Type of regression model to run. Options are `"linear"`, `"quadratic"` or `"spline"`.
        - `with_dummies`: Boolean indicating whether to include dummy variables for positive and negative deviations.
        
        # Returns
        A tuple containing:
        - `spi_symbols`: Array of symbols for SPI variables.
        - `temp_symbols`: Array of symbols for temperature variables.
        """
        spi_symbols = []
        temp_symbols = []
        
        if model_type == "linear" && !with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(stat)")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(stat)")]
        elseif model_type == "quadratic" && !with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(stat)"), Symbol("$(drought_ind)$(months)_$(time)_$(stat)_sq")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(stat)"), Symbol("$(temp)_$(time)_$(stat)_sq")]
        elseif model_type == "linear" && with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(stat)_neg"), Symbol("$(drought_ind)$(months)_$(time)_$(stat)_pos")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(stat)_neg"), Symbol("$(temp)_$(time)_$(stat)_pos")]
        elseif model_type == "quadratic" && with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(stat)_sq_neg"), Symbol("$(drought_ind)$(months)_$(time)_$(stat)_sq_pos")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(stat)_sq_neg"), Symbol("$(temp)_$(time)_$(stat)_sq_pos")]
        elseif model_type == "spline"
            spi_symbols = [
                Symbol("$(drought_ind)$(months)_$(time)_$(stat)_ltm$(sp_threshold)"), 
                Symbol("$(drought_ind)$(months)_$(time)_$(stat)_bt0m$(sp_threshold)"), 
                Symbol("$(drought_ind)$(months)_$(time)_$(stat)_bt0$(sp_threshold)"), 
                Symbol("$(drought_ind)$(months)_$(time)_$(stat)_gt$(sp_threshold)")
            ]
            temp_symbols = [
                Symbol("$(temp)_$(time)_$(stat)_ltm$(sp_threshold)"),
                Symbol("$(temp)_$(time)_$(stat)_bt0m$(sp_threshold)"),
                Symbol("$(temp)_$(time)_$(stat)_bt0$(sp_threshold)"),
                Symbol("$(temp)_$(time)_$(stat)_gt$(sp_threshold)")
            ]            
        end

        return spi_symbols, temp_symbols
    end

    function run_models(df, controls, folder, extra, months; only_linear=false)
        """
            run_models(df, controls, folder, extra)
        
        Executes the standard models by iterating through multiple SPI time windows and time periods. 
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
            extra_original = extra
            sp_threshold = 0.5 # Set default value to avoid breaking the function when this parameter is not used
            for times in (["inutero_1m3m", "inutero_4m6m", "inutero_6m9m", "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m"])#, ["inutero", "1m3m", "4m12m"], ["inutero", "1m12m"])
                i = 1
                for temp in ["stdm_t", "absdifm_t", "absdif_t", "std_t", "t"]
                    for drought_ind in ["spi"]#, "spei"]        
                        for stat in ["avg"]#, "minmax"]

                            extra_with_time = extra_original #* " - times$(i)"
                            # Linear and Quadratic models - all cases
                            stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true)
                            if only_linear
                                continue
                            end
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true, fixed_effects="quadratic_time")
                            stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="quadratic")
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="quadratic", fixed_effects="quadratic_time")

                            # Spline models - only for standardized variables (std_t, stdm_t):
                            for sp_threshold in ["1", "2"]
                                extra_with_threshold = extra_with_time * " - spthreshold$(sp_threshold)"
                                stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_threshold, model_type="spline")
                            end
                        end
                    end
                end
                i += 1
            end
        end
    end

    

    function run_heterogeneity_dummy(df, controls, heterogeneity_var, months)
        df = dropmissing(df, Symbol(heterogeneity_var))

        df_filtered = filter(row -> row[heterogeneity_var] == 0, df)
        suffix = " - $(heterogeneity_var)0"
        CustomModels.run_models(df_filtered, controls, "heterogeneity\\$(heterogeneity_var)", suffix, months, only_linear=true)

        df_filtered = filter(row -> row[heterogeneity_var] == 1, df)
        suffix = " - $(heterogeneity_var)1"
        CustomModels.run_models(df_filtered, controls, "heterogeneity\\$(heterogeneity_var)", suffix, months, only_linear=true)
    end

    function run_heterogeneity(df, controls, heterogeneity_var, months)

        groups = unique(df[!, heterogeneity_var])

        # Create a progress bar
        prog = Progress(length(groups), 1)
        
        # Loop over unique values
        for group in groups
            next!(prog) # Update progress bar
            try
                df_filtered = filter(row -> row[heterogeneity_var] == group, df)
                CustomModels.run_models(df_filtered, controls, "heterogeneity\\$(heterogeneity_var)", " - $(group)", months)
            catch
                println("Error en ", group)
            end
        end
    end


    
end # module SteppedRegression
