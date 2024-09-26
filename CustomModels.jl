module CustomModels

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

    function stepped_regression(df, months, temp, drought_ind, controls, times, folder, extra; model_type="linear", with_dummies=false)
        """
            run_regression(df, months, controls, times, folder, extra; model_type="linear", with_dummies=false)
        
        Runs the specified regression models on the provided DataFrame `df` for different time periods. 
        This function supports multiple model types, including linear, quadratic, and quadratic with dummy variables.
        
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

        nameind = "avg"
        spiind = "avg"
        tind = "avg"

        outpath = "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)"
        mkpath(outpath)
        outtxt = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)$(months)_$(nameind)_$(temp) $(extra).txt" 
        outtex = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)$(months)_$(nameind)_$(temp) $(extra).tex"

        if isfile(outtxt) && isfile(outtex)
            println("File exists, moving to next iteration.")
            return
        end
        
        spi_previous = [] 
        temp_previous = []
        order_spi = []
        order_temp = []
        regs = []
        
        for time in 1:(length(times)-1)
            time1 = times[time]
            time2 = times[time + 1]

            # Get SPI and temperature symbols based on the model type and dummies
            spi_actual, temp_actual = get_symbols(months, temp, drought_ind, time2, spiind, model_type, with_dummies)
            if time == 1
                spi_start, temp_start = get_symbols(months, temp, drought_ind, time1, tind, model_type, with_dummies)
                append!(order_spi, spi_start)
                append!(order_temp, temp_start)
                append!(spi_previous, spi_start)
                append!(temp_previous, temp_start)
            else
                # Filter out children that did not survive the previous time period
                df = filter(row -> row[Symbol("child_agedeath_$(time1)")] == 0, df)
            end

            for i in 1:3
                fixed_effects = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
                reg_model = reg(
                    df, 
                    term(Symbol("child_agedeath_$(time2)")) ~ sum(term.(spi_previous)) + sum(term.(spi_actual))  + sum(term.(temp_previous)) + sum(term.(temp_actual)) + sum(controls) + fixed_effects, 
                    Vcov.cluster(Symbol("ID_cell$i")), 
                    method=:CUDA
                )
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

    end

    function get_symbols(months, temp, drought_ind, time, index, model_type, with_dummies)
        """
            get_symbols(months, temp, drought_ind, time, index, model_type, with_dummies)
        
        Generates the appropriate SPI and temperature symbols for the regression based on the model type and the presence of dummy variables.
        
        # Arguments
        - `months`: String specifying the SPI time window (e.g., "1", "3", "6", "9", "12").
        - `temp`: String specifying the temperature variable.
        - `drought_ind`: String specifying the drought indicator variable
        - `time`: Time period for which the symbols are generated.
        - `index`: Index type for SPI and temperature (e.g., "avg").
        - `model_type`: Type of regression model to run. Options are `"linear"` or `"quadratic"`.
        - `with_dummies`: Boolean indicating whether to include dummy variables for positive and negative deviations.
        
        # Returns
        A tuple containing:
        - `spi_symbols`: Array of symbols for SPI variables.
        - `temp_symbols`: Array of symbols for temperature variables.
        """
        spi_symbols = []
        temp_symbols = []
        
        if model_type == "linear" && !with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(index)")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(index)")]
        elseif model_type == "quadratic" && !with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(index)"), Symbol("$(drought_ind)$(months)_$(time)_$(index)_sq")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(index)"), Symbol("$(temp)_$(time)_$(index)_sq")]
        elseif model_type == "linear" && with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(index)_neg"), Symbol("$(drought_ind)$(months)_$(time)_$(index)_pos")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(index)_neg"), Symbol("$(temp)_$(time)_$(index)_pos")]
        elseif model_type == "quadratic" && with_dummies
            spi_symbols = [Symbol("$(drought_ind)$(months)_$(time)_$(index)_sq_neg"), Symbol("$(drought_ind)$(months)_$(time)_$(index)_sq_pos")]
            temp_symbols = [Symbol("$(temp)_$(time)_$(index)_sq_neg"), Symbol("$(temp)_$(time)_$(index)_sq_pos")]
        end

        return spi_symbols, temp_symbols
    end

    function run_models(df, controls, folder, extra)
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
        for months in ["1", "3", "6", "9", "12", "24"]
            i = 1
            extra_original = extra
            for times in (["inutero", "30d", "2m12m"], )#, ["inutero", "1m3m", "4m12m"], ["inutero", "1m12m"])
                for temp in ["stdm_t","std_t", "t"]
                    for drought_ind in ["spi"]#, "spei"]        
                        extra_with_time = extra_original * " - times$(i)"
                        stepped_regression(df, months, temp, drought_ind, controls, times, folder, extra_with_time, model_type="linear")
                        # stepped_regression(df, months, temp, drought_ind, controls, times, folder, extra_with_time, model_type="quadratic")
                        stepped_regression(df, months, temp, drought_ind, controls, times, folder, extra_with_time, model_type="linear", with_dummies=true)
                        # stepped_regression(df, months, temp, drought_ind, controls, times, folder, extra_with_time, model_type="quadratic", with_dummies=true)
                    end
                end
                i += 1
            end
        end
    end

    

    function run_heterogeneity_dummy(df, controls, controls_i, heterogeneity_var, folder, extra)
        df = dropmissing(df, Symbol(heterogeneity_var))

        df_filtered = filter(row -> row[heterogeneity_var] == 0, df)
        suffix = " - $(heterogeneity_var)0 - controls$(controls_i)"
        CustomModels.run_models(df_filtered, controls, "heterogeneity\\$(heterogeneity_var)", suffix)

        df_filtered = filter(row -> row[heterogeneity_var] == 1, df)
        suffix = " - $(heterogeneity_var)1 - controls$(controls_i)"
        CustomModels.run_models(df_filtered, controls, "heterogeneity\\$(heterogeneity_var)", suffix)
    end

    function run_heterogeneity(df, controls, controls_i, heterogeneity_var, folder, extra)

        groups = unique(df[heterogeneity_var])

        # Create a progress bar
        prog = Progress(length(groups), 1)
        
        # Loop over unique values
        for group in groups
            next!(prog) # Update progress bar
            try
                df_filtered = filter(row -> row[heterogeneity_var] == group, df)
                suffix = " - $(heterogeneity_var)$(group) - controls$(controls_i)"
                CustomModels.run_models(df_filtered, controls, "heterogeneity\\$(heterogeneity_var)", " - $(group)")
            catch
                printlnln("Error en ", group)
            end
        end
    end


end # module SteppedRegression
