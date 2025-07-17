module CustomModels

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter
    using StatsModels: termvars

    function stepped_regression(df, months, temp, drought_ind, controls,
        times, stat, sp_threshold, folder, extra;
        model_type      = "linear",
        with_dummies    = false,
        fixed_effects   = "standard",
        symbols         = "standard",
        cells           = [1,2,3]) 
        """
            run_regression(df, months, controls, times, folder, extra; model_type="linear", with_dummies=false)
        
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

        println("\rRunning Model: $(model_type) with dummies=$(with_dummies), symbols=$(symbols) ($(drought_ind)$(months)), fe=$(fixed_effects) \r")

        outpath = "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)"
        outtxt = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)$(months)_$(stat)_$(temp) $(extra) $(fixed_effects)_fe $(symbols)_sym.txt" 
        outtex = "$(outpath)\\$(model_type)_dummies_$(with_dummies)_$(drought_ind)$(months)_$(stat)_$(temp) $(extra) $(fixed_effects)_fe $(symbols)_sym.tex"
        mkpath(outpath)

        # Set the symbols that will be used in the regression. If use_hd_symbols is true, use the HD symbols, else use the standard ones.
        if symbols=="standard"
            get_symbols = get_symbols_standard
        elseif symbols=="hd35fd"
            get_symbols = get_symbols_hd35fd
        elseif symbols=="hd40fd"
            get_symbols = get_symbols_hd40fd
        elseif symbols=="hd35id"
            get_symbols = get_symbols_hd35id
        elseif symbols=="hd40id"
            get_symbols = get_symbols_hd40id
        else
            throw("Invalid symbols option: $(symbols). Use 'standard', 'hd35fd', 'hd40fd', 'hd35id' or 'hd40id'.")
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

            time0 = times[time] # Previous period
            time1 = times[time + 1] # Contemporary period
            # Remove the word "born_" from the string to get the time period
            agedeath0 = replace(time0, "born_" => "child_agedeath_")
            agedeath1 = replace(time1, "born_" => "child_agedeath_")

            # Get SPI and temperature symbols based on the model type and dummies
            spi_actual, temp_actual = get_symbols(df, months, temp, drought_ind, time1, stat, sp_threshold, model_type, with_dummies)
            if time == inutero_periods_number
                # Get the SPI and temperature symbols for the first three periods (inutero_1m3m, inutero_4m6m, inutero_6m9m)
                for i in 1:inutero_periods_number
                    spi_start, temp_start = get_symbols(df, months, temp, drought_ind, times[i], stat, sp_threshold, model_type, with_dummies)
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

            for i in cells
                if fixed_effects == "standard"
                    fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
                elseif fixed_effects == "quadratic_time"
                    fixed_effects_term = fe(Symbol("ID_cell$i")) & term(:chb_year) + fe(Symbol("ID_cell$i")) & term(:chb_year_sq) + fe(Symbol("ID_cell$i")) & fe(:chb_month)
                end
               
                reg_model = reg(
                    df, 
                    term(Symbol(agedeath1))  ~ sum(term.(spi_previous)) + sum(term.(spi_actual))  + sum(term.(temp_previous)) + sum(term.(temp_actual)) + sum(term.(controls)) + fixed_effects_term, 
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

    function get_symbols_standard(df, months, temp, drought_ind,
        time, stat, sp_threshold, model_type, with_dummies)
        """
            get_symbols_standard!(df, months, temp, drought_ind, time, stat, sp_threshold,
                        model_type::AbstractString, with_dummies::Bool)
    
        Return the vectors of Symbols that should go into the regression **and**
        (when `with_dummies == true`) make sure the corresponding interaction
        columns exist in `df`.  The routine is idempotent, so it is safe to call
        it many times inside a loop.
        """

        # Containers we will return
        spi_syms  = Symbol[]
        temp_syms = Symbol[]

        spi_base  = Symbol("$(drought_ind)$(months)_$(time)_$(stat)")
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

    function get_symbols_hd40fd(df, months, temp, drought_ind,
        time, stat, sp_threshold, model_type, with_dummies)
        """
            get_symbols_hotfrostdays(df, months, temp, drought_ind, time, stat, sp_threshold,
                        model_type::AbstractString, with_dummies::Bool)
    
        Return the vectors of Symbols that should go into the regression **and**
        (when `with_dummies == true`) make sure the corresponding interaction
        columns exist in `df`.  The routine is idempotent, so it is safe to call
        it many times inside a loop.
        """

        # Containers we will return
        spi_syms  = Symbol[] 
        temp_syms = Symbol[]

        #### ---------- Hot & frost days as dummies -------------------------------
        hd40_col = Symbol("hd40_$(time)_$(stat)")
        fd_col   = Symbol("fd_$(time)_$(stat)")
    
        # Helper to build the three dummies for one source column
        function add_binned_dummies!(df, src::Symbol; prefix::String)
            bins = ((1,10), (10,20), (20,30))   # (lower, upper) bounds
            syms = Symbol[]
            for (i, (lo, hi)) in enumerate(bins)
                new_sym = Symbol("$(prefix)_bin$(hi)")
                if with_dummies
                    df[!, new_sym] .= passmissing(x -> (lo ≤ x < hi) ? 1 : 0).(df[!, src])
                end
                push!(syms, new_sym)
            end
            return syms
        end
    
        append!(temp_syms, add_binned_dummies!(df, hd40_col; prefix = string(hd40_col)))
        append!(temp_syms, add_binned_dummies!(df, fd_col;   prefix = string(fd_col)))
    
        #### ---------- Compute the interactions for the SPI
        spi_base  = Symbol("$(drought_ind)$(months)_$(time)_$(stat)")

        # 1.  Dummy indicators
        spi_neg_d = Symbol("$(spi_base)_neg")
        spi_pos_d = Symbol("$(spi_base)_pos")

        # 2.  Build the interaction columns
        spi_neg_x = Symbol("$(spi_base)_neg_int")   # continuous × neg-dummy
        spi_pos_x = Symbol("$(spi_base)_pos_int")   # continuous × pos-dummy

        df[!,  spi_neg_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_neg_d])
        df[!,  spi_pos_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_pos_d])

        ## Indicators of temperature

        append!(spi_syms,  (spi_neg_x,  spi_pos_x))

        return spi_syms, temp_syms
    end

    function get_symbols_hd35fd(df, months, temp, drought_ind,
        time, stat, sp_threshold, model_type, with_dummies)
        """
            get_symbols_hotfrostdays(df, months, temp, drought_ind, time, stat, sp_threshold,
                        model_type::AbstractString, with_dummies::Bool)
    
        Return the vectors of Symbols that should go into the regression **and**
        (when `with_dummies == true`) make sure the corresponding interaction
        columns exist in `df`.  The routine is idempotent, so it is safe to call
        it many times inside a loop.
        """

        # Containers we will return
        spi_syms  = Symbol[] 
        temp_syms = Symbol[]

        #### ---------- Hot & frost days as dummies -------------------------------
        hd35_col = Symbol("hd35_$(time)_$(stat)")
        fd_col   = Symbol("fd_$(time)_$(stat)")
    
        # Helper to build the three dummies for one source column
        function add_binned_dummies!(df, src::Symbol; prefix::String)
            bins = ((1,10), (10,20), (20,30))   # (lower, upper) bounds
            syms = Symbol[]
            for (i, (lo, hi)) in enumerate(bins)
                new_sym = Symbol("$(prefix)_bin$(hi)")
                if with_dummies
                    df[!, new_sym] .= passmissing(x -> (lo ≤ x < hi) ? 1 : 0).(df[!, src])
                end
                push!(syms, new_sym)
            end
            return syms
        end
    
        append!(temp_syms, add_binned_dummies!(df, hd35_col; prefix = string(hd35_col)))
        append!(temp_syms, add_binned_dummies!(df, fd_col;   prefix = string(fd_col)))
    
        #### ---------- Compute the interactions for the SPI
        spi_base  = Symbol("$(drought_ind)$(months)_$(time)_$(stat)")

        # 1.  Dummy indicators
        spi_neg_d = Symbol("$(spi_base)_neg")
        spi_pos_d = Symbol("$(spi_base)_pos")

        # 2.  Build the interaction columns
        spi_neg_x = Symbol("$(spi_base)_neg_int")   # continuous × neg-dummy
        spi_pos_x = Symbol("$(spi_base)_pos_int")   # continuous × pos-dummy

        df[!,  spi_neg_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_neg_d])
        df[!,  spi_pos_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_pos_d])

        ## Indicators of temperature

        append!(spi_syms,  (spi_neg_x,  spi_pos_x))

        return spi_syms, temp_syms
    end

    function get_symbols_hd40id(df, months, temp, drought_ind,
        time, stat, sp_threshold, model_type, with_dummies)
        """
            get_symbols_hotfrostdays(df, months, temp, drought_ind, time, stat, sp_threshold,
                        model_type::AbstractString, with_dummies::Bool)
    
        Return the vectors of Symbols that should go into the regression **and**
        (when `with_dummies == true`) make sure the corresponding interaction
        columns exist in `df`.  The routine is idempotent, so it is safe to call
        it many times inside a loop.
        """

        # Containers we will return
        spi_syms  = Symbol[] 
        temp_syms = Symbol[]

        #### ---------- Hot & frost days as dummies -------------------------------
        hd40_col = Symbol("hd40_$(time)_$(stat)")
        id_col   = Symbol("id_$(time)_$(stat)")
    
        # Helper to build the three dummies for one source column
        function add_binned_dummies!(df, src::Symbol; prefix::String)
            bins = ((1,10), (10,20), (20,30))   # (lower, upper) bounds
            syms = Symbol[]
            for (i, (lo, hi)) in enumerate(bins)
                new_sym = Symbol("$(prefix)_bin$(hi)")
                if with_dummies
                    df[!, new_sym] .= passmissing(x -> (lo ≤ x < hi) ? 1 : 0).(df[!, src])
                end
                push!(syms, new_sym)
            end
            return syms
        end
    
        append!(temp_syms, add_binned_dummies!(df, hd40_col; prefix = string(hd40_col)))
        append!(temp_syms, add_binned_dummies!(df, id_col;   prefix = string(id_col)))
    
        #### ---------- Compute the interactions for the SPI
        spi_base  = Symbol("$(drought_ind)$(months)_$(time)_$(stat)")

        # 1.  Dummy indicators
        spi_neg_d = Symbol("$(spi_base)_neg")
        spi_pos_d = Symbol("$(spi_base)_pos")

        # 2.  Build the interaction columns
        spi_neg_x = Symbol("$(spi_base)_neg_int")   # continuous × neg-dummy
        spi_pos_x = Symbol("$(spi_base)_pos_int")   # continuous × pos-dummy

        df[!,  spi_neg_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_neg_d])
        df[!,  spi_pos_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_pos_d])

        ## Indicators of temperature

        append!(spi_syms,  (spi_neg_x,  spi_pos_x))

        return spi_syms, temp_syms
    end

    function get_symbols_hd35id(df, months, temp, drought_ind,
        time, stat, sp_threshold, model_type, with_dummies)
        """
            get_symbols_hotfrostdays(df, months, temp, drought_ind, time, stat, sp_threshold,
                        model_type::AbstractString, with_dummies::Bool)
    
        Return the vectors of Symbols that should go into the regression **and**
        (when `with_dummies == true`) make sure the corresponding interaction
        columns exist in `df`.  The routine is idempotent, so it is safe to call
        it many times inside a loop.
        """

        # Containers we will return
        spi_syms  = Symbol[] 
        temp_syms = Symbol[]

        spi_base  = Symbol("$(drought_ind)$(months)_$(time)_$(stat)")
        t_base_pos    = Symbol("hd35_$(time)_$(stat)")
        t_base_neg    = Symbol("id_$(time)_$(stat)")

        ## Compute the interactions for the SPI
        # 1.  Dummy indicators
        spi_neg_d = Symbol("$(spi_base)_neg")
        spi_pos_d = Symbol("$(spi_base)_pos")

        # 2.  Build the interaction columns
        spi_neg_x = Symbol("$(spi_base)_neg_int")   # continuous × neg-dummy
        spi_pos_x = Symbol("$(spi_base)_pos_int")   # continuous × pos-dummy

        df[!,  spi_neg_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_neg_d])
        df[!,  spi_pos_x] = passmissing(Float16).(df[!, spi_base] .* df[!, spi_pos_d])

        ## Indicators of temperature

        append!(spi_syms,  (spi_neg_x,  spi_pos_x))
        append!(temp_syms, (t_base_pos, t_base_neg))

        return spi_syms, temp_syms
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
            for times in (["inutero_1m3m", "inutero_4m6m", "inutero_6m9m", "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m", "born_12m15m", "born_15m18m", "born_18m21m", "born_21m24m"], )
                i = 1
                for temp in ["stdm_t", "std_t"]#, "absdifm_t", "absdif_t",  "t"]
                    for drought_ind in ["spi"]#, "spei"]        
                        for stat in ["avg"]#, "minmax"]

                            extra_with_time = extra_original #* " - times$(i)"
                            # Linear and Quadratic models - all cases
                            stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true)
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true, symbols="hd35fd", cells=[1])
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true, symbols="hd35id", cells=[1])
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true, symbols="hd40fd", cells=[1])
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true, symbols="hd40id", cells=[1])

                            # if only_linear
                            #     continue
                            # end
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="linear", with_dummies=true, fixed_effects="quadratic_time")
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="quadratic")
                            # stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_time, model_type="quadratic", fixed_effects="quadratic_time")

                            # Spline models - only for standardized variables (std_t, stdm_t):
                            # for sp_threshold in ["1"]#, "2"]
                            #     extra_with_threshold = extra_with_time * " - spthreshold$(sp_threshold)"
                            #     stepped_regression(df, month, temp, drought_ind, controls, times, stat, sp_threshold, folder, extra_with_threshold, model_type="spline")
                            # end
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
