module CustomModels

# Cosas:
# Trend en decades en vez de año ----> CustonModelsDecade
# Effectos fijos por país
# Efecto fijo por edad

    export spi_temp_linear_regression

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

    function spi_temp_linear_regression(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 1: spi + temp models (linear)
        #################################################################
        println("\rRunning Model VERSION 1: spi + temp models (linear) - SPI$(months)\r")

        for temp in ["t", "std_t", "stdm_t"]
            for ind in [("min-max", "min", "max"), ("avg", "avg", "avg")]
                nameind = ind[1]
                spiind = ind[2]
                tind = ind[3]
                spi_previous = [Symbol("spi$(months)_inutero_$(spiind)")] # FIXME: Revisar que no me esté cruzando min y max!!
                temp_previous = [Symbol("$(temp)_inutero_$(tind)")]
                regs = []
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df, 
                        term(:child_agedeath_30d) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d_$(spiind)")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_30d_$(tind)")) + sum(controls) + fixed_effects, 
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights = :hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Model 2 - only children that survived
                df_temp = filter(row -> row.child_agedeath_30d == 0, df)
                push!(spi_previous, Symbol("spi$(months)_30d_$(spiind)"))
                push!(temp_previous, Symbol("$(temp)_30d_$(tind)"))
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df_temp, 
                        term(:child_agedeath_2m12m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_2m12m_$(spiind)")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_2m12m_$(tind)")) + sum(controls) + fixed_effects, 
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights = :hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Generate regression table
                regtable(
                    regs...; 
                    render = AsciiTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 1 SPI$months $(nameind) $(temp) $(extra).txt",
                    order=[
                        "spi$(months)_inutero_$(spiind)", "spi$(months)_30d_$(spiind)", "spi$(months)_2m12m_$(spiind)",
                        "$(temp)_inutero_$(tind)", "$(temp)_30d_$(tind)", "$(temp)_2m12m_$(tind)",
                    ], 
                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 1 SPI$months $(nameind) $(temp) $(extra).tex", 
                    order=[
                        "spi$(months)_inutero_$(spiind)", "spi$(months)_30d_$(spiind)", "spi$(months)_2m12m_$(spiind)",
                        "$(temp)_inutero_$(tind)", "$(temp)_30d_$(tind)", "$(temp)_2m12m_$(tind)",
                    ], 
                )
            end
        end
    end

    function spi_temp_quadratic_regression(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 2: spi + temp models (quadratic) ###
        #################################################################
        println("\rRunning Model VERSION 2: spi + temp models (quadratic) - SPI$(months)\r")

        for temp in ["t", "std_t", "stdm_t"]
            for ind in [("min-max", "min", "max"), ("avg", "avg", "avg")]
                nameind = ind[1]
                spiind = ind[2]
                tind = ind[3]
                # Model 1 - all children
                spi_previous = [Symbol("spi$(months)_inutero_$(tind)"), Symbol("spi$(months)_inutero_$(tind)_sq")]
                temp_previous = [Symbol("$(temp)_inutero_$(tind)"), Symbol("$(temp)_inutero_$(tind)_sq")]
                regs = []
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df, 
                        term(:child_agedeath_30d) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d_$(spiind)")) + term(Symbol("spi$(months)_30d_$(spiind)_sq")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_30d_$(tind)")) + term(Symbol("$(temp)_30d_$(tind)_sq")) + sum(controls) + fixed_effects, 
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights = :hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Model 2 - only children that survived
                df_temp = filter(row -> row.child_agedeath_30d == 0, df)
                append!(spi_previous, [Symbol("spi$(months)_30d_$(spiind)"), Symbol("spi$(months)_30d_$(spiind)_sq")])
                append!(temp_previous, [Symbol("$(temp)_30d_$(tind)"), Symbol("$(temp)_30d_$(tind)_sq")])
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df_temp, term(:child_agedeath_2m12m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_2m12m_$(spiind)")) + term(Symbol("spi$(months)_2m12m_$(spiind)_sq")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_2m12m_$(tind)")) + term(Symbol("$(temp)_2m12m_$(tind)_sq")) + sum(controls) + fixed_effects, 
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights=:hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Generate regression table
                regtable(
                    regs...; 
                    render = AsciiTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 2 SPI$(months) $(nameind) $(temp) $(extra).txt", 
                    order=[
                        "spi$(months)_inutero_$(tind)", "spi$(months)_inutero_$(tind)_sq", "spi$(months)_30d_$(spiind)", "spi$(months)_30d_$(spiind)_sq", "spi$(months)_2m12m_$(spiind)", "spi$(months)_2m12m_$(spiind)_sq",
                        "$(temp)_inutero_$(tind)", "$(temp)_inutero_$(tind)_sq", "$(temp)_30d_$(tind)", "$(temp)_30d_$(tind)_sq", "$(temp)_2m12m_$(tind)", "$(temp)_2m12m_$(tind)_sq"
                    ], 

                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 2 SPI$(months) $(nameind) $(temp) $(extra).tex", 
                    order=[
                        "spi$(months)_inutero_$(tind)", "spi$(months)_inutero_$(tind)_sq", "spi$(months)_30d_$(spiind)", "spi$(months)_30d_$(spiind)_sq", "spi$(months)_2m12m_$(spiind)", "spi$(months)_2m12m_$(spiind)_sq",
                        "$(temp)_inutero_$(tind)", "$(temp)_inutero_$(tind)_sq", "$(temp)_30d_$(tind)", "$(temp)_30d_$(tind)_sq", "$(temp)_2m12m_$(tind)", "$(temp)_2m12m_$(tind)_sq"
                    ], 
                )

            end
        end
    end
    function drought_excessiverain_regression(df, months, threshold, controls, folder, extra)
        
        #################################################################
        ### VERSION 3: Droughts/Excessive Rain                        ###
        #################################################################
        println("\rRunning Model VERSION 3: Droughts/Excessive Rain  - SPI$(months) $(threshold)std \r")

        for ind in [("min-max", "min", "max", "max"), ("avg", "avg", "avg", "avg")]
            nameind = ind[1]
            drind = ind[2]
            erind = ind[3]
            tind = ind[4]

            regs = []
            drought = "drought$(months)_$(threshold)"
            excessiverain = "excessiverain$(months)_$(threshold)"
            drought_previous = [Symbol("$(drought)_inutero_$(drind)")]
            excessiverain_previous = [Symbol("$(excessiverain)_inutero_$(erind)")]
            for i in 1:4
                fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                reg_model = reg(
                    df, 
                    term(:child_agedeath_30d) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d_$(drind)")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d_$(erind)")) + sum(controls) + fixed_effects, 
                    Vcov.cluster(Symbol("ID_cell$i")), 
                    weights=:hv005,
                    method=:CUDA
                )
                push!(regs, reg_model)
            end

            # Model 2 - only children that survived
            df_temp = filter(row -> row.child_agedeath_30d == 0, df)
            push!(drought_previous, Symbol("$(drought)_30d_$(drind)"))
            push!(excessiverain_previous, Symbol("$(excessiverain)_30d_$(erind)"))
            for i in 1:4
                fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                reg_model = reg(
                    df_temp, 
                    term(:child_agedeath_2m12m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_2m12m_$(drind)")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_2m12m_$(erind)")) + sum(controls) + fixed_effects,
                    Vcov.cluster(Symbol("ID_cell$i")), 
                    weights=:hv005,
                    method=:CUDA
                )
                push!(regs, reg_model)
            end

            # Generate regression table
            regtable(
                regs...; 
                render = AsciiTable(), 
                file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 3 drought SPI$(months) $(threshold) $(nameind) $(extra).txt", 
                order=[
                    "$(drought)_inutero_$(drind)", "$(drought)_30d_$(drind)", "$(drought)_2m12m_$(drind)", 
                    "$(excessiverain)_inutero_$(erind)", "$(excessiverain)_30d_$(erind)", "$(excessiverain)_2m12m_$(erind)"
                ]
            )
            regtable(
                regs...; 
                render = LatexTable(), 
                file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 3 drought SPI$(months) $(threshold) $(nameind) $(extra).tex", 
                order=[
                    "$(drought)_inutero_$(drind)", "$(drought)_30d_$(drind)", "$(drought)_2m12m_$(drind)", 
                    "$(excessiverain)_inutero_$(erind)", "$(excessiverain)_30d_$(erind)", "$(excessiverain)_2m12m_$(erind)"
                ]
            )
        end
    end

    function drought_excessiverain_temp_regression(df, months, threshold, controls, folder, extra)
        #################################################################
        ### VERSION 4: Droughts/Excessive Rain +  linear temp         ###
        #################################################################
        println("\rRunning Model VERSION 4: Droughts/Excessive Rain + linear Temp - SPI$(months) $(threshold)std \r")

        for temp in ["t", "std_t", "stdm_t"]
            for ind in [("min-max", "min", "max", "max"), ("avg", "avg", "avg", "avg")]
                nameind = ind[1]
                drind = ind[2]
                erind = ind[3]
                tind = ind[4]
    
                regs = []
                drought = "drought$(months)_$(threshold)"
                excessiverain = "excessiverain$(months)_$(threshold)"
                drought_previous = [Symbol("$(drought)_inutero_$(drind)")]
                excessiverain_previous = [Symbol("$(excessiverain)_inutero_$(erind)")]
                temp_previous = [Symbol("$(temp)_inutero_$(tind)")]

                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df, 
                        term(:child_agedeath_30d) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d_$(drind)")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d_$(erind)")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_30d_$(tind)")) + sum(controls) + fixed_effects, 
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights=:hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Model 2 - only children that survived
                df_temp = filter(row -> row.child_agedeath_30d == 0, df)
                push!(drought_previous, Symbol("$(drought)_30d_$(drind)"))
                push!(excessiverain_previous, Symbol("$(excessiverain)_30d_$(erind)"))
                push!(temp_previous, Symbol("$(temp)_30d_$(tind)"))
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df_temp, 
                        term(:child_agedeath_2m12m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_2m12m_$(drind)")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_2m12m_$(erind)")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_2m12m_$(tind)")) + sum(controls) + fixed_effects,
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights=:hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Generate regression table
                regtable(
                    regs...; 
                    render = AsciiTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 4 drought SPI$(months) $(threshold) $(nameind) $(temp) $(extra).txt", 
                    order=[
                        "$(drought)_inutero_$(drind)", "$(drought)_30d_$(drind)", "$(drought)_2m12m_$(drind)", 
                        "$(excessiverain)_inutero_$(erind)", "$(excessiverain)_30d_$(erind)", "$(excessiverain)_2m12m_$(erind)",
                        "$(temp)_inutero_$(tind)", "$(temp)_30d_$(tind)", "$(temp)_2m12m_$(tind)",
                    ]
                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 4 drought SPI$(months) $(threshold) $(nameind) $(temp) $(extra).tex", 
                    order=[
                        "$(drought)_inutero_$(drind)", "$(drought)_30d_$(drind)", "$(drought)_2m12m_$(drind)", 
                        "$(excessiverain)_inutero_$(erind)", "$(excessiverain)_30d_$(erind)", "$(excessiverain)_2m12m_$(erind)",
                        "$(temp)_inutero_$(tind)", "$(temp)_30d_$(tind)", "$(temp)_2m12m_$(tind)",   
                    ]
                )
            end
        end
    end

    
    function run_models(df, controls, folder, extra)
        println("\rRunning Standard Models for $(folder)\r")
        for months in ["12", "6", "3", "1"]
            CustomModels.spi_temp_linear_regression(df, months, controls, folder, extra)
            CustomModels.spi_temp_quadratic_regression(df, months, controls, folder, extra)
            for threshold in ["1_5", "2_0", "2_5"]
                CustomModels.drought_excessiverain_regression(df, months, threshold, controls, folder, extra)
                CustomModels.drought_excessiverain_temp_regression(df, months, threshold, controls, folder, extra)
            end
        end
    end

end