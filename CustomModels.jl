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

        for temp in ["std_t"]
            for ind in [("avg", "avg", "avg")]
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

        for temp in ["std_t"]
            for ind in [("avg", "avg", "avg")]
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

    function spi_temp_quadratic_regression_dummy(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 2: spi + temp models (quadratic) ###
        #################################################################
        println("\rRunning Model VERSION 3: spi + temp models (quadratic dummy) - SPI$(months)\r")

        for temp in ["std_t"]
            for ind in [("avg", "avg", "avg")]
                nameind = ind[1]
                spiind = ind[2]
                tind = ind[3]
                # Model 1 - all children
                spi_previous = [Symbol("spi$(months)_inutero_$(tind)"), Symbol("spi$(months)_inutero_$(tind)_sq_pos"), Symbol("spi$(months)_inutero_$(tind)_sq_neg")]
                temp_previous = [Symbol("$(temp)_inutero_$(tind)"), Symbol("$(temp)_inutero_$(tind)_sq_pos"), Symbol("$(temp)_inutero_$(tind)_sq_neg")]
                regs = []
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df, 
                        term(:child_agedeath_30d) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d_$(spiind)")) + term(Symbol("spi$(months)_30d_$(spiind)_sq_pos"))  + term(Symbol("spi$(months)_30d_$(spiind)_sq_neg"))+ sum(term.(temp_previous)) + term(Symbol("$(temp)_30d_$(tind)")) + term(Symbol("$(temp)_30d_$(tind)_sq_neg")) + term(Symbol("$(temp)_30d_$(tind)_sq_pos")) + sum(controls) + fixed_effects, 
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights = :hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Model 2 - only children that survived
                df_temp = filter(row -> row.child_agedeath_30d == 0, df)
                append!(spi_previous, [Symbol("spi$(months)_30d_$(spiind)"), Symbol("spi$(months)_30d_$(spiind)_sq_neg"), Symbol("spi$(months)_30d_$(spiind)_sq_pos")])
                append!(temp_previous, [Symbol("$(temp)_30d_$(tind)"), Symbol("$(temp)_30d_$(tind)_sq_neg"), Symbol("$(temp)_30d_$(tind)_sq_pos")])
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df_temp, term(:child_agedeath_2m12m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_2m12m_$(spiind)")) + term(Symbol("spi$(months)_2m12m_$(spiind)_sq_pos")) + term(Symbol("spi$(months)_2m12m_$(spiind)_sq_neg")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_2m12m_$(tind)")) + term(Symbol("$(temp)_2m12m_$(tind)_sq_neg")) + term(Symbol("$(temp)_2m12m_$(tind)_sq_pos")) +sum(controls) + fixed_effects, 
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
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 3 SPI$(months) $(nameind) $(temp) $(extra).txt", 
                    order=[
                        "spi$(months)_inutero_$(tind)", "spi$(months)_inutero_$(tind)_sq_neg", "spi$(months)_inutero_$(tind)_sq_pos", "spi$(months)_30d_$(spiind)", "spi$(months)_30d_$(spiind)_sq_neg","spi$(months)_30d_$(spiind)_sq_pos", "spi$(months)_2m12m_$(spiind)", "spi$(months)_2m12m_$(spiind)_sq_neg", "spi$(months)_2m12m_$(spiind)_sq_pos",
                        "$(temp)_inutero_$(tind)", "$(temp)_inutero_$(tind)_sq_neg", "$(temp)_inutero_$(tind)_sq_pos", "$(temp)_30d_$(tind)", "$(temp)_30d_$(tind)_sq_neg" , "$(temp)_30d_$(tind)_sq_pos", "$(temp)_2m12m_$(tind)", "$(temp)_2m12m_$(tind)_sq_neg", "$(temp)_2m12m_$(tind)_sq_pos"
                    ], 

                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 3 SPI$(months) $(nameind) $(temp) $(extra).tex", 
                    order=[
                        "spi$(months)_inutero_$(tind)", "spi$(months)_inutero_$(tind)_sq_neg", "spi$(months)_inutero_$(tind)_sq_pos", "spi$(months)_30d_$(spiind)", "spi$(months)_30d_$(spiind)_sq_neg","spi$(months)_30d_$(spiind)_sq_pos", "spi$(months)_2m12m_$(spiind)", "spi$(months)_2m12m_$(spiind)_sq_neg", "spi$(months)_2m12m_$(spiind)_sq_pos",
                        "$(temp)_inutero_$(tind)", "$(temp)_inutero_$(tind)_sq_neg", "$(temp)_inutero_$(tind)_sq_pos", "$(temp)_30d_$(tind)", "$(temp)_30d_$(tind)_sq_neg" , "$(temp)_30d_$(tind)_sq_pos", "$(temp)_2m12m_$(tind)", "$(temp)_2m12m_$(tind)_sq_neg", "$(temp)_2m12m_$(tind)_sq_pos"
                    ], 
                )

            end
        end
    end

    function spi_temp_linear_regression_dummy(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 2: spi + temp models (quadratic) ###
        #################################################################
        println("\rRunning Model VERSION 4: spi + temp models (linear dummy) - SPI$(months)\r")

        for temp in ["std_t"]
            for ind in [("avg", "avg", "avg")]
                nameind = ind[1]
                spiind = ind[2]
                tind = ind[3]
                # Model 1 - all children
                spi_previous = [Symbol("spi$(months)_inutero_$(tind)_neg"), Symbol("spi$(months)_inutero_$(tind)_pos")]
                temp_previous = [Symbol("$(temp)_inutero_$(tind)_neg"), Symbol("$(temp)_inutero_$(tind)_pos")]
                regs = []
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df, 
                        term(:child_agedeath_30d) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d_$(spiind)_neg")) + term(Symbol("spi$(months)_30d_$(spiind)_pos")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_30d_$(tind)_neg")) + term(Symbol("$(temp)_30d_$(tind)_pos")) + sum(controls) + fixed_effects, 
                        Vcov.cluster(Symbol("ID_cell$i")), 
                        weights = :hv005,
                        method=:CUDA
                    )
                    push!(regs, reg_model)
                end

                # Model 2 - only children that survived
                df_temp = filter(row -> row.child_agedeath_30d == 0, df)
                append!(spi_previous, [Symbol("spi$(months)_30d_$(spiind)_neg"), Symbol("spi$(months)_30d_$(spiind)_pos")])
                append!(temp_previous, [Symbol("$(temp)_30d_$(tind)_neg"), Symbol("$(temp)_30d_$(tind)_pos")])
                for i in 1:4
                    fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                    reg_model = reg(
                        df_temp, term(:child_agedeath_2m12m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_2m12m_$(spiind)_pos")) + term(Symbol("spi$(months)_2m12m_$(spiind)_neg")) + sum(term.(temp_previous)) + term(Symbol("$(temp)_2m12m_$(tind)_neg")) + term(Symbol("$(temp)_2m12m_$(tind)_pos")) +sum(controls) + fixed_effects, 
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
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 4 SPI$(months) $(nameind) $(temp) $(extra).txt", 
                    order=[
                        "spi$(months)_inutero_$(tind)_neg", "spi$(months)_inutero_$(tind)_pos", "spi$(months)_30d_$(spiind)_neg","spi$(months)_30d_$(spiind)_pos", "spi$(months)_2m12m_$(spiind)_neg", "spi$(months)_2m12m_$(spiind)_pos",
                        "$(temp)_inutero_$(tind)_neg", "$(temp)_inutero_$(tind)_pos", "$(temp)_30d_$(tind)_neg" , "$(temp)_30d_$(tind)_pos", "$(temp)_2m12m_$(tind)_neg", "$(temp)_2m12m_$(tind)_pos"
                    ], 

                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 4 SPI$(months) $(nameind) $(temp) $(extra).tex", 
                    order=[
                        "spi$(months)_inutero_$(tind)_neg", "spi$(months)_inutero_$(tind)_pos", "spi$(months)_30d_$(spiind)_neg","spi$(months)_30d_$(spiind)_pos", "spi$(months)_2m12m_$(spiind)_neg", "spi$(months)_2m12m_$(spiind)_pos",
                        "$(temp)_inutero_$(tind)_neg", "$(temp)_inutero_$(tind)_pos", "$(temp)_30d_$(tind)_neg" , "$(temp)_30d_$(tind)_pos", "$(temp)_2m12m_$(tind)_neg", "$(temp)_2m12m_$(tind)_pos"
                    ], 
                )

            end
        end
    end

    
    function run_models(df, controls, folder, extra)
        println("\rRunning Standard Models for $(folder)\r")
        for months in ["1", "3", "6", "9", "12"]
            CustomModels.spi_temp_linear_regression(df, months, controls, folder, extra)
            CustomModels.spi_temp_quadratic_regression(df, months, controls, folder, extra)
            CustomModels.spi_temp_quadratic_regression_dummy(df, months, controls, folder, extra)
            CustomModels.spi_temp_linear_regression_dummy(df, months, controls, folder, extra)
        end
    end

end