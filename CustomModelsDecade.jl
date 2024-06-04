module CustomModelsDecade

# Cosas:
# ----> Trend en decades en vez de año
# Effectos fijos por país
# Efecto fijo por edad

    export spi_temp_linear_regression

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

    function spi_temp_linear_regression(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 1: spi + temp models (linear)
        #################################################################
        # Model 1 - all children
        spi_previous = [Symbol("spi$(months)_q1"), Symbol("spi$(months)_q2"), Symbol("spi$(months)_q3")]
        temp_previous = [Symbol("temp_q1"), Symbol("temp_q2"), Symbol("temp_q3")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df, 
                term(:child_agedeath_30d) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d")) + sum(term.(temp_previous)) + term(Symbol("temp_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights = :hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df)
        push!(spi_previous, Symbol("spi$(months)_30d"))
        push!(temp_previous, Symbol("temp_30d"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_30d3m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d3m")) + sum(term.(temp_previous)) + term(Symbol("temp_30d3m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights = :hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 3 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
        push!(spi_previous, Symbol("spi$(months)_30d3m"))
        push!(temp_previous, Symbol("temp_30d3m"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_3m6m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_3m6m")) + sum(term.(temp_previous)) + term(Symbol("temp_3m6m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights = :hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 4 - only children that survived
        df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
        push!(spi_previous, Symbol("spi$(months)_3m6m"))
        push!(temp_previous, Symbol("temp_3m6m"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_6m12m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_6m12m")) + sum(term.(temp_previous)) + term(Symbol("temp_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
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
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 1 $months months - Decade.txt",
            order=[
                "spi$(months)_q1", "spi$(months)_q2", "spi$(months)_q3", "spi$(months)_30d", "spi$(months)_30d3m", "spi$(months)_3m6m", "spi$(months)_6m12m",
                "temp_q1", "temp_q2", "temp_q3", "temp_30d", "temp_30d3m", "temp_3m6m", "temp_6m12m",
            ], 
        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 1 $months months - Decade.tex", 
            order=[
                "spi$(months)_q1", "spi$(months)_q2", "spi$(months)_q3", "spi$(months)_30d", "spi$(months)_30d3m", "spi$(months)_3m6m", "spi$(months)_6m12m",
                "temp_q1", "temp_q2", "temp_q3", "temp_30d", "temp_30d3m", "temp_3m6m", "temp_6m12m",
            ], 
        )
    end


    function spi_temp_quadratic_regression(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 2: spi + temp models (quadratic) ###
        #################################################################

        # Model 1 - all children
        spi_previous = [Symbol("spi$(months)_q1"), Symbol("spi$(months)_q1_sq"), Symbol("spi$(months)_q2"), Symbol("spi$(months)_q2_sq"), Symbol("spi$(months)_q3"), Symbol("spi$(months)_q3_sq")]
        temp_previous = [Symbol("temp_q1"), Symbol("temp_q1_sq"), Symbol("temp_q2"), Symbol("temp_q2_sq"), Symbol("temp_q3"), Symbol("temp_q3_sq")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df, 
                term(:child_agedeath_30d) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d")) + term(Symbol("spi$(months)_30d_sq")) + sum(term.(temp_previous)) + term(Symbol("temp_30d")) + term(Symbol("temp_30d_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights = :hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df)
        append!(spi_previous, [Symbol("spi$(months)_30d"), Symbol("spi$(months)_30d_sq")])
        append!(temp_previous, [Symbol("temp_30d"), Symbol("temp_30d_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_30d3m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_30d3m")) + term(Symbol("spi$(months)_30d3m_sq")) + sum(term.(temp_previous)) + term(Symbol("temp_30d3m")) + term(Symbol("temp_30d3m_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                 Vcov.cluster(Symbol("ID_cell$i")), 
                 weights = :hv005,
                 method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 3 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
        append!(spi_previous, [Symbol("spi$(months)_30d3m"), Symbol("spi$(months)_30d3m_sq")])
        append!(temp_previous, [Symbol("temp_30d3m"), Symbol("temp_30d3m_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_3m6m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_3m6m")) + term(Symbol("spi$(months)_3m6m_sq")) + sum(term.(temp_previous)) + term(Symbol("temp_3m6m")) + term(Symbol("temp_3m6m_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$(i)")), 
                weights=:hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 4 - only children that survived
        df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
        append!(spi_previous, [Symbol("spi$(months)_3m6m"), Symbol("spi$(months)_3m6m_sq")])
        append!(temp_previous, [Symbol("temp_3m6m"), Symbol("temp_3m6m_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, term(:child_agedeath_6m12m) ~ sum(term.(spi_previous)) + term(Symbol("spi$(months)_6m12m")) + term(Symbol("spi$(months)_6m12m_sq")) + sum(term.(temp_previous)) + term(Symbol("temp_6m12m")) + term(Symbol("temp_6m12m_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
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
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 2 $months months - Decade.txt", 
            order=[
                "spi$(months)_q1", "spi$(months)_q1_sq", "spi$(months)_q2", "spi$(months)_q2_sq", "spi$(months)_q3", "spi$(months)_q3_sq", "spi$(months)_30d", "spi$(months)_30d_sq", "spi$(months)_30d3m", "spi$(months)_30d3m_sq", "spi$(months)_3m6m", "spi$(months)_3m6m_sq", "spi$(months)_6m12m", "spi$(months)_6m12m_sq",
                "temp_q1", "temp_q1_sq", "temp_q2", "temp_q2_sq", "temp_q3", "temp_q3_sq", "temp_30d", "temp_30d_sq", "temp_30d3m", "temp_30d3m_sq", "temp_3m6m", "temp_3m6m_sq", "temp_6m12m", "temp_6m12m_sq"
            ], 

        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 2 $months months - Decade.tex", 
        )

    end

    function drought_excessiverain_regression(df, months, threshold, controls, folder, extra)
        
        #################################################################
        ### VERSION 4: Droughts/Excessive Rain + linear Temp          ###
        #################################################################
        # Model 1 - all children
        regs = []
        drought = "drought$(months)_$(threshold)"
        excessiverain = "excessiverain$(months)_$(threshold)"
        drought_previous = [Symbol("$(drought)_q1"), Symbol("$(drought)_q2"), Symbol("$(drought)_q3")]
        excessiverain_previous = [Symbol("$(excessiverain)_q1"), Symbol("$(excessiverain)_q2"), Symbol("$(excessiverain)_q3")]
        for i in 1:4
            reg_model = reg(
                df, 
                term(:child_agedeath_30d) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights=:hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df)
        push!(drought_previous, Symbol("$(drought)_30d"))
        push!(excessiverain_previous, Symbol("$(excessiverain)_30d"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_30d3m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d3m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d3m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights=:hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 3 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
        push!(drought_previous, Symbol("$(drought)_30d3m"))
        push!(excessiverain_previous, Symbol("$(excessiverain)_30d3m"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_3m6m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_3m6m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_3m6m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights=:hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 4 - only children that survived
        df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
        push!(drought_previous, Symbol("$(drought)_3m6m"))
        push!(excessiverain_previous, Symbol("$(excessiverain)_3m6m"))        
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_6m12m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_6m12m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
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
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 3 drought $threshold $months months - Decade.txt", 
            order=[
                "$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(drought)_3m6m", "$(drought)_6m12m", 
                "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"
            ]
        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 3 drought $threshold $months months - Decade.tex", 
            order=[
                "$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(drought)_3m6m", "$(drought)_6m12m", 
                "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"
            ]
        )
    end

    function drought_excessiverain_temp_regression(df, months, threshold, controls, folder, extra)
        #################################################################
        ### VERSION 4: Droughts/Excessive Rain +  linear temp         ###
        #################################################################
        # Model 1 - all children
        regs = []
        drought = "drought$(months)_$(threshold)"
        excessiverain = "excessiverain$(months)_$(threshold)"
        drought_previous = [Symbol("$(drought)_q1"), Symbol("$(drought)_q2"), Symbol("$(drought)_q3")]
        excessiverain_previous = [Symbol("$(excessiverain)_q1"), Symbol("$(excessiverain)_q2"), Symbol("$(excessiverain)_q3")]
        temp_previous = [Symbol("temp_q1"), Symbol("temp_q2"), Symbol("temp_q3")]

        for i in 1:4
            reg_model = reg(
                df, 
                term(:child_agedeath_30d) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d")) + sum(term.(temp_previous)) + term(Symbol("temp_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights=:hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df)
        push!(drought_previous, Symbol("$(drought)_30d"))
        push!(excessiverain_previous, Symbol("$(excessiverain)_30d"))
        push!(temp_previous, Symbol("temp_30d"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_30d3m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d3m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d3m"))+ sum(term.(temp_previous)) + term(Symbol("temp_30d3m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights=:hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 3 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
        push!(drought_previous, Symbol("$(drought)_30d3m"))
        push!(excessiverain_previous, Symbol("$(excessiverain)_30d3m"))
        push!(temp_previous, Symbol("temp_30d3m"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_3m6m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_3m6m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_3m6m")) + sum(term.(temp_previous)) + term(Symbol("temp_3m6m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
                weights=:hv005,
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 4 - only children that survived
        df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
        push!(drought_previous, Symbol("$(drought)_3m6m"))
        push!(excessiverain_previous, Symbol("$(excessiverain)_3m6m"))    
        push!(temp_previous, Symbol("temp_3m6m"))    
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_6m12m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_6m12m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_6m12m")) + sum(term.(temp_previous)) + term(Symbol("temp_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:decade) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
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
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 4 drought $threshold $months months - Decade.txt", 
            order=[
                "$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(drought)_3m6m", "$(drought)_6m12m", 
                "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m",
                "temp_q1", "temp_q2", "temp_q3", "temp_30d", "temp_30d3m", "temp_3m6m", "temp_6m12m",
            ]
        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 4 drought $threshold $months months - Decade.tex", 
            order=[
                "$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(drought)_3m6m", "$(drought)_6m12m", 
                "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m",
                "temp_q1", "temp_q2", "temp_q3", "temp_30d", "temp_30d3m", "temp_3m6m", "temp_6m12m",   
            ]
        )
    end

    
    function run_models(df, controls, folder, extra)
        print("\rRunning Models with decade trend FE for $(folder)$(extra)\r")
        for months in ["12", "6", "3", "1"]
            spi_temp_linear_regression(df, months, controls, folder, extra)
        end

        for months in ["12", "6", "3", "1"]
            spi_temp_quadratic_regression(df, months, controls, folder, extra)
        end

        for months in ["12", "6", "3", "1"]
            for threshold in ["1_5", "2_0", "2_5"]
                drought_excessiverain_regression(df, months, threshold, controls, folder, extra)
                drought_excessiverain_temp_regression(df, months, threshold, controls, folder, extra)
            end
        end
    end

end