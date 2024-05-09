module CustomModels

    export precipitation_linear_regression

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

    function precipitation_linear_regression(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 1: Precipitation models (linear precipitation)
        #################################################################
        # Model 1 - all children
        prec_previous = [Symbol("prec$(months)_q1"), Symbol("prec$(months)_q2"), Symbol("prec$(months)_q3")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df, 
                term(:child_agedeath_30d) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df)
        push!(prec_previous, Symbol("prec$(months)_30d"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_30d3m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d3m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 3 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
        push!(prec_previous, Symbol("prec$(months)_30d3m"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_3m6m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_3m6m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 4 - only children that survived
        df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
        push!(prec_previous, Symbol("prec$(months)_3m6m"))
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_6m12m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Generate regression table
        regtable(
            regs...; 
            render = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 1 $months months.txt",
            order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 1 $months months.tex", 
            order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
        )
    end


    function precipitation_quadratic_regression(df, months, controls, folder, extra)
        #################################################################
        ### VERSION 2: Precipitation models (quadratic precipitation) ###
        #################################################################

        # Model 1 - all children
        prec_previous = [Symbol("prec$(months)_q1"), Symbol("prec$(months)_q1_sq"), Symbol("prec$(months)_q2"), Symbol("prec$(months)_q2_sq"), Symbol("prec$(months)_q3"), Symbol("prec$(months)_q3_sq")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df, 
                term(:child_agedeath_30d) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d")) + term(Symbol("prec$(months)_30d_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df)
        append!(prec_previous, [Symbol("prec$(months)_30d"), Symbol("prec$(months)_30d_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_30d3m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d3m")) + term(Symbol("prec$(months)_30d3m_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                 Vcov.cluster(Symbol("ID_cell$i")), 
                 method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 3 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
        append!(prec_previous, [Symbol("prec$(months)_30d3m"), Symbol("prec$(months)_30d3m_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, 
                term(:child_agedeath_3m6m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_3m6m")) + term(Symbol("prec$(months)_3m6m_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 4 - only children that survived
        df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
        append!(prec_previous, [Symbol("prec$(months)_3m6m"), Symbol("prec$(months)_3m6m_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, term(:child_agedeath_6m12m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_6m12m")) + term(Symbol("prec$(months)_6m12m_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Generate regression table
        regtable(
            regs...; 
            render = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 2 $months months.txt", 
            order=["prec$(months)_q1", "prec$(months)_q1_sq", "prec$(months)_q2", "prec$(months)_q2_sq", "prec$(months)_q3", "prec$(months)_q3_sq", "prec$(months)_30d", "prec$(months)_30d_sq", "prec$(months)_30d3m", "prec$(months)_30d3m_sq", "prec$(months)_3m6m", "prec$(months)_3m6m_sq", "prec$(months)_6m12m", "prec$(months)_6m12m_sq"], 

        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 2 $months months.tex", 
        )

    end


    function drought_excessiverain_regression(df, months, threshold, controls, folder, extra)
        #################################################################
        ### VERSION 3: Droughts/Excessive Rain                        ###
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
                term(:child_agedeath_30d) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
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
                term(:child_agedeath_30d3m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d3m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d3m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
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
                term(:child_agedeath_3m6m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_3m6m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_3m6m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
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
                term(:child_agedeath_6m12m) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_6m12m")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month),
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Generate regression table
        regtable(
            regs...; 
            render = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 3 drought $threshold $months months.txt", 
            order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
        )
        regtable(
            regs...; 
            render = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)$(extra)version 3 drought $threshold $months months.tex", 
            order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
        )
    end

    function run_models(df, controls, folder, extra)
        # for months in ["_12", "_6", "_3", ""]
        #     CustomModels.precipitation_linear_regression(df, months, controls, folder, extra)
        # end

        for months in ["_12", "_6", "_3", ""]
            CustomModels.precipitation_quadratic_regression(df, months, controls, folder, extra)
        end

        # for months in ["_12", "_6", "_3", ""]
        #     for threshold in ["2_5", "3_0", "3_5"]
        #         CustomModels.drought_excessiverain_regression(df, months, threshold, controls, folder, extra)
        #     end
        # end
    end

end