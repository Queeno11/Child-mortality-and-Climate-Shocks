using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

# Load the data
print("Cargando dataset...")
df = CSV.read("Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks.csv", DataFrame)
df.child_agedeath_30d = df.child_agedeath_30d .* 1000
df.child_agedeath_30d3m = df.child_agedeath_30d3m .* 1000
df.child_agedeath_3m6m = df.child_agedeath_3m6m .* 1000
df.child_agedeath_6m12m = df.child_agedeath_6m12m .* 1000
rename!(df,:ID_cell => :ID_cell1)
print("Dataset cargado!")

controls = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub])
# * " + " * mother_ageb_formula  
# * " + " * mother_eduy_formula



#################################################################
### VERSION 1: Precipitation models (linear precipitation)
#################################################################


for months in ["_12", "_6", "_3", ""]
    # Model 1 - all children
    println("Running Version 1 for $months months...")
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
        renderSettings = AsciiTable(), 
        file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 1 $months months.txt",
        order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
    )
    regtable(
        regs...; 
        renderSettings = LatexTable(), 
        file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 1 $months months.tex", 
        order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
    )

end



# #################################################################
# ### VERSION 2: Precipitation models (quadratic precipitation) ###
# #################################################################

for months in ["_12", "_6", "_3", ""]
    # Model 1 - all children
    println("Running Model Version 2 for $months...")

    df[!, Symbol("prec$(months)_q1_sq")]   = df[!, Symbol("prec$(months)_q1")]    .*  df[!, Symbol("prec$(months)_q1")]
    df[!, Symbol("prec$(months)_q2_sq")]   = df[!, Symbol("prec$(months)_q2")]    .*  df[!, Symbol("prec$(months)_q2")]
    df[!, Symbol("prec$(months)_q3_sq")]   = df[!, Symbol("prec$(months)_q3")]    .*  df[!, Symbol("prec$(months)_q3")]
    df[!, Symbol("prec$(months)_30d_sq")]  = df[!, Symbol("prec$(months)_30d")]   .*  df[!, Symbol("prec$(months)_30d")]
    df[!, Symbol("prec$(months)_30d3m_sq")]= df[!, Symbol("prec$(months)_30d3m")] .*  df[!, Symbol("prec$(months)_30d3m")]
    df[!, Symbol("prec$(months)_3m6m_sq")] = df[!, Symbol("prec$(months)_3m6m")]  .*  df[!, Symbol("prec$(months)_3m6m")]
    df[!, Symbol("prec$(months)_6m12m_sq")]= df[!, Symbol("prec$(months)_6m12m")] .*  df[!, Symbol("prec$(months)_6m12m")]

    prec_previous = [Symbol("prec$(months)_q1"), Symbol("prec$(months)_q1_sq"), Symbol("prec$(months)_q2"), Symbol("prec$(months)_q2_sq"), Symbol("prec$(months)_q3"), Symbol("prec$(months)_q3_sq")]
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
    append!(prec_previous, [Symbol("prec$(months)_30d"), Symbol("prec$(months)_30d_sq")])
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
    append!(prec_previous, [Symbol("prec$(months)_30d3m"), Symbol("prec$(months)_30d3m_sq")])
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
    append!(prec_previous, [Symbol("prec$(months)_3m6m"), Symbol("prec$(months)_3m6m_sq")])
    for i in 1:4
        reg_model = reg(
            df_temp, term(:child_agedeath_6m12m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
            Vcov.cluster(Symbol("ID_cell$i")), 
            method=:CUDA
        )
        push!(regs, reg_model)
    end

    # Generate regression table
    append!(prec_previous, [Symbol("prec$(months)_6m12m"), Symbol("prec$(months)_6m12m_sq")])
    regtable(
        regs...; 
        renderSettings = AsciiTable(), 
        file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 2 $months months.txt", 
        order=["prec$(months)_q1", "prec$(months)_q1_sq", "prec$(months)_q2", "prec$(months)_q2_sq", "prec$(months)_q3", "prec$(months)_q3_sq", "prec$(months)_30d", "prec$(months)_30d_sq", "prec$(months)_30d3m", "prec$(months)_30d3m_sq", "prec$(months)_3m6m", "prec$(months)_3m6m_sq", "prec$(months)_6m12m", "prec$(months)_6m12m_sq"], 

    )
    regtable(
        regs...; 
        renderSettings = LatexTable(), 
        file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 2 $months months.tex", 
    )

end

#################################################################
### VERSION 3: Droughts/Excessive Rain                        ###
#################################################################


for months in ["_12", "_6", "_3", ""]
    regs = []
    for threshold in ["2_5", "3_0", "3_5"]
        # Model 1 - all children
        println("Running Model Version 3 for $threshold $months...")

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
            renderSettings = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 3 drought $threshold $months months.txt", 
            order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
        )
        regtable(
            regs...; 
            renderSettings = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 3 drought $threshold $months months.tex", 
            order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
        )
    end
end


#################################################################
###  All by income
#################################################################

incomegroup = unique(df.wbincomegroup)

# Create a progress bar
prog = Progress(length(incomegroup), 1)

# Loop over unique values
for incomegroup in incomegroup
    next!(prog) # Update progress bar

    df_incomegroup = filter(row -> row.wbincomegroup == incomegroup, df)

    for months in ["_12", "_6", "_3", ""]
        # Model 1 - all children
        println("Running Version 1 for $months months...")
        prec_previous = [Symbol("prec$(months)_q1"), Symbol("prec$(months)_q2"), Symbol("prec$(months)_q3")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df_incomegroup, 
                term(:child_agedeath_30d) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df_incomegroup)
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
        push!(prec_previous, Symbol("prec$(months)_6m12m"))
        regtable(
            regs...; 
            renderSettings = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\income_group\\$incomegroup - version 1 $months months.txt",
            order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
        )
        regtable(
            regs...; 
            renderSettings = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\income_group\\$incomegroup - version 1 $months months.tex", 
            order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
        )

    end



    #################################################################
    ### VERSION 2: Precipitation models (quadratic precipitation) ###
    #################################################################

    for months in ["_12", "_6", "_3", ""]
        # Model 1 - all children
        println("Running Model Version 2 for $months...")

        prec_previous = [Symbol("prec$(months)_q1"), Symbol("prec$(months)_q1_sq"), Symbol("prec$(months)_q2"), Symbol("prec$(months)_q2_sq"), Symbol("prec$(months)_q3"), Symbol("prec$(months)_q3_sq")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df_incomegroup, 
                term(:child_agedeath_30d) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df_incomegroup)
        append!(prec_previous, [Symbol("prec$(months)_30d"), Symbol("prec$(months)_30d_sq")])
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
        append!(prec_previous, [Symbol("prec$(months)_30d3m"), Symbol("prec$(months)_30d3m_sq")])
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
        append!(prec_previous, [Symbol("prec$(months)_3m6m"), Symbol("prec$(months)_3m6m_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, term(:child_agedeath_6m12m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Generate regression table
        append!(prec_previous, [Symbol("prec$(months)_6m12m"), Symbol("prec$(months)_6m12m_sq")])
        regtable(
            regs...; 
            renderSettings = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\income_group\\$incomegroup - version 2 $months months.txt", 
            order=["prec$(months)_q1", "prec$(months)_q1_sq", "prec$(months)_q2", "prec$(months)_q2_sq", "prec$(months)_q3", "prec$(months)_q3_sq", "prec$(months)_30d", "prec$(months)_30d_sq", "prec$(months)_30d3m", "prec$(months)_30d3m_sq", "prec$(months)_3m6m", "prec$(months)_3m6m_sq", "prec$(months)_6m12m", "prec$(months)_6m12m_sq"], 

        )
        regtable(
            regs...; 
            renderSettings = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\income_group\\$incomegroup - version 2 $months months.tex", 
        )

    end

    #################################################################
    ### VERSION 3: Droughts/Excessive Rain                        ###
    #################################################################


    for months in ["_12", "_6", "_3", ""]
        for threshold in ["2_5", "3_0", "3_5", "4_0"]
            regs = []
            # Model 1 - all children
            println("Running Model Version 3 for $threshold $months...")

            drought = "drought$(months)_$(threshold)"
            excessiverain = "excessiverain$(months)_$(threshold)"
            drought_previous = [Symbol("$(drought)_q1"), Symbol("$(drought)_q2"), Symbol("$(drought)_q3")]
            excessiverain_previous = [Symbol("$(excessiverain)_q1"), Symbol("$(excessiverain)_q2"), Symbol("$(excessiverain)_q3")]
            for i in 1:4
                reg_model = reg(
                    df_incomegroup, 
                    term(:child_agedeath_30d) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                    Vcov.cluster(Symbol("ID_cell$i")), 
                    method=:CUDA
                )
                push!(regs, reg_model)
            end

            # Model 2 - only children that survived
            df_temp = filter(row -> row.child_agedeath_30d == 0, df_incomegroup)
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
                renderSettings = AsciiTable(), 
                file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\income_group\\$incomegroup - version 3 drought $threshold $months months.txt", 
                order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
            )
            regtable(
                regs...; 
                renderSettings = LatexTable(), 
                file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\income_group\\$incomegroup - version 3 drought $threshold $months months.tex", 
                order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
            )
        end
    end
        
end




#################################################################
###  All by countries
#################################################################

countries = unique(df.code_iso3)

# Create a progress bar
prog = Progress(length(countries), 1)

# Loop over unique values
for country in countries
    next!(prog) # Update progress bar

    df_country = filter(row -> row.code_iso3 == country, df)

    for months in ["_12", "_6", "_3", ""]
        # Model 1 - all children
        println("Running Version 1 for $months months...")
        prec_previous = [Symbol("prec$(months)_q1"), Symbol("prec$(months)_q2"), Symbol("prec$(months)_q3")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df_country, 
                term(:child_agedeath_30d) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df_country)
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
        push!(prec_previous, Symbol("prec$(months)_6m12m"))
        regtable(
            regs...; 
            renderSettings = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\countries\\$country - version 1 $months months.txt",
            order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
        )
        regtable(
            regs...; 
            renderSettings = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\countries\\$country - version 1 $months months.tex", 
            order=["prec$(months)_q1", "prec$(months)_q2", "prec$(months)_q3", "prec$(months)_30d", "prec$(months)_30d3m", "prec$(months)_3m6m", "prec$(months)_6m12m"], 
        )

    end



    #################################################################
    ### VERSION 2: Precipitation models (quadratic precipitation) ###
    #################################################################

    for months in ["_12", "_6", "_3", ""]
        # Model 1 - all children
        println("Running Model Version 2 for $months...")

        prec_previous = [Symbol("prec$(months)_q1"), Symbol("prec$(months)_q1_sq"), Symbol("prec$(months)_q2"), Symbol("prec$(months)_q2_sq"), Symbol("prec$(months)_q3"), Symbol("prec$(months)_q3_sq")]
        regs = []
        for i in 1:4
            reg_model = reg(
                df_country, 
                term(:child_agedeath_30d) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Model 2 - only children that survived
        df_temp = filter(row -> row.child_agedeath_30d == 0, df_country)
        append!(prec_previous, [Symbol("prec$(months)_30d"), Symbol("prec$(months)_30d_sq")])
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
        append!(prec_previous, [Symbol("prec$(months)_30d3m"), Symbol("prec$(months)_30d3m_sq")])
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
        append!(prec_previous, [Symbol("prec$(months)_3m6m"), Symbol("prec$(months)_3m6m_sq")])
        for i in 1:4
            reg_model = reg(
                df_temp, term(:child_agedeath_6m12m) ~ sum(term.(prec_previous)) + term(Symbol("prec$(months)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                Vcov.cluster(Symbol("ID_cell$i")), 
                method=:CUDA
            )
            push!(regs, reg_model)
        end

        # Generate regression table
        append!(prec_previous, [Symbol("prec$(months)_6m12m"), Symbol("prec$(months)_6m12m_sq")])
        regtable(
            regs...; 
            renderSettings = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\countries\\$country - version 2 $months months.txt", 
            order=["prec$(months)_q1", "prec$(months)_q1_sq", "prec$(months)_q2", "prec$(months)_q2_sq", "prec$(months)_q3", "prec$(months)_q3_sq", "prec$(months)_30d", "prec$(months)_30d_sq", "prec$(months)_30d3m", "prec$(months)_30d3m_sq", "prec$(months)_3m6m", "prec$(months)_3m6m_sq", "prec$(months)_6m12m", "prec$(months)_6m12m_sq"], 

        )
        regtable(
            regs...; 
            renderSettings = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\countries\\$country - version 2 $months months.tex", 
        )

    end

    #################################################################
    ### VERSION 3: Droughts/Excessive Rain                        ###
    #################################################################


    for months in ["_12", "_6", "_3", ""]
        regs = []
        for threshold in ["2_5", "3_0", "3_5", "4_0"]
            # Model 1 - all children
            println("Running Model Version 3 for $threshold $months...")

            drought = "drought$(months)_$(threshold)"
            excessiverain = "excessiverain$(months)_$(threshold)"
            drought_previous = [Symbol("$(drought)_q1"), Symbol("$(drought)_q2"), Symbol("$(drought)_q3")]
            excessiverain_previous = [Symbol("$(excessiverain)_q1"), Symbol("$(excessiverain)_q2"), Symbol("$(excessiverain)_q3")]
            for i in 1:4
                reg_model = reg(
                    df_country, 
                    term(:child_agedeath_30d) ~ sum(term.(drought_previous)) + term(Symbol("$(drought)_30d")) + sum(term.(excessiverain_previous)) + term(Symbol("$(excessiverain)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), 
                    Vcov.cluster(Symbol("ID_cell$i")), 
                    method=:CUDA
                )
                push!(regs, reg_model)
            end

            # Model 2 - only children that survived
            df_temp = filter(row -> row.child_agedeath_30d == 0, df_country)
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

        end
        # Generate regression table
        regtable(
            regs...; 
            renderSettings = AsciiTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\countries\\$country - version 3 drought $months months.txt", 
            order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
        )
        regtable(
            regs...; 
            renderSettings = LatexTable(), 
            file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\countries\\$country - version 3 drought $months months.tex", 
            order=["$(drought)_q1", "$(drought)_q2", "$(drought)_q3", "$(drought)_30d", "$(drought)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m", "$(excessiverain)_q1", "$(excessiverain)_q2", "$(excessiverain)_q3", "$(excessiverain)_30d", "$(excessiverain)_30d3m", "$(excessiverain)_3m6m", "$(excessiverain)_6m12m"]
        )
    end

end


