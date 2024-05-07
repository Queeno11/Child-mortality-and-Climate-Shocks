using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA

# Load the data
println("Cargando dataset...")
df = CSV.read("Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks.csv", DataFrame; limit=1000)
df.child_agedeath_30d = df.child_agedeath_30d .* 1000
df.child_agedeath_30d3m = df.child_agedeath_30d3m .* 1000
df.child_agedeath_3m6m = df.child_agedeath_3m6m .* 1000
df.child_agedeath_6m12m = df.child_agedeath_6m12m .* 1000
rename!(df,:ID_cell => :ID_cell1)
println("Dataset cargado!")

controls = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub])

function symbol_to_term(sym::Symbol)
    # Implement your logic here to create a Term object from a symbol
    return Term(sym)
end

### VERSION 1: Precipitation models (linear precipitation)

# Loop over the different precipitation variables
for prec_prefix in ["prec_12", "prec_6", "prec_3", "prec"]
    
    print(prec_prefix)
    # Model 1 - all children
    println("Running models for $prec_prefix months...")
    prec_previous = term.([Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q3")])
    regs = []
    for i in 1:4
        reg_model = reg(df, term(:child_agedeath_30d) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_30d")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
        push!(regs, reg_model)
    end

    # # Model 2 - only children that survived
    # df_temp = filter(row -> row.child_agedeath_30d == 0, df)
    # push!(prec_previous, [Symbol("$(prec_prefix)_30d")])
    # for i in 1:4
    #     reg_model = reg(df_temp, term(:child_agedeath_30d3m) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_30d3m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
    #     push!(regs, reg_model)
    # end

    # # Model 3 - only children that survived
    # df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
    # append!(prec_previous, term.([Symbol("$(prec_prefix)_30d3m")]))
    # for i in 1:4
    #     reg_model = reg(df_temp, term(:child_agedeath_3m6m) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_3m6m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
    #     push!(regs, reg_model)
    # end

    # # Model 4 - only children that survived
    # df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
    # append!(prec_previous, term.([Symbol("$(prec_prefix)_3m6m")]))
    # for i in 1:4
    #     reg_model = reg(df_temp, term(:child_agedeath_6m12m) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
    #     push!(regs, reg_model)
    # end

    # Generate regression table
    regtable(
        regs...; 
        renderSettings = AsciiTable(), 
        file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 1 $prec_prefix months.txt", 
        order=[Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_30d"), Symbol("$(prec_prefix)_30d3m"), Symbol("$(prec_prefix)_3m6m"), Symbol("$(prec_prefix)_6m12m")])
    regtable(
        regs...; 
        renderSettings = LatexTable(), 
        file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 1 $prec_prefix months.tex", 
        order=[Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_30d"), Symbol("$(prec_prefix)_30d3m"), Symbol("$(prec_prefix)_3m6m"), Symbol("$(prec_prefix)_6m12m")])

end


### VERSION 1: Precipitation models (quadratic precipitation)

# # Loop over the different precipitation variables
# for prec_prefix in ["prec_12", "prec_6", "prec_3", "prec"]
    
#     print(prec_prefix)
#     df[Symbol("$(prec_prefix)_q1_sq")] = df[Symbol("$(prec_prefix)_q1")] .*  df[Symbol("$(prec_prefix)_q1")]
#     df[Symbol("$(prec_prefix)_q2_sq")] = df[Symbol("$(prec_prefix)_q2")] .*  df[Symbol("$(prec_prefix)_q2")]
#     df[Symbol("$(prec_prefix)_q3_sq")] = df[Symbol("$(prec_prefix)_q3")] .*  df[Symbol("$(prec_prefix)_q3")]
#     df[Symbol("$(prec_prefix)_30d_sq")] = df[Symbol("$(prec_prefix)_30d")] .*  df[Symbol("$(prec_prefix)_30d")]
#     df[Symbol("$(prec_prefix)_30d3m_sq")] = df[Symbol("$(prec_prefix)_30d3m")] .*  df[Symbol("$(prec_prefix)_30d3m")]
#     df[Symbol("$(prec_prefix)_3m6m_sq")] = df[Symbol("$(prec_prefix)_3m6m")] .*  df[Symbol("$(prec_prefix)_3m6m")]
#     df[Symbol("$(prec_prefix)_6m12m_sq")] = df[Symbol("$(prec_prefix)_6m12m")] .*  df[Symbol("$(prec_prefix)_6m12m")]

#     # Model 1 - all children
#     println("Running models for $prec_prefix months...")
#     prec_previous = term.([Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q1_sq"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q2_sq"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_q3_sq")])
#     regs = []
#     for i in 1:4
#         reg_model = reg(df, term(:child_agedeath_30d) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_30d")) + term(Symbol("$(prec_prefix)_30d_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
#         push!(regs, reg_model)
#     end

#     # Model 2 - only children that survived
#     df_temp = filter(row -> row.child_agedeath_30d == 0, df)
#     prec_previous = term.([Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q1_sq"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q2_sq"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_q3_sq"), term(Symbol("$(prec_prefix)_30d")), term(Symbol("$(prec_prefix)_30d_sq")) ])
#     for i in 1:4
#         reg_model = reg(df_temp, term(:child_agedeath_30d3m) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_30d3m")) + term(Symbol("$(prec_prefix)_30d3m_sq"))+ sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
#         push!(regs, reg_model)
#     end

#     # Model 3 - only children that survived
#     df_temp = filter(row -> row.child_agedeath_30d3m == 0, df_temp)
#     prec_previous = term.([Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q1_sq"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q2_sq"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_q3_sq"), term(Symbol("$(prec_prefix)_30d")), term(Symbol("$(prec_prefix)_30d_sq")), term(Symbol("$(prec_prefix)_30d3m")), term(Symbol("$(prec_prefix)_30d3m_sq"))])
#     for i in 1:4
#         reg_model = reg(df_temp, term(:child_agedeath_3m6m) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_3m6m")) + term(Symbol("$(prec_prefix)_3m6m_sq")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
#         push!(regs, reg_model)
#     end

#     # Model 4 - only children that survived
#     df_temp = filter(row -> row.child_agedeath_3m6m == 0, df_temp)
#     prec_previous = term.([Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q1_sq"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q2_sq"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_q3_sq"), term(Symbol("$(prec_prefix)_30d")), term(Symbol("$(prec_prefix)_30d_sq")), term(Symbol("$(prec_prefix)_30d3m")), term(Symbol("$(prec_prefix)_30d3m_sq"))])
#     for i in 1:4
#         reg_model = reg(df_temp, term(:child_agedeath_6m12m) ~ sum(prec_previous) + term(Symbol("$(prec_prefix)_6m12m")) + sum(controls) + fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month), Vcov.cluster(Symbol("ID_cell$i")), method=:CUDA)
#         push!(regs, reg_model)
#     end

#     # Generate regression table
#     regtable(
#         regs...; 
#         renderSettings = AsciiTable(), 
#         file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 1 $prec_prefix months.txt", 
#         order=[Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_30d"), Symbol("$(prec_prefix)_30d3m"), Symbol("$(prec_prefix)_3m6m"), Symbol("$(prec_prefix)_6m12m")])
#     regtable(
#         regs...; 
#         renderSettings = LatexTable(), 
#         file="Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\version 1 $prec_prefix months.tex", 
#         order=[Symbol("$(prec_prefix)_q1"), Symbol("$(prec_prefix)_q2"), Symbol("$(prec_prefix)_q3"), Symbol("$(prec_prefix)_30d"), Symbol("$(prec_prefix)_30d3m"), Symbol("$(prec_prefix)_3m6m"), Symbol("$(prec_prefix)_6m12m")])

# end

