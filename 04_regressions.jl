include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles
@assert CUDA.functional()

# Load the data
print("Cargando dataset...")
# df = DataFrame(load("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v6.dta"))
df = CSV.read("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v6_all_shocks.csv", DataFrame)

# rename!(df,:ID_cell => :ID_cell1)
print("Dataset cargado!")

controls1 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_eduy])
controls2 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub])
controls3 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :mother_age, :mother_eduy])
controls_all = [controls1, controls2, controls3]

#################################################################
###  Pooled all countries into regression
#################################################################

# contr_i = 1
# for controls in controls_all
#     global contr_i
#     suffix = " - controls$(contr_i)"
#     CustomModels.run_models(df, controls, "", suffix)
#     contr_i += 1
# end

#################################################################
###  Heterogeneity
#################################################################

# Urban & Rural
CustomModels.run_heterogeneity_dummy(df, controls1, 1, "rural", "", "")

# Mechanisms

CustomModels.run_heterogeneity_dummy(df, controls1, 1, "pipedw", "", "")
CustomModels.run_heterogeneity_dummy(df, controls1, 1, "href", "", "")
CustomModels.run_heterogeneity_dummy(df, controls1, 1, "hhelectemp", "", "")

# Gender
CustomModels.run_heterogeneity_dummy(df, controls1, 1, "child_fem", "", "")


#################################################################
###  All by income
#################################################################

incomegroup = unique(df.wbincomegroup)

# Create a progress bar
prog = Progress(length(incomegroup), 1)

# Loop over unique values
for incomegroup in incomegroup
    next!(prog) # Update progress bar
    for controls in [controls1]
        try
            df_incomegroup = filter(row -> row.wbincomegroup == incomegroup, df)
            CustomModels.run_models(df_incomegroup, controls, "incomegroups\\", " - $(incomegroup)")
        catch
            printlnln("Error en ", incomegroup)
        end

    end
end
