include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles
@assert CUDA.functional()

# Load the data
print("Cargando dataset...") # Only load 20000 rows
df = CSV.read("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks.csv", DataFrame)

df.decade = floor.(df.chb_year / 10) * 10

# rename!(df,:ID_cell => :ID_cell1)
print("Dataset cargado!")

controls1 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_eduy])
controls2 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub])
controls3 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :mother_age, :mother_eduy])


#################################################################
###  Pooled all countries into regression
#################################################################

# # # From 2003 and last 10 years
# contr_i = 1
# for controls in [controls1, controls2, controls3]
#     global contr_i
#     suffix = " - controls$(contr_i)"
#     CustomModels.run_models(df, controls, "", suffix)
#     contr_i += 1
# end

#################################################################
###  All by income
#################################################################

incomegroup = unique(df.wbincomegroup)

# Create a progress bar
prog = Progress(length(incomegroup), 1)

# Loop over unique values
for incomegroup in incomegroup
    next!(prog) # Update progress bar
    for controls in [controls1, controls2, controls3]
        try
            df_incomegroup = filter(row -> row.wbincomegroup == incomegroup, df)
            CustomModels.run_models(df_incomegroup, controls, "incomegroups\\", " - $(incomegroup)")
        catch
            printlnln("Error en ", incomegroup)
        end

    end
end
# #################################################################
# ###  All by countries
# #################################################################

# countries = unique(df.code_iso3)

# # Create a progress bar
# prog = Progress(length(countries), 1)

# # Loop over unique values
# for country in countries
#     next!(prog) # Update progress bar
#     try
#         df_country = filter(row -> row.code_iso3 == country, df)
#         CustomModels.run_models(df_country, controls, "countries//", "$(country) - ")
#     catch
#         printlnln("Error en ", country)
#     end
# end