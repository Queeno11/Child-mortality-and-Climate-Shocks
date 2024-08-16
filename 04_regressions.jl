include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles
@assert CUDA.functional()

# Load the data
print("Cargando dataset...") # Only load 20000 rows
df = CSV.read("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks.csv", DataFrame)

# rename!(df,:ID_cell => :ID_cell1)
print("Dataset cargado!")

controls1 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_eduy])
controls2 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub])
controls3 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :mother_age, :mother_eduy])
controls_all = [controls1, controls2, controls3]

#################################################################
###  Pooled all countries into regression
#################################################################

# From 2003 and last 10 years
contr_i = 1
for controls in controls_all
    global contr_i
    suffix = " - controls$(contr_i)"
    CustomModels.run_models(df, controls, "", suffix)
    contr_i += 1
end

# Urban & Rural
df_urban = filter(row -> row.rural == 0, df)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_urban, controls, "urban\\", suffix)
end

df_rural = filter(row -> row.rural == 1, df)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_rural, controls, "rural\\", suffix)
end

# Female & Male
df_male = filter(row -> row.child_fem == 0, df)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_male, controls, "male\\", suffix)
end

df_female = filter(row -> row.child_fem == 1, df)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_female, controls, "female\\", suffix)
end

###### Mechanisms

# Piped water
df_piped_all = dropmissing(df, :pipedw)

df_piped = filter(row -> row.pipedw == 1, df_piped_all)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_piped, controls, "piped_water\\", suffix)
end

df_nopiped = filter(row -> row.pipedw == 0, df_piped_all)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_nopiped, controls, "no_piped_water\\", suffix)
end

# Refrigerator
df_refrigerator_all = dropmissing(df, :href)

df_refrigerator = filter(row -> row.href == 1, df_refrigerator_all)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_refrigerator, controls, "refrigerator\\", suffix)
end

df_norefrigerator = filter(row -> row.href == 0, df_refrigerator_all)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_norefrigerator, controls, "no_refrigerator\\", suffix)
end

# Temperature managment
df_electemp_all = dropmissing(df, :hhelectemp)

df_electemp = filter(row -> row.hhelectemp == 1, df_electemp_all)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_electemp, controls, "electemp\\", suffix)
end

df_noelectemp = filter(row -> row.hhelectemp == 0, df_electemp_all)
for controls in [controls1]
    suffix = " - controls1"
    CustomModels.run_models(df_noelectemp, controls, "no_electemp\\", suffix)
end


#################################################################
###  All by income
#################################################################

# incomegroup = unique(df.wbincomegroup)

# # Create a progress bar
# prog = Progress(length(incomegroup), 1)

# # Loop over unique values
# for incomegroup in incomegroup
#     next!(prog) # Update progress bar
#     for controls in [controls1, controls2, controls3]
#         try
#             df_incomegroup = filter(row -> row.wbincomegroup == incomegroup, df)
#             CustomModels.run_models(df_incomegroup, controls, "incomegroups\\", " - $(incomegroup)")
#         catch
#             printlnln("Error en ", incomegroup)
#         end

#     end
# end
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