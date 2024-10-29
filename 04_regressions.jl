include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles
@assert CUDA.functional()

# Load the data
print("Cargando dataset...")
# df = DataFrame(load("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks.dta"))
df = CSV.read("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v4_clibands.csv", DataFrame)
# df = CSV.read("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v7temp-spi_v8spei_all_shocks.csv", DataFrame)

# rename!(df,:ID_cell => :ID_cell1)
print("Dataset cargado!")

controls1 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_eduy])
controls2 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub])
controls3 = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :mother_age, :mother_eduy])
controls_all = [controls1]#, controls2, controls3]

# #################################################################
# ###  Pooled all countries into regression
# #################################################################

# contr_i = 1
# for controls in controls_all
#     global contr_i
#     suffix = " - controls$(contr_i)"
#     CustomModels.run_models(df, controls, "", suffix)
#     contr_i += 1
# end

# #################################################################
# ###  Heterogeneity
# #################################################################

# # Urban & Rural
# CustomModels.run_heterogeneity_dummy(df, controls1, 1, "rural", "", "")

# # Mechanisms

# CustomModels.run_heterogeneity_dummy(df, controls1, 1, "pipedw", "", "")
# CustomModels.run_heterogeneity_dummy(df, controls1, 1, "href", "", "")
# CustomModels.run_heterogeneity_dummy(df, controls1, 1, "hhelectemp", "", "")

# # Gender
# CustomModels.run_heterogeneity_dummy(df, controls1, 1, "child_fem", "", "")


# #################################################################
# ###  All by income
# #################################################################

# incomegroups = unique(df.wbincomegroup)

# # Create a progress bar
# prog = Progress(length(incomegroups), 1)

# # Loop over unique values
# for incomegroup in incomegroups
#     next!(prog) # Update progress bar
#     for controls in [controls1]
#         try
#             df_incomegroup = filter(row -> row.wbincomegroup == incomegroup, df)
#             CustomModels.run_models(df_incomegroup, controls, "incomegroups\\", " - $(incomegroup)")
#         catch
#             printlnln("Error en ", incomegroup)
#         end

#     end
# end

#################################################################
###  All by Climatic Bands
#################################################################

climate_bands = unique(df.climate_band_5)

# Create a progress bar
prog = Progress(length(climate_bands), 1)

# Loop over unique values
for climate_band in climate_bands
    next!(prog) # Update progress bar
    for controls in [controls1]
        try
            df_climate_bands = filter(row -> row.climate_band_5 == climate_band, df)
            CustomModels.run_models(df_climate_bands, controls, "climate_bands\\", " - $(climate_band)")
        catch
            printlnln("Error en ", climate_band)
        end

    end
end
