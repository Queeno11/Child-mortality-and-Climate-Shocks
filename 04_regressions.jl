include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles, Arrow

@assert CUDA.functional()

## Load the data
controls1 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_ageb, :mother_eduy]
controls2 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_ageb, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls2b = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :rwi, :mother_ageb, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls3 = [:child_fem, :child_mulbirth, :birth_order, :rural, :mother_ageb, :mother_eduy]
controls4 = [:child_fem, :child_mulbirth, :birth_order, :rural, :rwi, :mother_ageb, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls = term.(controls2b) # controls3, controls1

path = "D:\\World Bank\\Paper - Child Mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v10.feather"
tbl = Arrow.Table(path)
df_lazy = DataFrame(tbl) 

#################################################################
###  Pooled all countries into regression
#################################################################
m=1
CustomModels.run_models(df_lazy, controls, "", "controls2", [m])

# if m != "1"
#     # Only run heterogeneity for SPI1
#     continue
# end
# # #################################################################
# # ###  heterogeneity
# # #################################################################


# # Climate Bands (3 classifications)
CustomModels.run_heterogeneity(df_lazy, controls, :climate_band_1, [m])
# CustomModels.run_heterogeneity(df_lazy, controls, :climate_band_2, [m])

# Income Group
CustomModels.run_heterogeneity(df_lazy, controls, :wbincomegroup, [m])

# #################################################################
# ###  Mechanisms
# #################################################################

# Urban & Rural
CustomModels.run_heterogeneity(df_lazy, controls, :rural, [m])

# Mechanisms

CustomModels.run_heterogeneity(df_lazy, controls, :pipedw, [m])
CustomModels.run_heterogeneity(df_lazy, controls, :href , [m])
CustomModels.run_heterogeneity(df_lazy, controls, :hhelectemp, [m])
CustomModels.run_heterogeneity(df_lazy, controls, :hhfan, [m])
CustomModels.run_heterogeneity(df_lazy, controls, :hhaircon, [m])
CustomModels.run_heterogeneity(df_lazy, controls, :helec, [m])

# Gender
CustomModels.run_heterogeneity(df_lazy, controls, :child_fem, [m])

