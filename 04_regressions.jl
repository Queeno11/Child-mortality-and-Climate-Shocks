include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles, Arrow

@assert CUDA.functional()

## Load the data
controls1 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_ageb, :mother_eduy]
controls2 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_ageb, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls3 = [:child_fem, :child_mulbirth, :birth_order, :rural, :mother_ageb, :mother_eduy]
controls4 = [:child_fem, :child_mulbirth, :birth_order, :rural, :rwi, :mother_ageb, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls = term.(controls2) # controls3, controls1

path = "D:\\World Bank\\Paper - Child Mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v11_nanmean.feather"
tbl = Arrow.Table(path)
df_lazy = DataFrame(tbl)

#################################################################
###  Pooled all countries into regression
#################################################################
for m in [1,]#, 3, 6, 12, 24]

    CustomModels.run_models(df_lazy, controls, "", "", [m]; models=["linear", "spline", "extremes", "horserace"])
    
    # Only run heterogeneity/mechanisms for SPI1
    if m == 1
        #################################################################
        ###  heterogeneity
        #################################################################
       
        # Income Quintiles/Deciles
        CustomModels.run_heterogeneity(df_lazy, controls, :poor, [m]) # DHS quintiles
        CustomModels.run_heterogeneity(df_lazy, controls, :weatlh_ind, [m]) # DHS quintiles
        CustomModels.run_heterogeneity(df_lazy, controls, :rwi_tertiles, [m])
        # CustomModels.run_heterogeneity(df_lazy, controls, :rwi_quintiles, [m])
        # CustomModels.run_heterogeneity(df_lazy, controls, :rwi_deciles, [m])

        # Climate Bands (3 classifications)
        CustomModels.run_heterogeneity(df_lazy, controls, :climate_band_1, [m]; models=["linear", "horserace"])
        # CustomModels.run_heterogeneity(df_lazy, controls, :climate_band_2, [m])

        # Income Group
        CustomModels.run_heterogeneity(df_lazy, controls, :wbincomegroup, [m])

        # Gender
        CustomModels.run_heterogeneity(df_lazy, controls, :child_fem, [m])

        # Urban & Rural
        CustomModels.run_heterogeneity(df_lazy, controls, :rural, [m])

        #################################################################
        ###  Mechanisms
        #################################################################

        CustomModels.run_heterogeneity(df_lazy, controls, :high_quality_housing, [m]) # Grouped mother educ
        CustomModels.run_heterogeneity(df_lazy, controls, :high_heat_protection, [m]) # Electricity
        CustomModels.run_heterogeneity(df_lazy, controls, :high_cold_protection, [m]) # Electricity
        CustomModels.run_heterogeneity(df_lazy, controls, :mother_educ, [m]) # Grouped mother educ
        CustomModels.run_heterogeneity(df_lazy, controls, :electricity, [m]) # Electricity
        CustomModels.run_heterogeneity(df_lazy, controls, :pipedw, [m]) # Piped Water
        CustomModels.run_heterogeneity(df_lazy, controls, :refrigerator , [m]) # Refrigerator
        CustomModels.run_heterogeneity(df_lazy, controls, :hhfan, [m]) # Has fan 
        CustomModels.run_heterogeneity(df_lazy, controls, :hhaircon, [m]) # Air condition

    end

end