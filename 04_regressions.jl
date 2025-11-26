include("C:\\Working Papers\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using DataFrames, RDatasets, RegressionTables, FixedEffectModels, ProgressMeter, StatFiles, Arrow

# @assert CUDA.functional()

## Load the data
controls1 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_ageb, :mother_eduy]
controls2 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_ageb, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls3 = [:child_fem, :child_mulbirth, :birth_order, :rural, :mother_ageb, :mother_eduy]
controls4 = [:child_fem, :child_mulbirth, :birth_order, :rural, :rwi, :mother_ageb, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls = term.(controls2) # controls3, controls1

path = "C:\\Working Papers\\Paper - Child Mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v11_full.feather"
tbl = Arrow.Table(path)
df_lazy = DataFrame(tbl)

#################################################################
###  Pooled all countries into regression
#################################################################
for m in [1,]#, 3, 6, 12, 24]

    CustomModels.run_models(df_lazy, controls, "", "", [m]; models=["spline"])#, "linear", "horserace",])#, "extremes", ])
    stop
    # Only run heterogeneity/mechanisms for SPI1
    if m == 1

        #################################################################
        ###  Shock Analysis (High vs. Low In-Utero Shocks)
        #################################################################
        
        # Run the analysis for shocks > 1 Standard Deviation
        # CustomModels.run_shock_analysis(df_lazy, controls, "", [m]; 
        #                                 models=["linear"])

        # Example of running for a different threshold, e.g., 2 Standard Deviations
        # CustomModels.run_shock_analysis(df_lazy, controls, "", "", [m]; <
        #                                 models=["linear"], 
        #                                 shock_threshold=2.0)

        #################################################################
        ###  heterogeneity
        #################################################################

        # Country-level indicators
        CustomModels.run_heterogeneity(df_lazy, controls, :high_nd_gain_2023, [m]) # DHS quintiles
        CustomModels.run_heterogeneity(df_lazy, controls, :high_world_risk, [m]) # World Risk Index
        CustomModels.run_heterogeneity(df_lazy, controls, :high_vulnerability, [m]) # Vulnerability index
        CustomModels.run_heterogeneity(df_lazy, controls, :high_adaptive_capacity, [m]) # Vulnerability index
        CustomModels.run_heterogeneity(df_lazy, controls, :high_coping_mechanisms, [m]) # Vulnerability index
        CustomModels.run_heterogeneity(df_lazy, controls, :high_exposure, [m]) # Exposure index
        CustomModels.run_heterogeneity(df_lazy, controls, :wbincomegroup, [m])       
        
        # Income indicators
        CustomModels.run_heterogeneity(df_lazy, controls, :poor, [m]) # DHS Poor indicator
        CustomModels.run_heterogeneity(df_lazy, controls, :weatlh_ind, [m]) # DHS quintiles
        CustomModels.run_heterogeneity(df_lazy, controls, :rwi_tertiles, [m]) # RWI Tertiles
        # CustomModels.run_heterogeneity(df_lazy, controls, :rwi_quintiles, [m])
        # CustomModels.run_heterogeneity(df_lazy, controls, :rwi_deciles, [m])
        
        # Climate Bands (3 classifications)
        CustomModels.run_heterogeneity(df_lazy, controls, :climate_band_1, [m]; models=["linear", "horserace"])
        # CustomModels.run_heterogeneity(df_lazy, controls, :climate_band_2, [m])
        
        # Gender
        CustomModels.run_heterogeneity(df_lazy, controls, :child_fem, [m])
    
        
        # Household Indicators
        CustomModels.run_heterogeneity(df_lazy, controls, :rural, [m])
        # CustomModels.run_heterogeneity(df_lazy, controls, :high_quality_housing, [m]) # Grouped mother educ
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