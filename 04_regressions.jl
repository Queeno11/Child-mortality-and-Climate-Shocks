include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles
@assert CUDA.functional()

## Load the data
controls1 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_eduy]
controls2 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls3 = [:child_fem, :child_mulbirth, :birth_order, :rural, :mother_age, :mother_eduy]
controls_all = [controls2]#, controls3, controls1]

# # Read the file schema without loading data
file = CSV.File(
    "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v9.csv"; limit=1000
)

for m in ["1", "3", "6", "9", "12", "24"]

    # Define columns to exclude
    local columns_to_include
    columns_to_include = [
        name for name in file.names 
        if any(occursin(string(including), string(name)) for including in [Symbol("spi$(m)_"), "stdm_t_",  "absdifm_t_", "absdif_t_", "std_t_", "t_", "ID_cell", "child_agedeath_"])
    ]
    columns_to_include = vcat(columns_to_include, controls2, [:chb_month, :chb_year, :rural, :pipedw, :href, :hhelectemp, :wbincomegroup, :climate_band_5])

    print("Cargando dataset...")
    local df
    df = CSV.read(
        "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v9.csv", DataFrame; 
        select=columns_to_include,
        # silencewarnings = true,
    )
    print("Dataset cargado!")

    #################################################################
    ###  Pooled all countries into regression
    #################################################################

    # local contr_i
    # contr_i = 1
    # for controls in controls_all
    #     suffix = " - controls$(contr_i)"
    #     controls = term.(controls)
    #     CustomModels.run_models(df, controls, "", suffix, [m])
    #     contr_i += 1
    # end

    # #################################################################
    # ###  Heterogeneity
    # #################################################################

    # # Urban & Rural
    # CustomModels.run_heterogeneity_dummy(df, controls1, "rural", "", "", [m])

    # # Mechanisms

    # CustomModels.run_heterogeneity_dummy(df, controls1, "pipedw", "", "", [m])
    # CustomModels.run_heterogeneity_dummy(df, controls1, "href", "", "", [m])
    # CustomModels.run_heterogeneity_dummy(df, controls1, "hhelectemp", "", "", [m])

    # # Gender
    # CustomModels.run_heterogeneity_dummy(df, controls1, "child_fem", "", "", [m])


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
    #             CustomModels.run_models(df_incomegroup, controls, "incomegroups\\", " - $(incomegroup)", [m])
    #         catch
    #             printlnln("Error en ", incomegroup)
    #         end

    #     end
    # end

    #################################################################
    ###  All by Climatic Bands 1
    #################################################################

    run_heterogeneity(df, controls2, "climate_band_1", months)
    run_heterogeneity(df, controls2, "climate_band_2", months)
    run_heterogeneity(df, controls2, "climate_band_3", months)

    run_heterogeneity_dummy(df, controls2, "southern", months)


end
