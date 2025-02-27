include("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\CustomModels.jl")

using .CustomModels
using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles
@assert CUDA.functional()

println("Running script with ", Threads.nthreads(), " threads")

## Load the data
controls1 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_eduy]
controls2 = [:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub]
controls3 = [:child_fem, :child_mulbirth, :birth_order, :rural, :mother_age, :mother_eduy]
controls = controls2 # controls3, controls1

# # Read the file schema without loading data
file = CSV.File(
    "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v9.csv"; limit=100
)

## Loop over months to avoid overloading memory, only load the necessary columns
for m in ["1", "3", "6", "9", "12", "24"]

    # Define columns to exclude
    local columns_to_include
    columns_to_include = [
        name for name in file.names 
        if any(occursin(string(including), string(name)) for including in [Symbol("spi$(m)_"), "stdm_t_",  "absdifm_t_", "absdif_t_", "std_t_", "t_", "ID_cell", "child_agedeath_"])
    ]
    columns_to_include = vcat(columns_to_include, controls, [:chb_month, :chb_year, :chb_year_sq, :rural, :pipedw, :href, :hhelectemp, :wbincomegroup, :climate_band_1, :climate_band_2, :climate_band_3, :southern])

    # Remove columns with minmax string in the name
    columns_to_include = [name for name in columns_to_include if !occursin("minmax", string(name))]

    println("Cargando dataset...")
    local df
    df = CSV.read(
        "D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v9.csv", DataFrame;
        select=columns_to_include,
        rows_to_check=1000,
    )
    print("   Dataset cargado!")

    #################################################################
    ###  Pooled all countries into regression
    #################################################################

    termcontrols = term.(controls)
    CustomModels.run_models(df, termcontrols, "", "", [m])

    if m != 1
        # Only run heterogeneity for SPI1
        continue
    end
    # # #################################################################
    # # ###  heterogeneity
    # # #################################################################
    
    
    # # Climate Bands (3 classifications)
    CustomModels.run_heterogeneity(df, controls, "climate_band_1", [m])
    CustomModels.run_heterogeneity(df, controls, "climate_band_2", [m])
    
    # Northern & Southern Hemisphere
    CustomModels.run_heterogeneity_dummy(df, controls, "southern", [m])
    
    # Income Group
    CustomModels.run_heterogeneity(df, controls, "wbincomegroup", [m])

    # #################################################################
    # ###  Mechanisms
    # #################################################################

    # Urban & Rural
    CustomModels.run_heterogeneity_dummy(df, controls, "rural", [m])

    # Mechanisms

    CustomModels.run_heterogeneity_dummy(df, controls, "pipedw", [m])
    CustomModels.run_heterogeneity_dummy(df, controls, "href", [m])
    CustomModels.run_heterogeneity_dummy(df, controls, "hhelectemp", [m])

    # Gender
    CustomModels.run_heterogeneity_dummy(df, controls, "child_fem", [m])


end
