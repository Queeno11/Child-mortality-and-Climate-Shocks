using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter, StatFiles, Parquet

# Read the file schema without loading data
file = CSV.File("D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v9c.csv"; limit=100)

# Define the two column selections
columns_to_include_1 = [:child_fem, :child_mulbirth, :birth_order, :rural, 
    :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, 
    :mother_age, :mother_eduy]

columns_to_include_2 = [
    name for name in file.names if any(
        occursin(string(including), string(name))
        for including in [Symbol("spi1_"), "std_t_", "stdm_t_", "ID_cell", "child_agedeath_"]
    )
]

println("Number of columns in first selection: ", length(columns_to_include_1))
println("Number of columns in second selection: ", length(columns_to_include_2))

# Test time to read a Parquet file
println("Reading Parquet DataFrame:")
# List all files in the folder with a ".parquet" extension
parquet_folder = "D:\\World Bank\\Paper - Child Mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks_v9c.parquet"
parquet_files = filter(f -> endswith(f, ".parquet"), readdir(parquet_folder, join = true))
println("Found ", length(parquet_files), " parquet files.")

# Read each Parquet file into a DataFrame and vertically concatenate them
@time begin
    dfs = [DataFrame(Parquet.File(file)) for file in parquet_files]
    df3 = vcat(dfs...)
    println("Combined Parquet DataFrame size: ", size(df3))
end

