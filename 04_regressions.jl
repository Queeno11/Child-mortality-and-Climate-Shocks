using .CustomModels
using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

# Load the data
print("Cargando dataset...")
df = CSV.read("Z:\\Laboral\\World Bank\\Paper - Child mortality and Climate Shocks\\Data\\Data_out\\DHSBirthsGlobal&ClimateShocks.csv", DataFrame)
df.child_agedeath_30d = df.child_agedeath_30d .* 1000
df.child_agedeath_30d3m = df.child_agedeath_30d3m .* 1000
df.child_agedeath_3m6m = df.child_agedeath_3m6m .* 1000
df.child_agedeath_6m12m = df.child_agedeath_6m12m .* 1000
for months in ["_12", "_6", "_3", ""]
    df[!, Symbol("prec$(months)_q1_sq")]   = df[!, Symbol("prec$(months)_q1")]    .*  df[!, Symbol("prec$(months)_q1")]
    df[!, Symbol("prec$(months)_q2_sq")]   = df[!, Symbol("prec$(months)_q2")]    .*  df[!, Symbol("prec$(months)_q2")]
    df[!, Symbol("prec$(months)_q3_sq")]   = df[!, Symbol("prec$(months)_q3")]    .*  df[!, Symbol("prec$(months)_q3")]
    df[!, Symbol("prec$(months)_30d_sq")]  = df[!, Symbol("prec$(months)_30d")]   .*  df[!, Symbol("prec$(months)_30d")]
    df[!, Symbol("prec$(months)_30d3m_sq")]= df[!, Symbol("prec$(months)_30d3m")] .*  df[!, Symbol("prec$(months)_30d3m")]
    df[!, Symbol("prec$(months)_3m6m_sq")] = df[!, Symbol("prec$(months)_3m6m")]  .*  df[!, Symbol("prec$(months)_3m6m")]
    df[!, Symbol("prec$(months)_6m12m_sq")]= df[!, Symbol("prec$(months)_6m12m")] .*  df[!, Symbol("prec$(months)_6m12m")]
end
rename!(df,:ID_cell => :ID_cell1)
print("Dataset cargado!")

controls = term.([:child_fem, :child_mulbirth, :birth_order, :rural, :d_weatlh_ind_2, :d_weatlh_ind_3, :d_weatlh_ind_4, :d_weatlh_ind_5, :mother_age, :mother_ageb_squ, :mother_ageb_cub, :mother_eduy, :mother_eduy_squ, :mother_eduy_cub])

#################################################################
###  Pooled all countries into regression
#################################################################

CustomModels.run_models(df, controls, "", "")

#################################################################
###  All by income
#################################################################

incomegroup = unique(df.wbincomegroup)

# Create a progress bar
prog = Progress(length(incomegroup), 1)

# Loop over unique values
for incomegroup in incomegroup
    next!(prog) # Update progress bar
    try
        df_incomegroup = filter(row -> row.wbincomegroup == incomegroup, df)
        CustomModels.run_models(df_incomegroup, controls, "incomegroups//", "$(incomegroup) - ")
    catch
        println("Error en ", incomegroup)
    end
end

#################################################################
###  All by countries
#################################################################

countries = unique(df.code_iso3)

# Create a progress bar
prog = Progress(length(countries), 1)

# Loop over unique values
for country in countries
    next!(prog) # Update progress bar
    try
        df_country = filter(row -> row.code_iso3 == country, df)
        CustomModels.run_models(df_country, controls, "countries//", "$(country) - ")
    catch
        println("Error en ", country)
    end
end