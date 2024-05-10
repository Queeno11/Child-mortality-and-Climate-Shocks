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

controls = term.([:rural])

#################################################################
###  Pooled all countries into regression
#################################################################

CustomModels.drought_excessiverain_regression_no_timetrend(df, "_3", "2_5", controls, "", "")
CustomModels.drought_excessiverain_regression_no_cell_fe(df, "_3", "2_5", controls, "", "")
CustomModels.drought_excessiverain_regression_no_cell_fe(df, "_3", "2_5", controls, "", "")