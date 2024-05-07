using RegressionTables, DataFrames, FixedEffectModels, RDatasets

df = dataset("datasets", "iris")
df[:SpeciesDummy] = pool(df[:Species])

rr1 = reg(df, @model(SepalLength ~ SepalWidth   , fe = SpeciesDummy))
rr2 = reg(df, @model(SepalLength ~ SepalWidth + PetalLength   , fe = SpeciesDummy))
rr3 = reg(df, @model(SepalLength ~ SepalWidth + PetalLength + PetalWidth  , fe = SpeciesDummy))
rr4 = reg(df, @model(SepalWidth ~ SepalLength + PetalLength + PetalWidth  , fe = SpeciesDummy))

regtable(rr1,rr2,rr3,rr4; renderSettings = asciiOutput())
