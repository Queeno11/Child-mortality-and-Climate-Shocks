# Installs the Julia packages for 04_regressions.jl / CustomModels.jl into the
# local project (./Project.toml + Manifest.toml). UUIDs are resolved from the
# registry automatically — no hand-editing.
#
#   julia --project=. install_julia_deps.jl
#
# (setup_wsl_env.sh calls this for you.)
using Pkg
Pkg.activate(@__DIR__)
Pkg.add([
    "Arrow",
    "CSV",
    "CUDA",              # GPU regressions; works on WSL2 with an NVIDIA driver
    "DataFrames",
    "FixedEffectModels",
    "ProgressMeter",
    "RDatasets",
    "RegressionTables",
    "StatFiles",         # reads Stata .dta
    "StatsModels",
    "Tables",
    # Statistics is a stdlib and needs no add, but listing is harmless:
    "Statistics",
])
Pkg.instantiate()
Pkg.precompile()
@info "Julia deps installed into $(Base.active_project())"
