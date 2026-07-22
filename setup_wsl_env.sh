#!/usr/bin/env bash
# One-line environment bootstrap for this project on WSL2 / Linux.
#
#   bash setup_wsl_env.sh
#
# Creates the conda env (Python + Julia binary) and installs the Julia packages.
# Idempotent-ish: re-running updates the env instead of failing.
set -euo pipefail
cd "$(dirname "$0")"

ENV_NAME="climate-mortality"

# Prefer mamba if present (much faster solver), else conda.
CONDA="conda"
command -v mamba >/dev/null 2>&1 && CONDA="mamba"

echo ">> Creating/updating conda env '$ENV_NAME' ..."
if conda env list | grep -qE "^\s*$ENV_NAME\s"; then
    "$CONDA" env update -n "$ENV_NAME" -f environment.yml --prune
else
    "$CONDA" env create -f environment.yml
fi

echo ">> Installing Julia packages ..."
# Use the julia that ships inside the env.
conda run -n "$ENV_NAME" julia --project=. install_julia_deps.jl

# Register the env's Python as a Jupyter kernel (for the .ipynb files).
conda run -n "$ENV_NAME" python -m ipykernel install --user \
    --name "$ENV_NAME" --display-name "Python ($ENV_NAME)" || true

cat <<'DONE'

Done. Activate with:
    conda activate climate-mortality

Notes:
  * ERA5 downloads (00_query_ERA5_*.py) need a CDS API key in ~/.cdsapirc.
  * CUDA.jl needs an NVIDIA driver on the Windows host; WSL2 exposes the GPU
    automatically (no CUDA toolkit install required — CUDA.jl bundles its own).
DONE
