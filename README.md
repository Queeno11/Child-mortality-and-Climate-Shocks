# Child Mortality and Climate Shocks

Estimating the effect of in-utero and early-life climate shocks (temperature
anomalies, SPI, extreme-heat/cold days) on child mortality, using DHS births
merged with ERA5 reanalysis. Runs on **WSL2 / Linux**.

## 1. One-time setup

```bash
bash setup_wsl_env.sh          # creates the conda env + installs Julia deps
conda activate climate-mortality
cp .env.example .env           # then edit .env with your dataset paths (see §3)
```

`setup_wsl_env.sh` is idempotent-ish (re-running updates instead of failing). It:
- creates/updates the `climate-mortality` conda env from `environment.yml`
  (Python + the Julia binary), and
- installs the Julia packages via `install_julia_deps.jl`.

### Python deps
Managed by conda: `conda env create -f environment.yml` (or `mamba`).

### Julia deps (regression stage)
Pinned in `Project.toml` (`[deps]`) + `Manifest.toml` — commit both; the
`Manifest.toml` is the lockfile that makes the build reproducible. Install with
any of:

```bash
julia --project=. -e 'using Pkg; Pkg.instantiate()'   # exact pinned versions (fastest)
julia --project=. install_julia_deps.jl               # add + instantiate + precompile
bash setup_wsl_env.sh                                  # does the above for you
```

Notes:
- Packages install into the conda env's Julia depot
  (`$CONDA_PREFIX/share/julia`), not `~/.julia`.
- `CUDA.jl` installs without a GPU; it only needs an NVIDIA driver at runtime.
  WSL2 exposes the host GPU automatically (no CUDA toolkit install needed).

## 2. Paths: `paths.py` + `.env`

**No absolute paths are hard-coded in the scripts.** All paths come from
`paths.py`:

- **Project-internal** folders (`Data/Data_in`, `Data/Data_proc`,
  `Data/Data_out`, `Outputs`) are derived automatically from the project
  location — no configuration needed.
- **External datasets** (too big to keep in the repo) are read from `.env`.
  Copy `.env.example` → `.env` and fill in the paths. `.env` is git-ignored;
  `.env.example` is the tracked template.

Python scripts do `from paths import DATA_IN, DATA_PROC, DATA_OUT, OUTPUTS, ...`.
Julia scripts derive project-internal paths from `@__DIR__` (they only touch
`Data/` and `Outputs/`, so they need no `.env`).

## 3. External datasets (set in `.env`)

| Env variable            | Used by            | What it points to                              |
|-------------------------|--------------------|------------------------------------------------|
| `ERA5_MONTHLY_DIR`      | `00`, `01`         | Monthly ERA5 single-levels dir (`data_<year>`) |
| `ERA5_DAILY_DIR`        | `00b`              | Daily ERA5 single-levels dir                   |
| `CLIMATE_BANDS`         | `02b`              | Köppen-Geiger grid file (`KG_1986-2010.grd`)   |
| `COUNTRY_CLASSIFICATION`| `03`               | Country income-group lookup workbook (`.xlsx`) |

DHS births, RWI, ND-Gain, World Risk Index, etc. live under `Data/Data_in/`
and need no env var.

## 4. Pipeline

Run in order (or use `python main.py`, which runs `02`→`06`):

| Script                          | Lang   | Does                                              |
|---------------------------------|--------|--------------------------------------------------|
| `00_query_ERA5_data.py`         | Python | Download monthly ERA5 (needs `~/.cdsapirc`)      |
| `00b_query_ERA5_dialy.py`       | Python | Download daily ERA5                              |
| `01_compute_climate_indices.py` | Python | Build SPI + temperature-anomaly climate dataset  |
| `02_assign_shocks_to_DHS.py`    | Python | Assign climate shocks to each DHS birth          |
| `02b_assign_climatic_bands.py`  | Python | Assign Köppen bands, RWI, country indices        |
| `03_merge_climate_and_DHS.py`   | Python | Merge everything → `.feather` for regressions    |
| `04_regressions.jl`             | Julia  | Fixed-effects regressions (`CustomModels.jl`)    |
| `06_charts.py`                  | Python | Figures (`plot_tools.py`)                        |

`00`/`00b`/`01` (raw ERA5 download + compilation) are slow and normally run
once; `main.py` starts from `02`. Scripts that read/write `Data/` are run by
Nicolas.

## 5. Tests

```bash
pytest tests/           # path config + a compile smoke-test over the pipeline
```
