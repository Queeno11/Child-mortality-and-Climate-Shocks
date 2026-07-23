"""Central path configuration for the Child Mortality & Climate Shocks project.

All scripts import their paths from here instead of hard-coding absolute paths.

Two kinds of locations:

1. **Project-internal** dirs (``Data/``, ``Outputs/`` and their sub-folders).
   These live inside the project folder and are derived automatically from this
   file's location, so they work on any machine / OS without configuration.

2. **External datasets** that are too big to keep inside the repo (ERA5
   reanalysis, Köppen climate bands, the country-classification lookup). Their
   locations differ per machine, so they are read from a ``.env`` file in the
   project root (see ``.env.example`` for the template). Following the pattern
   used in the NY State Aerial Imagery project, each external dataset gets its
   own env string, e.g. ``CLIMATE_BANDS`` for the Köppen grid.

Usage
-----
    from paths import DATA_IN, DATA_PROC, DATA_OUT, OUTPUTS, CLIMATE_BANDS
"""

from pathlib import Path
import os

try:
    from dotenv import load_dotenv
except ImportError:  # dotenv is optional; env vars can also be set in the shell
    def load_dotenv(*_args, **_kwargs):
        return False


# --- Project-internal locations (derived, no configuration needed) -----------
PROJECT_ROOT = Path(__file__).resolve().parent

# Load .env from the project root (if present). Values already set in the real
# environment take precedence over the file, so shell overrides keep working.
load_dotenv(PROJECT_ROOT / ".env")

DATA = PROJECT_ROOT / "Data"
DATA_IN = DATA / "Data_in"
DATA_PROC = DATA / "Data_proc"
DATA_OUT = DATA / "Data_out"
OUTPUTS = PROJECT_ROOT / "Outputs"


# --- External datasets (configured via .env) ---------------------------------
def _env_path(name, required=False):
    """Return the env var ``name`` as a ``Path``, or ``None`` if unset.

    If ``required`` is True and the variable is missing, raise a clear error
    pointing at ``.env.example`` so the user knows what to configure.
    """
    value = os.getenv(name)
    if value:
        return Path(value)
    if required:
        raise RuntimeError(
            f"Environment variable '{name}' is not set. Copy .env.example to "
            f".env and fill in the path to the '{name}' dataset."
        )
    return None


# Directory holding the monthly ERA5 single-level reanalysis (00_*, 01_*).
ERA5_MONTHLY_DIR = _env_path("ERA5_MONTHLY_DIR")

# Directory holding the daily ERA5 single-level reanalysis (00b_*).
ERA5_DAILY_DIR = _env_path("ERA5_DAILY_DIR")

# Köppen-Geiger climate-classification grid file (.grd) used to assign the
# climatic bands in 02b_assign_climatic_bands.py.
CLIMATE_BANDS = _env_path("CLIMATE_BANDS")

# Country income-group / metadata lookup workbook used in 03_merge_climate_and_DHS.py.
COUNTRY_CLASSIFICATION = _env_path("COUNTRY_CLASSIFICATION")
