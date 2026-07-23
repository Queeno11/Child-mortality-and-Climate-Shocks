"""Tests for the central path configuration (paths.py).

Run from the project root:  pytest tests/test_paths.py
"""

import os
import sys
import py_compile
from pathlib import Path

import pytest

# Make the project root importable (scripts live in the root).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import paths  # noqa: E402


def test_project_root_is_repo_root():
    assert paths.PROJECT_ROOT == PROJECT_ROOT
    assert (paths.PROJECT_ROOT / "paths.py").exists()


def test_internal_dirs_derived_from_root():
    assert paths.DATA == PROJECT_ROOT / "Data"
    assert paths.DATA_IN == PROJECT_ROOT / "Data" / "Data_in"
    assert paths.DATA_PROC == PROJECT_ROOT / "Data" / "Data_proc"
    assert paths.DATA_OUT == PROJECT_ROOT / "Data" / "Data_out"
    assert paths.OUTPUTS == PROJECT_ROOT / "Outputs"


def test_internal_dirs_are_pathlib_paths():
    for p in (paths.DATA, paths.DATA_IN, paths.DATA_PROC, paths.DATA_OUT, paths.OUTPUTS):
        assert isinstance(p, Path)


EXTERNAL_VARS = ("ERA5_MONTHLY_DIR", "ERA5_DAILY_DIR", "CLIMATE_BANDS", "COUNTRY_CLASSIFICATION")


def test_external_env_vars_loaded_from_dotenv():
    """The four external-dataset keys must be present in .env.

    Reading .env may be blocked inside the Claude sandbox (it's on the OS
    deny-read list); in that case dotenv returns nothing and we skip, since the
    real environment (where Nicolas runs the scripts) can read the file fine.
    """
    from dotenv import dotenv_values

    values = dotenv_values(paths.PROJECT_ROOT / ".env")
    if not values:
        pytest.skip(".env not readable here (sandbox denies ./.env)")
    for name in EXTERNAL_VARS:
        assert name in values, f"{name} missing from .env"
        assert values[name], f"{name} is empty in .env"


def test_external_vars_resolve_when_set(monkeypatch):
    """paths exposes each external dataset as a Path when the env var is set."""
    import importlib

    for name in EXTERNAL_VARS:
        monkeypatch.setenv(name, f"/some/where/{name}")
    reloaded = importlib.reload(paths)
    try:
        for name in EXTERNAL_VARS:
            value = getattr(reloaded, name)
            assert value == Path(f"/some/where/{name}")
            assert isinstance(value, Path)
    finally:
        importlib.reload(reloaded)  # restore module state for other tests


def test_env_path_missing_returns_none(monkeypatch):
    monkeypatch.delenv("SOME_UNSET_DATASET", raising=False)
    assert paths._env_path("SOME_UNSET_DATASET") is None


def test_env_path_required_raises(monkeypatch):
    monkeypatch.delenv("SOME_UNSET_DATASET", raising=False)
    with pytest.raises(RuntimeError):
        paths._env_path("SOME_UNSET_DATASET", required=True)


def test_env_path_reads_value(monkeypatch):
    monkeypatch.setenv("SOME_TEST_DATASET", "/tmp/some/where")
    assert paths._env_path("SOME_TEST_DATASET") == Path("/tmp/some/where")


@pytest.mark.parametrize(
    "script",
    [
        "paths.py",
        "00_query_ERA5_data.py",
        "00b_query_ERA5_dialy.py",
        "01_compute_climate_indices.py",
        "02_assign_shocks_to_DHS.py",
        "02b_assign_climatic_bands.py",
        "03_merge_climate_and_DHS.py",
        "06_charts.py",
        "plot_tools.py",
        "plot_tools_b.py",
        "plot_tools_alt.py",
    ],
)
def test_pipeline_scripts_compile(script):
    """Every pipeline script must be syntactically valid after the refactor."""
    py_compile.compile(str(PROJECT_ROOT / script), doraise=True)
