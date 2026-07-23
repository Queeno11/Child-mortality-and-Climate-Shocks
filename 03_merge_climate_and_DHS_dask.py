# ==============================================================================
# 03_merge_climate_and_DHS_dask.py
#
# Bounded-memory replacement for the Dask version of script 03, which died with
# `KilledWorker` before it produced a single row.
#
# WHY THE DASK VERSION DIED
# --------------------------------------------------------------------------
# ClimateShocks_assigned_v11_full.parquet holds 4,490,607 rows in only 5 row
# groups (~1,048,576 rows each). Dask cannot split a partition below a row-group
# boundary, so ONE partition is
#       1,048,576 rows x 478 kept columns x 4 bytes = 2.0 GB
# before any copy, cast or merge. With `memory_limit="3GB"` (2.79 GiB usable) a
# worker died the moment it materialised a single partition -- exactly what the
# traceback showed (`read_parquet-fused` killed on 4 workers, at the first task).
# `CHUNK_ROWS = 100_000` never applied to the climate side; it only set
# `npartitions` for the small DHS frame.
#
# Two further landmines behind that one:
#   * `.persist()` tried to hold a ~13 GB wide frame (8.6 GB of float climate
#     columns + ~4 GB of `_pos`/`_neg` booleans) in a 4 x 3 GB = 12 GB cluster.
#   * `.shuffle(on="ID_R")` moved all 478 climate columns across the network
#     just to rank births within a mother.
#
# WHAT THIS SCRIPT DOES INSTEAD
# --------------------------------------------------------------------------
# The climate parquet contributes ONLY the 468 climate-shock columns plus `ID`,
# `lat`, `lon`. Every operation that needs global information -- qcut
# thresholds, `groupby().ngroup()` factorisation, `groupby().rank()` for birth
# order, the age-at-death dummies -- depends solely on the DHS births extract
# and the climate-bands file, i.e. the *small* side (5.1M rows x ~30 columns).
#
#   Phase 1 (in RAM, small): build the entire non-climate table in plain pandas.
#           Exact qcut, exact ngroup, exact rank -- no Dask approximations.
#   Phase 2 (streamed, bounded): iterate the climate parquet in row batches,
#           align each batch to Phase 1 by ID, add the `_pos`/`_neg` dummies,
#           and append straight to the output Feather file.
#
# Peak RSS is (small table, ~2-3 GB) + (one batch, ~0.5 GB) + pyarrow read-ahead,
# instead of the 20+ GB the wide table needs. No cluster, no shuffle, no spill
# directory. Dask is no longer imported.
#
# BUGS FIXED RELATIVE TO THE DASK VERSION
# --------------------------------------------------------------------------
# 1. `rwi_tertiles` / `rwi_quintiles` came out as STRING categories ('3','2','1').
#    Cause: `qcut_from_edges()` returned a nullable `Int64` column, and
#    `categorize_columns()` then called `.unique().compute()` on it -- Dask
#    materialises nullable-integer uniques through an object/string path, so the
#    resulting `CategoricalDtype` had string categories. (The debug print showed
#    a clean `Int64` because the corruption happens *inside* `categorize_columns`,
#    not before it.) Fixed by dropping both helpers and using the original's
#    exact expression, `pd.qcut(x, n, labels=False) + 1` followed by
#    `pd.Categorical(...)`, which yields float64 categories 1.0/2.0/3.0 --
#    matching the reference file (`dictionary<values=double, indices=int8>`).
#
# 2. `ID_country` / `IDsurvey_country` "missing from the final output". This was
#    NOT a Dask bug, and `dask_ngroup()` is innocent: the original script never
#    exported them either. Section 6 builds `fixed_effects` as
#    `[c for c in births.columns if c.startswith("ID_cell")]`, which does not
#    match `ID_country`, and neither name appears anywhere else in `keep_vars`.
#    Confirmed against the reference file: both are absent from
#    DHSBirthsGlobal&ClimateShocks_v11_full.feather, and nothing in
#    04_regressions.jl / CustomModels.jl reads them. They are still computed
#    here; pass --keep-country-ids to append them to the output (which then
#    differs from the reference by exactly those two columns).
#
# 3. `climate.set_index("ID", sorted=True)` declared divisions that do not hold:
#    `ID` is NOT monotonically increasing (4,490,607 unique values in
#    [0, 5,145,098], `is_monotonic_increasing` is False), so that join could
#    silently misalign rows. This script aligns by hash lookup on `ID`, so
#    sortedness is irrelevant.
#
# 4. float16 down-casting used `max()` alone, so a column with max 10 and min
#    -1e10 was cast to float16 and its tail became -inf. The rule now uses
#    max(|min|, |max|), which can only ever widen a column to float32. Checked
#    against the real data: all 468 climate columns land on float16 under both
#    rules, so the output schema is unchanged.
#
# OUTPUT AND VERIFICATION
# --------------------------------------------------------------------------
# Writes DHSBirthsGlobal&ClimateShocks_v11_full_dask.feather (note the `_dask`
# suffix) so it can be diffed against the reference file produced by
# 03_merge_climate_and_DHS.py without overwriting it. Verify with:
#
#     pytest tests/test_merge_climate_and_DHS_dask.py          # synthetic data
#     python tests/compare_outputs.py                          # 1:1 vs reference
#
# USAGE
#   python 03_merge_climate_and_DHS_dask.py
#   python 03_merge_climate_and_DHS_dask.py --max-batches 2 --out /tmp/smoke.feather
# ==============================================================================

from __future__ import annotations

import argparse
import gc
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from paths import DATA_IN, DATA_PROC, DATA_OUT

DATA_IN = str(DATA_IN)
DATA_PROC = str(DATA_PROC)
DATA_OUT = str(DATA_OUT)


# ------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------
DEFAULT_BATCH_ROWS = 100_000  # rows per streamed batch -- the only RAM knob

# The original script computes ID_country / IDsurvey_country but drops them in
# section 6 (see header note 2). Leave False to match the reference file.
KEEP_COUNTRY_IDS = False

CLIMATE_PREFIXES = (
    "t_", "std_t_", "stdm_t_", "absdif_t_", "absdifm_t_",
    "spi", "hd35", "hd40", "fd", "id",
)

EXCLUDED_EXTREMES = ["hd35", "hd40", "fd", "id"]
EXCLUDED_WINDOWS = [f"b_w{i}" for i in range(1, 10)]

CLIMATE_LIST = ["absdifm_t", "stdm_t", "spi1", "hd35", "hd40", "fd", "id"]
TIME_LIST = [
    # TIMEFRAMES_QUARTERLY
    "inutero_1m3m", "inutero_3m6m", "inutero_6m9m",
    "born_1m3m", "born_3m6m", "born_6m9m", "born_9m12m",
    # TIMEFRAMES_BIANNUALY
    "inutero", "born_1m6m", "born_6m12m",
    "born_12m18m", "born_18m24m", "born_24m30m", "born_30m36m",
    # TIMEFRAMES_IUFOCUS
    "born_1m", "born_2m3m",
    # TIMEFRAMES_MONTHLY
    "inutero_1m", "inutero_2m", "inutero_3m",
    "inutero_4m", "inutero_5m", "inutero_6m",
    "inutero_7m", "inutero_8m", "inutero_9m",
    "born_2m", "born_3m", "born_4m", "born_5m", "born_6m",
]
STATS_LIST = [
    "q_min", "q_max", "q_avg",
    "m_avg",
    "iu_max", "iu_min", "iu_avg",
    "b_max", "b_min", "b_avg",
    "b_w1", "b_w2", "b_w3", "b_w4", "b_w5", "b_w6", "b_w7", "b_w8", "b_w9",
]

BINS_LABELS = {
    "quarterly": {
        "bins": [0, 3, 6, 9, 12, 15, np.inf],
        "labels": ["1m3m", "3m6m", "6m9m", "9m12m", "12m15m", "alive"],
    },
    "biannual": {
        "bins": [0, 6, 12, 18, 24, 30, 36, np.inf],
        "labels": ["1m6m", "6m12m", "12m18m", "18m24m", "24m30m", "30m36m", "alive"],
    },
    "inutero": {
        "bins": [0, 1, 3, 7, np.inf],
        "labels": ["1m", "2m3m", "3m7m", "alive"],
    },
    "months": {
        "bins": [0, 1, 2, 3, 4, 5, 6, np.inf],
        "labels": ["1m", "2m", "3m", "4m", "5m", "6m", "alive"],
    },
}

HETEROGENEITY_INDEXES = [
    "housing_quality_index", "heat_protection_index", "cold_protection_index",
    "World Risk Index", "Exposure Index", "Adaptive Capacity", "Coping Mechanisms",
    "ND Gain Index 2023",
]

MOTHER_EDUC_BINS = [0, 7, 13, 35, np.inf]
MOTHER_EDUC_LABELS = [
    "6 years or less", "6-12 years", "more than 12 years", "No data",
]

# Only the DHS variables that survive into the output (or feed something that
# does) are read from the .dta. The `child_agedeath_*` columns present in the
# .dta are deliberately NOT read: each is either recreated by the age-at-death
# dummy loop (3m6m, 6m12m) or dropped as obsolete (30d, 30d3m, 12m).
DHS_COLUMNS = [
    "v000", "ID_R", "ID_CB", "ID_HH", "code_iso3",
    "chb_year", "chb_month", "child_agedeath",
    "child_fem", "child_mulbirth", "rural", "poor",
    "weatlh_ind", "d_weatlh_ind_1", "d_weatlh_ind_2", "d_weatlh_ind_3",
    "d_weatlh_ind_4", "d_weatlh_ind_5",
    "mother_ageb", "mother_eduy",
    "LATNUM", "LONGNUM",
    "pipedw", "refrigerator", "electricity", "hhaircon", "hhfan",
    "housing_quality_index", "heat_protection_index", "cold_protection_index",
]

BANDS_COLUMNS = [
    "ID_HH", "ND Gain Index 2023", "World Risk Index", "Vulnerability Index",
    "Exposure Index", "Adaptive Capacity", "Coping Mechanisms",
    "climate_band_3", "climate_band_2", "climate_band_1", "rwi", "southern",
]

ID_VARS = ["ID", "ID_R", "ID_CB", "ID_HH", "lat", "lon", "code_iso3", "child_agedeath"]
CONTROLS = [
    "child_fem", "child_mulbirth", "birth_order", "rural", "poor",
    "weatlh_ind", "d_weatlh_ind_1", "d_weatlh_ind_2", "d_weatlh_ind_3",
    "d_weatlh_ind_4", "d_weatlh_ind_5",
    "mother_ageb", "mother_ageb_squ", "mother_ageb_cub",
    "mother_eduy", "mother_eduy_squ", "mother_eduy_cub", "mother_educ",
    "chb_month", "chb_year", "chb_year_sq", "rwi",
]
MECHANISM_BASE = [
    "pipedw", "refrigerator", "electricity", "hhaircon", "hhfan",
    "housing_quality_index", "heat_protection_index", "cold_protection_index",
]
HETEROGENEITIES = [
    "climate_band_3", "climate_band_2", "climate_band_1", "southern",
    "wbincomegroup", "rwi_tertiles", "rwi_quintiles",
]
OBSOLETE_DEATH_VARS = [
    "child_agedeath_30d", "child_agedeath_30d3m", "child_agedeath_12m",
]

# The original down-casts only these two width classes, which is why
# `birth_order` (explicitly int16) and the int16 death dummies survive as int16.
DOWNCAST_FLOAT_KINDS = ["float64", "float32"]
DOWNCAST_INT_KINDS = ["int64", "int32"]

# Plain Python floats on purpose. Under NumPy 2's NEP-50 weak promotion,
# `python_float < np.finfo(np.float16).max` casts the LEFT operand down to
# float16 first, so 65503.0 rounds up to 65504.0 and compares equal -- the
# boundary column would be widened to float32 (and 1e40 raises an overflow
# RuntimeWarning). Comparing against float64 constants keeps the arithmetic in
# float64, which is what the original script did (its left operand was a numpy
# float64 scalar from `.max()`).
FLOAT16_MAX = float(np.finfo(np.float16).max)
FLOAT32_MAX = float(np.finfo(np.float32).max)


# ------------------------------------------------------------------------
# 1. Column selection & dtype helpers
# ------------------------------------------------------------------------
def excluded_climate_columns(all_columns):
    """Columns removed by the `extremes x b_w*` exclusion rule of script 03."""
    excluded = set()
    for extreme in EXCLUDED_EXTREMES:
        for window in EXCLUDED_WINDOWS:
            excluded.update(
                col for col in all_columns if extreme in col and window in col
            )
    return excluded


def climate_shock_columns(all_columns):
    """Climate-shock columns to stream, in parquet order, after exclusions."""
    excluded = excluded_climate_columns(all_columns)
    return [
        col
        for col in all_columns
        if col.startswith(CLIMATE_PREFIXES) and col not in excluded
    ]


def smallest_float_dtype(max_abs):
    """
    Smallest float dtype that can hold ``max_abs``.

    Driven by max(|min|, |max|) rather than the max alone, so a column with a
    large negative tail is not cast to float16 and turned into -inf. Returns
    None when even float32 is too small (caller leaves the column untouched).
    """
    if max_abs is None or pd.isna(max_abs):
        return None
    max_abs = float(max_abs)
    if max_abs < FLOAT16_MAX:
        return "float16"
    if max_abs < FLOAT32_MAX:
        return "float32"
    return None


def smallest_int_dtype(min_val, max_val):
    """Equivalent of ``pd.to_numeric(..., downcast='integer')`` from bounds."""
    if pd.isna(min_val) or pd.isna(max_val):
        return None
    for dtype in ("int8", "int16", "int32", "int64"):
        info = np.iinfo(dtype)
        if min_val >= info.min and max_val <= info.max:
            return dtype
    return None


def parquet_absolute_maxima(parquet_file, columns):
    """
    max(|min|, |max|) per column, read from the parquet footer statistics.

    Every column chunk of ClimateShocks_assigned_v11_full.parquet carries
    min/max statistics, so the whole down-casting decision costs zero bytes of
    data read -- the previous versions paid a full pass over 8.6 GB for it.
    Columns without statistics map to None (left untouched by the caller).
    """
    wanted = set(columns)
    metadata = parquet_file.metadata
    maxima = {col: None for col in columns}

    for rg_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_index)
        for col_index in range(row_group.num_columns):
            chunk = row_group.column(col_index)
            name = chunk.path_in_schema
            if name not in wanted:
                continue
            stats = chunk.statistics
            if stats is None or not stats.has_min_max:
                continue
            candidate = max(abs(float(stats.min)), abs(float(stats.max)))
            current = maxima[name]
            maxima[name] = candidate if current is None else max(current, candidate)
    return maxima


def build_float_cast_map(maxima):
    """{column: dtype} for every column that can be narrowed."""
    cast_map = {}
    for col, max_abs in maxima.items():
        dtype = smallest_float_dtype(max_abs)
        if dtype is None:
            print(f"Column {col} too large for float32, skipping...")
            continue
        cast_map[col] = dtype
    return cast_map


def _absolute_max(series):
    """max(|x|) ignoring NaN; NaN when the column is empty or all-NaN."""
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype="float64")
    if values.size == 0:
        return np.nan
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return np.nanmax(np.abs(values))


def downcast_floats(df, columns=None):
    """Narrow float columns in place, using each column's own range."""
    if columns is None:
        columns = df.select_dtypes(include=DOWNCAST_FLOAT_KINDS).columns
    for col in columns:
        dtype = smallest_float_dtype(_absolute_max(df[col]))
        if dtype is None:
            print(f"Column {col} too large for float32, skipping...")
            continue
        df[col] = df[col].astype(dtype)
    return df


def downcast_ints(df, columns=None):
    """Narrow integer columns in place."""
    if columns is None:
        columns = df.select_dtypes(include=DOWNCAST_INT_KINDS).columns
    for col in columns:
        series = df[col]
        if not len(series):
            continue
        dtype = smallest_int_dtype(series.min(), series.max())
        if dtype is not None:
            df[col] = series.astype(dtype)
    return df


# ------------------------------------------------------------------------
# 2. Climate-side feature engineering (runs per streamed batch)
# ------------------------------------------------------------------------
def pos_neg_base_columns(available):
    """
    Ordered list of climate columns that receive `_pos` / `_neg` dummies.

    Mirrors the triple loop of the original script, so the generated column
    order is identical to the reference file's (CLIMATE_LIST-major, which is
    NOT the parquet column order).
    """
    available = set(available)
    return [
        f"{var}_{timeframe}_{stat}"
        for var in CLIMATE_LIST
        for timeframe in TIME_LIST
        for stat in STATS_LIST
        if f"{var}_{timeframe}_{stat}" in available
    ]


def pos_neg_column_names(bases):
    names = []
    for base in bases:
        names.append(f"{base}_pos")
        names.append(f"{base}_neg")
    return names


def add_pos_neg_dummies(chunk, bases):
    """
    Append the `_pos` / `_neg` boolean dummies for ``bases``.

    Computed after the float16 cast, exactly as in the original (which cast the
    climate table before the join and built the dummies after it). NaN rows get
    False on both sides; exact zeros get True on both.
    """
    if not bases:
        return chunk
    dummies = {}
    for base in bases:
        series = chunk[base]
        dummies[f"{base}_pos"] = (series >= 0).astype(bool)
        dummies[f"{base}_neg"] = (series <= 0).astype(bool)
    return pd.concat([chunk, pd.DataFrame(dummies, index=chunk.index)], axis=1)


# ------------------------------------------------------------------------
# 3. Births-side feature engineering (runs once, in RAM)
# ------------------------------------------------------------------------
def add_mother_covariates(df):
    """Squared/cubed mother age & education plus the education bands."""
    age = df["mother_ageb"]
    df["mother_ageb_squ"] = pd.to_numeric(age ** 2, downcast="float")
    df["mother_ageb_cub"] = pd.to_numeric(age ** 3, downcast="float")

    edu = df["mother_eduy"]
    df["mother_eduy_squ"] = pd.to_numeric(edu ** 2, downcast="integer")
    df["mother_eduy_cub"] = pd.to_numeric(edu ** 3, downcast="integer")

    df["mother_educ"] = pd.cut(
        df["mother_eduy"], MOTHER_EDUC_BINS, labels=MOTHER_EDUC_LABELS
    )
    return df


def add_birth_order(df):
    """Rank of each birth within its mother, by year * 12 + month."""
    months = df["chb_year"].astype(int) * 12 + df["chb_month"].astype(int)
    df["birth_order"] = (
        months.groupby(df["ID_R"]).rank(method="first", ascending=True).astype("int16")
    )
    return df


def add_wealth_indicators(df):
    """
    Exact tertile / quintile splits of the relative wealth index.

    `labels=False` + 1 reproduces the original's expression. The explicit
    float64 cast pins the result to the reference file's
    `dictionary<values=double, indices=int8>`: `pd.qcut(..., labels=False)`
    returns int64 when the input has NO missing values, and float64 otherwise,
    so without the cast the output dtype would silently depend on whether `rwi`
    happens to have NaNs. This is the fix for the string-category bug of the
    Dask version -- see header note 1.
    """
    df["rwi_tertiles"] = (pd.qcut(df["rwi"], 3, labels=False) + 1).astype("float64")
    df["rwi_quintiles"] = (pd.qcut(df["rwi"], 5, labels=False) + 1).astype("float64")
    return df


def heterogeneity_dummy_name(index_name):
    return "high_" + index_name.lower().replace(" ", "_").replace("_index", "")


def add_heterogeneity_dummies(df, indexes=None):
    """Median splits (`qcut(..., 2)`) of each vulnerability/adaptation index."""
    for index_name in HETEROGENEITY_INDEXES if indexes is None else indexes:
        df[heterogeneity_dummy_name(index_name)] = pd.qcut(
            df[index_name], 2, labels=False
        )
    return df


def add_high_vulnerability(df):
    """
    Fixed cut-point from the World Risk Index Report 2023.

    Reproduces the original's two `.loc` assignments, including that rows with a
    missing Vulnerability Index stay NaN rather than becoming 0.
    """
    df.loc[df["Vulnerability Index"] >= 25.02, "high_vulnerability"] = 1
    df.loc[df["Vulnerability Index"] < 25.02, "high_vulnerability"] = 0
    return df


def add_child_agedeath_dummies(df, bins_labels=None, verbose=True):
    """
    Per-1,000-births death dummies for each binning scheme.

    Returns ``(df, death_vars)`` where ``death_vars`` is the resulting column
    order. Labels shared between schemes (e.g. "alive", created by all four) are
    dropped and recreated, which moves them to the end -- reproducing the
    reference file's ordering.
    """
    bins_labels = BINS_LABELS if bins_labels is None else bins_labels
    df["child_agedeath"] = df["child_agedeath"].fillna(1000)

    death_vars = []
    for spec in bins_labels.values():
        bins, labels = spec["bins"], spec["labels"]
        names = [f"child_agedeath_{lab}" for lab in labels]

        df.drop(columns=names, errors="ignore", inplace=True)
        death_vars = [c for c in death_vars if c not in names]

        cat = pd.cut(df["child_agedeath"], bins=bins, labels=labels, right=False)
        if verbose:
            print(cat.value_counts())
        for lab, name in zip(labels, names):
            df[name] = ((cat == lab) * 1_000).astype("int16")
            assert df[name].mean() > 0, lab
            death_vars.append(name)
    return df, death_vars


def add_location_fixed_effects(df, keep_country_ids=None):
    """0.25 deg (ERA5 cell), 0.5 deg and 1 deg cell IDs, plus country/survey IDs."""
    if keep_country_ids is None:
        keep_country_ids = KEEP_COUNTRY_IDS

    df["lat_climate_1"] = df["lat"]
    df["lon_climate_1"] = df["lon"]

    lat, lon = df["LATNUM"], df["LONGNUM"]
    df["lat_climate_2"] = np.round(lat * 2) / 2
    df["lon_climate_2"] = np.round(lon * 2) / 2
    df["lat_climate_3"] = np.round(lat)
    df["lon_climate_3"] = np.round(lon)

    for j in range(1, 4):
        df[f"ID_cell{j}"] = df.groupby(
            [f"lat_climate_{j}", f"lon_climate_{j}"], sort=False
        ).ngroup()

    # Always computed (the original does too); only exported when requested.
    df["ID_country"] = df.groupby("code_iso3", sort=False).ngroup()
    df["IDsurvey_country"] = df.groupby("v000").ngroup()
    df.attrs["keep_country_ids"] = bool(keep_country_ids)
    return df


def categorize(df, columns):
    for col in columns:
        df[col] = pd.Categorical(df[col])
    return df


# ------------------------------------------------------------------------
# 4. Phase 1 -- build the non-climate table
# ------------------------------------------------------------------------
def load_births(dhs_path, columns=None):
    """Read the DHS births extract and attach the positional ``ID``."""
    births = pd.read_stata(dhs_path, columns=DHS_COLUMNS if columns is None else columns)
    births["ID"] = np.arange(len(births), dtype="int64")
    return births


def load_climate_coordinates(parquet_path):
    """`ID`, `lat`, `lon` only -- three columns, cheap to hold in full."""
    return pq.read_table(parquet_path, columns=["ID", "lat", "lon"]).to_pandas()


def load_income_groups(xlsx_path):
    df_iso = pd.read_excel(xlsx_path)
    df_iso = df_iso.rename(columns={"wbcode": "code_iso3"})
    return df_iso[["code_iso3", "wbincomegroup"]]


def load_bands(parquet_path, columns=None):
    columns = BANDS_COLUMNS if columns is None else columns
    available = set(pq.ParquetFile(parquet_path).schema_arrow.names)
    missing = [c for c in columns if c not in available]
    if missing:
        raise KeyError(f"Climate-bands file is missing columns: {missing}")
    return pq.read_table(parquet_path, columns=columns).to_pandas()


def build_births_table(births, coordinates, income_groups, bands,
                       keep_country_ids=None, verbose=True):
    """
    Phase 1: every non-climate column of the final dataset, in RAM.

    The join order reproduces the original script exactly -- climate (left) ->
    births -> income group -> climate bands, all inner joins -- so the surviving
    rows come out in the climate parquet's own row order. (Verified against the
    reference file: the parquet's ID sequence, filtered by these joins, equals
    the reference file's ID sequence element for element.)
    """
    merged = coordinates.merge(births, on="ID", how="inner")
    if verbose:
        print(f"After climate/births join: {len(merged)}")

    merged = merged.merge(income_groups, on="code_iso3", how="inner")
    if verbose:
        print(f"After income-group join: {len(merged)}")

    merged = merged.merge(bands, on="ID_HH", how="inner")
    if verbose:
        print(f"Data loaded! Number of observations: {len(merged)}")

    if not merged["ID"].is_unique:
        raise ValueError(
            "ID is no longer unique after the merges -- the country-classification "
            "or climate-bands lookup has duplicate keys, which would duplicate "
            "births rows."
        )

    if verbose:
        print("Creating variables...")
    merged = add_mother_covariates(merged)
    merged = add_birth_order(merged)
    merged["chb_year_sq"] = merged["chb_year"] ** 2

    for col in ["hhaircon", "hhfan"]:
        merged[col] = merged[col].map(lambda x: 1 if x == "Yes" else 0).astype(bool)

    merged = add_wealth_indicators(merged)
    merged = add_heterogeneity_dummies(merged)
    merged = add_high_vulnerability(merged)
    merged, death_vars = add_child_agedeath_dummies(merged, verbose=verbose)

    if verbose:
        print("Creating location and time fixed effects...")
    merged = add_location_fixed_effects(merged, keep_country_ids=keep_country_ids)

    return merged, death_vars


def select_and_compress(births_table, death_vars, verbose=True):
    """Apply `keep_vars` (non-climate part) and down-cast dtypes."""
    fixed_effects = [c for c in births_table.columns if c.startswith("ID_cell")]
    mechanisms = MECHANISM_BASE + [c for c in births_table.columns if "high_" in c]
    country_ids = (
        ["ID_country", "IDsurvey_country"]
        if births_table.attrs.get("keep_country_ids", KEEP_COUNTRY_IDS)
        else []
    )

    if verbose:
        print("Dropping variables...")
    keep_raw = (
        ID_VARS + CONTROLS + death_vars + fixed_effects
        + mechanisms + HETEROGENEITIES + country_ids
    )
    seen = set()
    keep = [c for c in keep_raw if not (c in seen or seen.add(c))]
    keep = [c for c in keep if c not in OBSOLETE_DEATH_VARS]

    missing = [c for c in keep if c not in births_table.columns]
    if missing:
        raise KeyError(f"Expected columns absent from the births table: {missing}")

    table = births_table[keep].copy()

    if verbose:
        print("Recasting categoricals...")
    table = categorize(table, [c for c in HETEROGENEITIES if c in table.columns])

    if verbose:
        print("Recasting floats...")
    table = downcast_floats(table)

    if verbose:
        print("Recasting ints...")
    table = downcast_ints(table)

    return table


# ------------------------------------------------------------------------
# 5. Phase 2 -- stream the climate parquet into the output
# ------------------------------------------------------------------------
def final_column_order(non_climate_columns, shock_columns, dummy_columns):
    """
    `keep_vars` order of the original script: IDs, then every prefix-matching
    climate column (parquet columns first, then the generated dummies), then
    controls / death vars / fixed effects / mechanisms / heterogeneities.
    """
    ids = [c for c in ID_VARS if c in non_climate_columns]
    rest = [c for c in non_climate_columns if c not in ids]
    return ids + list(shock_columns) + list(dummy_columns) + rest


def align_batch(chunk, non_climate):
    """
    Inner-join one climate batch to the Phase-1 table on ``ID``.

    Rows of ``chunk`` whose ID is absent from ``non_climate`` were dropped by one
    of the Phase-1 inner joins. Returns ``(left, chunk)``, both positionally
    aligned with a fresh RangeIndex, with ``ID`` removed from ``chunk``.
    """
    positions = non_climate.index.get_indexer(chunk["ID"].to_numpy())
    keep = positions >= 0
    if keep.all():
        chunk = chunk.reset_index(drop=True)
    else:
        chunk = chunk.loc[keep].reset_index(drop=True)
        positions = positions[keep]

    left = non_climate.iloc[positions].reset_index(drop=True)
    return left, chunk.drop(columns=["ID"])


def stream_climate_to_feather(
    climate_path,
    non_climate,
    shock_columns,
    cast_map,
    out_path,
    batch_rows=DEFAULT_BATCH_ROWS,
    max_batches=None,
):
    """
    Phase 2: read the climate parquet batch by batch, join, and append.

    Only ever holds one batch plus the (already built) Phase-1 table, so peak
    memory is independent of the 4.5M x 468 size of the climate table.
    """
    bases = pos_neg_base_columns(shock_columns)
    dummy_columns = pos_neg_column_names(bases)
    column_order = final_column_order(
        list(non_climate.columns), shock_columns, dummy_columns
    )

    parquet_file = pq.ParquetFile(climate_path)
    total_batches = -(-parquet_file.metadata.num_rows // batch_rows)
    if max_batches is not None:
        total_batches = min(total_batches, max_batches)

    schema = None
    writer = None
    rows_written = 0

    with pa.OSFile(str(out_path), "wb") as sink:
        batches = parquet_file.iter_batches(
            batch_size=batch_rows, columns=["ID"] + list(shock_columns)
        )
        for batch_index, batch in enumerate(
            tqdm(batches, total=total_batches, desc="climate -> feather")
        ):
            if max_batches is not None and batch_index >= max_batches:
                break

            chunk = batch.to_pandas()
            left, chunk = align_batch(chunk, non_climate)
            if len(chunk) == 0:
                continue

            chunk = chunk.astype(
                {c: d for c, d in cast_map.items() if c in chunk.columns}
            )
            chunk = add_pos_neg_dummies(chunk, bases)

            out = pd.concat([left, chunk], axis=1)[column_order]

            if writer is None:
                table = pa.Table.from_pandas(out, preserve_index=False)
                schema = table.schema
                writer = pa.ipc.new_file(
                    sink, schema, options=pa.ipc.IpcWriteOptions(compression="zstd")
                )
            else:
                table = pa.Table.from_pandas(out, schema=schema, preserve_index=False)

            for record_batch in table.to_batches():
                writer.write_batch(record_batch)
            rows_written += len(out)

            del chunk, left, out, table
            gc.collect()

        if writer is None:
            raise RuntimeError("No rows survived the merges -- nothing was written.")
        writer.close()

    return rows_written, column_order


def run_pipeline(climate_path, dhs_path, bands_path, iso_path, out_path,
                 batch_rows=DEFAULT_BATCH_ROWS, max_batches=None,
                 keep_country_ids=None, verbose=True):
    """Full Phase 1 + Phase 2 run. Returns ``(rows_written, column_order)``."""
    if verbose:
        print("Loading and merging data...")
    parquet_file = pq.ParquetFile(climate_path)
    shock_columns = climate_shock_columns(parquet_file.schema_arrow.names)
    if verbose:
        print(
            f"{len(parquet_file.schema_arrow.names)} columns in the climate file, "
            f"{len(shock_columns)} climate-shock columns streamed"
        )
        print("Choosing float dtypes from the parquet footer statistics...")
    cast_map = build_float_cast_map(parquet_absolute_maxima(parquet_file, shock_columns))

    births = load_births(dhs_path)
    if verbose:
        print(f"DHS births rows: {len(births)}")

    coordinates = load_climate_coordinates(climate_path)
    income_groups = load_income_groups(iso_path)
    bands = load_bands(bands_path)

    births_table, death_vars = build_births_table(
        births, coordinates, income_groups, bands,
        keep_country_ids=keep_country_ids, verbose=verbose,
    )
    del births, coordinates, income_groups, bands
    gc.collect()

    non_climate = select_and_compress(births_table, death_vars, verbose=verbose)
    del births_table
    gc.collect()

    non_climate.index = pd.Index(non_climate["ID"].to_numpy(), name="ID")
    if verbose:
        print(
            f"Non-climate table: {non_climate.shape[0]} rows x "
            f"{non_climate.shape[1]} columns, "
            f"{non_climate.memory_usage(deep=True).sum() / 1e9:.2f} GB in RAM"
        )
        print("Writing files...")

    return stream_climate_to_feather(
        climate_path, non_climate, shock_columns, cast_map, out_path,
        batch_rows=batch_rows, max_batches=max_batches,
    )


# ------------------------------------------------------------------------
# 6. Entry point
# ------------------------------------------------------------------------
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Stream-merge the climate shocks onto the DHS births extract."
    )
    parser.add_argument(
        "--batch-rows", type=int, default=DEFAULT_BATCH_ROWS,
        help="rows per streamed batch (the memory knob)",
    )
    parser.add_argument(
        "--max-batches", type=int, default=None,
        help="stop after N batches (smoke test; produces a partial file)",
    )
    parser.add_argument(
        "--out",
        default=rf"{DATA_OUT}/DHSBirthsGlobal&ClimateShocks_v11_full_dask.feather",
        help="output Feather path",
    )
    parser.add_argument(
        "--keep-country-ids", action="store_true",
        help="also export ID_country / IDsurvey_country (absent from the reference)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    rows_written, column_order = run_pipeline(
        climate_path=rf"{DATA_PROC}/ClimateShocks_assigned_v11_full.parquet",
        dhs_path=rf"{DATA_IN}/DHS/DHSBirthsGlobalAnalysis_07272025.dta",
        bands_path=(
            rf"{DATA_PROC}/DHSBirthsGlobalAnalysis_07272025_climate_bands_assigned.parquet"
        ),
        iso_path=rf"{DATA_IN}/WB Country Classification/wb_country_classification.xlsx",
        out_path=args.out,
        batch_rows=args.batch_rows,
        max_batches=args.max_batches,
        keep_country_ids=True if args.keep_country_ids else None,
    )

    print(
        f"✓ Files written:\n  • {args.out}"
        f"\n  {rows_written} rows x {len(column_order)} columns"
    )


if __name__ == "__main__":
    main()
