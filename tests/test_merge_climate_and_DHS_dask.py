"""
Synthetic-data unit tests for 03_merge_climate_and_DHS_dask.py.

    pytest tests/test_merge_climate_and_DHS_dask.py -v

No real data is touched: every fixture is generated in a tmp_path.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_module():
    """The script name starts with a digit, so it needs an explicit loader."""
    path = ROOT / "03_merge_climate_and_DHS_dask.py"
    spec = importlib.util.spec_from_file_location("merge_climate_dask", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


m = _load_module()


SAMPLE_COLUMNS = [
    "ID", "lat", "lon", "point_ID", "birth_date",
    "spi1_inutero_1m3m_q_avg",
    "stdm_t_inutero_1m3m_q_avg",
    "absdifm_t_inutero_1m3m_q_avg",
    "hd35_inutero_1m3m_q_avg",
    "absdifm_t_born_1m3m_b_max",
    "stdm_t_born_1m6m_b_w1",
    "hd35_born_1m6m_b_w1",        # dropped: extreme x b_w window
]
SHOCK_COLUMNS = [
    "spi1_inutero_1m3m_q_avg",
    "stdm_t_inutero_1m3m_q_avg",
    "absdifm_t_inutero_1m3m_q_avg",
    "hd35_inutero_1m3m_q_avg",
    "absdifm_t_born_1m3m_b_max",
    "stdm_t_born_1m6m_b_w1",
]


# ------------------------------------------------------------------ column selection
def test_excluded_climate_columns_targets_extremes_times_windows():
    excluded = m.excluded_climate_columns(SAMPLE_COLUMNS)
    assert "hd35_born_1m6m_b_w1" in excluded
    assert "stdm_t_born_1m6m_b_w1" not in excluded   # b_w window, but not an extreme
    assert "hd35_inutero_1m3m_q_avg" not in excluded  # extreme, but not a b_w window


def test_climate_shock_columns_preserves_parquet_order():
    assert m.climate_shock_columns(SAMPLE_COLUMNS) == SHOCK_COLUMNS


def test_climate_shock_columns_drops_non_climate():
    got = m.climate_shock_columns(SAMPLE_COLUMNS)
    for name in ("ID", "lat", "lon", "point_ID", "birth_date"):
        assert name not in got


# ------------------------------------------------------------------ dtype helpers
@pytest.mark.parametrize(
    "max_abs, expected",
    [
        (3.09, "float16"),
        (65503.0, "float16"),
        (65504.0, "float32"),
        (1e30, "float32"),
        (1e40, None),
        (np.nan, None),
        (None, None),
    ],
)
def test_smallest_float_dtype(max_abs, expected):
    assert m.smallest_float_dtype(max_abs) == expected


@pytest.mark.parametrize(
    "lo, hi, expected",
    [
        (0, 1, "int8"),
        (-128, 127, "int8"),
        (0, 200, "int16"),
        (0, 5_145_098, "int32"),
        (0, 2**40, "int64"),
        (np.nan, 5, None),
    ],
)
def test_smallest_int_dtype(lo, hi, expected):
    assert m.smallest_int_dtype(lo, hi) == expected


def test_parquet_absolute_maxima_uses_footer_statistics(tmp_path):
    """Bug 4: a big negative tail must widen the column, not overflow float16."""
    path = tmp_path / "stats.parquet"
    table = pa.table(
        {
            "narrow": pa.array([-3.09, 0.0, 3.09], type=pa.float32()),
            "negative_tail": pa.array([-100_000.0, 0.0, 10.0], type=pa.float32()),
        }
    )
    pq.write_table(table, path, row_group_size=2)

    maxima = m.parquet_absolute_maxima(pq.ParquetFile(path), ["narrow", "negative_tail"])
    assert maxima["narrow"] == pytest.approx(3.09, rel=1e-5)
    assert maxima["negative_tail"] == pytest.approx(100_000.0, rel=1e-5)

    cast_map = m.build_float_cast_map(maxima)
    assert cast_map["narrow"] == "float16"
    assert cast_map["negative_tail"] == "float32"   # the old max-only rule gave -inf


def test_downcast_floats_and_ints_respect_the_original_width_classes():
    df = pd.DataFrame(
        {
            "small_float": np.array([1.0, 2.0], dtype="float64"),
            "big_float": np.array([-1e6, 1.0], dtype="float64"),
            "wide_int": np.array([0, 300], dtype="int64"),
            "already_int16": np.array([0, 1], dtype="int16"),
        }
    )
    m.downcast_floats(df)
    m.downcast_ints(df)

    assert df["small_float"].dtype == "float16"
    assert df["big_float"].dtype == "float32"
    assert df["wide_int"].dtype == "int16"
    # the original only down-casts int64/int32, which is why birth_order stays int16
    assert df["already_int16"].dtype == "int16"


# ------------------------------------------------------------------ pos/neg dummies
def test_pos_neg_base_columns_is_climate_list_major():
    """Order follows CLIMATE_LIST x TIME_LIST x STATS_LIST, not the parquet order."""
    assert m.pos_neg_base_columns(SHOCK_COLUMNS) == [
        "absdifm_t_inutero_1m3m_q_avg",
        "absdifm_t_born_1m3m_b_max",
        "stdm_t_inutero_1m3m_q_avg",
        "stdm_t_born_1m6m_b_w1",
        "spi1_inutero_1m3m_q_avg",
        "hd35_inutero_1m3m_q_avg",
    ]


def test_pos_neg_column_names_interleaves_pos_then_neg():
    assert m.pos_neg_column_names(["a", "b"]) == ["a_pos", "a_neg", "b_pos", "b_neg"]


def test_add_pos_neg_dummies_semantics():
    chunk = pd.DataFrame({"x": np.array([2.0, 0.0, -2.0, np.nan], dtype="float16")})
    out = m.add_pos_neg_dummies(chunk, ["x"])

    assert out["x_pos"].tolist() == [True, True, False, False]
    assert out["x_neg"].tolist() == [False, True, True, False]
    assert out["x_pos"].dtype == bool and out["x_neg"].dtype == bool


# ------------------------------------------------------------------ births-side features
def test_add_mother_covariates_education_bands():
    df = pd.DataFrame(
        {"mother_ageb": [20.0, 30.0], "mother_eduy": [5.0, 20.0]}
    )
    m.add_mother_covariates(df)

    assert df["mother_ageb_squ"].tolist() == [400.0, 900.0]
    assert df["mother_ageb_cub"].tolist() == [8000.0, 27000.0]
    assert list(df["mother_educ"]) == ["6 years or less", "more than 12 years"]


def test_add_mother_covariates_leaves_zero_education_missing():
    """pd.cut with bins starting at 0 and right=True excludes 0 itself."""
    df = pd.DataFrame({"mother_ageb": [20.0], "mother_eduy": [0.0]})
    m.add_mother_covariates(df)
    assert pd.isna(df["mother_educ"].iloc[0])


def test_add_birth_order_ranks_within_mother_and_breaks_ties_by_position():
    df = pd.DataFrame(
        {
            "ID_R": ["a", "a", "a", "b", "b"],
            "chb_year": [2005, 2001, 2003, 2010, 2010],
            "chb_month": [1, 6, 3, 5, 5],
        }
    )
    m.add_birth_order(df)
    assert df["birth_order"].tolist() == [3, 1, 2, 1, 2]
    assert df["birth_order"].dtype == "int16"


def test_add_wealth_indicators_yields_float_categories_not_strings():
    """Regression test for bug 1 (string categories '3','2','1')."""
    df = pd.DataFrame({"rwi": np.linspace(0.0, 1.0, 30)})
    m.add_wealth_indicators(df)
    m.categorize(df, ["rwi_tertiles", "rwi_quintiles"])

    tertiles = df["rwi_tertiles"]
    assert isinstance(tertiles.dtype, pd.CategoricalDtype)
    assert tertiles.cat.categories.dtype == np.float64
    assert sorted(tertiles.cat.categories.tolist()) == [1.0, 2.0, 3.0]
    assert sorted(df["rwi_quintiles"].cat.categories.tolist()) == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_add_wealth_indicators_keeps_missing_rwi_missing():
    df = pd.DataFrame({"rwi": [0.0, 1.0, 2.0, np.nan, 4.0, 5.0]})
    m.add_wealth_indicators(df)
    assert pd.isna(df["rwi_tertiles"].iloc[3])


def test_heterogeneity_dummy_names():
    assert m.heterogeneity_dummy_name("World Risk Index") == "high_world_risk"
    assert m.heterogeneity_dummy_name("ND Gain Index 2023") == "high_nd_gain_2023"
    assert m.heterogeneity_dummy_name("housing_quality_index") == "high_housing_quality"
    assert m.heterogeneity_dummy_name("Adaptive Capacity") == "high_adaptive_capacity"


def test_add_high_vulnerability_preserves_nan():
    df = pd.DataFrame({"Vulnerability Index": [10.0, 25.02, 30.0, np.nan]})
    m.add_high_vulnerability(df)
    assert df["high_vulnerability"].tolist()[:3] == [0.0, 1.0, 1.0]
    assert pd.isna(df["high_vulnerability"].iloc[3])


def test_add_child_agedeath_dummies_column_order_matches_reference_file():
    values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 13, 16, 20, 26, 32, 40, np.nan]
    df = pd.DataFrame({"child_agedeath": np.array(values, dtype="float64")})

    df, death_vars = m.add_child_agedeath_dummies(df, verbose=False)

    assert death_vars == [
        "child_agedeath_1m3m", "child_agedeath_3m6m", "child_agedeath_6m9m",
        "child_agedeath_9m12m", "child_agedeath_12m15m",
        "child_agedeath_1m6m", "child_agedeath_6m12m", "child_agedeath_12m18m",
        "child_agedeath_18m24m", "child_agedeath_24m30m", "child_agedeath_30m36m",
        "child_agedeath_2m3m", "child_agedeath_3m7m",
        "child_agedeath_1m", "child_agedeath_2m", "child_agedeath_3m",
        "child_agedeath_4m", "child_agedeath_5m", "child_agedeath_6m",
        "child_agedeath_alive",
    ]
    assert df["child_agedeath"].iloc[-1] == 1000       # NaN -> 1000 sentinel
    assert set(df["child_agedeath_1m"].unique()) <= {0, 1000}
    assert df["child_agedeath_1m"].dtype == "int16"


def test_add_location_fixed_effects_rounding_and_group_ids():
    df = pd.DataFrame(
        {
            "lat": [1.25, 1.25, 9.75],
            "lon": [2.50, 2.50, 8.25],
            "LATNUM": [1.3, 1.3, 9.8],
            "LONGNUM": [2.6, 2.6, 8.1],
            "code_iso3": ["AAA", "AAA", "BBB"],
            "v000": ["A1", "A1", "B1"],
        }
    )
    m.add_location_fixed_effects(df)

    assert df["lat_climate_2"].tolist() == [1.5, 1.5, 10.0]
    assert df["lat_climate_3"].tolist() == [1.0, 1.0, 10.0]
    assert df["ID_cell1"].tolist() == [0, 0, 1]
    assert df["ID_country"].tolist() == [0, 0, 1]
    assert df["IDsurvey_country"].tolist() == [0, 0, 1]


# ------------------------------------------------------------------ streaming layer
def test_final_column_order_places_ids_then_climate_then_the_rest():
    non_climate = ["ID", "lat", "code_iso3", "birth_order", "rwi_tertiles"]
    order = m.final_column_order(non_climate, ["shock_a"], ["shock_a_pos", "shock_a_neg"])
    assert order == [
        "ID", "lat", "code_iso3",
        "shock_a", "shock_a_pos", "shock_a_neg",
        "birth_order", "rwi_tertiles",
    ]


def test_align_batch_inner_joins_and_preserves_chunk_order():
    non_climate = pd.DataFrame({"ID": [10, 30], "v": ["a", "b"]})
    non_climate.index = pd.Index(non_climate["ID"].to_numpy(), name="ID")
    chunk = pd.DataFrame({"ID": [30, 20, 10], "x": [3.0, 2.0, 1.0]})

    left, right = m.align_batch(chunk, non_climate)

    assert right["x"].tolist() == [3.0, 1.0]       # ID 20 dropped, order kept
    assert left["v"].tolist() == ["b", "a"]        # aligned row-for-row
    assert "ID" not in right.columns


# ------------------------------------------------------------------ end-to-end
def _write_climate_parquet(path, ids, row_group_size=8, seed=0):
    rng = np.random.default_rng(seed)
    n = len(ids)
    data = {"ID": np.asarray(ids, dtype="int64"),
            "lat": np.round(rng.uniform(-10, 10, n) * 4) / 4,
            "lon": np.round(rng.uniform(-10, 10, n) * 4) / 4}
    for col in SHOCK_COLUMNS:
        data[col] = rng.normal(size=n).astype("float32")
    pq.write_table(pa.table(data), path, row_group_size=row_group_size)
    return path


def _synthetic_frames(n=24, seed=1):
    """births / income-group / bands frames covering every required column."""
    rng = np.random.default_rng(seed)
    ids = np.arange(n, dtype="int64")

    births = {c: rng.normal(size=n) for c in m.DHS_COLUMNS}
    births.update(
        {
            "ID": ids,
            "v000": np.where(ids < n // 2, "A1", "B1"),
            "ID_R": np.repeat(np.arange(n // 3), 3)[:n].astype(str),
            "ID_CB": ids.astype(str),
            "ID_HH": np.array([f"hh{i}" for i in ids]),
            "code_iso3": np.where(ids < n // 2, "AAA", "BBB"),
            "chb_year": np.full(n, 2005.0) + (ids % 5),
            "chb_month": (ids % 12) + 1.0,
            "child_agedeath": np.array(
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 13, 16, 20, 26, 32, 40,
                 0, 2, 5, 9, 14, 22, 28, np.nan][:n], dtype="float64"
            ),
            "mother_ageb": 20.0 + (ids % 15),
            "mother_eduy": (ids % 18).astype("float64") + 1.0,
            "LATNUM": rng.uniform(-10, 10, n),
            "LONGNUM": rng.uniform(-10, 10, n),
            "hhaircon": np.where(ids % 2 == 0, "Yes", "No"),
            "hhfan": np.where(ids % 3 == 0, "Yes", "No"),
        }
    )
    births = pd.DataFrame(births)

    income = pd.DataFrame(
        {"code_iso3": ["AAA", "BBB"], "wbincomegroup": ["Low", "High"]}
    )

    bands = {c: rng.normal(size=n) for c in m.BANDS_COLUMNS}
    bands.update(
        {
            "ID_HH": np.array([f"hh{i}" for i in ids]),
            "climate_band_1": np.where(ids % 2 == 0, "Aw", "Bw"),
            "climate_band_2": np.where(ids % 2 == 0, "A", "B"),
            "climate_band_3": np.where(ids % 3 == 0, "trop", "arid"),
            "southern": ids % 2 == 0,
            "rwi": np.linspace(-1.0, 2.0, n),
            "Vulnerability Index": np.linspace(10.0, 40.0, n),
        }
    )
    bands = pd.DataFrame(bands)

    return ids, births, income, bands


def _build_non_climate(parquet_path, births, income, bands):
    coordinates = m.load_climate_coordinates(parquet_path)
    table, death_vars = m.build_births_table(
        births, coordinates, income, bands, verbose=False
    )
    non_climate = m.select_and_compress(table, death_vars, verbose=False)
    non_climate.index = pd.Index(non_climate["ID"].to_numpy(), name="ID")
    return non_climate


@pytest.fixture
def pipeline(tmp_path):
    ids, births, income, bands = _synthetic_frames()
    parquet_path = _write_climate_parquet(tmp_path / "climate.parquet", ids)
    non_climate = _build_non_climate(parquet_path, births, income, bands)
    cast_map = m.build_float_cast_map(
        m.parquet_absolute_maxima(pq.ParquetFile(parquet_path), SHOCK_COLUMNS)
    )
    return parquet_path, non_climate, cast_map


def test_streaming_output_is_independent_of_batch_size(pipeline, tmp_path):
    """The whole point of the rewrite: batching must not change the result."""
    parquet_path, non_climate, cast_map = pipeline

    one_shot = tmp_path / "one.feather"
    streamed = tmp_path / "many.feather"

    rows_a, order_a = m.stream_climate_to_feather(
        parquet_path, non_climate, SHOCK_COLUMNS, cast_map, one_shot, batch_rows=10_000
    )
    rows_b, order_b = m.stream_climate_to_feather(
        parquet_path, non_climate, SHOCK_COLUMNS, cast_map, streamed, batch_rows=5
    )

    assert rows_a == rows_b
    assert order_a == order_b
    assert feather.read_table(one_shot).equals(feather.read_table(streamed))


def test_streaming_output_layout_and_dtypes(pipeline, tmp_path):
    parquet_path, non_climate, cast_map = pipeline
    out = tmp_path / "out.feather"

    rows, order = m.stream_climate_to_feather(
        parquet_path, non_climate, SHOCK_COLUMNS, cast_map, out, batch_rows=7
    )
    table = feather.read_table(out)

    # IDs first, then parquet-order shocks, then the CLIMATE_LIST-major dummies
    assert order[:8] == [c for c in m.ID_VARS]
    assert order[8:8 + len(SHOCK_COLUMNS)] == SHOCK_COLUMNS
    assert table.num_rows == rows == len(non_climate)
    assert table.column_names == order

    schema = table.schema
    assert str(schema.field("rwi_tertiles").type).startswith("dictionary<values=double")
    assert str(schema.field("birth_order").type) == "int16"
    assert str(schema.field("child_agedeath_alive").type) == "int16"
    assert str(schema.field("hhaircon").type) == "bool"
    for col in SHOCK_COLUMNS:
        assert str(schema.field(col).type) == "halffloat"
        assert str(schema.field(f"{col}_pos").type) == "bool"


def test_country_ids_are_excluded_by_default_and_opt_in_on_request(tmp_path):
    ids, births, income, bands = _synthetic_frames()
    parquet_path = _write_climate_parquet(tmp_path / "c.parquet", ids)
    coordinates = m.load_climate_coordinates(parquet_path)

    default_table, death_vars = m.build_births_table(
        births.copy(), coordinates, income, bands, verbose=False
    )
    default_cols = m.select_and_compress(default_table, death_vars, verbose=False).columns
    assert "ID_country" not in default_cols
    assert "IDsurvey_country" not in default_cols

    opted_in, death_vars = m.build_births_table(
        births.copy(), coordinates, income, bands,
        keep_country_ids=True, verbose=False,
    )
    opted_cols = m.select_and_compress(opted_in, death_vars, verbose=False).columns
    assert "ID_country" in opted_cols
    assert "IDsurvey_country" in opted_cols


def test_duplicate_merge_keys_are_rejected(tmp_path):
    """A duplicated ID_HH in the bands file would silently multiply births rows."""
    ids, births, income, bands = _synthetic_frames()
    bands = pd.concat([bands, bands.iloc[[0]]], ignore_index=True)
    parquet_path = _write_climate_parquet(tmp_path / "c.parquet", ids)
    coordinates = m.load_climate_coordinates(parquet_path)

    with pytest.raises(ValueError, match="no longer unique"):
        m.build_births_table(births, coordinates, income, bands, verbose=False)
