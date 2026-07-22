"""Synthetic tests for effect_map_ratio (the in-sample attributable-death ratio).

No real data is touched: we hand-build a tiny DHS-like frame with known
coefficients, anomalies and deaths, and check the per-child attributable deaths,
the observed-death indicator, and the monthly ratio aggregation.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

import effect_map_ratio as emr  # noqa: E402


def _coeffs():
    # band 1: heat coeffs on the three windows; cold coeffs (negative, as in the
    # regression output). band 2: only an in-utero heat coeff.
    return {
        1: {
            "inutero_pos": 2.0, "inutero_neg": -3.0,
            "born_1m6m_pos": 1.0, "born_1m6m_neg": 0.0,
            "born_6m12m_pos": 0.5, "born_6m12m_neg": -1.0,
        },
        2: {
            "inutero_pos": 4.0, "inutero_neg": 0.0,
            "born_1m6m_pos": 0.0, "born_1m6m_neg": 0.0,
            "born_6m12m_pos": 0.0, "born_6m12m_neg": 0.0,
        },
    }


def _df():
    return pd.DataFrame(
        {
            "climate_band_1": [1, 1, 2, 3],  # band 3 has no coeffs -> excluded
            "stdm_t_inutero_b_avg":   [2.0, -1.0, 1.0, 5.0],
            "stdm_t_born_1m6m_b_avg": [1.0,  0.0, 0.0, 5.0],
            "stdm_t_born_6m12m_b_avg":[0.0, -2.0, 0.0, 5.0],
            "child_agedeath":         [3,    1000, 40, 2],   # died, alive, died<60, died
            "chb_year":  [2000, 2000, 2000, 2001],
            "chb_month": [1, 1, 6, 3],
        }
    )


def test_summed_window_coeffs_from_notebook_shape():
    all_climate_coeffs = {
        1: {
            "inutero_b_avg_pos_int": {"coef": [2.0, 0.3, np.nan]},
            "inutero_b_avg_neg_int": {"coef": [-1.0, -0.5, 0.0]},
            "born_1m6m_b_avg_pos_int": {"coef": [1.0, 0.0]},
            "born_1m6m_b_avg_neg_int": {"coef": [0.0, 0.0]},
            "born_6m12m_b_avg_pos_int": {"coef": [0.5]},
            "born_6m12m_b_avg_neg_int": {"coef": [-1.0]},
        }
    }
    c = emr.summed_window_coeffs(all_climate_coeffs)
    assert c[1]["inutero_pos"] == 2.3           # nansum ignores the NaN
    assert c[1]["inutero_neg"] == -1.5
    assert c[1]["born_1m6m_pos"] == 1.0
    assert c[1]["born_6m12m_neg"] == -1.0


def test_per_child_attributable_heat_and_cold():
    df = _df()
    heat, cold = emr.per_child_attributable_deaths(df, _coeffs())

    # Child 0 (band 1): heat = 2*2.0 + 1*1.0 + 0*0.5 = 5.0 -> /1000
    assert abs(heat[0] - 5.0 / 1000) < 1e-12
    assert cold[0] == 0.0  # no negative anomaly parts

    # Child 1 (band 1): negative anomalies only.
    # cold = inutero_neg*(-1) + 6m12m_neg*(-2) = (-3)*(-1) + (-1)*(-2) = 3 + 2 = 5 -> /1000
    assert abs(cold[1] - 5.0 / 1000) < 1e-12
    assert heat[1] == 0.0

    # Child 2 (band 2): heat = 4*1.0 = 4.0 -> /1000 ; no cold coeff
    assert abs(heat[2] - 4.0 / 1000) < 1e-12
    assert cold[2] == 0.0

    # Child 3 (band 3): no coefficients -> 0
    assert heat[3] == 0.0 and cold[3] == 0.0


def test_observed_deaths_under5_threshold():
    df = _df()
    died = emr.observed_deaths(df, max_age_months=60)
    assert list(died) == [1.0, 0.0, 1.0, 1.0]
    # First-year threshold reclassifies the 40-month death as "not counted".
    died_1y = emr.observed_deaths(df, max_age_months=12)
    assert list(died_1y) == [1.0, 0.0, 0.0, 1.0]


def test_monthly_ratio_excludes_bandless_and_aggregates():
    df = _df()
    out = emr.monthly_attributable_ratio(df, _coeffs(), max_age_months=60)

    # Band 3 child (2001-03) is excluded -> only 2000-01 and 2000-06 remain.
    assert list(out.index) == [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-06-01")]

    # 2000-01: two band-1 children. est_heat = 5/1000 (child0), est_cold = 5/1000
    # (child1). observed deaths = 1 (only child0 died). ratio = (5/1000)/1.
    jan = out.loc[pd.Timestamp("2000-01-01")]
    assert abs(jan["est_heat"] - 5.0 / 1000) < 1e-12
    assert abs(jan["est_cold"] - 5.0 / 1000) < 1e-12
    assert jan["observed"] == 1.0
    assert abs(jan["heat_ratio"] - 5.0 / 1000) < 1e-12
    assert abs(jan["ratio"] - 10.0 / 1000) < 1e-12

    # 2000-06: single band-2 child, heat = 4/1000, observed = 1.
    jun = out.loc[pd.Timestamp("2000-06-01")]
    assert abs(jun["ratio"] - 4.0 / 1000) < 1e-12


def test_zero_observed_month_is_nan_not_inf():
    df = pd.DataFrame(
        {
            "climate_band_1": [1],
            "stdm_t_inutero_b_avg": [2.0],
            "stdm_t_born_1m6m_b_avg": [0.0],
            "stdm_t_born_6m12m_b_avg": [0.0],
            "child_agedeath": [1000],  # alive -> observed deaths = 0 this month
            "chb_year": [2005],
            "chb_month": [7],
        }
    )
    out = emr.monthly_attributable_ratio(df, _coeffs(), max_age_months=60)
    assert np.isnan(out.iloc[0]["ratio"])
    assert not np.isinf(out.iloc[0]["ratio"])


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} checks passed.")
