"""In-sample climate-attributable mortality ratio for the temporal figure in
`Effect map.ipynb`.

The `deaths_over_time_stacked.png` figure extrapolates attributable deaths onto a
gridded population (GPW). This module instead computes, per calendar month of
birth, the ratio

        in-sample ESTIMATED deaths attributable to temperature shocks
        ------------------------------------------------------------
                    in-sample OBSERVED deaths

directly from the DHS regression micro-sample. Numerator and denominator are both
simple counts over the *same* births, so there is no population extrapolation.

Numerator, per child: expected excess deaths = sum over exposure windows of
    coefficient[window] * anomaly_part[window] / 1000
where the positive part of the anomaly uses the '*_pos' coefficient (heat) and the
negative part uses the '*_neg' coefficient (cold). The raw cold coefficients are
negative and the negative anomaly part is <= 0, so their product is a positive
death count (same double-negation convention the map figure uses).

Denominator, per child: 1 if the child is observed to have died before
`max_age_months` (default 60 = under-5), else 0.

The functions are deliberately free of any notebook/global state so they can be
unit-tested on synthetic data (see tests/test_effect_map_ratio.py).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Exposure windows entering the first-year attributable-death calculation, matching
# the map figure (in-utero + first year of life). The regression coefficients are
# summed over the outcome age-windows upstream (notebook cell `84e465a2`).
ATTRIB_WINDOWS = ("inutero", "born_1m6m", "born_6m12m")


def summed_window_coeffs(all_climate_coeffs):
    """band id -> {"{window}_pos": coef, "{window}_neg": coef} (per 1,000 births).

    `all_climate_coeffs` is the notebook object: band id -> dict keyed by regressor
    name (e.g. 'inutero_b_avg_pos_int') whose value has a 'coef' list over the
    regression's outcome columns, with non-significant entries already zeroed. We
    nansum each regressor's coef across outcome columns, i.e. the same per-window
    total the map figure builds on.
    """
    out = {}
    for band, coeffs in all_climate_coeffs.items():
        d = {}
        for w in ATTRIB_WINDOWS:
            d[f"{w}_pos"] = float(np.nansum(coeffs[f"{w}_b_avg_pos_int"]["coef"]))
            d[f"{w}_neg"] = float(np.nansum(coeffs[f"{w}_b_avg_neg_int"]["coef"]))
        out[band] = d
    return out


def per_child_attributable_deaths(df, band_window_coeffs, band_col="climate_band_1"):
    """Expected attributable deaths per child (COUNTS), split into (heat, cold).

    Children in a band without coefficients contribute 0. Missing anomalies are
    treated as 0 exposure. Returns two float arrays aligned with `df`.
    """
    n = len(df)
    heat = np.zeros(n)
    cold = np.zeros(n)
    band = np.asarray(df[band_col])

    # Positive / negative parts of each window's anomaly (missing -> 0 exposure).
    pos, neg = {}, {}
    for w in ATTRIB_WINDOWS:
        a = np.nan_to_num(
            pd.to_numeric(df[f"stdm_t_{w}_b_avg"], errors="coerce").to_numpy(dtype=float),
            nan=0.0,
        )
        pos[w] = np.clip(a, 0, None)
        neg[w] = np.clip(a, None, 0)

    for b, coeffs in band_window_coeffs.items():
        mask = band == b
        if not mask.any():
            continue
        h = np.zeros(int(mask.sum()))
        c = np.zeros(int(mask.sum()))
        for w in ATTRIB_WINDOWS:
            h += coeffs[f"{w}_pos"] * pos[w][mask]
            c += coeffs[f"{w}_neg"] * neg[w][mask]
        heat[mask] = h / 1000.0
        cold[mask] = c / 1000.0
    return heat, cold


def observed_deaths(df, death_col="child_agedeath", max_age_months=60):
    """0/1 observed-death indicator: child died before `max_age_months`.

    `child_agedeath` is age at death in months with a 1000 sentinel for children
    still alive (set in 03_merge_climate_and_DHS.py). Default 60 months = under-5.
    """
    age = pd.to_numeric(df[death_col], errors="coerce").to_numpy(dtype=float)
    death_indicator = (age < max_age_months).astype(float)
    print(f"Observed deaths (age < {max_age_months} months): {death_indicator.sum():,.0f} of {len(death_indicator):,}")
    return death_indicator


def monthly_attributable_ratio(
    df,
    band_window_coeffs,
    *,
    band_col="climate_band_1",
    year_col="chb_year",
    month_col="chb_month",
    death_col="child_agedeath",
    max_age_months=60,
):
    """Monthly time series of heat/cold attributable-death ratios.

    Restricts to children whose climate band has coefficients (the bands the
    attributable figure covers). Returns a DataFrame indexed by month-start date
    with columns: est_heat, est_cold, observed, heat_ratio, cold_ratio, ratio.
    Months with zero observed deaths yield NaN ratios rather than inf.
    """
    keep = np.isin(np.asarray(df[band_col]), list(band_window_coeffs.keys()))
    sub = df.loc[keep]

    heat, cold = per_child_attributable_deaths(sub, band_window_coeffs, band_col)
    died = observed_deaths(sub, death_col, max_age_months)

    date = pd.to_datetime(
        pd.DataFrame(
            {
                "year": sub[year_col].astype(int),
                "month": sub[month_col].astype(int),
                "day": 1,
            }
        ),
        errors="coerce",
    )

    out = (
        pd.DataFrame(
            {
                "date": date.to_numpy(),
                "est_heat": heat,
                "est_cold": cold,
                "observed": died,
            }
        )
        .dropna(subset=["date"])
        .groupby("date")
        .sum()
        .sort_index()
    )

    observed = out["observed"].replace(0, np.nan)
    out["heat_ratio"] = out["est_heat"] / observed
    out["cold_ratio"] = out["est_cold"] / observed
    out["ratio"] = out["heat_ratio"] + out["cold_ratio"]
    return out
