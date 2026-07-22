"""Synthetic checks for the two death-total fixes in `Effect map.ipynb`.

The notebook itself reads from Data/ and uses absolute Windows paths, so it can't
run here. Instead we replicate the exact arithmetic of the death computation
(cells `84e465a2` and `084bddd1`) on tiny synthetic inputs and lock in that the
corrected formulas match a first-principles cohort calculation, while the two
previous bugs inflate the totals in the expected way.

Bug 1 (temporal): `n_childs = total_population * born_ratio` is the ANNUAL birth
cohort, but the death series lives on the MONTHLY grid and is summed across
months. Each month must carry only ~annual/12 births; otherwise every month
re-spends the whole year's cohort -> ~12x too high. Fix: divide by 12.

Bug 2 (double count): the *_iushocks coefficient sum wrongly included the
postnatal (1m6m, 6m12m) coefficients, which are ALSO in *_1yearshock. Since the
death formula multiplies iushocks by the 9m anomaly and 1yearshock by the 6m
anomaly, the postnatal coefficients got weighted by BOTH anomalies. Fix:
*_iushocks holds the in-utero coefficient only.
"""

import numpy as np

# --- Synthetic "regression" coefficients (deaths per 1,000 births per 1 SD) ---
INUTERO = 2.0
BORN_1M6M = 1.0
BORN_6M12M = 0.5

ANNUAL_BIRTHS = 1200.0          # one grid cell, whole-year cohort
MONTHS = 12


def _iushocks(buggy):
    """In-utero coefficient sum. Buggy version also folds in the postnatal ones."""
    if buggy:
        return INUTERO + BORN_1M6M + BORN_6M12M
    return INUTERO


def _oneyear():
    """Postnatal (first-year) coefficient sum -- same in both versions."""
    return BORN_1M6M + BORN_6M12M


def _annual_total_deaths(anom_9m, anom_6m, *, buggy_double_count, buggy_annual_base):
    """Replicate the notebook: build a per-1000 rate per month, convert to deaths
    with a per-month birth base, and sum across the 12 months of a year."""
    iushocks = _iushocks(buggy_double_count)
    oneyear = _oneyear()
    n_childs = ANNUAL_BIRTHS if buggy_annual_base else ANNUAL_BIRTHS / MONTHS
    total = 0.0
    for t in range(MONTHS):
        rate_per_1000 = anom_9m[t] * iushocks + anom_6m[t] * oneyear
        total += n_childs * rate_per_1000 / 1000.0
    return total


def _cohort_truth(anom_9m, anom_6m):
    """First-principles: each monthly cohort (annual/12 births) suffers its own
    cumulative first-year rate = in-utero*anomaly_9m + postnatal*anomaly_6m."""
    monthly_births = ANNUAL_BIRTHS / MONTHS
    total = 0.0
    for t in range(MONTHS):
        rate = anom_9m[t] * INUTERO + anom_6m[t] * _oneyear()
        total += monthly_births * rate / 1000.0
    return total


def test_fixed_matches_cohort_truth_constant_anomaly():
    a9 = np.ones(MONTHS)
    a6 = np.ones(MONTHS)
    fixed = _annual_total_deaths(a9, a6, buggy_double_count=False, buggy_annual_base=False)
    assert fixed == _cohort_truth(a9, a6)
    # 100 births/mo * (2.0 + 1.5)/1000 * 12 = 4.2
    assert abs(fixed - 4.2) < 1e-9


def test_fixed_matches_cohort_truth_varying_anomaly():
    rng = np.random.default_rng(0)
    a9 = rng.uniform(0, 2, MONTHS)
    a6 = rng.uniform(0, 2, MONTHS)
    fixed = _annual_total_deaths(a9, a6, buggy_double_count=False, buggy_annual_base=False)
    assert abs(fixed - _cohort_truth(a9, a6)) < 1e-9


def test_annual_base_bug_inflates_by_12x():
    a9 = np.ones(MONTHS)
    a6 = np.ones(MONTHS)
    fixed = _annual_total_deaths(a9, a6, buggy_double_count=False, buggy_annual_base=False)
    annual_base = _annual_total_deaths(a9, a6, buggy_double_count=False, buggy_annual_base=True)
    assert abs(annual_base / fixed - 12.0) < 1e-9


def test_double_count_bug_inflates_postnatal():
    # With equal 9m and 6m anomalies the postnatal piece is counted twice:
    # buggy rate = a*(3.5) + a*(1.5) = 5.0a ; fixed rate = a*2.0 + a*1.5 = 3.5a
    a9 = np.ones(MONTHS)
    a6 = np.ones(MONTHS)
    fixed = _annual_total_deaths(a9, a6, buggy_double_count=False, buggy_annual_base=False)
    double = _annual_total_deaths(a9, a6, buggy_double_count=True, buggy_annual_base=False)
    assert abs(double / fixed - (5.0 / 3.5)) < 1e-9


def test_both_bugs_compound():
    a9 = np.ones(MONTHS)
    a6 = np.ones(MONTHS)
    fixed = _annual_total_deaths(a9, a6, buggy_double_count=False, buggy_annual_base=False)
    original = _annual_total_deaths(a9, a6, buggy_double_count=True, buggy_annual_base=True)
    # 12x (temporal) * (5.0/3.5) (double count) ~= 17.14x
    assert abs(original / fixed - 12.0 * (5.0 / 3.5)) < 1e-9


if __name__ == "__main__":
    import sys
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} checks passed.")
    sys.exit(0)
