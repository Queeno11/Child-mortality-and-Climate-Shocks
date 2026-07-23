# Figure 8: fully-interacted "second shock" specification + minimal replication driver

## Summary

Rebuild **Figure 8** — "Longlasting effects of in-utero shocks"
(`\label{fig:in_utero_shocks}`, file `Overleaf/Figures (Aug-2025)/Fig 5 - Affected in-utero.png`) —
and the regression machinery behind it.

Right now the figure compares two *separately estimated* subsamples (children
shocked in utero vs. not), so there is no single coefficient — and no standard
error — for the **difference** in how the two groups respond to a later shock. We
want to replace that sample split with a **single fully-interacted regression** in
which a previous-shock dummy is interacted with the subsequent ("consecutive")
shock terms. The interaction coefficient then reads directly as *"the additional
mortality effect of a shock in the current window for children who already had a
shock in an earlier window,"* with a proper SE and significance test on the
difference.

We also generalize this beyond the in-utero anchor to five anchor windows, produce
a new 5-panel figure, and slim `04_regressions.jl` into a replication-package-style
driver that runs only the regressions used in the paper.

**This is not a one-off.** Build a **general, reusable interaction function** —
mirroring the existing `run_shock_analysis` / `stepped_regression` style (the ones
currently driven by `if` branches), but implemented so that adding an arbitrary set
of interaction terms to the baseline specification is a parameter, not a
copy-paste. The goal is that I can later call the same machinery for other
interacted specifications (different anchors, different interacted variables)
without rewriting the regression loop.

**Deliverables**

1. New fully-interacted "consecutive shock" regression in `CustomModels.jl` (no
   sample split), swept over shock-threshold definitions, run for both temperature
   and precipitation anchors and for both a pooled and a directional dummy spec.
2. A treated / not-treated **counts table** across the whole sweep, to choose the
   preferred threshold empirically.
3. A new **5-panel figure** (pooled, headline) plus a **directional companion
   figure** for decomposition.
4. A minimal, figure-organized `04_regressions.jl`.

---

## Background — current state

**Regression side (`CustomModels.jl`).** Figure 8 is produced by
`run_shock_analysis(df_lazy, controls, extra, months; ...)` (≈ line 745). It:

- defines an in-utero temperature-shock indicator from `stdm_t_inutero_b_avg` being
  outside the `[p_lower, p_upper]` quantile band (defaults `lower_p=0.15`,
  `upper_p=0.85`);
- partitions children into `shock_group` and `control_group` by that indicator;
- runs the standard `stepped_regression` (via `run_models`) **separately on each
  subsample**, writing tables under `Outputs/heterogeneity/shock_analysis_p<...>_p<...>/`.

So the "affected vs. not affected in utero" contrast is a **between-sample
comparison of two separate regressions** — no coefficient (and no SE) for the
*difference* in the two groups' response to a later shock.

The baseline specification is built in `stepped_regression` (≈ line 133), with the
per-window temperature/precipitation symbols assembled by `get_symbols_standard`
(≈ line 282). The estimating equation is the discrete-time hazard in the paper
(Section "Empirical Strategy", eq. around `main.tex:116`): positive/negative
standardized temperature and precipitation anomalies for each exposure window
(in-utero, 0–6, 6–12, 12–18, 18–24, 24–30, 30–36), child + household controls,
cell-month FE and cell-specific linear trends, clustered by cell.

**Plot side (`06_charts.py` + `plot_tools.py`).** The figure is drawn by
`plot_tools.plot_heterogeneity("shock_analysis_p25p0_p75p0", ..., colors, labels, **main_config)`
(`06_charts.py:262–277`), which reads the two subsample LaTeX tables and overlays
the two groups' coefficient lines.

**Driver (`04_regressions.jl`).** Currently loops over months `[1,]` with a `stop`
after the first `run_models` call, and behind `if m == 1` would run
`run_shock_analysis` (commented out) plus ~25 `run_heterogeneity` calls. It is a
scratch/experiment driver, not a clean map from "code → figures in the paper."

---

## Deliverable 1 — Fully-interacted "second shock" specification

### The specification

For a given **anchor window** `ρ0` (e.g. in-utero), define a **previous-shock
dummy** `S_{ρ0}` equal to 1 when the standardized climate anomaly in window `ρ0`
qualifies as a shock. Then estimate a **single** regression that augments the
baseline with the **full interaction** of `S_{ρ0}` and the subsequent-window shock
regressors:

```
D_p  ~  Σ_ρ ( T+_ρ + T-_ρ + P+_ρ + P-_ρ )                      # baseline main effects
        + S_{ρ0}                                                # anchor-shock dummy (level)
        + Σ_{ρ > ρ0} ( T+_ρ + T-_ρ + P+_ρ + P-_ρ ) : S_{ρ0}    # FULL interaction with later windows
        + controls + cell-month FE + cell-specific trends
```

- Keep **both** the main effects and the interactions, so each interaction
  coefficient is a clean *difference*. E.g. `β^{T+}_{0–6} : S_{utero}` is the
  *extra* deaths-per-1,000 from a 1-SD heat shock at 0–6 months for children who
  were **also** shocked in utero, over and above the effect for children who were
  not.
- This delivers, per subsequent window, **one interaction coefficient + SE +
  p-value** for "does a prior shock raise vulnerability to the next one" — exactly
  what the sample split cannot give.

### Anchor windows (one regression each)

`ρ0 ∈ { in-utero, 0–6, 6–12, 12–18, 18–24 }` → **5 regressions**, mirroring the 5
figure panels. For each anchor the interactions cover only the **strictly later**
windows (a shock cannot precede itself), so the number of interaction terms shrinks
as the anchor moves later — expected and fine.

### Anchor climate variable

Define the anchor dummy on **both temperature and precipitation** — a
temperature-anchored version (shock defined on the temperature anomaly) and a
precipitation-anchored version (shock defined on the SPI anomaly), run in parallel.
Keep the two cleanly separable via a `climate ∈ {temp, precip}` argument so
precipitation can be dropped or moved to the appendix if it shows nothing.

### Threshold sweep

The shock threshold defining `S_{ρ0}` is **swept**, not fixed, so we can pick the
best specification empirically:

| Kind      | Options                              |
|-----------|--------------------------------------|
| SD-based  | ±1.0 SD, ±1.5 SD, ±2.0 SD            |
| Quantile  | p5/p95, p10/p90, p15/p85, p20/p80   |

→ **7 threshold definitions.**

> **⚠️ SD-based thresholds use the *empirical* standard deviation of the
> anomaly distribution, not the raw value of the standardized variable.**
> Because of how the anomaly indices are constructed, a value of `1` on the
> standardized (`stdm_*`) variable is **not** equal to one standard deviation of
> that variable's actual distribution. So "±1 SD" means:
> compute `σ_{ρ0} = std(skipmissing(anomaly_{ρ0}))` for the given anchor window and
> climate variable, then set the cut at `mean ± k·σ_{ρ0}` (`k ∈ {1.0, 1.5, 2.0}`) —
> **not** `anomaly > k` / `anomaly < -k`. The empirical σ must be computed per
> anchor window × climate variable (and reported), since the averaging window
> compresses the distribution differently for in-utero vs. postnatal windows (see
> `main.tex:100`). The quantile thresholds (p5/p95, …) are, as usual, empirical
> quantiles of the same distribution.

### Pooled and directional dummy specs (run both)

- **Pooled (symmetric) dummy:** `S_{ρ0} = 1` for any large |anomaly| (hot **or**
  cold for temperature; wet **or** dry for precipitation). This backs the headline
  5-panel figure.
- **Directional dummies:** separate anchor dummies per tail — `S^{hot}_{ρ0}` /
  `S^{cold}_{ρ0}` for temperature, `S^{wet}_{ρ0}` / `S^{dry}_{ρ0}` for
  precipitation — each entered as its own level term and interacted separately with
  the later-window shock regressors, within one regression per anchor.

**Why both:** the pooled dummy is the cleanest headline test. When the pooled effect
is significant, the directional model is the diagnostic that shows **where it comes
from** — whether only one tail drives it (e.g. only prior *heat* matters), or the
hot and cold coefficients are similar in magnitude but individually noisier (larger
SE from splitting the treated group) so only the pooled estimate clears
significance. Report the two directional coefficients side by side with the pooled
one for each significant pooled result.

### Full grid

```
5 anchor windows × 2 climate variables × 7 thresholds × 2 dummy specs
```

The new function takes the anchor window, the climate variable, the threshold spec,
and the dummy spec as arguments; the grid is driven from `04_regressions.jl`. We
need the **counts diagnostic** (below) across the whole grid; the final figures are
generated for the **one chosen threshold** once picked.

### Treated / not-treated counts table (required diagnostic)

Emit a table of how many observations are treated vs. not treated under each
threshold definition, so we can judge which specification has a sensible treated
share. One row per `(anchor window × climate variable × threshold)`, with columns:

- threshold kind + value (e.g. "SD 1.5", "quantile p10/p90");
- **realized cut points** actually applied: the empirical `σ_{ρ0}` and the resulting
  lower/upper bounds (`mean ± k·σ` for SD-based rows; the empirical quantile values
  for quantile rows) — so the cuts are auditable and the SD-vs-standardized-value
  distinction is visible in the table;
- anchor window;
- climate variable (temp / precip);
- **N treated pooled** (`S_{ρ0} = 1`, either tail), **N not treated**, **% treated**;
- **N treated per tail** — hot / cold (temp) or wet / dry (precip) — so the split
  behind the directional model is visible;
- N missing / dropped for that anchor (children with no valid anomaly in `ρ0`).

Write it as both a readable txt/markdown table and a machine-readable CSV at
`Outputs/consecutive_shocks/treated_counts.csv`. This is a first-class deliverable —
it is the basis for choosing the preferred specification.

### Implementation notes

- **Build a general, reusable interaction function** in `CustomModels.jl`, in the
  same spirit as the existing `run_shock_analysis` / `stepped_regression` (which are
  currently steered by `if` branches). The interaction structure must be a
  **parameter**, not hard-coded: the function should accept a description of "which
  dummy(ies) to build and which regressors to interact them with" and add the
  corresponding level + `term(x) & term(dummy)` terms to the baseline
  specification. `run_consecutive_shock_analysis` is then a thin wrapper that
  configures this general function for the anchor-window case — but the general
  function must be independently callable for other interacted specifications later
  (different anchors, different interacted variables) **without rewriting the
  regression loop**.
- Do **not** split the sample. Prefer extending the symbol builder
  (`get_symbols_standard`) to optionally emit the dummy level term and the
  interaction terms, so the same `stepped_regression` loop drives everything.
- Keep the same estimator, FE, clustering, controls, and CUDA path as the baseline.
- Keep the baseline survivorship conditioning: `stepped_regression` already drops
  children who did not survive the previous window (mask on `child_agedeath_* .== 0`);
  the interacted spec must keep the same conditioning so the sample matches the main
  results.
- Write tables to a folder that encodes the grid point, e.g.
  `Outputs/consecutive_shocks/<climate>/<dummy_spec>/<threshold>/anchor_<window>/...`,
  in the same `regtable` txt + tex format the plotting code already parses.
- Leave the old `run_shock_analysis` in place until the new figure is validated.

---

## Deliverable 2 — New figures

**Headline (pooled) figure — replaces `Fig 5 - Affected in-utero.png`.** A 5-panel
figure, one panel per anchor window:

- Panel title: "effect of a subsequent shock **given a shock in**
  {in-utero | 0–6 | 6–12 | 12–18 | 18–24}."
- Within each panel, plot the **pooled interaction coefficients** across the later
  exposure windows (x-axis = timing of the second shock / child age), with 95% CIs
  as shaded bands or vertical bars, consistent with `plot_temperature` /
  `plot_heterogeneity`.
- Horizontal zero reference line; matched y-limits across panels (other figures use
  `ylim=(-0.6, 2.7)` — revisit, interaction magnitudes may differ).
- Nature Climate Change house style: sans-serif, minimal chrome, per the existing
  `plot_tools` config.
- Keep all five panels for now; we'll **trim later** if 12–18 / 18–24 come out
  uninformative.

**Directional companion figure.** A second 5-panel figure from the directional
model that overlays the two tail-specific interaction series per panel (hot vs. cold
for temperature; wet vs. dry for precipitation), using `#ff5100`/orange = heat /
wet-side and `#3e9fe1`/blue = cold / dry-side. This is the "where does the pooled
effect come from" diagnostic. Keep it as a companion/appendix exhibit; the pooled
figure is the headline.

**Implementation.** Add a plotting entry (a new `plot_consecutive_shocks(...)` in
`plot_tools.py`, or a generalization of `plot_heterogeneity`) that reads the
per-anchor interaction-coefficient tables and lays out the 5 panels, parameterized
by dummy spec (pooled → single series; directional → two series per panel). Call it
from `06_charts.py` in place of the current
`plot_heterogeneity("shock_analysis_p25p0_p75p0", ...)` block (`06_charts.py:262`).

**Overleaf.** Once the PNGs are regenerated, update the `fig:in_utero_shocks` block
in `main.tex` (caption + Notes at `main.tex:345–351`) to describe the interacted
design, and the body text in the "Persistence of In-Utero Exposure" subsection
(`main.tex:181–185`).

---

## Deliverable 3 — Minimal replication-package driver (`04_regressions.jl`)

Rewrite `04_regressions.jl` so it runs **only the regressions used in the paper**,
laid out as a readable map from code → figure/table:

```julia
# =====================================================================
# Regressions for Figure X — <name> (\label{...})
# =====================================================================
<the specific call(s)>

# =====================================================================
# Regressions for Figure X+1 — ...
# =====================================================================
...
```

- Remove the `stop` and the giant `if m == 1` block; drop heterogeneity /
  robustness calls that don't back a paper exhibit (or move them to a clearly
  separated, commented-out "appendix / not-in-paper" section).
- Each active block is a small, single-responsibility call (repo modularity
  convention). Add the new `run_consecutive_shock_analysis` grid under a
  "Regressions for Figure 8" header.
- Keep it runnable top-to-bottom to reproduce every regression the paper needs.
- Confirm the final list of figures/tables the driver must cover with Nicolas — at
  minimum: the main temperature spec, the horserace, the spline coefficients, the
  climate-band / electricity / cold-protection heterogeneity, and the new Figure 8.

---

## Files expected to change

- `CustomModels.jl` — new `run_consecutive_shock_analysis` (+ optional
  `get_symbols_standard` extension for the level + interaction terms); leave
  `run_shock_analysis` in place until the new figure is validated.
- `04_regressions.jl` — rewritten minimal, figure-organized driver.
- `plot_tools.py` (and/or `plot_tools_b.py`) — new/updated `plot_consecutive_shocks`.
- `06_charts.py` — swap the Figure 8 plotting call.
- `Overleaf/main.tex` — caption, Notes, and body text for `fig:in_utero_shocks`;
  new figure PNGs under `Overleaf/Figures (Aug-2025)/`.

## Acceptance criteria

- [ ] The interaction function is **general and reusable**: the set of dummies and
      the regressors they interact with are passed in as arguments, and the function
      can be called for a non-anchor interacted specification in a unit test without
      touching the regression loop. `run_consecutive_shock_analysis` is a thin
      wrapper over it.
- [ ] Unit tests (synthetic data, in `tests/`) for the new symbol/interaction
      builder: given a small mock frame, the interacted design matrix contains the
      anchor dummy (or per-tail dummies), the later-window main effects, **and**
      their interactions, and omits interactions for windows at or before the anchor.
- [ ] SD-based thresholds are computed from the **empirical standard deviation** of
      each anchor window × climate variable's anomaly distribution (`mean ± k·σ`),
      **not** from the raw standardized value — verified by a unit test on a mock
      distribution whose empirical σ ≠ 1.
- [ ] The interacted regression reproduces the baseline main-effect coefficients
      when the anchor dummy is held constant (sanity check).
- [ ] Threshold sweep works: the counts table is produced for all
      `5 anchors × 2 climate vars × 7 thresholds` grid points, with correct
      treated / not-treated / % treated and per-tail columns (validated on synthetic
      data with known cut-points), written to
      `Outputs/consecutive_shocks/treated_counts.csv`.
- [ ] Both dummy specs estimate: pooled (single interaction series per anchor) and
      directional (separate hot/cold, wet/dry interaction terms in one regression per
      anchor), emitting parseable tables.
- [ ] `plot_consecutive_shocks` produces the pooled 5-panel headline PNG and the
      directional 5-panel companion PNG (two tail series per panel), both with
      correct panel titles, zero line, CIs, and NCC styling.
- [ ] `04_regressions.jl` runs end-to-end and produces exactly the paper's
      regressions, grouped by figure with comment headers.

> **Note on data access.** Scripts that read/write `Data/` are Nicolas's to run.
> The coding and synthetic-data tests can be done in-repo, but the final run against
> `Data/Data_out/DHSBirthsGlobal&ClimateShocks_v11_full.feather`, the counts table,
> and the PNG regeneration will be executed by Nicolas.
