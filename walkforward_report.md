# Walk-Forward Validation Report — Smart Zone Strategy

**Date:** 2026-07-03 (updated same day with full-engine 2-year backtest, then V2 engine changes; 2021 validation added 2026-07-07)

## PART -4 — Fourth fresh validation: full year 2021 (bull year + Q4 correction) — PASSED

Frozen V2, zero changes, single run (2026-07-07). Pre-registered criteria fixed
before the run, same as the 2022 exam: positive @2pt cost AND maxDD better than -16R.

Result (198 trades): **+19.94R raw / +12.09R @2pt / +4.25R @4pt. PASS.**
Points: +641 raw. Winrate 37.4%, mean risk 55.8 pts.
**MaxDD -8.26R raw (-10.10R @2pt) — PASS.** Bootstrap @2pt: P(<=0) = 21.4%.
8/12 months positive; the Q4 top + correction (Oct–Dec) cost only -4.67R total.
Trades: `backtest_v2_validation_2021.csv`.

Setup mix flipped again (regime effect, consistent with prior windows):
SUPPORT_REACTION carried the year (+28.06R, n=61) — the same setup that was net
negative in the 2025 audit; the round-2 decision NOT to drop setups is vindicated.
TREND_CONTINUATION negative again (-6.38R, n=19) — now the weakest setup in
essentially every window. CE +26.96R vs PE -7.02R — bull-regime direction skew,
as expected, not edge.

**All fresh windows combined (2021 + 2022 + 2023 + 2026-H1, 673 trades):**
+56.64R raw, **+29.99R @2pt (+0.045R/trade), bootstrap P(<=0) = 14.0%** (was 22.4%
before 2021). Four out of four fresh years positive after costs. Still short of
formal significance; forward paper trading remains the deciding gate. 2021 is now
burned for tuning like all other windows.

---

## PART -3 — Third fresh validation: full year 2022 (bear/volatile regime) — PASSED

Frozen V2, zero changes, single run. Pre-registered criteria: positive @2pt cost
AND maxDD better than -16R.

Result (189 trades): **+18.64R raw / +11.20R @2pt / +3.76R @4pt. PASS.**
Points: +703 raw / +325 @2pt. **MaxDD -8.58R raw (-537 points) — PASS.**
8/12 months positive; war month Feb 2022 +4.66R; Apr–Jun bear grind only -3.9R
total; worst month Oct -4.32R. Best fresh-window year so far, in the hardest regime.
Bootstrap @2pt: P(<=0) = 24.7%.

**All fresh windows combined (2022 + 2023 + 2026-H1, 475 trades):**
+36.70R raw, +17.89R @2pt (+0.038R/trade), bootstrap P(<=0) = 22.4%.
Full V2 record now spans 4.5 years / 761 trades. Evidence status: edge is small,
positive in every fresh year tested, resilient across bull/bear/volatile/rangebound
regimes, with observed worst drawdown -15.7R — but still short of formal statistical
significance. 2022 is now burned. Next gate: forward paper trading of the frozen engine.

---

## PART -2 — Enhancement design round 2 (before 2022 validation): NO candidate passed

With 2023+2026H1 burned, all seen data (42 months, 572 V2 trades after full V2
re-run of 2024H2+2025) became design data. Two pre-registered enhancement
candidates were evaluated for cross-window consistency (2023, 2024H2, 2025, 2026H1):

1. **Monthly loss circuit-breaker** (stop month after -X R): REJECTED.
   -4R helps 2023 (+4.0R, DD -15.2→-11.2) but HURTS 2025 (-2.9R, DD -9.5→-12.4 —
   skipped post-trigger trades were net winners). -3R hurts two windows. -5R ~net zero.
   The breaker was an Aug/Nov-2023 fit, not a general rule.
2. **HTF daily regime filter** (20-day efficiency, price vs 20-DMA, 5d/20d vol ratio):
   REJECTED. On the full 572-trade sample no feature shows a monotone or
   window-consistent pattern (e.g. worst vol_ratio bucket: -7.9R in 2023 but
   positive in all three other windows — a 2023 artifact).

**Decision: V2 engine stays frozen — zero new rules.** Full V2 42-month record:
+41.17R raw (572 trades), ~+22R after 2pt costs, maxDD -15.7R. 2022 (bear/volatile
year) will be run as a third pure validation of the frozen engine when backfilled.

---

## PART -1 — Second fresh validation: full year 2023 (added after 2026-H1 was burned)

2023 data predates every tuning decision (dev = 2024H2+2025, holdout = 2026H1),
so it is a second uncontaminated exam for the frozen V2 engine. Pre-registered
pass criteria: positive after 2pt costs AND max drawdown better than -8R.

Result (202 trades): **+10.59R raw / +2.01R @2pt cost / -6.56R @4pt.**
Points: +726 raw / +322 @2pt. Winrate 39.1%.
**Max drawdown -15.2R raw (-18.7R @2pt, -685 points) — FAILS the -8R criterion.**
Bootstrap @2pt: P(profit <= 0) = 45%.

Split personality: H1-2023 **+22.95R** (six months, five positive) vs
H2-2023 **-12.36R** (Aug -6.41R, Nov -6.31R). The engine had a 5-month losing
regime. Setup mix also flipped vs other windows (BREAK_CONFIRMATION -8.2R here).

**Verdict across all fresh windows (2023 + 2026-H1, 286 trades):** +6.7R total
after 2pt costs ≈ +0.023R/trade. V2 beats V1 in every window tested, but the
edge is thin, regime-dependent, and statistically unproven. The system is NOT
"profitable in all situations": it earns in favorable regimes and bleeds for
months in unfavorable ones. Any further engine changes now require new unseen
data — both holdouts are burned. The only honest next step is forward paper
trading; a monthly loss circuit-breaker (e.g. pause after -5R in a month) is a
candidate risk overlay but must be validated forward, not fitted to 2023.

---

## PART 0 — V2 engine changes and final holdout validation

Two changes were designed on the DEV window only (2024-H2 + 2025), then the frozen
engine was run ONCE on the untouched holdout (2026-H1):

1. **Trend-day gate for breakout setups** (`smart_trade_breakout_min_day_efficiency = 0.35`):
   BREAK_CONFIRMATION and TREND_CONTINUATION only trade when the day-so-far Kaufman
   efficiency ratio >= 0.35. Rationale: breakouts fail in chop by definition.
   Stable across the whole 0.25–0.45 threshold range on dev.
2. **Earlier breakeven** (`paper_breakeven_after_r`: 1.0 → 0.7): protect winners sooner.
   The 0.6–0.8 plateau all beat baseline on dev; 0.7 chosen as plateau midpoint (not the peak).

Rejected candidates (inconsistent across dev sub-windows — kept out on purpose):
with-day-direction filter, compressed-prev-day skip, gap filter, exhausted-day skip
(knife-edge), dropping any setup entirely.

### Holdout 2026-H1 (never used for any decision), real engine, spot P&L:

| Metric | V1 (old) | V2 (new) |
|---|---|---|
| Trades | 123 | 84 |
| Total R (0 cost) | +5.44 | **+7.47** |
| Mean R/trade | +0.044 | **+0.089** |
| Total R @2pt futures cost | +1.06 | **+4.68** |
| Total R @4pt cost | -3.32 | **+1.88** |
| Max drawdown | -10.73R | **-6.01R** |
| Months positive | 3/6 | 4/6 (May -0.59, Jun -0.09 ~flat) |

Dev sanity checks: worst old months 2024-10 (-3.19R → +0.35R) and 2025-11 (-4.35R → +3.04R).

**Honest statistical caveat:** bootstrap on holdout @2pt cost: 95% CI [-13.0R, +23.6R],
P(profit <= 0) = **32%**. The improvement is real and consistent in direction
(better per-trade edge, halved drawdown, survives costs), but 84 trades cannot prove
a statistically significant edge. Roughly a 1-in-3 chance the true edge is still <= 0.
Forward paper trading remains mandatory before real money.

---

## PART A — Full-engine backtest, 2 years of DB candles (spot P&L, futures trading)

Real `BacktestRunner` executed month-by-month over `nifty_index_candles_5m`
(2024-07-01 → 2026-06-30, 10-day warmup per month). 2025 is IN-SAMPLE (the
ignore-filters and config were tuned on 2025 trades). 2024-H2 and 2026-H1 are
TRUE OUT-OF-SAMPLE — the engine never saw them during tuning.

| Period | Status | n | Total R | Mean R | Winrate | Points |
|---|---|---|---|---|---|---|
| 2024-H2 | **OUT-OF-SAMPLE** | 113 | +0.64 | +0.006 | 49.6% | +214 |
| 2025-H1 | in-sample (tuned) | 119 | **+37.78** | +0.317 | 64.7% | +2178 |
| 2025-H2 | in-sample (tuned) | 135 | +1.58 | +0.012 | 51.1% | +556 |
| 2026-H1 | **OUT-OF-SAMPLE** | 123 | +5.44 | +0.044 | 48.0% | +411 |

**Combined out-of-sample: +6.08R over 236 trades (12 months).**

Futures round-trip cost scenarios on the OOS 12 months (avg risk ~62 pts/trade):

| Cost/round-trip | OOS Total R | OOS Total points |
|---|---|---|
| 0 pts | +6.08 | +625 |
| 1 pt | +1.85 | +389 |
| 2 pts (realistic: slippage+brokerage+STT) | **-2.38** | +153 |
| 4 pts | -10.84 | -319 |

Bootstrap on OOS @2pts: 95% CI [-35.8R, +30.9R], **P(profit <= 0) = 56%** — a coin flip.
OOS max drawdown: **-11.2R** sequential.

Key observations:
- The +37.78R in 2025-H1 vs ~0 everywhere else is the overfitting signature:
  the system performs spectacularly exactly on the window its filters came from.
- Even 2025-H2 (also in the tuning data) produced only +1.58R.
- Direction flips by regime: CE positive / PE negative in 2024-H2, exactly
  reversed in 2026-H1. Directional performance is market regime, not edge.
- Setup stability across the two OOS windows is weak: reversal-type setups
  (REJECTION/RETEST/SUPPORT_REACTION) made +13.85R in 2024-H2 but only +0.96R
  in 2026-H1 (RETEST was -4.53R there). BREAK_CONFIRMATION (-5.5R) and
  TREND_CONTINUATION (-3.2R) are the biggest OOS losers and the most-traded setups.

**Part A verdict: on never-seen data with realistic futures costs, expected
P&L is ~0 (slightly negative at 2pts cost). The system as configured is
"ready" only for Jan-Jun 2025 — the exact window it was tuned on.**

---

## PART B — Earlier audit-CSV walk-forward (options-era analysis, kept for reference)
**Data:** `updated_logic_ignored_trade_audit.csv` — 368 trades, Jan–Dec 2025
**Method:** Filters were selected using ONLY Jan–Jun (H1) data, frozen, then tested
untouched on Jul–Dec (H2). This is the honest estimate of forward performance.
Script: reproducible via the walk-forward analysis (tags + setup + time rules,
selection criterion: flagged trades must total negative R on train with n >= 8).

## Headline numbers

| Variant | Window | n | Total R | Mean R | Winrate |
|---|---|---|---|---|---|
| Raw engine (no filters) | Full year | 368 | +1.37 | +0.004 | 51.4% |
| Repo's DEFINITE_IGNORE filters | Full year (in-sample) | 295 | **+15.25** | +0.052 | 53.9% |
| Repo's DEFINITE_IGNORE filters | Jul–Dec only (honest OOS view) | 149 | **-2.68** | -0.018 | 48.3% |
| H1-selected frozen tag filters | Jul–Dec OUT-OF-SAMPLE | 108 | +4.56 | +0.042 | 50.0% |
| H1-selected tags + drop SUPPORT_REACTION + no entry >= 13:00 | Jul–Dec OUT-OF-SAMPLE | 85 | **+7.30** | +0.086 | 51.8% |

In-sample → out-of-sample decay for the best ruleset: **+17.60R (H1) → +7.30R (H2)** — ~60% of the apparent edge evaporates on unseen data.

## Cost sensitivity (best OOS variant, 85 trades / 6 months)

| Cost per trade | 6-month Total R |
|---|---|
| 0.00 R (frictionless) | +7.30 |
| 0.05 R (~3 pts on ~57-pt risk: realistic minimum) | +3.05 |
| 0.10 R (options slippage + theta realistic) | **-1.20** |

Bootstrap 95% CI on OOS total R: **[-11.7, +26.7]**, P(total <= 0) = **23%**.
The edge is statistically indistinguishable from zero.

## What is robust across BOTH halves (worth keeping)

- **SMART_ZONE_RETEST_CONFIRMATION**: positive in both halves (+4.40R n=9, +3.03R n=14). Best setup; sample is small — validate forward.
- **Blocking entries >= 13:00**: hours 13 and 14 were net negative in both halves.
- **Dropping SMART_ZONE_SUPPORT_REACTION_CONFIRMATION**: negative in both halves (-2.97R, -3.26R).
- **SAME_DAY_ZONE_LOSS_COOLDOWN**: flagged trades lost money in both halves (-9.32R, -3.52R).
- **ROUND_NUMBER targets > SMART_ZONE targets**: smart-zone-target trades were net negative in both halves.

## What is NOT robust (regime luck, do not trust)

- **SMART_ZONE_TREND_CONTINUATION**: +6.68R in H1 → **-12.64R** in H2. Collapsed completely out-of-sample.
- **CE-side performance**: raw CE +7.16R in H1 → -8.82R in H2 (tracks Nifty's direction, i.e. regime, not edge). Any CE/PE asymmetry in results is market direction, not strategy skill.
- **June 2025**: +11.25R in one month — a single-month outlier that carried the whole year's in-sample profit.
- The +15.25R "filtered" full-year number: filters were derived from the same trades they were scored on. Evaluated honestly on Jul–Dec they produced **-2.68R**.

## Verdict

After honest walk-forward validation and realistic transaction costs, the strategy's
expected edge is **~0R, possibly negative**. It is not overfit to one month — it is
overfit to the full-year sample via the filter-derivation loop. Engineering quality
(no lookahead bias, conservative same-candle SL priority, backtest/replay/live parity)
is genuinely good; the market edge is what's missing.

## Required before any real money

1. Freeze the current ruleset completely (no more parameter edits).
2. Forward paper trade 3+ months; require positive R after 0.05R/trade costs.
3. Backfill option-premium candles so P&L reflects what you'd actually trade
   (index points ignore theta/IV — `read.md` already flags this gap).
4. Concentrate on the two robust elements: RETEST_CONFIRMATION setups and the
   pre-13:00 window. Consider retiring TREND_CONTINUATION entirely.
5. Do not re-tune on 2025 data again — every additional tuning pass on the same
   368 trades makes the backtest number less meaningful.
