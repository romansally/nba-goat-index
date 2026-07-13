# Validation Log

## 2026-07-12 — T6: star schema + DuckDB SQL transforms

**Result:** `make transform` runs clean → contract validation → SQL transforms →
`06_validation_checks.sql` end-to-end offline (`pipeline/transform.py` runs the Pandera
contracts on its input frames itself — the CLAUDE.md validate-before-transform order is
enforced in-process, not assumed). Every mart row count reconciles with its staged input
(PRD T6 acceptance criterion 2):

| mart | rows | reconciles against |
|---|---|---|
| dim_player | 20 | players (20) |
| dim_season | 70 | league_seasons (70) |
| fact_player_season | 335 | player_seasons (335) |
| fact_accolade | 668 | accolades (668) |
| mart_player_season_metrics | 335 | fact_player_season (335) |
| mart_player_component_inputs | 20 | dim_player (20) |
| mart_player_award_rates | 120 | 20 players × 6 awards |

Reconciliation is enforced, not just observed: the count checks in `06_validation_checks.sql`
compare marts against the staged inputs on every run and `pipeline/transform.py` raises on any
mismatch. All 24 named checks return zero rows on the real seed and on the fixture trio
(20 at initial commit — an earlier revision of this entry miscounted them as 16 — plus 3
added in the first review round and 1 in the re-review round below).

### Worksheet reproduction (the correctness proof)

`tests/unit/test_transforms.py` reproduces every hand-computed intermediate in
`docs/methodology/v1_hand_worksheet.md` — per-season REL/SPI/AVAIL/REL_TS, playoff P_SPI with
missed-postseason nulls, all raw component values, and all 18 award-rate cells (including
PlayerB's null DPOY rate and PlayerC's 1999-shrunk All-Star denominator) — at the worksheet's
4-decimal tolerance. Peak scope on the trio provably equals career scope (worksheet §7),
pinning the §12.9 scope-filter mechanism.

### Real-data findings from the T6 investigation

1. **Jerry West's 1967 one-minute playoff cameo grades P_SPI 1.052 off a single rebound**
   (1 gp, 1 mp, 0 pts, 1 trb → ~2.6 possessions → REL_trb ≈ 4.2). Bounded by construction:
   `p_spi × po_gp` adds ~1.05 of his 206.56 career playoff raw (~0.5%, ≈0.05 final points).
   v1.md §5.4 has no minutes qualifier — implemented exactly as specified, documented in
   `docs/data_model.md`, pinned by a regression sentinel test. A qualifier would be a v2
   (behavior-changing) decision.
2. **DPOY zero-eligibility is real, ×4:** Wilt, West, Russell, Oscar retired before 1983.
   Their DPOY rate is emitted as null with `eligible_seasons = 0` (never 0.0), the §12.7
   exclude-and-renormalize input. A test asserts the exact four-player set.
3. **Peak-window degenerate rules never fire on the seed** (minimum qualifying-season count is
   11, Jokić) but are implemented and exercised by the fixture trio (all ≤ 3 qualifying
   seasons → fewer-than-N rule; `peak_fallback` flag ships for run metadata).
4. **No zero-playoff careers in the seed** (minimum 8 runs), but `playoff_raw` coalesces a
   would-be-null SUM to 0 per §5.4; a check asserts non-null.
5. **Low-minute regular seasons don't distort REL ratios:** pool SPI spans [0.795, 2.092];
   the smallest seasons (Moses Malone 1993: 104 mp; Curry 2020: 139 mp) yield in-range SPI
   and ≤ 0.2 Longevity contribution after the AVAIL multiplier. Verified, no guard needed.
6. **TS denominator hole closed:** contracts floor `fga`/`fta` at 0, which alone would admit a
   0/0 season; `ts_denominator_positive` now requires `fga + 0.44·fta > 0` on every row.

### Negative proof (both directions, verified once and reverted)

1. **Checks raise on real violations:** temporarily tightening `avail_in_bounds` to
   `avail > 0.99` made `pipeline.transform` fail loudly, listing the named check and the
   violating player-seasons.
2. **Tests guard the checks:** temporarily neutering `p_spi_null_iff_missed_postseason`
   (`WHERE FALSE`) made `test_validation_checks_have_teeth` fail — the test that corrupts a
   fixture row (a `po_gp` without the other po_* columns) and asserts the named check catches
   it. That test is permanent, so silently weakening this check now breaks `make check`.

### Post-review fixes (Codex 4-Point review, 2026-07-12)

Three severe bugs, all demonstrated empirically by the reviewer with reproducible steps,
all fixed with tests that fail against the pre-fix code:

- **Bug (confirmed): transform never validated its inputs.** `make transform` chained
  clean → SQL directly; an impossible 200-game season sailed through (`LEAST(1, …)` capped
  AVAIL and the bad GP corrupted games-weighted SRS) despite Pandera rejecting it in
  isolation. Fixed: `run_transforms` now calls `validate_all(frames)` before any SQL —
  clean → validate → transform is enforced in-process for every caller. A `validate=False`
  escape hatch initially existed for tests that must reach the sql/06 guard layer with
  corruption contracts would intercept first — **superseded in the re-review round below:
  the public parameter was removed entirely in favor of a private test seam.**
  Tests: `test_contracts_gate_runs_before_sql` plus five parametrized denominator-guard
  cases (zero mp/po_mp/gp, zero pace, unmapped All-NBA team).
- **Bug (confirmed): award facts could be silently discarded.** The rate mart is driven by
  `accolade_intro_season`; deleting `mvp` from config left 3 real MVP facts in
  `fact_accolade`, emitted zero MVP rate rows, and every existing check stayed green.
  Fixed three ways: `pipeline/transform.py` enforces exact award parity (config intro
  seasons == the contract award registry, keys AND seasons, and == `accolade_weights` keys);
  sql/06 gained `award_key_known_to_config` and `all_nba_points_mapped` (data-side parity);
  and `accolades_reach_rate_mart` joins facts to the rate mart on `(player_id, award)` —
  not just player-season — requiring positive weighted wins for every in-scope eligible
  fact (**strengthened to exact two-way reconciliation in the re-review round below**).
  Tests: `test_award_key_parity_enforced`, `test_seed_award_wins_match_accolades`
  (independent pandas aggregation compared to the mart, all 120 cells).
- **Bug (confirmed): award numerator lacked the denominator's eligibility filter.** The
  wins CTE counted any selection joined to a played in-scope season; an illegal 1999
  All-Star selection would inflate `weighted_wins` for a player with other eligible seasons
  (the `weighted_wins > 0 AND eligible_seasons = 0` guard misses that case). Fixed: the
  wins CTE applies the identical intro-season + `asg_held` predicate as the eligibility
  CTE — defense-in-depth beneath the contracts, which already forbid such rows.
  Test: `test_ineligible_win_cannot_inflate_rate` (bypasses contracts deliberately).

Minor fixes from the same review: check-count claim corrected (20, not 16 — now 23);
`peak_n` validated as a positive integer (no silent `int()` truncation); `peak_min_avail`
validated finite in [0, 1]; `production_blend` validated for exact keys, finiteness, and
non-negativity; `all_nba_team_points` validated for exact keys, finiteness, and
non-negativity (NaN can no longer pass). Review test suggestions added: nontrivial
peak-scope restriction (7-season player, awards/playoffs inside AND outside the window —
the trio-only test was degenerate), fallback window ordering, peak tie-break at
`peak_n = 1`, and the full per-season worksheet grid (poss, per-75, REL, playoff REL,
both rank columns). Transform suite: 37 → 49 tests.

### Post-re-review fixes (Codex 4-Point re-review, 2026-07-12)

The re-review confirmed all three original bugs genuinely fixed, then found the new guards
had gaps of the same shape — proven by adversarial inputs, all now closed:

- **Moderate (confirmed): the reconciliation check only tested positivity.** Reducing a
  win aggregate by one (partial silent loss — the original Bug 2 shape) passed all checks
  because `weighted_wins > 0` still held. Fixed: `accolades_reach_rate_mart` is now an
  exact two-way reconciliation — an independent re-aggregation of the in-scope eligible
  facts FULL-JOINed to the mart, flagging any mismatch beyond 1e-9, missing mart rows
  (coalesced to a −1 sentinel so absence can't masquerade as zero), and phantom mart wins
  with no backing facts. Test: `test_reconciliation_catches_partial_award_loss` tampers a
  3-win aggregate to 2 mid-pipeline and asserts the named check fires.
- **Own finding, same shape (open investigation): `rate` itself was unreconciled.** The
  wins reconciliation cannot see a corrupt `rate` with intact `weighted_wins` — and rate
  is the value scoring actually consumes. Added `award_rate_consistent`
  (`rate = weighted_wins / eligible_seasons` wherever eligible > 0; check count 23 → 24)
  plus tamper test `test_rate_consistency_catches_corrupt_rate`.
- **Moderate (confirmed): All-NBA point config domain was inconsistent across layers.**
  Empirically reproduced before fixing: `third = 1.1` and `third = 0.0` both passed
  validation — a zero silently nulls every third-team credit, a value above 1 breaks the
  rate ≤ 1 guarantee that `award_rate_in_bounds` asserts. Fixed:
  `0 < third ≤ second ≤ first ≤ 1` enforced in `_validate_config`; seven parametrized
  domain tests (zero, above-one, reversed ordering, NaN, Inf, missing/extra key).
- **Moderate (confirmed): the new SQL guards had no direct negative tests.** Added direct
  proofs that each fires on its own, via the private seam:
  `test_unknown_award_check_fires` (unknown award type → `award_key_known_to_config`),
  `test_unmapped_all_nba_team_check_fires` (team 4 → `all_nba_points_mapped`), the two
  tamper tests above, and — found in the open investigation, same gap in an *older*
  guard — `test_ts_denominator_check_fires` (a contract-legal 0-FGA/0-FTA season, where
  the points identity forces pts = 0, produces IEEE NaN True Shooting in DuckDB, which
  survives the NOT NULL DDL; the named check makes it loud).
- **`validate=False` removed (optional item, taken):** `run_transforms` now validates
  config and contracts unconditionally with no bypass parameter. Corruption tests use the
  private seam (`_execute_transforms` / `_check_transforms` / `_fetch_marts`) — the same
  functions `run_transforms` composes, so the tested paths are the production paths.
- **Stealth ineligible-win case added:** with two eligible seasons and one valid
  selection, unfiltered numerator logic yields 2/2 = 1.0 — in bounds, invisible to every
  range check; `test_stealth_ineligible_win_stays_filtered` asserts 1/2 = 0.5.
- **Process:** sql/06 (300+ lines) now carries the CLAUDE.md Rule 6 complexity-budget
  justification in its header, contracts.py-style; `docs/data_model.md` no longer claims
  the West final-score effect is regression-tested (only the per-season P_SPI is — the
  final-score assertion is the T7 item below). Config branch coverage: 11 parametrized
  `_validate_config` cases. Transform suite 49 → 73 tests.

### Open investigation (re-review round — full-codebase sweep)

Beyond the reviewer's items, the sweep examined the SQL files, runner, config validation,
tests, and docs for anything wrong, fragile, or methodology-inconsistent. Findings:

1. **`rate` unreconciled** — found and fixed (above).
2. **`ts_denominator_positive` was unreachable-unproven** — probed DuckDB 1.5.4 division
   semantics (`1.0/0.0 = inf`, `0.0/0.0 = NaN`, integer division likewise; never NULL,
   never an error), which confirms the degenerate row *reaches* the check rather than
   dying at the DDL — proven reachable with a direct test (above).
3. **Examined and found sound, with reasoning:** the reconciliation check re-derives
   expected wins from the facts side, so it cannot catch a bug replicated identically in
   both aggregations — that residual risk is covered by the *independent pandas*
   aggregation in `test_seed_award_wins_match_accolades` and the hand-worksheet rate
   cells; `python -m pipeline.transform` run directly could read stale processed parquet
   (mitigated: `make transform` chains clean first, the frames that are transformed are
   the frames that are validated, and T8's orchestrator owns sequencing); float ties in
   peak ranking are deterministic via the season tiebreak, and the two worksheet tie
   pairs were verified bit-exact before their ranks were pinned; FK insert order in
   sql/02 (dims before facts) is correct for DuckDB's enforced REFERENCES; the f-string
   `SET VARIABLE` interpolation only ever renders values `_validate_config` has already
   proven finite numeric. Nothing further found — the investigation was done, and this
   list is its complete yield.

### Post-fourth-pass fixes (Codex review, 2026-07-13 — conditional clearance)

The fourth pass confirmed the re-review remediation and found one blocking gap plus one
minor, both reproduced against the pre-fix code before fixing (probe output on record):

- **Blocking (confirmed): the reconciliation's expected side only covered keys with award
  facts.** Two demonstrated escapes: a ghost zero-win row under an unknown key (total row
  count preserved — nothing on the expected side to compare against) and a corrupted
  `eligible_seasons` with a self-consistent rate (PlayerA MVP 3 → 2 with rate 1/3 → 1/2 —
  `award_rate_consistent` holds by construction, and eligibility was reconciled nowhere).
  Both passed all 24 checks. Fixed: `accolades_reach_rate_mart`'s expected side is now the
  COMPLETE scoped player × configured-award grid (scoped seasons CROSS JOIN the award
  registry, facts LEFT-JOINed on), carrying independently re-derived `eligible_seasons`
  AND `weighted_wins`; the FULL JOIN reconciles keys in both directions plus both values.
  Tests: `test_reconciliation_catches_eligibility_corruption`,
  `test_reconciliation_catches_ghost_zero_win_row`.
- **Minor (confirmed): `scope=""` silently selected career scope** through the falsey
  `scope or config["scope"]` fallback. Fixed with an explicit `is None` test so any
  non-None scope reaches `_set_params` and its career/peak validation. Test:
  `test_empty_scope_raises`.

Named checks remain 24 (the reconciliation check was strengthened in place). Transform
suite 73 → 76 tests; full gate 149 → 152. Codex confirmed no other blocking issue beyond
these two items.

**Deferred to T7 (logged, not implemented):** a West marginal-impact regression — once the
scoring engine exists, remove only the 1967 playoff cameo and assert the default final
score decreases by ~0.05 (reviewer's independent derivation: 0.05217), confirming the
documented cameo bound holds end-to-end through min–max scaling and weights.

## 2026-07-12 — T5: Pandera contracts + designed-bad fixtures

**Result:** `make validate` passes on the committed real seed (players 20 · player_seasons 335 ·
accolades 668 · league_seasons 70). The valid synthetic mini-set (the locked hand-worksheet
trio) passes. All 32 designed-bad fixture rows fail via the specific named check each is
labeled with — confirmed both by `tests/unit/test_contracts.py` (parametrized, one test per
row) and by `make validate` itself, which re-runs the hole check on every invocation.

### Real-data extremes that shaped the contracts

Naive versions of five rules would have falsely rejected committed real data:

| naive rule | real data that breaks it | contract actually written |
|---|---|---|
| `gp ≤ season_games` | Jokić 2019-20: 73 gp vs modal 72 (COVID restart); Walt Bellamy precedent: 88 gp in the 82-game 1968-69 via mid-season trade | `gp ≤ season_games + 6` (`gp_within_schedule`) |
| `mp ≤ 48·gp` | Wilt 1961-62: 3882 mp in 80 gp (48.5/game — overtime) | `mp ≤ 48·gp + 60` (`mp_ot_ceiling`); additive, not a ratio, so 1-game samples can absorb a multi-OT game (record: 78 min, 6 OT) |
| `po_mp/po_gp ≤ 48` | seed playoff max is 48.75 min/game | same additive rule (`po_mp_ot_ceiling`) |
| `ws ≥ 0` | seed minimum is −0.4 (negative Win Shares are real) | `ws ∈ [−3, 26]` (ceiling: Kareem's record 25.4, in the seed) |
| playoff stats ≥ 1 | Jerry West 1967: po_gp 1, po_mp 1, po_pts/trb/ast 0 (a real 1-minute playoff cameo) | floors exactly `po_gp ≥ 1`, `po_mp ≥ 1`, counting stats ≥ 0 |

### Observed → enforced promotions

Rules profiling.md reported as "observed, 0 violations" are now hard contracts:
the points identity `pts = 2·fgm + fg3m + ftm` (fg3m as 0 where era-null; exact on all 335
rows, every era — the strongest single corruption catch) and the shot chain
`fgm ≤ fga · ftm ≤ fta · fg3m ≤ fg3a · fg3a ≤ fga · fg3m ≤ fgm`.

### Rules found in T5 investigation (not in profiling.md)

- **Accolade era gates** — the awards-side analog of the stat era gates: award season ≥ its
  intro season (a 1971 DPOY is as impossible as 1965 3PT attempts), no `all_star` where
  `asg_held` is false (1999), All-NBA 3rd team only from 1989, `all_nba_team` null **iff**
  award ≠ all_nba and in {1, 2, 3} when present.
- **Tighter accolade grain** — `(player_id, season, award)` is unique across all 668 rows;
  the previous 4-column key (with `all_nba_team`) would have admitted All-NBA 1st + 2nd in
  the same season. Contract enforces the tighter key; `clean.py::TABLE_KEYS` aligned.
- **Bidirectional player referential integrity** — every player_seasons row joins players
  AND every players row has ≥ 1 season (the reverse direction was previously unchecked).
- **Career span integrity** — `first_season`/`last_season` must equal the min/max of the
  player's actual season rows.
- **League regularities** — `pace_estimated` true exactly for pre-1978 seasons (pace needs
  turnovers, tracked 1977-78+); value ranges for pace and all league baselines.

### Deliberate non-rules (considered and rejected, with reasoning)

- **`is_active` vs last_season** — today, exactly the five players with `last_season = 2026`
  are active, but "active iff last season is the newest" is a data-freshness heuristic, not
  an invariant (a retirement immediately after a final season, or an active player missing a
  full year, breaks it either direction). Typed boolean only.
- **league_seasons contiguity** — the seed is contiguous 1957–2026, but the method only
  requires lookup coverage (every player-season joins a league season, which IS contracted);
  a contiguity rule would falsely reject the sparse 6-season fixture league table.
- **`po_gp ≤ gp`** — regular season and playoffs are independent samples; an injury-year
  return can legitimately have more playoff than regular-season games.

### Bound calibration note

Ceilings are NBA single-season records — many of which the seed itself holds (Wilt's 4029
pts / 2149 trb, Curry's 402 fg3m, Jordan's 759 playoff pts, Magic's 303 playoff ast, Kareem's
25.4 ws) — or the locked worksheet fixture values where those are larger (PlayerB's 2520 trb,
3360 fga, 1680 fgm), plus headroom. They exist to catch unit-scale corruption, not to encode
typicality.

### Negative proof

Verified once and reverted: temporarily raising `OT_ALLOWANCE` to 10 000 makes the
`mp_ot_ceiling` bad-fixture test fail and `make validate` exit with a "contract holes"
error — the guardrails are not trivially passing.

### Post-review fixes (Codex 4-Point review, 2026-07-12)

- **Bug (confirmed): `fg3a` ceiling was 1000, below the real record** — Harden's 1,028
  three-point attempts (2018-19). The seed max (Curry's 886) masked it: the record holder is
  outside the 20-player pool, unlike every other ceiling's record holder. Raised to 1150; a
  full re-audit of all other ceilings against records *outside* the pool found no second
  case (ast 1164 Stockton < 1300 · stl 301 Robertson < 350 · blk 456 Eaton < 500 · tov 464
  Harden < 500). Regression test: a synthetic season with fg3a = 1028 must pass.
- **Missing-column handling**: `_cross_failures` now short-circuits with the same
  `column_in_dataframe` failure name Pandera reports, instead of raising KeyError from a
  join (was fail-loud either way, but outside the aggregated-failure interface). Tested.
- **Coverage added**: designed-bad fixture rows for the two previously unproven rules,
  `players_have_seasons` and `po_mp_ot_ceiling` (30 → 32 rows); a direct `clean.py` test of
  the tightened accolade grain; and parameterized both-direction boundary tests for every
  era-gated column at intro−1/intro (off-by-one guard across the whole `ERA_INTRO` set).
