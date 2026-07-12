# Validation Log

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
