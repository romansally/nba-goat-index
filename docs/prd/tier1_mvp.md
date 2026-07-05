# PRD: Tier-1 MVP — Real-Data NBA GOAT Index (analysis + report)

**Author:** Roman Sally (drafted with Claude Code)
**Date:** 2026-07-04
**Status:** Ready
**Branch:** `feat/<task-name>` per task or batch of tasks (see CLAUDE.md Workflow)
**Governs:** all Tier-1 work. Tasks below need no additional per-feature PRDs (ADR-0001).

---

## Pre-Implementation Checklist

- [x] Planning level determined: Complex (whole-phase plan) — this PRD is the planning artifact
- [ ] Interrogation session for `docs/methodology/v1.md` (required before task T2 — CLAUDE.md
      routes methodology design through an AskUserQuestion interview)
- [ ] `/clear` between this planning session and the first implementation session
- [ ] Plan Mode for multi-file tasks (T3 onward)

---

## 1. Objective

Take the repo from governance-only scaffolding to a **finished, runnable Data Analyst portfolio
piece**: a committed real seed dataset of 15–30 all-time greats, a transparent era-adjusted
scoring engine (v1 methodology), pairwise player comparison, a GOAT ranking, hand-written DuckDB
SQL transforms, Pandera validation with era-conditional rules, run metadata, one strong
visualization inside a question-driven analyst report, and a recruiter-legible README + data
dictionary — all reproducible from a fresh clone with one command and no network.

## 2. Non-Goals (deferred to labeled post-Tier-1 phases below)

- dbt models/tests/lineage (Phase 2)
- Cloud warehouse — PostgreSQL/Neon, BigQuery (Phase 3)
- Interactive dashboard — Streamlit / Power BI (Phase 3)
- Live/refreshable ingestion, scheduled runs (Phase 3/4)
- CI, pre-commit, mypy, sqlfluff, structlog, expanded test suite (Phase 4)
- Sensitivity/bootstrap analysis (Phase 5); FastAPI/Docker (Phase 5, optional/last)
- Learned/ML weights (deferred indefinitely, Vision §3); team builder & matchup simulator
  (future arc, post-DA-role)

## 3. Ordered Task List (empty repo → runnable Tier-1)

Sized for ~1–3 hrs/day over 2–3 weeks; one task ≈ one to two sessions. Do them in order — each
task's outputs are the next task's inputs. **Daily minimum applies:** if a session stalls, ship
one SQL file, one validation rule, one chart, or one doc section.

---

### T1 — Scaffolding + analytical questions
**Files created:** `Makefile` (`check`, `run`, `validate` targets), updated `pyproject.toml`
(deps: pandas, duckdb, pyarrow, pandera, pytest, ruff, nba_api, matplotlib), `.gitignore` update
(`data/raw/`, `data/processed/`, `data/marts/`), directory skeleton (`pipeline/`, `sql/`,
`data/seed/`, `tests/fixtures/`, `tests/unit/`, `tests/golden/`, `qa/`, `results/run_metadata/`),
`docs/questions.md`, delete stub `main.py`.
**Acceptance criteria:**
1. `uv sync` succeeds; `make check` passes (trivially green on the empty skeleton).
2. `docs/questions.md` lists 3–5 concrete analytical questions the report will answer (e.g.,
   which era produced the most top-10 seasons under v1; which players are most
   weighting-sensitive; which components drive each top-10 score; pairwise verdicts for 2–3
   marquee matchups). Each question names the output table/chart that will answer it.

### T2 — Methodology v1 (BEFORE any scoring code)
**Files created:** `docs/methodology/v1.md`, `config/scoring_v1.yaml` (weights + `method_version`
— the single authoritative source).
**Process:** AskUserQuestion interview per CLAUDE.md, then write the doc.
**Acceptance criteria:**
1. Defines all six components (Peak, Longevity, Winning/Impact, Playoff, Accolades,
   Efficiency/Advanced), each with: exact input stats, era availability, normalization
   (per-possession/pace + era-relative z-scores), and how it scales to a component score.
2. Winning/Impact uses an all-era-available proxy (no on/off-only inputs — pre-1997 legends must
   not break). Accolades logic is era-aware (no MVP pre-1955-56, no DPOY pre-1982-83).
3. Default weights table sums to exactly 1.0 and lives in config, not code. Era adjustment is
   always-on. Custom Mode = same engine, different weights.
4. States the iron rule ("fix the method, not the player") and the consensus Spearman
   sanity-check policy (report honestly, never chase).
5. A reader can hand-compute a player's final score from the doc given the inputs — verified by
   actually hand-computing one synthetic player and keeping that worksheet for T6's golden test.

### T3 — Real seed data acquisition
**Files created:** `pipeline/fetch_seed.py` (only network-touching code), `data/seed/players.csv`,
`data/seed/player_seasons.csv`, `data/seed/accolades.csv` (committed), `docs/sources.md`
provenance log updated with pull date/endpoints/hand-assembled values.
**Acceptance criteria:**
1. Seed covers 15–30 players including the ten confirmed names (Jordan, LeBron, Bird, Magic,
   Kareem, Curry, Russell, Durant, Kobe, Wilt), regular season + playoffs at player-season grain,
   plus hand-assembled accolades with verification sources logged.
2. Script is re-runnable, throttled, and dumps raw responses to `data/raw/` (gitignored — verify
   `git status` shows no raw files).
3. Era gaps appear as nulls (not zeros) so contracts can enforce era rules.

### T4 — Cleaning + profiling
**Files created:** `pipeline/clean.py`, `pipeline/profile.py`, `data/processed/`
player_seasons_clean.parquet (gitignored), `docs/profiling.md`.
**Acceptance criteria:**
1. Cleaning handles nulls/types/name-consistency; before/after row counts logged in
   `docs/profiling.md`.
2. Profiling doc reports null rates by field and era, ranges, and duplicate-key check — and its
   findings visibly informed at least one contract rule or cleaning step (note which).

### T5 — Pandera contracts + designed-bad fixtures
**Files created:** `pipeline/contracts.py`, `tests/fixtures/synthetic_valid.csv`,
`tests/fixtures/synthetic_invalid.csv`, `tests/unit/test_contracts.py`, `make validate` wired
into `make check`, `qa/validation_log.md` started.
**Acceptance criteria:**
1. Contracts enforce: player-season grain uniqueness, score/stat bounds, era-conditional rules
   (3PT ≥ 1979-80, steals/blocks ≥ 1973-74, turnovers ≥ 1977-78), null-rate expectations.
2. `make validate` passes on the real seed.
3. Each designed-bad fixture row (out-of-bounds value, duplicate key, missing key,
   impossible-for-era stat, negative games) FAILS validation, and a test asserts each specific
   failure. The valid mini-set (PlayerA…, per CLAUDE.md Test Fixtures Design) passes.

### T6 — Star schema + hand-written DuckDB SQL transforms
**Files created:** `docs/data_model.md` (grain, keys, diagram), `sql/01_create_schema.sql`,
`sql/02_staging.sql`, `sql/03_player_season_metrics.sql` (window functions),
`sql/04_scoring_components.sql` (CASE/era logic), `sql/05_final_goat_scores.sql` (ranking),
`sql/06_validation_checks.sql`; outputs to `data/marts/` (gitignored).
**Acceptance criteria:**
1. SQL is commented and demonstrably uses CTEs, window functions, CASE, and ranking (the #1
   claimed-but-unproven resume cluster).
2. Files run end-to-end in DuckDB against processed data; mart row counts reconcile with seed
   counts (logged in `qa/validation_log.md`).
3. `docs/data_model.md` explains grain and keys in interview-ready terms.

### T7 — Scoring engine + invariants + golden snapshot
**Files created:** `pipeline/score.py` (reads `config/scoring_v1.yaml`),
`tests/unit/test_scoring_invariants.py`, `tests/golden/v1_scores.json` (from the synthetic valid
mini-set), `tests/unit/test_golden.py`.
**Acceptance criteria:**
1. Engine implements `docs/methodology/v1.md` exactly; output includes `method_version` from
   config and `git_sha`.
2. All six CLAUDE.md invariants tested and green: determinism, [0,100] bounds, weight sum 1.0,
   no NaN, component monotonicity, version tags.
3. Golden snapshot matches the T2 hand-computed worksheet; changing any weight in config makes
   the golden test fail (verified once, then reverted).
4. Pairwise compare function returns, for any two players: verdict, component-by-component
   breakdown, and career-vs-peak scope option.

### T8 — Pipeline orchestration + run metadata
**Files created:** `pipeline/run.py`, `make run` target, `results/run_metadata/run_*.json`
(committed), `results/goat_scores_v1.csv` (committed).
**Acceptance criteria:**
1. `make run` executes clean → profile → validate → SQL transforms → score → write outputs →
   emit run metadata, end-to-end from a fresh clone, offline.
2. Run metadata contains all CLAUDE.md-required fields with real values.
3. Running twice produces identical scores (determinism at pipeline level, not just engine level).

### T9 — Report + visualization (the analyst face)
**Files created:** `docs/report.md`, `screenshots/` (charts + final table), chart-generation code
in `pipeline/` or a small notebook.
**Acceptance criteria:**
1. Answers every question from `docs/questions.md` with a chart or table plus written
   interpretation — including at least 2 pairwise matchup verdicts *with the why* (component
   breakdown, higher/lower highlighting per Vision §6).
2. At least one strong, polished visualization (e.g., component-stacked top-10 breakdown or
   radar for a marquee matchup) committed as a screenshot.
3. Reports the Spearman correlation vs. one consensus list, with an honest one-paragraph
   interpretation of the biggest deviation (investigated per Vision §4: method flaw fixed, or
   defensible difference kept).

### T10 — README + data dictionary + QA log finish
**Files created/updated:** `README.md` (case study: what/why/how-to-run/methodology summary/
validation evidence/limitations/screenshots), `docs/data_dictionary.md` (every seed and mart
field, era boundaries), `qa/validation_log.md` completed.
**Acceptance criteria:**
1. The stranger test: a fresh reader can clone, `uv sync`, `make run`, and understand the
   project from README alone in ~5 minutes.
2. Data dictionary covers every committed column with type, grain, source, and era caveats.
3. No banned overclaiming language anywhere (CLAUDE.md Rule 9) — grep for the banned phrases.
4. Self-explanation paragraphs (interview prep) exist for: methodology, contracts, SQL, engine.

---

## 4. Data Contracts Impacted

Created from scratch in T5: player-season grain contract with era-conditional rules (canonical
list in CLAUDE.md + `docs/sources.md` gaps table). Every later task runs behind `make validate`.

## 5. Methodology Impact

T2 creates `docs/methodology/v1.md` and `method_version = "v1"` — the founding version, no bump
needed. After T7 commits `tests/golden/v1_scores.json`, any behavior-affecting change follows the
full CLAUDE.md path (v2 doc + ADR + bump + regenerate + "Intentional behavior change").

## 6. Acceptance Criteria — Tier-1 Definition of Done

1. From a fresh clone with no network: `uv sync && make check && make run` all pass.
2. Committed real seed dataset (15–30 players incl. the ten confirmed names) with provenance
   logged; no raw API dumps in git.
3. `results/goat_scores_v1.csv` ranks all seeded players, tagged `method_version="v1"` +
   `git_sha`; pairwise compare works for any pair with component-level "why".
4. Hand-written DuckDB SQL (CTEs, window functions, CASE, ranking) is committed and is the
   transform layer actually used by `make run`.
5. Pandera contracts pass on real seed AND provably fail on every designed-bad fixture.
6. Scoring invariants + golden snapshot tests green; golden keyed to synthetic fixtures.
7. Every run emits run metadata; at least one run JSON committed.
8. `docs/report.md` answers the T1 questions with ≥1 strong committed visualization and the
   consensus Spearman check.
9. README + data dictionary pass the stranger test; zero banned overclaiming phrases.
10. Every merge along the way had a green `make check`; scoring/contract diffs got 4-Point
    review.

## 7. Required Tests

- **Unit (`tests/unit/`):** contract pass/fail per designed-bad fixture (T5); scoring component
  math vs. hand-computed values (T7); pairwise verdict correctness on fixtures (T7).
- **Invariant (in `tests/unit/` for Tier-1):** the six CLAUDE.md invariants (T7).
- **Golden (`tests/golden/`):** `v1_scores.json` over the synthetic valid mini-set (T7).
- [x] No golden snapshot changes expected after T7 within Tier-1 (any change = full version-bump
  path).

## 8. Dependencies

pandas · duckdb · pyarrow · pandera · pytest · ruff · nba_api · matplotlib (or plotly — pick one
in T9, don't add both). Justification recorded in ADR-0001 §4; no other dependencies without a
recorded one-liner.

## 9. Edge Cases

- Pre-1979-80 seasons (no 3PT) and pre-1973-74 (no steals/blocks) must score without NaN leakage.
- Bill Russell/Wilt: no MVP-era or DPOY-era contamination in Accolades; no on/off dependency in
  Winning/Impact.
- Lockout/COVID-shortened seasons must not crater Longevity/Peak (per-season normalization).
- Short-career high-peak profiles must rank sensibly under both career and peak scopes.
- Active players (LeBron, Curry, Durant): partial careers — longevity handles "so far" honestly.
- nba_api returning zeros where history has no data → must be converted to nulls in T3, or
  contracts will falsely pass.

## 10. The Key Question

**What is the single easiest way this could be wrong, and what test would fail if it were?**
The scoring engine silently mishandles era-missing stats (treating pre-1979 3PT nulls as zeros),
deflating pre-modern legends across components. The component-monotonicity + no-NaN invariant
tests on the multi-era synthetic fixtures, and the golden snapshot (whose expected values were
hand-computed in T2), would fail — that is exactly why the fixtures include a pre-3PT-era player
and why golden values are hand-derived, not engine-derived.

---

## POST-Tier-1 Phases (labeled, sequenced — one phase PRD each before starting)

- **Phase 2 — dbt refactor:** port `sql/` into dbt-duckdb staging/intermediate/mart models with
  schema tests, sources, seeds, lineage docs. Gate: Tier-1 DoD met + README section written.
- **Phase 3 — Cloud + BI:** load marts to PostgreSQL (Neon), re-point dbt at BigQuery; build the
  Streamlit dashboard (ranking, filters, pairwise compare, Custom Mode weight sliders); optional
  Power BI over Neon. Gate: Phase 2 committed.
- **Phase 4 — Operational maturity:** GitHub Actions CI running `make check` (+ golden-snapshot
  guard enforcement), pre-commit, mypy, sqlfluff, structlog, expanded unit/invariant suite,
  scheduled refresh. Gate: Phase 3 core committed.
- **Phase 5 — Advanced analyst layer:** weight-sensitivity/scenario analysis, bootstrap rank
  stability; optional FastAPI/Docker **last and only if targeting BI-developer/AE roles**.
- **Future arc (post-DA-role, Vision §9):** team builder → matchup simulator → live ingestion →
  public web app.
