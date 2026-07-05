# CLAUDE.md — NBA GOAT Index Project Constitution

> **This file is the operational source of truth for all AI agents working on this project.**
> Intent (what this project is, its scope, its data policy, its soul) is governed by
> `docs/vision/NBA_GOAT_Index_Vision_and_Story.md` — the Vision doc. **Vision governs intent;
> CLAUDE.md governs operations.** If this file ever contradicts the Vision on intent, the Vision
> wins and this file must be updated (record the change in an ADR).
> Re-pointing from the original Data-Engineering plan is recorded in
> `docs/adr/0001-repoint-to-data-analyst-and-real-seed-data.md`.

## TL;DR (Paste This Into Any New AI Session)

> This is the NBA GOAT Index — a **Data Analyst portfolio project** that compares and ranks NBA
> players using real, era-adjusted statistics. Read CLAUDE.md (repo root) before writing any code;
> read docs/vision/NBA_GOAT_Index_Vision_and_Story.md before proposing any change of direction.
> Key rules: (1) **Never lose the NBA soul** — real players, real stats, real basketball questions.
> (2) Write `docs/methodology/v1.md` BEFORE scoring code. (3) **Real data is first-class**: the
> committed seed dataset under `data/seed/` drives all analysis; synthetic rows exist only as test
> fixtures that prove the guardrails catch errors. (4) `make check` must pass before any merge.
> (5) Never weaken tests to pass — fix source code. (6) Golden snapshots under `tests/golden/`
> change only with an explicit "Intentional behavior change" declaration AND a `method_version`
> bump. (7) Every scoring output carries `method_version` and `git_sha`. (8) If a result looks
> wrong, **fix the method, not the player** — players are never hand-placed. (9) If you can't
> explain a module in 2 minutes to an interviewer, simplify it. (10) No banned overclaiming
> language ("production-grade", "enterprise-scale", "real-time", "big data", "ML ranking model").
> Hierarchy: Vision doc (intent) > CLAUDE.md (operations) > docs/methodology/vX.md > docs/prd/* >
> docs/adr/* > Blueprint & PROJECT_WISDOM.md (context/rationale only).

---

## Identity

- **Project:** NBA GOAT Index — compares NBA players head-to-head and ranks the all-time greats
  using real, era-adjusted statistics, with a transparent scoring method whose objective inputs
  are cleanly separated from adjustable human weightings.
- **Purpose:** Portfolio-grade **Data Analyst** project (analyst-friendly analytics-engineering
  rigor welcome; the resume target is Data Analyst).
- **Target roles:** Data Analyst, Entry/Junior DA, Reporting Analyst, BI Analyst, Business Data
  Analyst, Data Operations Analyst.
- **Owner:** Solo developer (~1–3 hrs/day) using AI-assisted coding.
- **North star / failure mode:** "This project has gone in the wrong direction if an AI ever loses
  the NBA soul of the project." Never sand this into a generic could-be-any-dataset exercise.
- **Tie-breaker when priorities collide:** (a) Data Analyst resume value → (b) finishing a smaller
  version completely → (c) keeping the future roadmap open (team builder, matchup simulator,
  interactive app) → (d) breadth of skills shown.

---

## Definitions

- **Seed dataset:** the curated, committed REAL dataset under `data/seed/` (15–30 all-time greats,
  player-season grain plus accolades), acquired via `nba_api` + documented hand-assembly. It is
  the input to all analysis and reporting.
- **Test fixtures:** small, designed synthetic rows under `tests/fixtures/` used ONLY to prove
  correctness guardrails (contracts, invariants, golden snapshots). Never used for analysis.
- **PRD:** defines scope + acceptance criteria ("what" and "done"). Tier-1 runs off one PRD.
- **ADR:** records a structural decision + tradeoffs ("why").
- **Behavior-changing scoring change:** any change that could alter scoring outputs for the same
  input dataset.
- **Golden snapshot artifacts:** committed expected outputs under `tests/golden/` keyed by
  `method_version`, generated from the synthetic test fixtures (so expected values are
  mechanically provable, not opinions about real players).
- **Run metadata:** small JSON record of each pipeline execution, committed under
  `results/run_metadata/`.

---

## Non-Negotiable Rules

### 1. Methodology Before Code
- `docs/methodology/v1.md` must exist and be complete BEFORE any scoring code is written.
- The methodology implements the locked two-layer split from the Vision: an **objective layer**
  (real inputs, era adjustments, decomposed component scores — reproducible) and a **weighting
  layer** (explicit, config-driven, documented human judgment). Weights are chosen and documented,
  not learned from data. Era adjustment is always-on — no toggle.
- Every scoring output includes `method_version` (single authoritative source in config, echoed
  verbatim) and `git_sha` when available.
- To change defined scoring behavior: write `v2.md`, add an ADR, bump `method_version` in config.
  Never silently change the meaning of v1. Edits to `v1.md` are allowed only for typos/clarity.
- If unsure whether a change is behavior-preserving, treat it as behavior-changing.
- **The iron rule:** if a result looks wrong, fix the method, not the player. Players are NEVER
  hand-placed into rankings. Consensus lists (ESPN, The Ringer) are a sanity check via Spearman
  correlation — reported honestly, never chased toward 1.0.

### 2. Real Data First (Seed Dataset Policy)
- **Real NBA data is first-class from day one.** The analysis, rankings, report, and every
  user-facing output run on the committed real seed dataset in `data/seed/`.
- **Acquire once, commit as a versioned seed.** Acquisition (via `nba_api` plus documented
  hand-assembly for accolades/patchy older stats) is a script that is re-runnable and documented
  in `docs/sources.md` (endpoints, pull date, hand-assembled values and their sources). Live or
  refreshable ingestion is a later phase.
- **Network touches exactly one place:** the acquisition script, run explicitly and rarely.
  Everything downstream (clean → validate → transform → score → report) reads committed files and
  must run with no network access. `make run` and `make check` require no network.
- **Raw API response dumps are gitignored** (`data/raw/`). The curated seed CSVs are committed.
  Intermediate outputs (`data/processed/`, `data/marts/`) are gitignored and regenerable.
- **Synthetic data survives only as test fixtures** (see Test Fixtures Design below). It never
  feeds analysis, the report, or any ranking a human reads.

### 3. The Gate: `make check`
- Every merge to `main` requires a passing `make check`, run locally. `main` is always green.
- `make check` runs, in order: `ruff check` → `ruff format --check` → `pytest` → `make validate`.
- `make validate` runs Pandera contract validation against the seed dataset (and confirms the
  designed-bad fixtures still fail).
- `mypy` and CI (GitHub Actions running the same gate) join in the post-Tier-1 operational
  maturity phase — the gate's contents grow; the rule "no merge without a green gate" never
  changes.
- If `make check` fails, do not proceed. Fix first.

### 4. Test Integrity (Anti-Cheating)
- **NEVER** modify existing tests to make failing code pass.
- **NEVER** add `@pytest.mark.skip` / `@pytest.mark.xfail`, weaken or broaden assertions
  (e.g., to `assert result is not None`), delete tests, or reduce coverage to pass.
- If a test fails, fix the SOURCE CODE, not the test.
- Exception: a genuine bug in the test itself — document the fix in the commit message.

### 5. Golden Snapshot Guard
- Golden snapshots live under `tests/golden/`, keyed by `method_version`, and are generated from
  the synthetic test fixtures so expected values are mechanically provable.
- They may change ONLY with an explicit "Intentional behavior change" declaration AND a
  `method_version` bump (config bumped → methodology doc updated → regenerate → commit).
- Golden snapshots changing alongside source code without a version bump is a blocked merge,
  whether caught by CI (later) or by you (now).

### 6. Complexity Budget
- No implementation file exceeds 250 lines without documented justification in a top-of-file
  comment.
- No abstraction unless used in 2+ places, or explicitly justified.
- No new dependency without a one-line recorded justification (PRD, ADR, or commit message for
  small utilities).
- If you can't explain a module in 2 minutes to an interviewer, simplify it.
- Do the simplest thing that works. No "for later" scaffolding.

### 7. Planning Gate (right-sized for a solo sprint)
- **Tier-1 work runs off a single PRD:** `docs/prd/tier1_mvp.md`. Tasks in it need no additional
  per-feature PRDs. Each post-Tier-1 phase gets one phase-level PRD before it starts.
- **Out-of-plan changes:** a micro-PRD (3–5 lines in the PR/commit description: what, why, what
  test verifies it) is enough for bug fixes, small refactors, and config changes. Formatting/typo/
  doc-only edits need nothing beyond a clear commit message.
- **The strict path still exists:** any change that could alter scoring behavior, methodology
  outputs, or data contracts requires the full treatment — methodology doc update, ADR,
  `method_version` bump where outputs change. No exceptions.
- ADRs are required for structural decisions: new dependency of consequence, new storage format,
  pipeline architecture change, scoring framework/normalization approach.

### 8. Independent Review (scoped)
- A second AI (Codex or ChatGPT) reviews diffs using `prompts/review_codex.md` (4-Point Prompt)
  **whenever the diff touches scoring, contracts, or methodology** — the zones where a silent bug
  poisons everything downstream.
- For everything else, review is recommended when time allows, not required. A solo sprint at
  1–3 hrs/day cannot afford mandatory ceremony on every diff.

### 9. Attribution (No Tool Trailers)
- Commit messages and PR descriptions must never include tool-attribution trailers:
  no "Co-Authored-By: Claude …", no "Generated with Claude Code", no similar lines.
- Commits are authored by Roman Sally. AI assistance is understood context; it is not
  a co-author and must not appear in git history or PR bodies.

### 10. Honest Claims (Overclaiming Ban)
- Banned phrasing: "production-grade", "enterprise-scale", "architected a cloud backend",
  "machine-learning ranking model", "real-time", "big data", "full-stack analytics platform".
- Use: "built a reproducible analytics pipeline", "modeled validated player-season data",
  "documented methodology and validation rules".
- Claim only what a stranger could verify by cloning the repo and reading the README.

---

## 4-Point Review Prompt (for any reviewer)

1. **Does this diff satisfy the acceptance criteria?** List each criterion, pass/fail.
2. **What are the severe bugs?** List them first, before style issues.
3. **Would the developer struggle to explain any part in an interview?** Flag it.
4. **Propose tests that would FAIL if the code were wrong.** Not trivially-passing tests.

---

## Architecture Rules

### Rigor Zones
- **High rigor** (contracts, invariants, golden snapshots, review required): scoring code, Pandera
  contracts, the scoring SQL, `docs/methodology/`.
- **Medium rigor** (validated, spot-tested): acquisition, cleaning, profiling, orchestration.
- **Low rigor** (functional, iterated freely): report notebook/charts, README polish, `app/` (UI,
  later phases).

### Data Flow
```
nba_api + hand-assembly ──(acquisition script, explicit, network)──> data/seed/  [committed]
data/seed/ → clean → profile → validate (Pandera) → transform (DuckDB SQL) → score (engine)
          → results (ranking, pairwise, run metadata) → report/README (Tier-1) → dashboard (later)
```

### Key Directories
```
nba-goat-index/
├── CLAUDE.md                    # This file (operational constitution)
├── AGENTS.md                    # AI agent roles
├── PROJECT_WISDOM.md            # Rationale archive (partly superseded — see its status note)
├── Makefile                     # make check / make run / make validate
├── pyproject.toml               # uv project config
├── pipeline/                    # clean.py, profile.py, contracts.py, score.py, run.py,
│                                # fetch_seed.py (only file that touches network)
├── sql/                         # hand-written, commented DuckDB SQL (CTEs, window fns, ranking)
├── data/
│   ├── seed/                    # committed REAL seed dataset (players, player_seasons, accolades)
│   ├── raw/                     # raw API dumps (gitignored)
│   ├── processed/               # cleaned parquet (gitignored, regenerable)
│   └── marts/                   # scored/mart parquet (gitignored, regenerable)
├── results/
│   ├── goat_scores_v1.csv       # small committed final outputs
│   └── run_metadata/            # committed run JSON records
├── tests/
│   ├── fixtures/                # synthetic guardrail fixtures (committed)
│   ├── unit/                    # unit + invariant tests
│   └── golden/                  # golden snapshots (version-guarded)
├── qa/                          # validation_log.md
├── docs/
│   ├── vision/                  # Vision & Story (intent authority), Blueprint, ROI strategy
│   ├── methodology/             # v1.md, v2.md, ...
│   ├── prd/                     # tier1_mvp.md, template.md, phase PRDs
│   ├── adr/                     # decision records
│   ├── sources.md               # data sources, ToS, pull dates, hand-assembled values
│   ├── data_dictionary.md       # schema documentation
│   ├── questions.md             # the analytical questions the report answers
│   ├── data_model.md            # star schema + grain/keys
│   └── report.md                # the question-driven analyst report
├── dbt_project/                 # POST-Tier-1 (Phase 2)
└── app/                         # POST-Tier-1 Streamlit dashboard (Phase 3)
```

---

## Stack Policy (Approved Set)

### Tier-1 core (now)
Python 3.12+ · pandas · SQL · DuckDB · Parquet · Pandera · pytest · ruff · Makefile · uv ·
nba_api (acquisition only) · matplotlib/plotly (report charts)

### Post-Tier-1 analyst arc (in phase order, per the Blueprint)
dbt Core (dbt-duckdb first) → cloud warehouse (PostgreSQL/Neon, then BigQuery) → Streamlit
dashboard (+ optional Power BI over the hosted DB) → operational maturity (GitHub Actions CI,
pre-commit, mypy, sqlfluff, structlog, expanded tests) → advanced analyst layer (weight
sensitivity, bootstrap stability).

### Optional / last (engineering signal — only if targeting BI-developer/AE roles)
FastAPI · Docker

### Explicitly Not Used
Airflow · Spark · Kafka · Kubernetes · Terraform · Snowflake/Databricks · MLflow · learned/ML
weights (deferred indefinitely per Vision §3) · React/Next.js (far-future app phase only)

---

## Scoring Engine Invariants

These properties must ALWAYS hold. Test them (in `tests/unit/` for Tier-1):

1. **Determinism:** Same input → same output, every run.
2. **Score bounds:** All final scores fall within [0, 100].
3. **Weight sum:** Category weights sum to exactly 1.0.
4. **No NaN:** No NaN values in any final score output.
5. **Component monotonicity:** Increasing a positively-weighted metric increases that component's
   score (NOT global score — global monotonicity fails under normalization).
6. **Version tags:** Every output contains `method_version` matching config, and `git_sha` when
   available.

---

## Data Contracts (Pandera)

### Era-Conditional Rules
- `three_pt_pct`: non-null only for seasons >= 1979-80 (3-point line introduced)
- `blocks`, `steals`: non-null only for seasons >= 1973-74
- Turnovers: tracked from 1977-78
- Advanced stats (PER, WS, BPM): availability varies by source and era
- Impact metrics: on/off and plus-minus exist only from ~1996-97 — the Winning/Impact component
  must use an all-era-available proxy (see Vision §5)
- Document all era boundaries in `docs/data_dictionary.md`

### Contract Enforcement
- Contracts live in `pipeline/contracts.py`; `make validate` runs them against the seed dataset.
- Contracts validate data shape; tests validate logic. Both must pass in `make check`.
- The designed-bad synthetic fixtures must FAIL validation — a test asserts that they do. If a
  bad fixture ever passes, the contract has a hole.

---

## Test Fixtures Design (synthetic, guardrails only)

Synthetic rows exist to make correctness mechanically provable — never to stand in for real data.

- **Valid mini-set** (PlayerA, PlayerB, … — synthetic names so expected ordering is a mathematical
  consequence of the methodology, not an opinion): drives golden snapshots, determinism, and
  invariant tests. Cover edge cases: short career, pre-3PT-era seasons, multi-era span,
  lockout-shortened season, dominant-in-one-category profile.
- **Designed-bad rows** that MUST trip the contracts, e.g.: score out of [0, 100] bounds,
  duplicate player-season key, missing required key, impossible-for-era stat (3PT attempts in
  1965), negative games played.
- Committed under `tests/fixtures/`. Real player names never appear in fixtures; synthetic rows
  never appear in `data/seed/` or any analysis output.

---

## Run Metadata (Observability Without Infra)

Every pipeline run writes a JSON file under `results/run_metadata/`:
```json
{
  "method_version": "v1",
  "git_sha": "abc123",
  "timestamp": "2026-07-04T14:00:00Z",
  "row_count": 450,
  "validation_passed": true,
  "runtime_seconds": 12.4,
  "input_source": "data/seed"
}
```

---

## Workflow (solo cadence)

1. Pick the next task from `docs/prd/tier1_mvp.md` (or the current phase PRD).
2. Branch: `git checkout -b feat/<name>`. Batching several related PRD tasks per branch is fine.
3. Plan Mode (Shift+Tab) before edits that touch 2+ files or any public interface/schema/scoring
   logic; skip for trivial changes. Use AskUserQuestion interviews only for high-stakes design
   (methodology, data model, new scoring component). `/clear` between planning and implementation.
4. Build against the PRD task's acceptance criteria.
5. Run `make check`. Fix all failures.
6. If the diff touched scoring/contracts/methodology → independent review with
   `prompts/review_codex.md`.
7. Write a 5–8 sentence self-explanation (interview prep) for non-trivial work.
8. Merge when the gate is green and the task's acceptance criteria are met.
9. **Daily minimum:** one concrete artifact per session — one SQL file, one validation rule, one
   chart, one doc section. Layered projects die from stalling, not difficulty.

---

## Git Policy

- Feature branches for all work; `main` always passes `make check`.
- Committed: seed dataset (`data/seed/`), test fixtures, golden snapshots (version-guarded),
  run metadata, small final result CSVs, screenshots.
- Gitignored: `data/raw/`, `data/processed/`, `data/marts/`, virtualenvs.
