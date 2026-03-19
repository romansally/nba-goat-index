# CLAUDE.md — NBA GOAT Index Project Constitution

> **This file is the single source of truth for all AI agents working on this project.**
> When CLAUDE.md conflicts with any other document, CLAUDE.md wins.
> Read PROJECT_WISDOM.md for the full rationale behind every rule here.
> Canonical references always use repo paths; local download filenames (e.g., "CLAUDE (1).md") are non-authoritative.

## TL;DR (Paste This Into Any New AI Session)

> This is the NBA GOAT Index project. Read CLAUDE.md (repo root) before writing any code.
> Key rules: (1) Write `docs/methodology/v1.md` BEFORE scoring code. (2) No feature work without a PRD (micro/no-PRD allowed only for narrow cases in CLAUDE.md).
> (3) `make check` must pass before any merge. (4) Never weaken tests to pass — fix source code.
> (5) Golden snapshot artifacts under `tests/golden/` may change only with an explicit "Intentional behavior change" declaration AND a `method_version` bump. (6) Default `INGEST_MODE=offline`.
> (7) Scoring methodology is versioned — every output includes `method_version` and `git_sha` when available.
> (8) Builder and reviewer are different AI agents. Review with `prompts/review_codex.md`.
> (9) If you can't explain a module in 2 minutes, simplify it.
> Hierarchy: CLAUDE.md > docs/methodology/vX.md > docs/prd/* > docs/adr/* > PROJECT_WISDOM.md
> AGENTS.md and prompts/ are helper docs — if they conflict with the above, defer to CLAUDE.md.

---

## Identity

- **Project:** NBA GOAT Index — a player comparison and ranking system
- **Purpose:** Portfolio-grade Data Engineering / Analytics Engineering project
- **Owner:** Solo developer using AI-assisted coding (Claude Code + Codex)
- **Target roles:** Data Engineering, Analytics Engineering

---

## Definitions (to prevent ambiguity)

- **PRD:** defines feature scope + acceptance criteria (“what” and “done”).
- **ADR:** records a structural decision + tradeoffs (“why”), and must not replace PRDs or methodology.
- **Behavior-changing scoring change:** any change that could alter scoring outputs for the same input dataset.
- **Behavior-preserving refactor:** code rework intended to produce identical outputs for identical inputs.
- **Golden snapshot artifacts:** committed expected outputs under `tests/golden/` keyed by `method_version` (e.g., `v1_scores.json`).
- **Offline-first ingestion:** repo runs end-to-end on committed fixtures with no network required.
- **Run metadata:** small JSON record of pipeline execution, committed under `results/run_metadata/`.

---

## Non-Negotiable Rules

### 1. Methodology Before Code
- `docs/methodology/v1.md` must exist and be complete BEFORE any scoring code is written.
- Every scoring output must include `method_version` (e.g., `"v1"`) and `git_sha` when available in its metadata.
- The scoring engine must have a single authoritative source of `method_version` (config or constant), and all outputs must echo it verbatim.
- To change defined scoring behavior: write a new version doc (`v2.md`), add an ADR, update config — do not silently change the meaning of v1.
- Edits to `v1.md` are allowed ONLY for typos/clarity that do not change defined behavior.
- **Any change that could alter scoring outputs for the same input requires BOTH an ADR and a `method_version` bump. No exceptions.**
- Behavior-preserving refactors are allowed without a version bump ONLY if golden snapshots remain identical and invariants still pass.
- If unsure whether a change is behavior-preserving, treat it as behavior-changing (ADR + version bump).

### 2. Fixtures Before APIs
- The synthetic fixture dataset (`tests/fixtures/synthetic_players.csv`) must exist BEFORE any real API ingestion code.
- The ONLY canonical control for networked ingestion is `INGEST_MODE=offline|online` (default: offline). Any other phrasing is shorthand and must map to this.
- Default mode is `INGEST_MODE=offline` — all pipelines run against committed fixtures.
- CI and `make check` must succeed with `INGEST_MODE=offline` and without network access.
- CI should run with network disabled or blocked (policy requirement; implementation may vary).
- Online ingestion is allowed only when `INGEST_MODE=online` is explicitly set.
- When `INGEST_MODE=online`, cache raw responses under `data/raw/` (gitignored) and update `docs/sources.md` for any new/changed source.
- Real API data is never committed. Only synthetic/fixture data is committed.

### 3. The Gate: `make check`
- Every PR must pass `make check` before merge; `main` must always be green (CI passes).
- CI must run the same gate as local (`make check`) and block merges on failure.
- Run `make check` locally before opening/updating a PR; do not merge if it fails.
- `make check` runs, in order: `ruff check` → `ruff format --check` → `mypy` → `pytest` → `make validate`
- `make validate` runs Pandera contract validation against fixture data.
- If `make check` fails, do not proceed. Fix first.

### 4. Test Integrity (Anti-Cheating)
- **NEVER** modify existing tests to make failing code pass.
- **NEVER** add `@pytest.mark.skip`, `@pytest.mark.xfail`, or weaken assertions.
- **NEVER** delete tests or reduce coverage to pass CI.
- **NEVER** broaden assertions to vacuous checks (e.g., `assert result is not None`).
- If a test fails, fix the SOURCE CODE, not the test.
- Exception: if the test itself has a genuine bug, document the fix in the commit message.

### 5. Golden Snapshot Guard
- Golden snapshot artifacts are the files under `tests/golden/`, keyed by `method_version` (e.g., `v1_scores.json`).
- Updating golden snapshot artifacts is allowed only with an explicit "Intentional behavior change" declaration AND a `method_version` bump.
- CI fails if golden snapshots change alongside source code without a `method_version` increment.
- To update golden snapshots: bump version in config → update `docs/methodology/` → regenerate → commit.

### 6. Complexity Budget
- No implementation file under `src/` or `tests/` exceeds 250 lines without documented justification in a comment at the top.
- No abstraction unless used in 2+ places, or explicitly justified in the PRD/ADR.
- No new dependency without a PRD or ADR justification.
- If you can't explain a module in 2 minutes to an interviewer, simplify it.

### 7. PRD Gate
- No feature work begins without a PRD in docs/prd/<feature>.md (or a micro-PRD in the PR description for simple changes). No PRD is allowed only for formatting/typo/doc-only edits with no behavioral impact.
- ADRs are required for structural decisions (new dependency, new storage format, pipeline architecture change, scoring framework/normalization approach), and must not replace PRDs or methodology.
- Full PRD must include: objective, non-goals, files affected, data contracts impacted, acceptance criteria, required tests.
- For cross-module features, PRD must list all modules touched.
- If a change could alter scoring behavior, methodology outputs, or data contracts — it requires a Full PRD, no exceptions.

### 8. Dual-Model Review
- The AI that builds code is NOT the AI that reviews it.
- Claude Code builds → Codex reviews (or vice versa).
- Review uses the 4-Point Prompt (see below).

---

## 4-Point Review Prompt (for Codex or any reviewer)

When reviewing a diff, answer these four questions:

1. **Does this diff satisfy the PRD acceptance criteria?** List each criterion and pass/fail.
2. **What are the severe bugs?** List them first, before style issues.
3. **Would the developer struggle to explain any part in an interview?** Flag it.
4. **Propose tests that would FAIL if the code were wrong.** Not tests that pass trivially — tests that genuinely exercise correctness.

---

## Architecture Rules

### Rigor Zones
- **High rigor** (strict types, full test coverage, golden snapshots): `src/scoring/`, `src/contracts/`, `src/pipeline/`, `docs/methodology/`
- **Medium rigor** (tests, types, but lighter): `src/ingest/`, `src/api/`
- **Low rigor initially** (functional, tested later): `app/` (UI), deployment configs

### Data Flow
```
fixtures/raw data → ingest → validate (contracts) → transform (dbt/SQL) → score (engine) → serve (API) → display (UI)
```

### Key Directories
```
nba-goat-index/
├── CLAUDE.md                    # This file (constitution)
├── AGENTS.md                    # AI agent roles and permissions
├── PROJECT_WISDOM.md            # Accumulated insights and rationale
├── Makefile                     # Single command: make check
├── pyproject.toml               # uv project config
├── .github/
│   └── pull_request_template.md # PR checklist (enforces gates)
├── prompts/
│   ├── review_codex.md          # 4-Point Review Prompt (copy-paste)
│   ├── plan_feature.md          # PRD creation prompt
│   ├── test_pressure.md         # Test quality verification prompt
│   └── simplify_refactor.md     # Complexity budget enforcement prompt
├── src/
│   ├── ingest/                  # API clients, data fetching
│   ├── contracts/               # Pandera schemas, data contracts
│   ├── scoring/                 # Scoring engine (high rigor zone)
│   ├── pipeline/                # Pipeline orchestration
│   └── api/                     # FastAPI endpoints
├── tests/
│   ├── fixtures/                # Synthetic player data (committed)
│   ├── unit/                    # Unit tests
│   ├── invariant/               # Property-based invariant tests
│   └── golden/                  # Golden snapshot regression tests
├── docs/
│   ├── methodology/             # v1.md, v2.md, etc.
│   ├── prd/
│   │   └── template.md          # PRD template with checklist
│   ├── adr/                     # Architecture Decision Records
│   ├── sources.md               # Data source documentation + ToS
│   └── data_dictionary.md       # Schema documentation
├── dbt_project/                 # dbt models (added week 2-3)
├── app/                         # Streamlit dashboard
├── data/
│   └── raw/                     # Raw online ingestion cache (gitignored)
└── results/                     # Pipeline outputs (gitignored except results/run_metadata/)
```

---

## Stack Policy (Approved Set)

### Core (Week 1-2)
Python 3.12+ · SQL · DuckDB · Parquet · pytest · Pandera · ruff · mypy · GitHub Actions · Makefile · uv · nba_api · pre-commit

### Resume-Optimal (Week 3-5)
Add: dbt Core (dbt-duckdb → dbt-postgres) · PostgreSQL via Neon free tier · FastAPI · Streamlit · Docker · structlog

### Deferred (only when pain appears)
Dagster (when Makefile > 15 targets) · BigQuery free tier (when job requires it) · Great Expectations (when 50+ checks) · Next.js/TypeScript (when targeting data product roles, month 3+)

### Explicitly Not Used
Airflow · Spark · Kafka · Kubernetes · Terraform · Snowflake/Databricks · MLflow · Multiple quality frameworks

---

## Scoring Engine Invariants

These properties must ALWAYS hold. Test them in `tests/invariant/`:

1. **Determinism:** Same input → same output, every run.
2. **Score bounds:** All final scores fall within [0, 100].
3. **Weight sum:** Category weights sum to exactly 1.0.
4. **No NaN:** No NaN values in any final score output.
5. **Component monotonicity:** Increasing a positively-weighted metric increases that component's score (NOT global score — global monotonicity fails under normalization).
6. **Version tags:** Every output contains `method_version` matching the config, and `git_sha` when available.

---

## Data Contracts (Pandera)

### Era-Conditional Rules
- `three_pt_pct`: non-null only for seasons >= 1979-80 (3-point line introduced)
- `blocks`, `steals`: non-null only for seasons >= 1973-74
- Advanced stats (PER, WS, BPM): availability varies by source and era
- Document all era boundaries in `docs/data_dictionary.md`

### Contract Enforcement
- Contracts live in `src/contracts/`
- `make validate` runs all contracts against fixture data
- Contracts are SEPARATE from tests — contracts validate data shape, tests validate logic

---

## Synthetic Fixtures Design

- Use PlayerA, PlayerB, ..., PlayerH — NOT real player names
- 8 players, 4 eras, covering edge cases:
  - Short career (3 seasons)
  - Multi-era span
  - Missing stats (pre-3pt era)
  - Lockout-shortened season
  - Dominant in one category, weak in others
- Expected ordering is mechanically provable from the methodology, not subjective opinion
- Committed to `tests/fixtures/synthetic_players.csv`

---

## Daily Workflow Loop

1. **Determine planning level:** Complex / Standard / Simple / Trivial (see Planning Protocols below).
2. **PRD Gate:** Write `docs/prd/<feature>.md` (Full PRD) or micro-PRD in PR description, based on planning level.
3. **Branch:** `git checkout -b feat/<n>`
4. **`/clear`** Claude Code if a planning session preceded this (start implementation clean).
5. **Plan Mode:** Shift+Tab before first file edit (skip for trivial changes).
6. **Build:** Claude Code implements against PRD.
7. **Verify:** Run `make check`. Fix all failures.
8. **Review:** Codex reviews diff using `prompts/review_codex.md`.
9. **Explain:** Write 5-8 sentence self-explanation (interview prep).
10. **Merge:** Only when checks pass AND PRD criteria satisfied.

---

## Planning Protocols (Claude Code)

### Plan Mode (Shift+Tab)
Use Plan Mode before implementation if ANY of these are true:
- Change touches 2+ files
- Adds or changes a public interface (function signature, API contract, schema)
- Adds a new module or package
- Changes methodology, contracts, or scoring logic

Skip Plan Mode if:
- Single-file trivial change (typo, rename, small refactor)
- Pure documentation edit
- Formatting-only change

### Interrogation Mode (AskUserQuestion)
Use AskUserQuestion to interview the user when working on:
- Writing or updating `docs/methodology/vX.md`
- Designing a new data model, table grain, or schema
- Adding a new scoring component or normalization approach
- Any PRD where the user says "I'm not sure about the approach"

Do NOT use interrogation for:
- Implementing a feature with a complete PRD
- Adding tests for existing code
- Creating Pandera schemas from a defined contract
- Bug fixes or config changes

### Context Hygiene
- After any planning or exploration session, the user should `/clear` before implementation begins.
- Start every implementation session by reading CLAUDE.md and the relevant PRD. Nothing else.
- Do not carry planning conversation into implementation — start clean.

### PRD Levels
- **Full PRD** (new feature, new module, scoring changes): Complete `docs/prd/<feature>.md` using template.
- **Micro-PRD** (bug fix, small refactor, config change): 5-line intent note in the PR description. Must include: what changed, why, and what test verifies it.
- **No PRD** (formatting/typo/doc-only edits with no behavioral impact): Just commit with a clear message.

Rule: If a change could alter scoring behavior, methodology outputs, or data contracts — it requires a Full PRD, no exceptions.

---

## Run Metadata (Observability Without Infra)

Every pipeline run produces a JSON metadata file under `results/run_metadata/`:
```json
{
  "method_version": "v1",
  "git_sha": "abc123",
  "timestamp": "2026-02-16T14:00:00Z",
  "row_count": 450,
  "validation_passed": true,
  "runtime_seconds": 12.4,
  "input_source": "offline_fixtures"
}
```

---

## Git Policy

- Feature branches for all work
- `main` is always green (CI passes)
- Real data: gitignored
- Synthetic fixtures: committed
- Golden snapshots: committed, protected by version guard
- Run metadata: committed under `results/run_metadata/` (small JSON files); all other `results/` outputs are gitignored
