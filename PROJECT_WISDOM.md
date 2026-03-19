# PROJECT_WISDOM.md — Accumulated Insights for NBA GOAT Index

> **Purpose:** This document captures every valuable insight, principle, and design decision
> from the multi-round planning process (Claude Opus + ChatGPT cross-validation).
> Any AI agent working on this project should read this file to understand the *why*
> behind every rule in CLAUDE.md.
>
> **Authority rule:** This document EXPLAINS decisions. It contains NO enforceable rules.
> All enforceable rules live in CLAUDE.md. If you find a conflict between this file
> and CLAUDE.md, CLAUDE.md is correct and this file needs updating.
>
> **How to use:** Read this when starting a new session, when making architectural decisions,
> or when tempted to add complexity. Every section below represents a hard-won lesson.

---

## Part 1: Workflow & Process Insights

### 1.1 The Most Important Rule: Methodology Before Code

**The single most important decision in this entire project:**
**Design intent (enforced in CLAUDE.md):** Write `docs/methodology/v1.md` and create the synthetic fixture dataset BEFORE writing any scoring code.

Why this matters:
- The scoring methodology IS the intellectual product. Code is just the implementation.
- Without a written methodology, you can't distinguish bugs from design changes.
- Without versioning, old results don't match new results and you can't tell why.
- An interviewer will ask "why did you weight rebounds at 15%?" — the methodology doc is your answer.
- Both Claude and ChatGPT independently identified this as the #1 priority across 6 rounds of analysis.

### 1.2 Methodology Versioning: The Differentiator

Tag every output with `method_version`. This transforms the project from "coding exercise" to "rigorous analytical system."

How it works:
- `docs/methodology/v1.md` defines scoring logic, weights, normalization approach, era adjustments.
- Config file sets `method_version = "v1"`.
- All outputs (JSON, Parquet, API responses) include `method_version` in metadata.
- Golden snapshot tests ensure v1 methodology produces v1 results (regression safety).
- To change scoring: write `v2.md`, add ADR explaining why, update config, regenerate golden snapshots.
- Side-by-side comparison: run same data through v1 and v2 configs, compare outputs.

**Why this is rare and valuable:** Most portfolio projects don't version their logic. This is standard practice in production analytics but almost never seen in personal projects. Interviewers notice.

### 1.3 Synthetic Fixtures: Why PlayerA, Not Jordan

**Design intent (enforced in CLAUDE.md):** Use synthetic players with controlled stats, not real player names. This was a critical insight:

- **Real names bake in subjectivity.** "Jordan scores 95.2" — is that correct? Who decides? If methodology changes and Jordan drops to 93.8, is that a bug or a valid change? You can't tell.
- **Synthetic players make expected output a mathematical consequence.** If PlayerA has stats X, Y, Z and weights are A, B, C, the expected score is provably X*A + Y*B + Z*C. If the test fails, it's unambiguously a bug.
- **Edge cases are designable.** You control the fixture data, so you can create:
  - A player with a 3-season career (short career handling)
  - A player spanning pre/post 3-point era (missing stats handling)
  - A player dominant in scoring but weak in defense (weight interaction testing)
  - A lockout season (shortened season normalization)

### 1.4 The 4-Point Review Prompt

When any AI reviews code, use this exact prompt:

> Review this diff against the PRD. Answer these four questions:
> 1. Does this diff satisfy each PRD acceptance criterion? (list each, pass/fail)
> 2. What are the severe bugs? (list these first, before style issues)
> 3. Flag anything the developer would struggle to explain in an interview.
> 4. **Propose tests that would FAIL if the code were wrong.**

Point #4 is the most important and most unusual. Most reviews ask "are there tests?" This asks "would these tests actually catch bugs?" The difference is enormous. A test like `assert score is not None` passes trivially. A test like `assert score_player_a > score_player_b` (where A is mechanically better in all weighted categories) actually validates logic.

### 1.5 The Dual-Model Principle

The AI that builds code should NOT be the AI that reviews it. This prevents single-AI groupthink.

- Claude Code builds → Codex reviews (with 4-Point Prompt)
- Or: ChatGPT architects → Claude Code implements → Codex reviews
- The reviewer has no ego invested in the code and will catch things the builder rationalized away.
- This mirrors real engineering teams where the author doesn't review their own PR.

### 1.6 Test-Edit Guard: Preventing AI Test Cheating

AI agents will sometimes "fix" failing tests by weakening the tests rather than fixing the code. This is the most dangerous failure mode because it's invisible.

**Layered protection ideas:**

*These are suggested enforcement mechanisms; they are not active rules unless implemented and referenced by `CLAUDE.md` and/or CI.*

- **CLAUDE.md rule:** Explicitly forbids modifying tests to pass (agents read this).
- **CI soft flag:** Warn when `tests/` files change in the same PR as `src/` files. Require a "tests-reviewed" label.
- **CI hard fail on cheating patterns:** Detect and fail on:
  - Deleted test files
  - Added `@pytest.mark.xfail` or `@pytest.mark.skip`
  - Broadened assertions (e.g., `assertEqual` → `assertIsNotNone`)
  - Coverage drops below threshold
- **Golden snapshot guard (see `CLAUDE.md` → “Golden Snapshot Guard”):** Golden snapshot artifacts under `tests/golden/` may change only with an explicit “Intentional behavior change” declaration AND a `method_version` bump.

**Explanatory note (see `CLAUDE.md` → “Golden Snapshot Guard”):** CI fails if golden snapshots change alongside source code without a `method_version` increment.

### 1.7 Complexity Budget: The Anti-Overengineering Rule

AI coding agents love to overengineer. Without constraints, they'll create elaborate abstractions, deep inheritance hierarchies, and unnecessary design patterns. The complexity budget prevents this:

- No file exceeds 250 lines without documented justification.
- No abstraction unless used in 2+ places.
- No new dependency without PRD/ADR justification.
- **The interview test:** "If you can't explain a module in 2 minutes, simplify it."

This rule protects YOU from your tools. The AI doesn't care if the code is incomprehensible — you're the one who has to defend it.

### 1.8 Self-Explanation Step

After each feature, write a 5-8 sentence explanation as if you're in a job interview:
- What does this module do?
- Why did you design it this way?
- What trade-offs did you make?
- What would you change with more time?

This serves two purposes:
1. Interview prep (you've already rehearsed the answer).
2. Comprehension check (if you can't explain it, you don't understand it, and you need to simplify).

---

## Part 2: Data Engineering Insights

### 2.1 Data Contracts with Era-Conditional Rules

NBA data is messy across eras. Your Pandera schemas must account for this:

- **3-point line introduced 1979-80:** `three_pt_pct` is null/missing for all earlier seasons. A naive "non-null" constraint breaks on every pre-1979 player.
- **Blocks and steals tracked from 1973-74:** Same issue.
- **Advanced stats (PER, WS, BPM):** Availability varies by source and era. Basketball-Reference calculates some retroactively, but not all.
- **Pace differences:** 1960s teams played at 120+ possessions/game vs. ~100 in the 2000s. Raw totals are misleading without per-possession normalization.

**Implementation:** Pandera schemas with conditional checks:
```python
# Pseudocode — actual implementation in src/contracts/
if season >= "1979-80":
    assert three_pt_pct is not null
    assert three_pt_pct between 0.0 and 1.0
else:
    assert three_pt_pct is null or missing  # expected gap
```

This is what separates "toy pipeline" from "real data engineering thinking."

### 2.2 INGEST_MODE: Offline by Default

Your pipeline should default to running against committed fixtures, with real API calls behind an explicit switch:

**Canonical switch:** `INGEST_MODE=offline|online` (default: `offline`).

```
INGEST_MODE=offline  →  reads from tests/fixtures/ (committed, deterministic)
INGEST_MODE=online   →  calls nba_api, caches to data/raw/ (gitignored)
```

Online ingestion is allowed only when `INGEST_MODE=online` is explicitly set (cache under `data/raw/` and update `docs/sources.md`).

Why this matters:
- **Reproducibility:** Anyone can clone the repo and run `make check` without API access.
- **Stability:** NBA.com endpoints can change, rate-limit, or go down. Your tests shouldn't break because of an API outage.
- **Interview demo:** You can demonstrate the full pipeline in 30 seconds without network access.
- **This is the same pattern as your TV Series project** (real data gitignored, synthetic committed).

### 2.3 Data Source Documentation

**Design intent (enforced in CLAUDE.md):** Create `docs/sources.md` documenting every data source:
- **Source name** (e.g., `nba_api`)
- **License/ToS status** (MIT license for client, NBA.com ToS for data)
- **Rate limits** and your caching strategy
- **Data coverage** (which stats, which eras, known gaps)
- **Fallback plan** if source becomes unavailable

This shows data ethics awareness and operational thinking — both valued in DE roles.

### 2.4 Run Metadata: Observability Without Infrastructure

Every pipeline run produces a small JSON file recording:
- `method_version`
- `git_sha` (which code produced this output)
- `timestamp`
- `row_count` (how many players/records processed)
- `validation_passed` (did contracts pass?)
- `runtime_seconds`
- `input_source` ("offline_fixtures" or "online_api")

This is a concrete portfolio artifact that demonstrates "pipeline observability" without requiring Datadog or any monitoring infrastructure. Interviewers love it because it shows operational thinking.

### 2.5 DuckDB + Postgres: Best of Both

- **DuckDB for local development:** Fast, zero-setup, Parquet-native, great for testing and iteration.
- **PostgreSQL (Neon free tier) as production target:** Universal resume keyword, production-like behavior, what real teams use.
- **dbt with both adapters:** `dbt-duckdb` for fast local runs, `dbt-postgres` for "production-like" deployment.

Don't choose one — use both for different purposes. This is actually how many modern data teams work (lightweight local, cloud production).

### 2.6 dbt: Start Earlier Than You Think

dbt Core is the single most important analytics engineering keyword. The learning curve for basic models (staging → intermediate → marts) is about 2-3 hours with AI assistance. dbt is resume-valuable. If bandwidth allows in Weeks 1–2, you can experiment early, but it’s explicitly okay to add it in Week 3+ per CLAUDE.md stack sequencing.

What dbt gives you for free:
- Auto-generated documentation and lineage graphs
- Built-in testing framework (unique, not_null, accepted_values, relationships)
- Incremental models for efficiency
- Macros for DRY SQL
- The `dbt docs generate` command produces a portfolio-ready artifact

---

## Part 3: Testing Strategy Insights

### 3.1 Three-Layer Testing

Your test suite has three distinct layers, each catching different failure modes:

**Layer 1: Unit tests** (`tests/unit/`)
- Test individual functions in isolation.
- "Does `normalize_per_game()` correctly divide totals by games played?"
- Fast, focused, catch logic bugs.

**Layer 2: Invariant tests** (`tests/invariant/`)
- Test properties that must ALWAYS hold regardless of input:
  - Determinism: same input → same output
  - Score bounds: all scores in [0, 100]
  - Weight sum: category weights sum to 1.0
  - No NaN in final outputs
  - Component monotonicity: increasing a positively-weighted metric increases that component's score
- These catch systemic issues that unit tests miss.

**Layer 3: Golden snapshot tests** (`tests/golden/`)
- Run the full scoring engine on synthetic fixtures and compare output to a committed "golden" file.
- If output changes, the test fails. This catches unintended regressions.
- Golden files are protected: can only change when `method_version` is bumped.
- This is your "nothing changed that I didn't expect to change" safety net.

### 3.2 Component Monotonicity, NOT Global Monotonicity

This is a technically important distinction that emerged from the analysis:

- **Global monotonicity** ("if Player A is better in ALL metrics, A's final score is higher") does NOT reliably hold when you use z-scores, percentile normalization, or era adjustments. A player who is 99th percentile in everything gets compressed differently than a player who is 50th.
- **Component monotonicity** ("increasing a positively-weighted metric increases that specific component's score") IS provable under any reasonable normalization.

Test component monotonicity. Don't test global monotonicity — it will produce false failures that waste your time.

### 3.3 "Propose Tests That Would FAIL If Code Were Wrong"

This phrase from the 4-Point Review Prompt deserves its own section because it's the most underrated testing principle:

Most tests are written to confirm expected behavior. But the best tests are designed to catch SPECIFIC failure modes. When reviewing code, don't ask "is there a test?" Ask "if I introduced a bug here, would any test catch it?"

Examples:
- BAD: `assert score is not None` (passes even if score is always 0)
- GOOD: `assert score_player_a > score_player_b` (where A is mechanically better by the methodology)
- BAD: `assert len(results) > 0` (passes even if results are garbage)
- GOOD: `assert results[0].method_version == "v1"` (verifies metadata propagation)

---

## Part 4: Stack & Tool Insights

### 4.1 Industry Signal (add citations; update over time)

Use these as your primary references. Prefer primary/authoritative sources like these:

- **Stack Overflow 2025 Developer Survey:** Python and SQL consistently among most-used technologies. PostgreSQL dominant as a database. FastAPI strong in Python web frameworks. DuckDB adoption has been rising quickly; if you cite specific numbers, add the source + publication date to `docs/sources.md` (prefer primary reports).
- **dbt Labs State of Analytics Engineering:** dbt as core part of many teams' stacks, widely adopted.
- **GitHub Octoverse:** Python and TypeScript among top languages by contributor counts.

Ignore secondary analysis sites (365 Data Science, Prepare.sh, etc.) for strategic decisions. Use Stack Overflow, dbt Labs, and GitHub's own reporting.

### 4.2 The TypeScript Question (Settled)

- **Streamlit for Weeks 1-6.** This is a data engineering project, not a frontend project. Streamlit ships an interactive dashboard in an afternoon.
- **Next.js/TypeScript is deferred, not rejected.** Add it only if targeting "data product" or "analytics platform" teams where polished web UI matters.
- **The recruiter scanning a DE resume looks for:** Python, SQL, dbt, Postgres, Docker, CI/CD, data quality — not React or TypeScript.

### 4.3 Anti-Bloat List with Triggers

Do NOT add these tools unless the specific pain trigger occurs:

| Tool | Pain Trigger |
|------|-------------|
| Dagster/Prefect | Makefile has 15+ targets with complex dependencies |
| Great Expectations | Pandera schemas exceed 50 checks or need complex multi-table validation |
| Apache Spark | Data exceeds DuckDB's memory capacity (~50GB+) |
| Airflow | 20+ tasks with complex scheduling, dependencies, retries |
| Kafka/streaming | Live game data integration (not historical batch) |
| Kubernetes | 3+ services that need independent scaling |
| Snowflake/Databricks | Specific job posting requires it |
| Terraform | Managing actual cloud infrastructure |
| Next.js/React | Targeting data product roles, month 3+ |
| MLflow | Actual ML models (not deterministic scoring) |

### 4.4 uv Over Poetry

`uv` is the recommended Python package manager:
- 10-100x faster than Poetry for dependency resolution
- Made by the same team behind `ruff` (Astral)
- Growing rapidly in the Python ecosystem
- Better for AI-assisted development (agents frequently install/update packages, speed matters)

Not a resume keyword (yet), but the right ergonomic choice.

### 4.5 Start mypy Non-Strict

Enforce types on the scoring core first (`src/scoring/`), then gradually tighten:
- Week 1-2: mypy on `src/scoring/` and `src/contracts/` only
- Week 3-4: Expand to `src/pipeline/` and `src/api/`
- Week 5+: Full codebase

This prevents type-checking from becoming a barrier to shipping in Week 1 while still maintaining the "typed scoring boundaries" that matter most.

---

## Part 5: Resume & Portfolio Insights

### 5.1 What Makes This Project Different

Most portfolio projects have basic unit tests and a README. This project differentiates through:

1. **Versioned methodology** — most projects don't version their logic
2. **Data contracts** — most projects don't validate schemas
3. **Multi-layer testing** (unit + invariant + golden) — most projects have basic tests only
4. **Complexity budget** — most projects over-engineer with AI
5. **ADRs for decisions** — most projects don't document why
6. **Synthetic fixtures** — most projects use real data, can't share/reproduce
7. **Reproducible outputs** — most projects can't reproduce old results
8. **Run metadata** — most projects have no observability story

### 5.2 Resume Phrasing

> "Designed and built an end-to-end data platform for NBA player analysis using dbt Core for transformation modeling, DuckDB and PostgreSQL for storage, FastAPI for API endpoints, and Streamlit for interactive dashboards. Implemented versioned scoring methodology with reproducible outputs, data contracts with Pandera for schema validation, multi-layer testing (unit, invariant, golden snapshot), and CI/CD via GitHub Actions with Docker containerization."

### 5.3 Keywords Captured

Python · SQL · dbt · DuckDB · PostgreSQL · Parquet · FastAPI · Streamlit · pytest · Pandera · Docker · GitHub Actions · data modeling · data quality · data contracts · CI/CD · API development · analytics engineering · reproducibility · version control · testing · type safety

### 5.4 Proof Artifacts (What Interviewers See)

| Skill Claimed | Proof Artifact |
|---------------|----------------|
| SQL dimensional modeling | `dbt_project/models/` + schema diagram |
| Data pipeline design | `src/pipeline/` + methodology docs |
| Data quality/contracts | `src/contracts/` + `make validate` |
| CI/CD | `.github/workflows/ci.yml` |
| Type safety | `mypy.ini` + zero errors in CI |
| Multi-layer testing | `tests/unit/`, `tests/invariant/`, `tests/golden/` |
| Methodology governance | `docs/methodology/v1.md` + `docs/adr/` |
| API development | `src/api/` + OpenAPI docs |
| Containerization | `Dockerfile` |
| Pipeline observability | Run metadata JSON files |

---

## Part 6: Process & Governance Insights

### 6.1 Conflict Hierarchy (Reference Only)

This section is explanatory only. The enforceable hierarchy lives in `CLAUDE.md` and must be treated as canonical.
If the ordering below ever differs from `CLAUDE.md`, this section is wrong and must be updated.

Convenience copy (must exactly match `CLAUDE.md`):
1. `CLAUDE.md`
2. `docs/methodology/vX.md`
3. `docs/prd/*`
4. `docs/adr/*`
5. `PROJECT_WISDOM.md`

`AGENTS.md` and `prompts/` are helper docs; if they conflict with the hierarchy above, defer to `CLAUDE.md`.

### 6.2 When to Write an ADR

Canonical ADR requirements are defined in `CLAUDE.md`; the list below is illustrative guidance.

Situations where an Architecture Decision Record can be useful include:
- Adding a new dependency
- Changing the scoring methodology
- Changing the data model
- Choosing between two reasonable approaches
- Reversing a previous decision

ADR format: Title, Date, Status, Context, Decision, Consequences.

### 6.3 Pre-Commit: First Defense Layer

Set up pre-commit hooks on Day 1-2:
- `ruff check` (linting)
- `ruff format` (formatting)
- `mypy` (type checking, scoring core only at first)
- Trailing whitespace, end-of-file fixer

This prevents messy commits before they happen. Don't rely on the AI agent remembering to format.

### 6.4 Review Packet Generator

Create a `make review-packet` target that outputs:
- Link to the PRD
- Git diff summary
- `make check` output
- Files touched summary

This ensures every Codex review gets consistent, complete context.

---

## Part 7: Sequencing Insights (What Goes When)

### Week 1-2: Foundation
- Day 1: Repo skeleton, uv, directory structure, Makefile, CLAUDE.md, AGENTS.md, pre-commit
- Day 2: `docs/methodology/v1.md`, synthetic fixtures, `docs/sources.md`
- Day 3: Pydantic models, Pandera schemas (with era-conditional rules), `make check` green
- Day 4: Pipeline against fixtures (NOT APIs yet), contract validation, DuckDB + Parquet
- Day 5: Scoring engine v1, golden snapshot tests, invariant tests
- Day 6: GitHub Actions CI, first ADR, PRD template
- Day 7: End-to-end run against fixtures, run metadata JSON, README

### Week 2-3: Data & Modeling
- API integration behind INGEST_MODE=online flag
- dbt Core setup (try dbt-duckdb first, then dbt-postgres)
- PostgreSQL via Neon free tier
- `dbt docs generate` as portfolio artifact
- Data dictionary

### Week 3-5: Product Layer
- FastAPI endpoints
- Streamlit dashboard
- Docker (single Dockerfile)
- structlog for structured logging
- Scheduled GitHub Actions runs

### Week 5+: Polish
- Deploy Streamlit to Streamlit Cloud
- Deploy API to Railway/Render free tier
- Host dbt docs site
- Portfolio README with screenshots

### Deferred Indefinitely (until pain)
- Dagster, BigQuery, Iceberg, Great Expectations, Next.js, TypeScript

---

## Part 8: Lessons from TV Series Project (Transferable)

These lessons from your previous project carry forward:

1. **CLAUDE.md wins all conflicts** — same governance model works here.
2. **Real data gitignored, synthetic committed** — same pattern.
3. **Phase-gating: don't move forward until QA passes with zero errors** — same discipline.
4. **Claude Code CLI = implementation driver** — same muscle memory.
5. **ChatGPT = checkpoint auditor** — same verification role.

What's NEW for NBA project:
- PRD-per-feature (NBA is interconnected, not linear like TV Series)
- Methodology versioning (scoring logic changes, TV pipeline was static)
- Data contracts with era-conditional rules (NBA data is messier across eras)
- Golden snapshot testing (scoring engine needs regression protection)
- More comprehensive ADR practice (more design decisions to document)

---

## Part 9: Planning Workflow Quick Reference ("When to Use What")

This is a lookup table, not a rule document. Rules live in CLAUDE.md.

### What Planning Level Does This Task Need?

| Situation | Plan Mode? | Interrogation? | PRD Level | /clear before implementing? |
|-----------|-----------|---------------|-----------|---------------------------|
| Writing/updating methodology vX.md | Yes | YES — full interview | Full PRD | Yes |
| New data model or schema design | Yes | YES — full interview | Full PRD | Yes |
| New scoring component or normalization | Yes | YES — full interview | Full PRD | Yes |
| New module (e.g., src/api/, new ingest source) | Yes | No | Full PRD | Yes |
| Feature touching 2+ existing files | Yes | No | Full PRD | Yes |
| Adding tests for existing code | No | No | Micro-PRD (in PR) | No |
| New Pandera schema from defined contract | No | No | Micro-PRD (in PR) | No |
| Bug fix | No | No | Micro-PRD (in PR) | No |
| Config change | No | No | Micro-PRD (in PR) | No |
| Formatting / typo / doc edit | No | No | None | No |

### The Three Habits (from power-user analysis)

1. **Plan Mode for non-trivial changes** — Shift+Tab before touching files. Review the plan before execution.
2. **`/clear` between planning and coding** — Don't carry exploration noise into implementation. Start clean with PRD + CLAUDE.md.
3. **Interrogation for high-stakes design only** — Methodology, data model, scoring architecture. Not routine tasks.

### Mechanical Enforcement Ideas (when implemented)

These enforce rules without relying on memory or habits:
- **pre-commit hooks** — format/lint locally on every commit (Week 1)
- **`make check`** — full gate: format + lint + type + test + validate
- **PR template** — forces PRD link, make check output, golden snapshot check, "what test would fail" question
- **CI (GitHub Actions)** — runs `make check` on every PR, enforces golden snapshot guard
- **Claude Code hooks** — auto-lint after file edits (add Week 2-3 when comfortable)

---

## Appendix: Quotes and Principles to Remember

These are the sharpest formulations from the entire planning process. Reference them when making decisions:

- "Propose tests that would FAIL if the code were wrong" — not tests that pass trivially.
- "If you can't explain a module in 2 minutes, simplify it."
- "The scoring methodology IS the product. Code is just the implementation."
- "A shipped project with slightly fewer keywords beats an unfinished project with perfect keywords."
- "Don't add tools until you feel specific pain."
- "High rigor for the intellectual core. Low rigor for the presentation layer."
- "The AI that builds is not the AI that reviews."
- "Synthetic fixtures make expected output a mathematical consequence, not an opinion."
- "CI fails if golden snapshots changed alongside source code without a method_version bump."
- "Every hour on TypeScript is an hour not spent on dbt modeling."
- "Real data gitignored. Synthetic data committed. Always."
