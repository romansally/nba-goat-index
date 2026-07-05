# ADR-0001: Re-point governance to Data Analyst positioning and real committed seed data

**Date:** 2026-07-04
**Status:** Accepted
**Authority:** Executes the locked decisions in `docs/vision/NBA_GOAT_Index_Vision_and_Story.md`
(the Vision doc). Vision governs intent; CLAUDE.md governs operations.

---

## Context

The original governance (CLAUDE.md, AGENTS.md, docs/sources.md, PROJECT_WISDOM.md — committed
before the Vision doc existed) was written for a **Data Engineering** portfolio plan:
synthetic-fixtures-first, offline-by-default via an `INGEST_MODE` env var, "real API data is
never committed," FastAPI/Docker as "Resume-Optimal Week 3–5," and heavyweight per-feature
process (Full PRD per feature, mandatory dual-model review of every diff, CI from week 1).

The owner has since locked a different intent (Vision doc + Master Blueprint + ROI strategy):
this is a **Data Analyst** portfolio project targeting a DA role now, built on **real NBA data
from day one**, finishable as a 2–3 week Tier-1 MVP at ~1–3 hrs/day, with engineering-flavored
tooling pushed to later phases or cut. This ADR records how the governance was re-pointed and
why each safeguard was kept, changed, or cut.

## Decision

### 1. Positioning (changed)
Identity, purpose, and target roles in CLAUDE.md change from "Data Engineering / Analytics
Engineering" to **Data Analyst** (DA, BI Analyst, Reporting Analyst, Data Operations Analyst).
Analytics-engineering rigor (contracts, versioned methodology, dbt later) is retained because it
is a *differentiator for DA roles*, not because we're building an engineering showcase. The NBA
soul rule and the Vision's tie-breaker order are written into CLAUDE.md's identity section.

### 2. Data policy (changed — the core reversal)
- **Old:** synthetic fixtures are the primary dataset; `INGEST_MODE=offline` default; real API
  data never committed; fake player names everywhere.
- **New (per Vision §7, locked):** a curated **real seed dataset** (15–30 all-time greats,
  player-season grain + accolades) is acquired via nba_api + documented hand-assembly and
  **committed** to `data/seed/`. It drives all analysis, rankings, and the report. Raw API dumps
  stay gitignored. Synthetic rows survive **only** as designed test fixtures proving the
  guardrails catch errors (out-of-bounds score, duplicate/missing key, impossible-for-era stat)
  and as the mechanically-provable input for golden snapshots.
- **Why:** synthetic-only gutted the NBA soul (the Vision's #1 failure mode) and made every
  user-facing output fake. Committing the real seed keeps full reproducibility (clone → run, no
  network) while making the analysis real. The reproducibility *property* the old rule protected
  is preserved; only the mechanism changed.
- **Golden snapshots stay synthetic-based deliberately:** expected values for real players are
  opinions; expected values for designed synthetic rows are mathematical consequences of the
  methodology. Real data drives answers; synthetic data drives correctness.

### 3. `INGEST_MODE` env-var machinery (cut)
Replaced by a simpler rule: **network touches exactly one place** — the acquisition script
(`pipeline/fetch_seed.py`), run explicitly and rarely. Everything downstream reads committed
files. Tier-1 has no live/refreshable ingestion (Vision §7), so a runtime mode switch is
scaffolding for a phase that doesn't exist yet. When a refresh workflow arrives (Phase 3), its
PRD can reintroduce whatever switch it actually needs.

### 4. Stack sequencing (changed)
- FastAPI and Docker move from "Resume-Optimal Week 3–5" to **optional/last** (Phase 5, only if
  targeting BI-developer/AE roles) — per the ROI strategy, they signal junior-SWE, cost weeks,
  and add overclaiming risk for DA targets.
- Kept in the analyst arc, in Blueprint phase order: dbt (Phase 2) → PostgreSQL/Neon + BigQuery
  cloud warehouse (Phase 3) → Streamlit (+ optional Power BI) (Phase 3) → CI/pre-commit/mypy/
  sqlfluff/structlog (Phase 4) → sensitivity/bootstrap analysis (Phase 5).
- nba_api moves **into Tier-1** (acquisition only) because real data is now first-class.
- Added to Tier-1 core: matplotlib/plotly (the report needs a strong visualization).
- Explicitly-not-used list unchanged, plus: learned/ML weights deferred indefinitely (Vision §3).

### 5. Process right-sizing for a solo 1–3 hr/day sprint (changed/cut)
- **PRD ceremony (changed):** Tier-1 runs off a single ordered PRD (`docs/prd/tier1_mvp.md`);
  no per-feature Full PRDs during Tier-1. Each post-Tier-1 phase gets one phase PRD. Micro-PRDs
  (3–5 lines in PR/commit) cover out-of-plan small changes. The strict path survives untouched:
  anything that could alter scoring behavior, methodology outputs, or data contracts still
  requires methodology doc + ADR + `method_version` bump.
- **Dual-model review (narrowed):** mandatory only for diffs touching scoring, contracts, or
  methodology (where silent bugs poison everything downstream); recommended elsewhere.
  Mandatory review of every diff would stall a solo sprint for little marginal safety on
  low-rigor code.
- **`make check` contents (trimmed for Tier-1):** `ruff check` → `ruff format --check` →
  `pytest` → `make validate` (Pandera). **mypy is deferred to Phase 4** — type-checking ceremony
  slows Tier-1 and adds no DA resume value; the high-rigor scoring zone is instead protected by
  contracts, invariants, and golden snapshots. The *gate rule* (no merge without green
  `make check`) is unchanged and non-negotiable.
- **CI (deferred to Phase 4):** the Blueprint places GitHub Actions in the operational-maturity
  phase. Until then the same gate runs locally before every merge. Golden-snapshot enforcement
  is by policy + PR template until CI automates it.
- **Planning protocol (compressed):** the four-level planning matrix collapses to: Plan Mode for
  multi-file/interface/scoring changes; AskUserQuestion interviews only for high-stakes design
  (methodology, data model, scoring components); `/clear` between planning and implementation.

### 6. Safeguards kept (unchanged, with reasons)
- **Methodology before scoring code** — the methodology is the intellectual product and the
  interview answer; also enforces the Vision's objective-layer/weighting-layer split.
- **`make check` as the merge gate; main always green** — the single discipline that keeps the
  repo demoable at all times.
- **Test integrity (never weaken tests)** — the invisible-failure mode of AI-assisted coding.
- **Golden snapshot + `method_version` guard** — regression safety for the scoring engine and
  the versioned-methodology differentiator.
- **Run metadata** — observability-without-infra; direct Data Operations Analyst signal.
- **Pandera contracts with era-conditional rules** — the project's flagship data-quality
  differentiator; now validated against *real* seed data, with designed-bad fixtures proving the
  contracts actually catch errors.
- **Complexity budget (250 lines, 2-minute explainability, justified dependencies)** — protects
  interview defensibility.
- **4-Point Review Prompt, feature branches, PR template, self-explanation step** — kept; cheap
  and high-value.
- **Added from Vision §12:** overclaiming-language ban; "fix the method, not the player" iron
  rule; consensus lists as sanity check only.

### 7. Document authority (resolved)
New hierarchy: **Vision doc (intent) > CLAUDE.md (operations) > methodology > PRDs > ADRs >
Blueprint & PROJECT_WISDOM.md (context/rationale)**. CLAUDE.md no longer claims to win all
conflicts; it defers to the Vision on intent and must be updated (via ADR) when they diverge.
- The **Master Blueprint's Phase-1 wording** ("runs entirely on committed synthetic fixtures",
  nba_api in Phase 3) is superseded on that point by Vision §7 and this ADR: Tier-1 runs on the
  committed real seed, and acquisition happens in Tier-1. Per Vision §12 the Blueprint is
  updated to match (note added at its Phase-1 section). Its phase ordering otherwise stands.
- **PROJECT_WISDOM.md** is retained as a rationale archive with a status note marking its
  DE-era guidance (synthetic-first §1.3/§2.2, DE positioning/keywords Part 5, FastAPI/Docker
  sequencing Part 7) as superseded by this ADR. Its testing, review, and anti-bloat insights
  remain valid.

## Consequences

- The repo is governed for the project the owner is actually building; future AI sessions no
  longer receive contradictory instructions about what the project is or what data it uses.
- Committing real seed data means NBA.com-derived numbers live in the repo. Mitigation: small
  curated extract (15–30 players) for a non-commercial portfolio analysis, raw dumps excluded,
  provenance documented in `docs/sources.md` — comparable exposure to the thousands of public
  sports-data repos, and far less than automated scraping (which stays banned).
- Golden expected-values remain provable because they key off synthetic fixtures, not real
  players — preserving the "fix the method, not the player" rule.
- Less ceremony means more trust in the gate: `make check` + scoped review carry the load that
  per-feature PRDs and universal review carried before. If regressions slip through, tighten in
  Phase 4 (CI) rather than re-adding ceremony to Tier-1.
- `docs/prd/tier1_mvp.md` becomes the operative execution plan for Tier-1.
