# NBA GOAT Index — Master Project Blueprint
**Positioning · Phased build · Maximal high-ROI Data Analyst skill coverage**

Prepared for: Roman Sally · Target roles: Data Analyst, Entry/Junior DA, Reporting Analyst, BI Analyst, Business Data Analyst, Data Operations Analyst, Data Specialist.

---

## 0. Positioning (say this, not that)

**Say:**
> *NBA GOAT Index — a reproducible, versioned analytics pipeline that ingests, cleans, profiles, validates, models, transforms, scores, and reports on NBA player-season data, with documented methodology, data contracts, run metadata, and analyst-facing reporting.*

**Do NOT position it as:** "a full-stack NBA ranking app." That framing pulls you toward FastAPI, Docker, and frontend work — engineering signal that works *against* your target roles.

**The one rule that keeps this a Data Analyst project (not a data-engineering one):** the pipeline must visibly **answer analytical questions**, not just emit a scored table. Every phase must leave behind something an analyst or recruiter can *read and understand*, not just something that runs.

**How to maximize coverage without bloat:** you maximize the number of high-ROI skills by **phasing them**, not by cramming them into the first build. The MVP stays lean and finishable; the full roadmap captures nearly every valuable keyword. Each phase is a clean stopping point that adds *new* resume evidence.

---

## 1. Why raw SQL first, dbt second (the core design decision)

Write the transforms in **hand-written, commented SQL on DuckDB in the MVP.** Then, in Phase 2, **refactor that exact logic into dbt.**

This ordering is deliberate and it is what maximizes resume value:
- Hand-written window functions / CTEs / subqueries prove a keyword hiring managers specifically test ("show me a window function you wrote"). dbt-generated SQL does **not** prove this.
- Refactoring into dbt then proves dbt, tests, and lineage as a **separate** skill.
- You bank **two** skills instead of one, plus a clean "here's the raw SQL, here's the same logic as tested dbt models" interview narrative.
- It also de-risks execution: you get a *working, finished* pipeline before you take on dbt's learning curve.

---

## 2. Phase map (build order)

| Phase | Name | Theme | Gate before starting |
| --- | --- | --- | --- |
| **1** | **MVP Core** | Working analyst pipeline (Python + SQL + DuckDB + Parquet + validation + docs + report) | — |
| **2** | **MVP+ / Modern stack** | dbt-duckdb refactor: models, tests, lineage, docs | MVP Core committed & documented |
| **3** | **Real data + cloud + BI** | nba_api ingestion → Postgres/Neon → BigQuery → Streamlit (+ optional Power BI) | Phase 2 committed |
| **4** | **Operational maturity** | CI, expanded tests, golden snapshots, linting, scheduled refresh, logging | Phase 3 core committed |
| **5** | **Advanced analyst layer** | Sensitivity/scenario analysis, bootstrap stability, ADRs; optional FastAPI/Docker | Everything above done |

Never start a phase until the previous one is committed and its README section is written.

---

## 3. PHASE 1 — MVP Core (the resume-critical, finishable slice)

> **Update (2026-07-04, per Vision §7/§12 and ADR-0001):** Phase 1 runs on the **committed real
> seed dataset** (`data/seed/`, acquired once via nba_api + documented hand-assembly), not on
> synthetic fixtures — still deterministic and offline, because the seed is committed. Synthetic
> rows survive only as guardrail test fixtures. The operative Phase-1/Tier-1 task list is
> `docs/prd/tier1_mvp.md`. Phase ordering below otherwise stands.

Runs **entirely on committed synthetic fixtures** — deterministic, offline, no API flakiness. This is the "narrow implemented slice" that already stands as a serious portfolio piece even if you never build the later phases.

### 3.1 Skills included and why (each = a resume-visible artifact)

| # | Skill / action | Why high-ROI | Artifact |
| --- | --- | --- | --- |
| 1 | **Define analytical questions first** *(framing upgrade)* | Makes it a DA project, not a pipeline. Drives every downstream choice. | `docs/questions.md` |
| 2 | **Python / pandas cleaning** | Converts "Python/data cleaning/ETL" from a chatbot reference into DA-relevant proof | `pipeline/clean.py` + before/after row counts |
| 3 | **Data profiling / EDA** *(upgrade — you claim "Data Profiling" but don't prove it)* | Cheap, converts a claimed resume skill to proof; shows you inspect before you trust | `pipeline/profile.py`, `docs/profiling.md` |
| 4 | **Advanced SQL on DuckDB: CTEs, window functions, subqueries, CASE, ranking** | Your #1 claimed-but-unproven keyword cluster; highest gap-closer | `sql/*.sql` (commented) + result outputs |
| 5 | **DuckDB + Parquet analytical engine** | Modern-stack keywords proven at ~zero extra cost | `.parquet` outputs |
| 6 | **Dimensional modeling / star schema** | Proves data modeling (claimed, only planned) | `docs/data_model.md` + schema diagram |
| 7 | **Pandera data contracts + era-conditional validation** | **Differentiator.** "Data contracts" is rare; era-conditional rules are interview gold | `pipeline/validation_schema.py` |
| 8 | **QA validation log** | Makes your single strongest differentiator (data quality) visible in a new domain | `qa/validation_log.md` |
| 9 | **Excel/Sheets independent reconciliation** *(upgrade)* | Earns Excel keyword in an *analytical-validation* context; shows independent verification | `qa/reconciliation.xlsx` + note in log |
| 10 | **Metric layer + data dictionary + versioned methodology (v1)** | Proves metric definition, governance, documentation; versioned methodology is a differentiator | `docs/metrics.md`, `docs/data_dictionary.md`, `docs/methodology/v1.md` |
| 11 | **Run metadata / audit JSON** | **Differentiator** — direct signal for Data Operations Analyst / Data Specialist | `results/run_metadata/run_*.json` |
| 12 | **Reproducible one-command workflow** | Kills the "ETL sounds inflated" risk; proves pipeline/reproducibility | `Makefile` (`make run`) |
| 13 | **Static, question-driven analyst report** *(required, non-negotiable)* | Keeps the MVP DA-facing; answers the Phase-1 questions with charts | `docs/report.md` + `/screenshots` |
| 14 | **GitHub portfolio structure + README case study** | Makes all of the above recruiter-legible in 90 seconds | `README.md`, clean folders, screenshots |

### 3.2 The analytical questions to answer (drives the report)
Examples — pick 3–5 and answer them explicitly in `docs/report.md`:
- Which era produced the most top-10 player-seasons under v1 weighting?
- How much do rankings shift if you change the weight on efficiency vs. volume?
- Which players are most "weighting-sensitive" (rank swings most across reasonable weight sets)?
- What share of player-seasons fail each era-conditional validation rule, and why?
- Which components contribute most to the top-10 scores (component breakdown)?

### 3.3 Validation rules to implement (Pandera)
Grain integrity (one row per player-season); era-conditional required fields (3PT stats required only from 1979–80; blocks/steals only from 1973–74); score bounds `[0, 100]`; determinism (same input → same output); monotonicity (a strictly better stat line cannot lower the component score); duplicate-key detection; null-rate thresholds by field/era.

### 3.4 MVP Core — Definition of Done
`make run` executes end-to-end: **clean → profile → validate → model → score → write Parquet/CSV → emit run metadata → refresh static report.** A stranger can clone the repo, run one command, and read a README that explains the data, star schema, methodology, validation, outputs, and limitations. The report answers the analytical questions with charts.

### 3.5 MVP Core — resume bullets unlocked
- *Built a DuckDB + Parquet analytics pipeline using SQL CTEs, window functions, CASE logic, and ranking to generate versioned NBA player-season scoring outputs.*
- *Implemented Pandera data contracts enforcing player-season grain, era-specific stat availability, score bounds, determinism, and duplicate detection, logged in a QA validation report.*
- *Designed a star schema (player-season fact + player/season-era/team dimensions) supporting transparent, reproducible scoring.*
- *Profiled and reconciled pipeline outputs against an independent Excel recomputation to verify scoring accuracy before publishing results.*
- *Packaged the pipeline behind a single reproducible command with run metadata tracking row counts, validation status, methodology version, runtime, and git SHA.*

### 3.6 Keep OUT of the MVP (this is the anti-bloat line)
dbt · nba_api · hosted database · Streamlit · Power BI · FastAPI · Docker · CI · ML. Each is a later phase. Putting any one in the MVP is the exact trap that leaves you with a bloated, unfinished project.

---

## 4. PHASE 2 (MVP+) — dbt-duckdb refactor (analytics-engineering differentiator)

Refactor the working MVP SQL into dbt. This is your strongest single differentiator keyword and it stays firmly in the Data Analyst lane.

**Skills unlocked:** dbt · staging/intermediate/mart modeling · `schema.yml` tests · data lineage · auto-generated docs · analytics engineering · sources & seeds · (stretch) incremental models & snapshots.

**Models:** `stg_players`, `stg_player_seasons` → `int_player_season_metrics`, `int_scoring_components` → `mart_goat_scores`.

**Tests (`schema.yml`):** `not_null`, `unique`, `relationships`, `accepted_values`, plus custom tests for score bounds and player-season grain uniqueness and final-rank uniqueness.

**dbt features to include for keyword coverage:** `sources:` (declare the raw layer), `seeds/` (the dim_era / dim_position lookup as a seed), `dbt docs generate` + lineage graph. *Stretch:* one `incremental` model and a `snapshot` to earn those keywords.

**Artifacts:** `dbt_project/` (models + `schema.yml` + `sources.yml` + `seeds/`), `screenshots/dbt_lineage.png`, `screenshots/dbt_tests_passed.png`.

**Resume bullet:** *Refactored scoring transforms into dbt-duckdb staging, intermediate, and mart models with not-null/unique/relationships/accepted-values tests, custom bounds tests, seeds, sources, and auto-generated lineage docs.*

---

## 5. PHASE 3 — Real data + cloud warehouses + interactive BI

Adds the two most-requested keywords you currently lack (cloud data warehouse, interactive dashboard) plus real API ingestion.

### 5.1 Controlled `nba_api` ingestion
Small, cached, ToS-aware pull that feeds the *same* validated pipeline. Fixtures first, real data second — so API flakiness never blocks the core.
**Unlocks:** API ingestion · data extraction · raw caching · refresh workflow · messy real-world data handling · source documentation.
**Artifacts:** `pipeline/fetch_nba_api.py`, `data/raw/<date>/`, `docs/sources.md`, `results/run_metadata/api_run_*.json`.
**Bullet:** *Added controlled nba_api ingestion with cached raw responses, documented source policy, and refresh metadata feeding the same validated DuckDB/dbt pipeline.*

### 5.2 Cloud databases — do BOTH via dbt target swap (Postgres first)
Because dbt abstracts the target, both are cheap once the models exist. Do them in this order:

**5.2a — PostgreSQL via Neon (first).** Hosted relational database; the more universally requested keyword, easiest to explain.
- Artifacts: `sql/load_postgres.sql`, `profiles.yml.example`, `screenshots/neon_tables.png`.
- Bullet: *Loaded validated mart tables into hosted PostgreSQL (Neon) and configured dbt targets to run the same models against local DuckDB and a cloud relational database.*

**5.2b — BigQuery (fast follow).** Captures the "cloud data warehouse" keyword you have nowhere else. Free sandbox, standard SQL.
- Artifacts: `sql/bigquery_*.sql`, `screenshots/bigquery_query.png`.
- Bullet: *Re-pointed the same dbt models at Google BigQuery, validating warehouse-agnostic modeling across DuckDB, PostgreSQL, and a cloud data warehouse.*

*(Snowflake / Databricks stay deferred — not entry-realistic to do defensibly, and BigQuery already covers the cloud-warehouse keyword.)*

### 5.3 Interactive dashboard
**Primary: Streamlit** (Python-native, fits the pipeline). Features: ranking table, era/position filters, player comparison, component breakdown, methodology notes, downloadable CSV. Host free on Streamlit Community Cloud for a live URL.
- Artifacts: `app.py`, `screenshots/streamlit_dashboard.png`, `streamlit_app_url.txt`.
- Bullet: *Built and deployed an interactive Streamlit dashboard over validated ranking marts with era/position filters, player comparison, component breakdowns, and downloadable outputs.*

**Optional upgrade — Power BI → Neon (leverages your strongest existing BI skill).** Connect Power BI to the hosted Postgres, build KPI cards + slicers + a couple of DAX measures. This reuses your genuine Power BI/DAX strength against a *live database connection* — a very DA-shaped combo.
- Artifacts: `screenshots/powerbi_report.png`, `docs/measures.md`.
- Bullet: *Connected Power BI to hosted PostgreSQL and built DAX measures and slicer-driven KPI cards on live NBA ranking data.*

**Host the dbt docs publicly (GitHub Pages)** *(upgrade)* so the lineage graph is a clickable link in your README, not just a screenshot.

---

## 6. PHASE 4 — Operational maturity (automation & testing)

Adds CI/CD and testing-depth keywords. Keep these here, never in the MVP.

| Skill | Artifact | Value |
| --- | --- | --- |
| GitHub Actions CI (ruff → mypy → pytest → pandera → dbt build → golden) | `.github/workflows/ci.yml` + green badge | CI/CD, automated quality gates |
| Expanded pytest (unit + invariant + golden snapshot) | `tests/unit/`, `tests/invariant/`, `tests/golden/` | testing, regression protection, scoring-drift prevention |
| `sqlfluff` SQL linting *(upgrade)* | `.sqlfluff` config | SQL style discipline keyword |
| pre-commit hooks | `.pre-commit-config.yaml` | workflow discipline |
| Scheduled refresh (Actions cron) | scheduled workflow | **automated reporting** keyword |
| Structured logging (`structlog`) | log output | observability / data-ops |

**Bullet:** *Added a CI workflow running linting, type checks, unit/invariant/golden-snapshot tests, Pandera contracts, and dbt build on every PR to prevent data-quality and scoring regressions, with a scheduled refresh job.*

---

## 7. PHASE 5 — Advanced analyst layer (optional, high value if done carefully)

Adds honest statistical rigor without forcing ML/forecasting/A-B testing where they don't belong.

| Addition | Why | Guardrail |
| --- | --- | --- |
| **Weight sensitivity / scenario analysis** | Very natural for weighted scoring; strong business-analytics skill | Keep it simple; this is the best advanced add |
| **Bootstrap score-stability / confidence intervals** | Adds real statistical rigor | Don't overclaim; it's stability analysis, not inference on a population |
| **Era-comparison analytical writeup** | Strong interpretation piece | Avoid subjective "GOAT" claims; report patterns, not verdicts |
| **Methodology ADRs** | Governance / decision transparency | Keep concise |
| **FastAPI read-only endpoint + Docker** | Only if targeting BI-developer / analytics-engineering roles | **Lowest priority; engineering signal — defer otherwise** |

**Bullet:** *Ran weight-sensitivity and bootstrap stability analyses to identify robust rankings and the players most affected by methodology assumptions.*

---

## 8. Master high-ROI coverage map

MVP Core alone covers roughly the first two-thirds. The full plan covers essentially every high-ROI Data Analyst keyword except the correctly-deferred ones.

| High-ROI skill | MVP | P2 | P3 | P4 | P5 |
| --- | :-: | :-: | :-: | :-: | :-: |
| Python / pandas | ● | | ● | | ● |
| Data profiling / EDA | ● | | | | |
| SQL (CTEs, window fns, subqueries, CASE, ranking) | ● | ● | ● | | |
| DuckDB | ● | ● | | | |
| Parquet | ● | ● | | | |
| Data cleaning / ETL | ● | | ● | | |
| Dimensional modeling / star schema | ● | ● | | | |
| Pandera data contracts | ● | | | ● | |
| Era-conditional validation | ● | ● | | | |
| QA validation log | ● | | | ● | |
| Excel/Sheets independent reconciliation | ● | | | | |
| Metric layer | ● | ● | | | |
| Data dictionary | ● | ● | | | |
| Versioned methodology / ADRs | ● | | | | ● |
| Run metadata / audit (data-ops) | ● | | ● | ● | |
| Reproducible workflow (Makefile) | ● | | | ● | |
| Static analyst report (question-driven) | ● | | | | |
| GitHub portfolio + README | ● | ● | ● | ● | |
| dbt (models) | | ● | ● | ● | |
| dbt tests / docs / lineage / sources / seeds | | ● | | ● | |
| nba_api ingestion | | | ● | | |
| PostgreSQL / Neon | | | ● | | |
| BigQuery (cloud warehouse) | | | ● | | |
| Streamlit (deployed) | | | ● | | |
| Power BI + DAX (live DB) — optional | | | ● | | |
| CI/CD (GitHub Actions) | | | | ● | |
| pytest (unit/invariant/golden) | basic | | | ● | |
| sqlfluff / pre-commit | | | | ● | |
| Scheduled / automated reporting | | | | ● | |
| Structured logging | | | | ● | |
| Sensitivity / scenario / bootstrap analysis | | | | | ● |
| FastAPI / Docker (optional, lowest priority) | | | | | ● |

**Deferred deliberately (do not chase):** Snowflake, Databricks/Spark, machine learning, A/B testing, forecasting, hosted public API as a priority.

---

## 9. Full target folder structure (all phases)

```text
nba-goat-index/
├─ README.md
├─ Makefile
├─ .pre-commit-config.yaml            # P4
├─ .sqlfluff                          # P4
├─ .github/workflows/ci.yml           # P4
│
├─ pipeline/
│  ├─ clean.py                        # P1
│  ├─ profile.py                      # P1 (upgrade)
│  ├─ validate.py                     # P1
│  ├─ validation_schema.py            # P1 (Pandera)
│  ├─ build_outputs.py                # P1
│  └─ fetch_nba_api.py                # P3
│
├─ sql/
│  ├─ 01_create_schema.sql            # P1
│  ├─ 02_staging_cleaned.sql          # P1
│  ├─ 03_player_season_metrics.sql    # P1 (window fns)
│  ├─ 04_scoring_components.sql        # P1 (CASE)
│  ├─ 05_final_goat_scores.sql        # P1 (ranking)
│  ├─ 06_validation_checks.sql        # P1
│  ├─ load_postgres.sql               # P3
│  └─ bigquery_scores.sql             # P3
│
├─ dbt_project/                       # P2
│  ├─ dbt_project.yml
│  ├─ profiles.yml.example
│  ├─ seeds/                          # dim_era, dim_position
│  ├─ models/
│  │  ├─ staging/    stg_players.sql, stg_player_seasons.sql
│  │  ├─ intermediate/ int_player_season_metrics.sql, int_scoring_components.sql
│  │  ├─ marts/      mart_goat_scores.sql
│  │  ├─ sources.yml
│  │  └─ schema.yml
│
├─ docs/
│  ├─ questions.md                    # P1 (upgrade)
│  ├─ profiling.md                    # P1
│  ├─ data_model.md                   # P1
│  ├─ data_dictionary.md              # P1
│  ├─ metrics.md                      # P1
│  ├─ methodology/v1.md               # P1
│  ├─ adr/                            # P5
│  ├─ sources.md                      # P3
│  ├─ measures.md                     # P3 (Power BI, optional)
│  └─ report.md                       # P1 (required)
│
├─ qa/
│  ├─ validation_log.md               # P1
│  └─ reconciliation.xlsx             # P1 (upgrade)
│
├─ app.py                             # P3 (Streamlit)
├─ streamlit_app_url.txt              # P3
│
├─ data/
│  ├─ sample/  synthetic_players.csv, synthetic_player_seasons.csv   # P1
│  ├─ raw/     <date>/…                                              # P3 (gitignored)
│  ├─ processed/ player_seasons_clean.parquet                        # P1
│  └─ marts/  goat_scores_v1.parquet                                 # P1
│
├─ results/
│  ├─ goat_scores_v1.csv              # P1
│  └─ run_metadata/ run_*.json        # P1
│
├─ tests/                             # P4 (basic pytest in P1)
│  ├─ unit/  invariant/  golden/
│
└─ screenshots/
   ├─ final_output_table.png  validation_passed.png  data_model.png   # P1
   ├─ dbt_lineage.png  dbt_tests_passed.png                           # P2
   ├─ neon_tables.png  bigquery_query.png                             # P3
   ├─ streamlit_dashboard.png  powerbi_report.png                     # P3
```

---

## 10. Overclaiming guardrails (keep every interview defensible)

**Avoid this phrasing:** "production-grade," "enterprise-scale," "architected a cloud backend," "machine-learning ranking model," "deployed a full-stack analytics platform," "real-time," "big data."

**Use this phrasing:** "built a reproducible analytics pipeline," "modeled validated player-season data," "created dbt staging and mart models with tests," "documented methodology and validation rules," "loaded mart tables into PostgreSQL/Neon," "built an analyst-facing dashboard," "ran windowed analytical SQL in a cloud data warehouse."

**Rule of thumb:** claim only what a stranger could verify by cloning the repo and reading the README. If it isn't in an artifact, it isn't a bullet.

---

## 11. Execution discipline (how this actually gets finished)

- **Daily minimum:** one concrete thing per session — one SQL file, one Pandera rule, one README section, one chart, one dbt model. Layered projects die from stalling, not from difficulty.
- **Phase gates:** don't start a phase until the prior one is committed *and* its README section is written. A finished Phase 1 beats a half-built Phase 3.
- **Commit the report early and update it each phase** so the project always has a readable analyst face.
- **Rewrite resume bullets at the end of each phase** while the work is fresh — then keep applying; don't wait for Phase 5.

---

## 12. Recommended stopping points by job-search urgency

- **Applying heavily now:** finish **Phase 1 + Phase 2**, then stop building and update resume/LinkedIn/GitHub. That alone is a strong, differentiated portfolio piece (SQL + DuckDB/Parquet + star schema + data contracts + dbt + docs + run metadata + report).
- **Have 2–4 weeks of runway:** add **Phase 3** (Neon → BigQuery → Streamlit) for the cloud + live-dashboard keywords.
- **Want the fullest possible stack / more technical roles:** add **Phase 4**, then **Phase 5**.
