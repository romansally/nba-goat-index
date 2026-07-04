# Data Analyst Portfolio & Resume — Highest-ROI Additions
**Prepared for:** Roman Sally
**Source of truth:** current resume + TV Series inventory (Project #1) + NBA GOAT inventory (Project #2)
**Horizon:** next 1–4 weeks

> **Project numbering used throughout (matches your message):**
> **Project #1 = TV Series Drop-off Analysis** (IMDb / Tableau) — the clean, near-finished DA project.
> **Project #2 = NBA GOAT Index** (DuckDB / dbt / etc.) — the over-engineered one that needs *scoping down*, not building up.

---

## The single most important finding first

Your biggest weakness is **not** a missing skill. It's the **gap between what your resume claims and what your portfolio proves.** You have already *designed* almost everything a strong entry DA needs. Very little of it is *visible and finished.*

Concretely, three claims on your resume are currently **unbacked or thinly backed**, which is an interview-defensibility risk:

- **Tableau** is in your Skills section — but there is **zero Tableau artifact anywhere**. Project #1 only *plans* a Tableau Public dashboard.
- **Window functions / CTEs / subqueries** are listed under Databases — but the SQL you can actually point to is aggregation queries (`545 tickets`, `4-bucket telemetry`). The advanced SQL is *claimed*, not *shown in a committed file*.
- Your dashboards are mostly **specs**, not **published, screenshot-able artifacts** (Reflekt Me = "dashboard spec"; Project #1 Tableau = planned; Project #2 Streamlit = planned).

So the highest-ROI work for the next month is **finishing and making visible the skills you already claim**, not learning a pile of new tools. Every recommendation below is scored with that lens.

---

## PART 1 — Audit

### 1. Skills/tools you already demonstrate STRONGLY (stop adding evidence — it's redundant)
These are proven across multiple roles/projects. More reps here add ~0 resume value.

- **Excel to an advanced analyst level** — PivotTables, XLOOKUP, COUNTIFS, weighted scoring models, dashboards. Proven in 4+ places (Reflekt Me, mentorship, P&W, Microsoft). This is *saturated*.
- **Power BI (real, not claimed)** — you built an interactive report with KPI cards, slicers, DAX, and non-trivial DAX debugging (`KEEPFILTERS`, `COALESCE`). This is genuinely strong and above typical entry level.
- **Data quality / validation / data-quality-rule documentation** — Reflekt Me (4 rules, 4 streams), IT Ticket project (86.4% quality gap, 471/545). Strong and differentiated.
- **Data dictionary creation** — Reflekt Me "first data dictionary, 11 fields." Proven once, planned twice more. You're covered; one *visible* one is enough.
- **KPI definition & reporting** — proven repeatedly.
- **Stakeholder communication / presenting findings** — QBRs, IT leadership (59.1% of 6,684), P&W and Microsoft stakeholder presentations. Very strong for entry level.
- **Weighted composite / multi-criteria scoring** — P&W, Microsoft, WhatsApp chatbot, NBA index. *Over-proven.* Do not build another scoring model for resume value.

### 2. Present but need DEEPER proof (claimed or half-built — convert to visible artifacts)
These are the money zone. Small effort, large credibility jump.

- **Advanced SQL (window functions, CTEs, subqueries)** — listed, not shown. Needs a committed `.sql` file that unambiguously uses them.
- **Tableau** — listed, not shown. Needs one published Tableau Public dashboard. **Highest single gap.**
- **Python / pandas as a data-cleaning workhorse** — you have Python (chatbot, scoring) but not a clean "pandas cleaned messy public data" story with a script + before/after evidence.
- **Star schema / dimensional modeling** — planned in both projects, proven in neither. Needs one finished schema diagram + fact/dim tables.
- **ETL / data pipeline** — you *say* ETL and built a SQLite pipeline, but there's no visible, documented end-to-end "raw → clean → model → output" pipeline with a run command.
- **Cloud data (AWS)** — "S3 telemetry" and "Cloud Data (AWS)" are thin. Real, but a recruiter can't see it.

### 3. Important DA skills that are MISSING or WEAK
- **A published, live BI dashboard a recruiter can click** (Tableau Public link or Power BI published) — you have none live.
- **Cloud data warehouse exposure** (BigQuery / Snowflake) — entirely absent. Increasingly common in DA JDs, and you currently have no defensible keyword here.
- **dbt / analytics engineering transformation layer** — planned in Project #2 only; not built. A strong differentiator if done *lean*.
- **Modern analytical stack keywords proven** (DuckDB, Parquet) — planned in Project #2, not proven.
- **Reproducible, documented pipeline with a one-command run + run/QA log** — partially designed, not finished/visible.
- **Statistical testing / forecasting / A/B testing** — absent. (Deliberately low priority for you — see Part 3.)

### 4. Redundant — do NOT keep adding evidence for these
- More Excel scoring/weighted models.
- More "dashboard specs" (you need *built* dashboards, not more specs).
- More stakeholder-presentation bullets.
- Additional data dictionaries beyond one clean visible one.
- More basic aggregation SQL.

### 5. High-value but should be DELAYED (not realistic or not worth prioritizing in 1–4 weeks)
- **The full Project #2 engineering stack**: FastAPI, Docker, `structlog`, GitHub Actions CI, golden-snapshot regression tests, Neon/PostgreSQL deployment, hosted API. This signals **software/analytics engineering**, not Data Analyst, costs enormous time, and adds little to the DA/Reporting/BI target roles. It also creates real overclaiming risk in interviews.
- **Snowflake / Databricks hands-on or certs** — not entry-realistic to do defensibly in a month; BigQuery is the cheaper, more entry-friendly cloud-DW keyword.
- **ML / recommendation systems / forecasting models** — scope creep that *weakens* the clean DA story (your own Project #1 inventory correctly warns against this).
- **A/B testing infrastructure** — valuable long-term, but hard to demonstrate authentically without a real experiment; revisit after the portfolio is finished.

---

## PART 2 — Ranked high-ROI additions

Each is scored on your criteria. Ratings are High / Med / Low with a one-line reason. **"Category"** uses your seven buckets.

---

### #1 — Publish the Tableau Public dashboard (finish Project #1's Phase 4)
1. **Addition:** A live, public Tableau dashboard (show selector, KPI cards, weighted-rating trend, rolling-avg vs series-avg, season ranking bar, variance tile, and the top-N "license/promote" action table with an adjustable parameter).
2. **Why high ROI:** Closes your single largest claim-vs-proof gap. Tableau is on your resume with no evidence; this makes it real *and* gives you a clickable portfolio centerpiece. Nearly designed already — mostly execution.
3. **Resume gap filled:** Tableau (claimed→proven) + "published/live dashboard" (missing→present) + business-facing decision output.
4. **ATS improvement:** **High.** "Tableau" is a top-5 DA-JD keyword; right now it's a liability, not an asset.
5. **Interview credibility:** **High.** You can screen-share a live artifact and defend every metric.
6. **New / deepening / redundant:** **Deepening** (skill listed, now proven).
7. **Where:** **Project #1.**
8. **Exact task:** Finish the Tableau workbook per your Phase-4 plan; publish to Tableau Public; add the adjustable-N parameter to the action table; capture screenshots.
9. **Artifact:** Public Tableau URL + `tableau_link.txt` + 3–4 screenshots in `/screenshots`.
10. **Bullet unlocked:** *"Built and published an interactive Tableau dashboard analyzing episode-level IMDb ratings across [N] shows, surfacing a top-N season action table (adjustable parameter) to guide licensing/promotion prioritization."*
11. **Overclaim/overload risk:** Low. Only risk is claiming Tableau breadth you don't have — keep the bullet to what the workbook shows.
12. **Priority:** **#1. Do this first.**
**Category:** Deepen an existing skill already shown → *Add to Project #1.*

---

### #2 — Commit real advanced-SQL files (window functions + CTEs) to GitHub
1. **Addition:** Named `.sql` files that unambiguously use window functions (`OVER(...)`, `LAG`, `RANK`/`ROW_NUMBER`), CTEs, and a subquery/self-join — e.g., rolling 3-season average and the shark-jump / structural-break detection query.
2. **Why high ROI:** Turns three *listed-only* keywords into proof, at near-zero extra scope because Project #1's shark-jump/rolling-avg logic *requires* window functions anyway.
3. **Resume gap filled:** Window functions, CTEs, subqueries (claimed→proven in a readable file).
4. **ATS improvement:** **High.** These exact terms appear constantly in DA JDs and screening filters.
5. **Interview credibility:** **High.** "Walk me through a window function you wrote" becomes a real answer with a real file.
6. **New / deepening / redundant:** **Deepening.**
7. **Where:** **Both**, but prioritize **Project #1** (already needs it). Reuse the pattern in Project #2.
8. **Exact task:** Write `sql/schema.sql`, `sql/kpi_season.sql` (weighted avg + `LAG`/rolling window), `sql/shark_jump.sql` (CTE + window + comparison to series baseline). Commit with comments.
9. **Artifact:** 3 documented `.sql` files in a `/sql` folder, referenced in the README.
10. **Bullet unlocked:** *"Wrote analytical SQL (CTEs, window functions) to compute rolling 3-season rating trends and flag structural 'shark-jump' quality breaks against each series' baseline."*
11. **Overclaim/overload risk:** Very low.
12. **Priority:** **#2.**
**Category:** Deepen an existing skill already shown → *Add to both (lead with #1).*

---

### #3 — Finish the star schema / dimensional model (fact + dims + diagram)
1. **Addition:** A completed star schema: `fact_episode` (or `fact_season_kpi`) + `dim_show`, `dim_show_category`, `dim_season`, with a simple schema diagram.
2. **Why high ROI:** "Data modeling / dimensional modeling / star schema" is a high-value DA/BI keyword you currently *plan* but don't *prove*. One diagram + table definitions closes it.
3. **Resume gap filled:** Data modeling, dimensional modeling, star schema (missing-as-proven → present).
4. **ATS improvement:** **High.** Common in BI Analyst / Business Data Analyst JDs.
5. **Interview credibility:** **High.** You can explain grain (episode→season→show), keys (IMDb `tconst`), and why you chose a star.
6. **New / deepening / redundant:** **New** (as a proven artifact).
7. **Where:** **Project #1** (cleanest fit; keys and grain already defined).
8. **Exact task:** Formalize fact/dim tables in `sql/schema.sql`; add a `docs/data_model.md` with a diagram (draw.io/Mermaid) + grain and key notes.
9. **Artifact:** Schema diagram image + `data_model.md`.
10. **Bullet unlocked:** *"Designed a star schema (fact_episode + show/season/category dimensions) on stable IMDb keys to support season-level KPI aggregation and cross-show comparison."*
11. **Overclaim/overload risk:** Low. Don't call it a "data warehouse"; call it a star schema.
12. **Priority:** **#3.**
**Category:** Deepen an existing skill already shown → *Add to Project #1.*

---

### #4 — Ship a documented pandas cleaning script (Python as a DA cleaning workhorse)
1. **Addition:** A committed `pipeline/clean.py` that reads the raw IMDb TSVs (chunked/filtered), handles `\N` nulls, drops specials/season-0/missing episode numbers, verifies `tvEpisode`, and writes clean CSV/Parquet — with a short "what it does" note.
2. **Why high ROI:** Converts "Python" from a chatbot-flavored skill into a **DA-relevant data-cleaning** skill with a visible artifact. Python + pandas cleaning is one of the most-requested entry-DA competencies.
3. **Resume gap filled:** Python/pandas for cleaning large public data (present-but-thin → proven).
4. **ATS improvement:** **High.** "Python," "pandas," "data cleaning," "large datasets."
5. **Interview credibility:** **High.** Real messy-data decisions (null handling, exclusions, reconciliation) are great interview stories.
6. **New / deepening / redundant:** **Deepening.**
7. **Where:** **Project #1** primarily; the same pattern applies to Project #2's `nba_api` ingestion.
8. **Exact task:** Write and commit the cleaning script + a one-line run command; log row counts before/after in the README QA section.
9. **Artifact:** `pipeline/clean.py` + a before/after row-count table in the README.
10. **Bullet unlocked:** *"Built a Python/pandas cleaning pipeline for multi-GB IMDb TSVs — chunked reads, `\N` null handling, and episode-integrity filters — reducing [X] raw rows to [Y] validated tvEpisode rows."*
11. **Overclaim/overload risk:** Low. Use real counts from your run; don't invent them.
12. **Priority:** **#4.**
**Category:** Deepen an existing skill already shown → *Add to Project #1 (pattern reused in #2).*

---

### #5 — README + data dictionary + QA-evidence section (portfolio legibility layer)
1. **Addition:** A recruiter-legible README for Project #1 containing: business context, metric formulas, **data dictionary** (table grain, key columns, source fields, metric definitions), a compact **Validation** section (episode-count reconciliation, weighted-rating ±0.01 spot check, duplicate-key audit, `qa/validate.py` command), reproducible commands, and IMDb attribution.
2. **Why high ROI:** Your work is *governance-strong but invisible.* This is the layer that makes a recruiter believe the rest. Very low effort, high signal, and it re-uses proof you already generated.
3. **Resume gap filled:** Documentation, data governance, data dictionary, QA evidence — all made *visible*.
4. **ATS improvement:** **Med.** Fewer keyword hits, but adds "data dictionary," "data governance," "documentation," "reproducibility."
5. **Interview credibility:** **High.** A clean README is the difference between "he says he validates data" and "here's exactly how."
6. **New / deepening / redundant:** **Deepening** (you have the substance; you're packaging it).
7. **Where:** **Both** (Project #1 first; lighter version for #2).
8. **Exact task:** Write `README.md` with the sections above; embed screenshots; link the Tableau URL and `.sql` files.
9. **Artifact:** Polished `README.md` + `docs/data_dictionary.md`.
10. **Bullet unlocked:** *"Authored project documentation and a data dictionary (grain, keys, metric formulas) plus a QA-evidence section (count reconciliation, spot checks, duplicate audit) enabling full reproducibility from a single command."*
11. **Overclaim/overload risk:** Very low.
12. **Priority:** **#5.**
**Category:** Deepen an existing skill already shown → *Add to both (Project #1 first).*

---

### #6 — BigQuery (cloud data warehouse) exposure
1. **Addition:** Load your cleaned IMDb tables (or query IMDb-style data) in **Google BigQuery** (free sandbox), and run 2–3 of your analytical queries there — including one window-function query.
2. **Why high ROI:** Adds a genuine, defensible **cloud data warehouse** keyword you currently lack, at low cost. BigQuery is the most entry-realistic cloud DW (free sandbox, standard SQL, public datasets). This is your best *new* keyword add.
3. **Resume gap filled:** Cloud data warehouse (missing→present); strengthens thin "Cloud Data (AWS)."
4. **ATS improvement:** **High.** "BigQuery," "cloud data warehouse," "cloud" show up widely and you have nothing here now.
5. **Interview credibility:** **Med–High.** You can speak to loading data, standard SQL vs dialects, and querying at scale — enough for entry level, defensible.
6. **New / deepening / redundant:** **New.**
7. **Where:** **Either project** — easiest on **Project #1** (data is clean and small enough to load).
8. **Exact task:** Create a BigQuery sandbox, load the fact/dim tables, run + screenshot the KPI and window-function queries; save the SQL as `sql/bigquery_*.sql`.
9. **Artifact:** BigQuery SQL file(s) + a screenshot of a query result in the console, referenced in README.
10. **Bullet unlocked:** *"Loaded modeled IMDb tables into Google BigQuery and ran windowed analytical SQL to validate season-level KPIs in a cloud data warehouse environment."*
11. **Overclaim/overload risk:** **Med.** Don't claim "built a data warehouse" or "production BigQuery." Claim: loaded tables and ran analytical SQL. Keep it honest to what you did.
12. **Priority:** **#6.**
**Category:** New skill → *Add to Project #1 (or #2).*

---

### #7 — Lean dbt models (staging → mart) with tests + docs — Project #2, scoped down
1. **Addition:** A **minimal** dbt project on DuckDB: `stg_*` models, one or two `mart_*` models, a handful of `schema.yml` tests (`not_null`, `unique`, `accepted_values`), and `dbt docs` lineage.
2. **Why high ROI:** dbt is the strongest *differentiator* keyword available to you and is rising fast in DA/analytics-engineering JDs. It produces clean artifacts (models, tests, lineage graph). It also gives Project #2 a *reason to exist* as a DA piece instead of a SWE piece.
3. **Resume gap filled:** dbt, analytics engineering, SQL-first transformation layer, tests-as-data-quality (all missing→proven).
4. **ATS improvement:** **High** (for a differentiator). Fewer JDs than Tableau, but high signal where present.
5. **Interview credibility:** **Med–High.** Defensible if you built it lean and can explain staging vs mart and why tests matter. Risky only if you bolt on the whole engineering stack and can't defend it.
6. **New / deepening / redundant:** **New.**
7. **Where:** **Project #2** (already planned there; this becomes its centerpiece).
8. **Exact task:** Initialize `dbt-duckdb`; build 3–5 models + tests; run `dbt build` and `dbt docs generate`; screenshot the lineage graph.
9. **Artifact:** `dbt_project/` with models + `schema.yml` tests + a lineage screenshot in README.
10. **Bullet unlocked:** *"Built a dbt (DuckDB) transformation layer — staging and mart models with not-null/unique/accepted-values tests and auto-generated lineage docs — enforcing data contracts on NBA player/season stats."*
11. **Overclaim/overload risk:** **Med.** Keep it lean; do NOT also claim CI, Docker, FastAPI, Postgres deploy. The temptation to over-build here is your main trap.
12. **Priority:** **#7** (weeks 3–4, only after Project #1 is fully finished).
**Category:** New skill → *Add to Project #2 (as its new, scoped-down core).*

---

### #8 — Prove DuckDB + Parquet as the analytical engine (Project #2)
1. **Addition:** Actually run the Project #2 pipeline on **DuckDB** querying **Parquet**, with the transformations in SQL.
2. **Why high ROI:** "DuckDB" and "Parquet" are modern-stack keywords you currently only *plan*. They pair naturally with dbt (#7), so you get two keywords for one pipeline. Low incremental cost once dbt is in.
3. **Resume gap filled:** DuckDB, Parquet, columnar/analytical querying (planned→proven).
4. **ATS improvement:** **Med.** Niche but rising; strong in modern analytics roles.
5. **Interview credibility:** **Med.** Easy to defend ("in-process OLAP, columnar Parquet, why it's fast for analytics").
6. **New / deepening / redundant:** **New.**
7. **Where:** **Project #2.**
8. **Exact task:** Write cleaned data to Parquet; point DuckDB/dbt at it; run the scoring/aggregation SQL; save one `results/*.parquet` + run metadata JSON (row counts, method version, git SHA).
9. **Artifact:** Parquet output + a `run_metadata.json` (rows, runtime, source, version, SHA).
10. **Bullet unlocked:** *"Engineered a DuckDB + Parquet analytical pipeline producing versioned, reproducible outputs with run metadata (row counts, runtime, source, method version, git SHA)."*
11. **Overclaim/overload risk:** Low, if paired honestly with dbt.
12. **Priority:** **#8** (with #7).
**Category:** New skill → *Add to Project #2.*

---

### #9 — GitHub portfolio structure + screenshots (both repos)
1. **Addition:** Clean, consistent repo structure (`/sql`, `/pipeline`, `/qa`, `/docs`, `/screenshots`, `README`), a pinned repo, and screenshots embedded in each README.
2. **Why high ROI:** Enabler for *everything else.* Unstructured or private repos make all the above invisible. This is the cheap multiplier.
3. **Resume gap filled:** "GitHub portfolio," Git in a DA context (present but not showcased).
4. **ATS improvement:** **Low–Med** directly; **High** indirectly (it's what makes the rest legible).
5. **Interview credibility:** **Med–High.** Recruiters and hiring managers click the repo; structure signals professionalism.
6. **New / deepening / redundant:** **Deepening.**
7. **Where:** **Both.**
8. **Exact task:** Standardize folders, add READMEs, pin repos, embed screenshots, add a one-paragraph "how to run."
9. **Artifact:** Two clean public repos + pinned profile.
10. **Bullet unlocked:** (Supports all others; e.g.) *"Published two documented, reproducible analytics repos with structured SQL, QA scripts, and dashboard artifacts."*
11. **Overclaim/overload risk:** None.
12. **Priority:** **#9** (do incrementally alongside #1–#8).
**Category:** Deepen an existing skill already shown → *Add to both.*

---

### #10 — Reproducible one-command workflow + QA/run log (Data Operations angle)
1. **Addition:** A `Makefile` or documented command sequence that runs sample → clean → SQL → validate → output in one go, plus a written QA/validation log.
2. **Why high ROI:** Directly serves your **Data Operations Analyst / Data Specialist** targets. Reproducibility + a run/QA log is exactly the operational-analytics signal those roles want, and you already have most of the pieces (`qa/validate.py`).
3. **Resume gap filled:** Reproducible pipeline, operational/data-ops signal (partially designed→finished/visible).
4. **ATS improvement:** **Med.** "reproducibility," "data validation," "pipeline," "data operations."
5. **Interview credibility:** **High** for ops-flavored roles.
6. **New / deepening / redundant:** **Deepening.**
7. **Where:** **Both.**
8. **Exact task:** Add `make sample`, `make run`, `make validate`; write a short `qa/validation_log.md` with the checks and their results.
9. **Artifact:** `Makefile` + `qa/validation_log.md`.
10. **Bullet unlocked:** *"Packaged the full clean→model→validate→output pipeline behind a single `make run` command with a documented QA validation log (count reconciliation, spot checks, duplicate audit)."*
11. **Overclaim/overload risk:** Low.
12. **Priority:** **#10.**
**Category:** Deepen an existing skill already shown → *Add to both.*

---

### #11 — One lightweight, honest statistical rigor touch (optional, capped)
1. **Addition:** Add ONE simple, defensible statistical element to the shark-jump detection — e.g., flagging a break only when a season's mean drops beyond a set threshold *and* the change exceeds normal season-to-season volatility (using the rating standard deviation you already compute). No ML, no forecasting.
2. **Why high ROI:** Slightly upgrades your analytics story from "rule of thumb" to "threshold justified by variance," which reads more analytical — without scope creep.
3. **Resume gap filled:** Light "statistical analysis / variance-aware detection" (weak→slightly present).
4. **ATS improvement:** **Low–Med.**
5. **Interview credibility:** **Med.** Good "how did you avoid false positives?" answer.
6. **New / deepening / redundant:** **Deepening.**
7. **Where:** **Project #1.**
8. **Exact task:** Add a volatility-aware condition to the detection SQL/Python; document the threshold logic.
9. **Artifact:** Updated detection query + a short "method + caveats" note.
10. **Bullet unlocked:** *"Refined structural-break detection with a volatility-aware threshold (season mean vs. rating standard deviation) to reduce false-positive 'shark-jump' flags."*
11. **Overclaim/overload risk:** **Med** — do NOT call this "statistical significance testing" or "A/B testing." Call it a variance-aware threshold.
12. **Priority:** **#11** (only if Project #1 is otherwise done).
**Category:** Deepen an existing skill already shown → *Add to Project #1.*

---

### #12 — Streamlit mini-dashboard for Project #2 (optional second-tier)
1. **Addition:** A minimal Streamlit app for the NBA index (filters + ranking table + methodology note). No deployment required for resume value — screenshots suffice.
2. **Why high ROI:** Gives Project #2 a *visible* front end without the SWE stack, and adds a light "Python app / self-service analytics" flavor. Lower priority because you'll already have two published BI dashboards (Power BI + Tableau).
3. **Resume gap filled:** Streamlit (planned→proven, minor); Python-app breadth.
4. **ATS improvement:** **Low–Med.**
5. **Interview credibility:** **Med.**
6. **New / deepening / redundant:** **New**, but partially **redundant** with your other two dashboards.
7. **Where:** **Project #2.**
8. **Exact task:** Build a single-page Streamlit app over the dbt/DuckDB output; screenshot it.
9. **Artifact:** `app.py` + screenshots (deploy only if trivially free).
10. **Bullet unlocked:** *"Built a Streamlit analytics app over dbt/DuckDB outputs with interactive filters and a ranked, methodology-annotated leaderboard."*
11. **Overclaim/overload risk:** Low — but don't let it pull you toward FastAPI/Docker.
12. **Priority:** **#12** (nice-to-have; skip if time-constrained).
**Category:** New skill → *Add to Project #2 (optional).*

---

## PART 3 — Explicit classification (your seven buckets)

- **Add to Project #1 (TV Series):** #1 Tableau publish, #3 star schema, #4 pandas cleaning, #6 BigQuery, #11 variance-aware detection.
- **Add to Project #2 (NBA GOAT, scoped down):** #7 lean dbt, #8 DuckDB+Parquet, #12 Streamlit (optional).
- **Add to both projects:** #2 advanced-SQL files, #5 README + data dictionary + QA section, #9 GitHub structure, #10 one-command workflow + QA log.
- **Deepen an existing skill already shown:** #1, #2, #3, #4, #5, #9, #10, #11.
- **Replace/upgrade a tool or workflow already planned:** *Scope-down Project #2* — replace the FastAPI/Docker/CI/Neon plan with the lean dbt + DuckDB + Streamlit-screenshot plan.
- **Save for a future new project:** Nothing yet — see final recommendation. (If anything: cloud-DW-native or forecasting project, later.)
- **Do NOT prioritize right now:** FastAPI, Docker, `structlog`, GitHub Actions CI, golden-snapshot regression suite, Neon/PostgreSQL deployment, hosted API; Snowflake/Databricks; ML/recommenders/forecasting; formal A/B testing.

---

## PART 4 — Summary rankings

### 1) Top 10 highest-ROI additions overall
1. Publish the Tableau Public dashboard (P1)
2. Commit advanced-SQL files: window functions + CTEs (P1→both)
3. Finish the star schema / dimensional model (P1)
4. Ship a documented pandas cleaning script (P1)
5. README + data dictionary + QA-evidence section (both)
6. BigQuery cloud-DW exposure (P1)
7. Lean dbt models + tests + docs (P2)
8. Prove DuckDB + Parquet pipeline (P2)
9. GitHub portfolio structure + screenshots (both)
10. Reproducible one-command workflow + QA/run log (both)

### 2) Top 5 to do FIRST
1. Tableau publish (P1) — kills your biggest overclaim
2. Advanced-SQL files (P1) — cheapest high-value proof
3. Star schema (P1) — new proven modeling keyword
4. pandas cleaning script (P1) — real Python-for-DA proof
5. README + data dictionary + QA section (P1) — makes 1–4 visible

### 3) Top 3 for ATS impact
1. **Tableau** (published) — top-tier DA keyword, currently unbacked
2. **BigQuery / cloud data warehouse** — brand-new keyword you fully lack
3. **Window functions / CTEs (proven)** + **star schema / dimensional modeling** — high-frequency screening terms *(tie)*

### 4) Top 3 for interview credibility
1. Published Tableau dashboard (defend live)
2. Advanced-SQL files + star schema (explain grain, keys, window logic)
3. README + QA-evidence section (show exactly *how* you validated)

### 5) Top 3 for strongest resume bullets
1. Tableau published dashboard + top-N action table
2. Python/pandas cleaning of multi-GB IMDb data with real before/after counts
3. dbt + DuckDB/Parquet transformation layer with tests + lineage (differentiator bullet)

### 6) Best additions to integrate into Project #1 (TV Series)
Tableau publish, advanced-SQL files, star schema, pandas cleaning, README+data dictionary+QA, BigQuery, variance-aware detection. **This project alone closes most of your gaps.**

### 7) Best additions to integrate into Project #2 (NBA GOAT)
Lean dbt (staging→mart + tests + docs), DuckDB+Parquet proven pipeline, run-metadata JSON, optional Streamlit screenshots, lightweight README. **Scope it DOWN to this — drop the engineering stack.**

### 8) Additions to save for a NEW project
None right now. Your two projects, finished, cover the DA skill map plus differentiators. A third project would fragment focus and repeat skills.

### 9) Additions to AVOID for now
FastAPI, Docker, `structlog`, GitHub Actions CI, golden-snapshot regression suite, Neon/PostgreSQL deploy, hosted API, Snowflake/Databricks, ML/recommenders/forecasting, formal A/B testing, and any *additional* Excel scoring model or dashboard *spec*.

### 10) Execution sequence

**2-week sprint — finish Project #1 (TV Series) end to end**
- **Days 1–2:** pandas cleaning script (#4) → commit; log real before/after row counts.
- **Days 3–4:** Finalize star schema + `data_model.md` diagram (#3); write the analytical `.sql` files with window functions + CTEs (#2).
- **Days 5–7:** Build + publish the Tableau dashboard incl. adjustable top-N action table (#1); capture screenshots.
- **Days 8–9:** Load tables into BigQuery, run + screenshot windowed SQL (#6).
- **Days 10–11:** Write the README with data dictionary + QA-evidence + reproducible commands (#5, #10); add `Makefile`.
- **Days 12–14:** Clean GitHub structure, pin repo, embed screenshots (#9); optional variance-aware detection tweak (#11); rewrite resume bullets for this project.

**Weeks 3–4 — scope down and finish Project #2 (NBA GOAT), lean**
- **Days 15–18:** Build the actual DuckDB + Parquet pipeline on synthetic fixtures (#8); emit `run_metadata.json`.
- **Days 19–23:** Add lean dbt (staging→mart, tests, `dbt docs`) (#7); screenshot lineage.
- **Days 24–26:** Optional minimal Streamlit + screenshots (#12); write lean README + one methodology note.
- **Days 27–28:** GitHub polish; finalize resume bullets; **explicitly do NOT** add FastAPI/Docker/CI.

---

## PART 5 — Final recommendation

**Do a hybrid, in this order — and it's a specific hybrid, not a vague one:**

1. **Upgrade your current projects first — do NOT start a new one.** You are not short on projects; you are short on *finished, visible* projects. A new build would repeat skills you already show and delay proof of the ones you claim. The fastest path to a stronger resume is completion, not expansion.

2. **Finish Project #1 (TV Series) completely in the first 2 weeks.** It is the ideal Data Analyst piece — clean BI/analytics story, no engineering baggage — and it single-handedly closes your biggest gaps: Tableau (claimed→published), advanced SQL (claimed→proven), dimensional modeling (planned→built), Python/pandas cleaning (thin→real), and documentation/QA (invisible→visible). Add BigQuery here for a brand-new cloud-DW keyword at low cost.

3. **Then scope Project #2 DOWN, not up.** Its current plan (FastAPI, Docker, CI, `structlog`, Neon, golden snapshots, hosted API) is an analytics/software-*engineering* portfolio, which works *against* your Data Analyst / Reporting / BI targets, costs weeks, and creates overclaiming risk. Strip it to a lean analytics-engineering showcase: **DuckDB + Parquet + dbt (models, tests, lineage docs) + run metadata + optional Streamlit screenshots.** That keeps the two genuinely valuable differentiators (dbt, DuckDB) and drops everything that signals "junior SWE" instead of "strong DA."

**Net effect:** in ~4 weeks you convert *five listed-but-unproven skills into proven artifacts* and *add three defensible new keywords* (BigQuery, dbt, DuckDB/Parquet) — with zero redundant reps of Excel/scoring/spec-dashboards, and zero time sunk into engineering the market isn't asking entry DAs to demonstrate.

**One honest caution:** until Tableau and the advanced-SQL files exist in your portfolio, consider softening or footnoting "Tableau" and the advanced-SQL keywords on the resume, or finish #1 and #2 *before* applying widely — being asked to defend an unbacked keyword in an interview is a worse outcome than a slightly shorter skills list.

**Last note — validate against your local market:** before finalizing keywords, pull 8–10 live DA/BI/Reporting Analyst postings in the Indianapolis area and note the exact tools they name. If they lean Power BI + SQL + Excel + Azure/Snowflake rather than Tableau + BigQuery, adjust which cloud/BI keyword you invest the marginal hours in. The *strategy* above holds either way; only the specific cloud/BI target should flex to what your local employers actually ask for.
