# NBA GOAT Index — Project Vision & Story
**The source-of-truth for what this project is, what it is not, and where it goes.**
_Read this before proposing any change to scope, tooling, or direction. If a suggestion conflicts with this document, this document wins._

Owner: Roman Sally · Status: living document · Last locked from a full requirements interview.

---

## 0. How to read this document (note to any human or AI)

This project has two audiences and two lifespans, and past AI assistance repeatedly drifted by optimizing for the wrong one. Before suggesting anything, obey these rules:

1. **Right now, this is a Data Analyst portfolio project.** Not a web app, not a software-engineering showcase, not an ML project. Optimize every near-term decision for *Data Analyst resume value and analyst-skill demonstration.*
2. **Long-term, it is a thing Roman genuinely wants to build and keep.** So near-term decisions must **foreclose nothing** — every choice must leave the door open to the fuller vision (team builder, matchup simulator, interactive app).
3. **The NBA soul is non-negotiable.** The stated #1 failure mode is: *"This project has gone in the wrong direction if an AI ever loses the NBA soul of the project."* Real players, real stats, real basketball questions. Never sand this into a generic, could-be-any-dataset exercise.
4. **When priorities collide,** the tie-breaker order is: (a) Data Analyst resume value → (b) finishing a smaller version completely → (c) keeping the full future roadmap open → (d) breadth of skills shown. Never sacrifice (a) or (b) to chase (d).

---

## 1. The one-sentence vision

> **NBA GOAT Index** — a reproducible sports-analytics project that compares NBA players head-to-head and ranks the all-time greats using real, era-adjusted statistics, with a transparent scoring method whose objective inputs are cleanly separated from adjustable human weightings.

**Headline / title used publicly:** *"NBA GOAT Index"* (chosen for engagement).
**True core question:** *"Who is better, Player A vs Player B?"* — pairwise comparison is the primary mechanic. The GOAT ranking is the same engine applied to everyone at once.

**What Roman wants to be able to say it is:**
- *Now:* "A data analysis project that compares and ranks NBA players by the numbers."
- *Later:* "A website/app that uses data and sports analytics to rank and compare NBA players and settle greatness debates."

---

## 2. The story (why this project exists)

Roman loves the NBA and loves the endless "who's better / who's the GOAT / which team beats which / how good would this team be" debates. The fun is in **turning subjective barroom arguments into something you can actually compute** — letting the numbers do the talking and reaching a defensible answer through data instead of vibes.

The project channels that genuine enthusiasm into a portfolio piece that proves Data Analyst skills, while remaining a real thing Roman wants to keep building for years. The enthusiasm is the fuel; the resume value is the near-term objective; the two are aligned because a project you actually care about is one you'll actually finish and can speak about with conviction in an interview.

---

## 3. What "objective" means here (locked philosophy)

There is **no ground-truth GOAT** — "greatness" is a value judgment, so no method is fully objective. This project doesn't pretend otherwise. Instead it earns credibility through **method integrity**, via a deliberate two-layer split:

- **Objective layer (data-driven, reproducible):** the inputs (real stats), the transformations (per-possession + pace normalization, era-relative z-scores), and the **decomposed component scores** (Peak, Longevity, Winning/Impact, Playoff, Accolades, Efficiency/Advanced). Same input always produces the same output. Every score traces back to its components.
- **Weighting layer (explicit, adjustable, clearly labeled as human judgment):** how the components combine into one number. This is where preference legitimately enters, and the project is *transparent about exactly where that happens.*

**Locked decisions:**
- **Weights are chosen and documented, NOT learned from data — for now.** Learned weights only relocate the subjectivity into the choice of prediction target (wins? rings? All-NBA votes?) and are harder to defend. A learned "Objective Mode v2" is allowed *much later* as an explicit enhancement, framed honestly as its own modeling choice. It is **out of scope for the MVP.**
- **Two modes ship:** a **Default Mode** (one principled, documented default weighting) and a **Custom Mode** (user/Roman moves the weights; ranking shifts accordingly — e.g., value rings more → ring-heavy players rise).
- **Era adjustment is always-on by default. No toggle.** A toggle just creates "which is the real answer?" confusion.
- **The interview-winning line this enables:** *"The features and method are objective and reproducible; the weighting is explicit and adjustable, and I deliberately separated the two so the method is transparent about exactly where human judgment enters."*

---

## 4. Consensus and bias (locked rule)

Consensus expert rankings (ESPN, The Ringer, etc.) are used as a **sanity check, never as a target.**

- Compute a **Spearman rank correlation** between the Default-Mode ranking and one or two consensus lists, and report it honestly. Do **not** chase it toward 1.0.
- A large deviation from consensus is a **smoke detector**: investigate it. Usually it reveals a real method flaw (most often an era distortion or a mis-calibrated component) → **fix the method.** Occasionally it's a genuine, explainable, defensible difference → **keep it** (that's what makes the project interesting and *Roman's own*).
- **The iron rule, stated in the methodology doc and defended in interviews:** *"If a result looks wrong, fix the method, not the player."* Players are **never** hand-placed into rankings. Ever.

---

## 5. Scoring model architecture (locked shape, flexible values)

The model is built as **separable, era-adjusted components with weights in an editable config file.** The exact weights are explicitly *not* locked — they are meant to be iterated on forever, and Custom Mode depends on them being swappable.

**Components (v1 set):**
- **Peak** — value of best N seasons (default N configurable).
- **Longevity** — sustained career value (its own component; this is also what makes the "career default vs peak-only optional" toggle natural).
- **Winning / Impact** — impact on team success. **Critical constraint:** on/off and plus-minus data only exist from ~1996–97 onward, so impact **must use an all-era-available proxy** (team SRS relative to league, win shares, teammate-adjusted measures) — never on/off alone, or pre-1997 legends break.
- **Playoff** — postseason performance.
- **Accolades** — MVPs, rings, All-NBA, DPOY, etc. **Critical constraint:** era-contaminated (no MVP pre-1955–56, no DPOY pre-1982–83, no 3PT line pre-1979–80, changing team sizes). Must be era-aware or it punishes players for awards that didn't exist.
- **Efficiency / Advanced + Counting** — PER, BPM, WS, VORP, and raw stats.

**Roman's proposed default priority (a fine v1 starting point, not set in stone):**
Peak → Winning/Impact → Playoff → Accolades → Advanced+Counting.
Longevity added as its own axis per the discussion.

**Design cautions baked in:** don't triple-count correlated signals (accolades, advanced, and winning partly measure the same thing); era-adjust before combining; keep weights in config so iteration never requires code changes.

**What matters is the architecture** (separable + era-adjusted + config-driven weights), because that is what makes the model defensible, iterable forever, and Custom-Mode-ready. The specific numbers will be tuned over time.

---

## 6. Comparison & ranking outputs (what the user actually sees)

**Pairwise "Who's Better?" (primary):**
- Pick Player A vs Player B → a clear verdict **plus the why.**
- Visual, side-by-side, "entertaining" presentation: component/stat numbers shown side by side, **higher number highlighted green, lower red**, plus charts (e.g., a radar/comparison chart) so it's visually appealing, not a raw table.
- **Scope toggle:** default = **overall career**; optional = **peak seasons.**

**GOAT Ranking (the headline):**
- The same score function run across the whole player set → a ranked list.
- Default Mode shows the principled default ranking; Custom Mode exposes the weights and re-ranks live as they change.

---

## 7. Data (locked policy)

- **Real NBA data from day one — non-negotiable.** Synthetic-only would gut the NBA soul and is explicitly rejected. This drives the actual analysis and rankings.
- **Acquire once, commit as a versioned "seed" dataset.** Pull real numbers via **nba_api** (plus modest hand-assembly for accolades and patchy older advanced stats), cache, and **commit the real dataset to the repo.** The pipeline runs on the committed real data → fully real *and* fully reproducible, never blocked on a live API. The acquisition step is documented and re-runnable. Live/refreshable ingestion is a later phase.
- **Source choice is pragmatic:** whatever reliably yields the needed real stats with the least risk. nba_api preferred over Basketball-Reference scraping (BBR scraping carries ToS risk). Exporting/joining multiple real tables is a welcome *bonus* skill if it doesn't add meaningful risk or time, but ease + real numbers win.
- **Synthetic data survives only as tiny test fixtures** — a few deliberately-broken rows to prove the validation layer catches errors (e.g., an out-of-bounds score must trip the guardrail). Real data for answers; synthetic only for testing guardrails.
- **Player set:** small, **15–30 players**, all-time greats. Confirmed names include Michael Jordan, LeBron James, Larry Bird, Magic Johnson, Kareem Abdul-Jabbar, Stephen Curry, Bill Russell, Kevin Durant, Kobe Bryant, Wilt Chamberlain (extend toward ~30 as desired).

---

## 8. Where it lives (locked for now, open for later)

- **The MVP does NOT need to be an interactive app.** It can be a **reproducible analysis + report + ranked/comparison tables + visualizations.** Interactivity is deferred *as long as the MVP can genuinely be transformed into an interactive app later* — which this architecture guarantees (clean data + scored outputs feed a dashboard directly).
- **Preferred eventual interactive layer:** the **most analyst-flavored path that maximizes Data Analyst skill coverage** — i.e., a BI/analyst tool (Streamlit and/or Power BI/Tableau), **not** a React/engineer-flavored web app in the analyst phase. (React-style app belongs to the far-future "website/app" vision, post-job.)
- **Public deployment / live URL** is deferred: it's not itself a differentiating analyst skill, and it can be added later, so it should not consume MVP time.
- **Environment:** built in VS Code (or the equivalent), version-controlled on GitHub as a public portfolio repo.

---

## 9. Scope boundaries (the hard line)

**In the near-term Data-Analyst arc (MVP + immediate analyst layering):**
Real-data acquisition → cleaning → profiling → era-adjusted scoring engine → pairwise comparison → GOAT ranking → validation/QA → documentation → visualizations/report → (then) dbt, cloud warehouse, and an interactive analyst dashboard. This is where **all the high-ROI Data Analyst skills** get demonstrated.

**Explicitly FUTURE (real commitments, deferred by timing — build after landing the DA role, or when analyst value is exhausted):**
- **Team Builder** (draft 5 players → lineup rating). *Definite intent, stretch by timing.*
- **Team-vs-Team Matchup Simulator** ("which team beats which," custom teams face off). *Definite intent, later.*
- **Live/refreshable data ingestion**, public deployment, and a polished web/app front end.

**Ordering of the full arc (confirmed):** compare players → rank players → build teams → simulate matchups.

**Nothing is permanently ruled out.** Per Roman: keep options open; only reject something if it *actively hampers or worsens* the project. "GOAT debate" as *argumentative/LLM-generated narrative* is currently **not wanted** — let the stats do the talking — but not banned forever.

---

## 10. Success and failure (the north stars)

**Success:** *"This project is a success if I learn, implement, and can display the maximum amount of Data Analyst high-value / high-ROI skills, dashboards, visualizations, and tools"* — via a finished, defensible, shareable NBA project.

**Primary failure mode:** *"This project has gone in the wrong direction if an AI ever loses the NBA soul of the project."*

**Secondary failure modes to guard against:**
- Over-engineering into a software/ML project before the analyst story is complete.
- A half-built "everything" instead of a finished smaller version (finishing beats cramming).
- Any near-term choice that **forecloses** the future roadmap (team builder, simulator, interactive app).
- Chasing consensus correlation or hand-placing players (violates §4).

---

## 11. Effort & cadence (reality-checked)

- **Availability:** ~1–3 hrs/day (~7–21 hrs/week).
- **Target:** full Tier-1 MVP finished in **~2–3 weeks** — realistic at this cadence. At the higher end of hours, dbt and an interactive dashboard also come into reach within the month.
- **Method:** a **daily minimum** (one SQL file, one validation rule, one chart, one doc section). Layered projects die from stalling, not difficulty.
- **Tiering as a safety net (not a compromise):** a **Tier-1 core** (real data on the small set + transparent era-adjusted scoring + pairwise compare + GOAT ranking + real SQL + basic validation + one strong visualization/report + README & data dictionary) is the finish-line that guarantees a defensible portfolio piece; dbt, cloud warehouse, the full validation/testing suite, and the interactive dashboard layer on top in a fixed sequence. This honors "finish small completely, foreclose nothing."

---

## 12. Guardrails carried from the build blueprint

- **Raw SQL first, dbt second** — hand-written window functions/CTEs prove a keyword dbt-generated SQL can't; refactoring to dbt then banks a *second* skill.
- **Overclaiming language is banned:** no "production-grade," "enterprise-scale," "architected cloud backend," "machine-learning ranking model," "real-time," "big data." Use "built a reproducible analytics pipeline," "modeled validated player-season data," "documented methodology and validation rules," etc.
- **Claim only what a stranger could verify by cloning the repo and reading the README.** If it isn't an artifact, it isn't a résumé bullet.
- Full phase-by-phase build plan lives in the companion **Master Project Blueprint**; this Vision doc governs *intent*, the Blueprint governs *execution*. If they ever conflict, this Vision doc wins on intent and the Blueprint is updated to match.
