# Data Sources

> Documents every data source: name, license/ToS status, rate limits, coverage, and how it feeds
> the committed seed dataset. Canonical data policy lives in `CLAUDE.md` (Rule 2: Real Data
> First); this file records the source-level facts that policy depends on.

---

## Seed Dataset (`data/seed/`, committed)

The pipeline runs on a curated, committed **real** dataset — acquired once, versioned in git, so
the project is both real and reproducible with no network access.

- **Contents:** 15–30 all-time greats at player-season grain (`player_seasons.csv`), plus a
  player table (`players.csv`) and a hand-assembled accolades table (`accolades.csv`).
- **Acquisition:** `pipeline/fetch_seed.py` (the only code that touches the network), run
  explicitly and rarely. Raw API responses are dumped to `data/raw/` (gitignored); the script's
  curation step writes the seed CSVs, which are committed.
- **Provenance log (update on every pull/edit):**

| Date | What was pulled/edited | Source | Notes |
|------|------------------------|--------|-------|
| _(none yet — first acquisition happens in Tier-1 task T3)_ | | | |

- **Hand-assembled values** (accolades, patchy pre-1974 advanced stats): each hand-entered field
  must list its verification source (NBA.com history pages, official award lists) in the
  provenance log above. No hand-entered *performance* stats — hand-assembly is for discrete
  facts (MVPs, rings, All-NBA selections, DPOY) and clearly-flagged gap fills only.
- **Refresh policy:** live/refreshable ingestion is a later phase (Vision §7). Until then, the
  seed changes only via deliberate, logged re-pulls.

---

## Sources

### nba_api (Python package) — PRIMARY
- **Type:** Unofficial community client for public NBA.com endpoints (not an official NBA
  product; endpoint stability not guaranteed)
- **License:** MIT (client library)
- **Data license:** Subject to NBA.com Terms of Use
- **Rate limits:** Undocumented; be conservative — throttle requests in the acquisition script
  and record observed behavior here after the first pull.
- **Coverage:** Player stats, team stats, game logs. Coverage and advanced-stat availability vary
  by era (see gaps table below).
- **Role:** sole automated source for the seed dataset.

### NBA.com official history / award pages — MANUAL REFERENCE
- **Role:** verification source for hand-assembled accolades (MVP, DPOY, All-NBA, championships).
- **Access:** manual lookup only; no scraping.

### Basketball-Reference — NOT USED FOR INGESTION
- **Type:** Web resource (Sports Reference LLC)
- **License:** Proprietary. ToS restrict automated access: "Use without license or authorization
  is expressly prohibited."
- **Status:** NOT used for automated ingestion or bulk copying. Manual spot-verification of
  individual facts only.

---

## Data Boundaries (what is and isn't in git)

| Artifact | Location | In git? |
|----------|----------|---------|
| Raw API response dumps | `data/raw/` | No (gitignored) |
| Curated real seed dataset | `data/seed/` | **Yes** |
| Cleaned/intermediate parquet | `data/processed/`, `data/marts/` | No (regenerable) |
| Synthetic guardrail fixtures | `tests/fixtures/` | Yes (tests only, never analysis) |
| Final scored outputs (small CSV) + run metadata | `results/` | Yes |

---

## Known Data Gaps by Era

| Statistic | Available From | Notes |
|-----------|---------------|-------|
| 3-point field goals | 1979-80 season | 3-point line introduced |
| Steals | 1973-74 season | Not tracked before |
| Blocks | 1973-74 season | Not tracked before |
| Turnovers | 1977-78 season | Not tracked before |
| On/off, plus-minus | ~1996-97 season | Winning/Impact component must use all-era proxy (Vision §5) |
| MVP award | 1955-56 season | Accolades must be era-aware |
| DPOY award | 1982-83 season | Accolades must be era-aware |
| PER, WS, BPM | Varies | Some calculated retroactively; availability varies by source |
| Pace/possessions | Varies | Estimated for older eras |

These gaps drive the era-conditional rules in the Pandera contracts (see `CLAUDE.md`) and the
era-adjustment requirements in `docs/methodology/v1.md`.
