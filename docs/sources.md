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
| 2026-07-05 | Career + playoff season totals, 20 players (335 player-seasons, 1957–2026) | nba_api `PlayerProfileV2` (`per_mode36="Totals"`) | ProfileV2 chosen over `PlayerCareerStats`: identical season-totals schema, and the PlayerCareerStats URL for player 2544 (LeBron) was stuck on a poisoned server-side cache returning an empty envelope (echoed `LeagueID: '99'`). Era-gapped stats arrive as nulls and are additionally forced to null in curation. |
| 2026-07-05 | Accolades, 20 players (668 rows: MVP, ring, Finals MVP, All-NBA + team number, DPOY, All-Star) | nba_api `PlayerAwards` | Exact-string mapping of six `DESCRIPTION` values; lookalikes ("NBA Sporting News Most Valuable Player of the Year") excluded. Cross-checked against known career totals for all 20 players (e.g., Russell 11 rings/5 MVP, Jordan 6 rings/5 MVP/6 FMVP, Kareem 6 MVP) — all exact. |
| 2026-07-06 | Accolades: removed 1 orphan row (669 → 668) | Codex review of T3 | Magic Johnson's Feb 1992 All-Star selection has no 1991-92 player-season (he retired pre-season with zero games), so it cannot join a played season as v1.md §5.5/§7 require. Full-table orphan scan found exactly this 1 row of 669. Excluded via `ACCOLADE_EXCLUSIONS` in `fetch_seed.py`; curation now fails loudly on any undocumented orphan. Decision reasoning: v1.md §12.10. |
| 2026-07-05 | League baselines per season, 1957–2026 (70 seasons) | nba_api `LeagueLeaders` (all players, Totals) | Replaces the planned `LeagueDashTeamStats`, which returns no rows before 1996-97. Baselines are player-attributed league sums: PTS/AST match published team totals exactly (1961-62 computes to 118.76 pts per team-game, published 118.8; league TS% .4787 vs published ~.478); TRB excludes unattributed "team rebounds" — a uniform all-era definition, documented in the data dictionary (T10). |
| 2026-07-05 | Team game results per season, 1957–2026 (for SRS + schedule lengths) | nba_api `LeagueGameLog` | Margins from paired PTS by GAME_ID. Team SRS computed by `pipeline/srs.py` (least-squares, unit-tested); validation: computed 1995-96 Bulls SRS = 11.80, matching the published value exactly. `season_games` = modal team games per season (computed, all eras). |
| 2026-07-05 | Season Win Shares, 335 rows (drafted; verified 2026-07-11 — see below) | `data/hand_assembled/win_shares.csv` | nba_api does not serve WS in any form. Drafted from model knowledge with `verified=false` on every row; 2025-26 rows intentionally blank (past knowledge cutoff — Roman supplies). Verification completed 2026-07-11 (per-player log below). |
| 2026-07-05 | League pace 1957–1977, 21 rows (drafted; resolved 2026-07-11 — see below) | `data/hand_assembled/pace_estimates.csv` | Player-level TOV (needed for the possession estimate) exists only from 1977-78, so pace for 1978+ is computed from LeagueLeaders sums and pace before 1978 is hand-assembled, flagged `pace_estimated`. |
| 2026-07-11 | Season Win Shares: all 335 rows verified, corrections applied inline, 5 blank 2025-26 rows filled (LeBron, Curry, Durant, Giannis, Jokić) | basketball-reference.com player pages (per-player URLs below) | Every row now `verified=true`. Committed in 8ab43a6. |
| 2026-07-11 | League pace: 4 rows (1974–77) verified as B-Ref **tracked** pace; 17 rows (1957–73) reclassified as **unverified estimates** and their source column corrected | basketball-reference.com League Averages (1974–77 only) | B-Ref's pace column is blank for every season before 1973-74 (confirmed manually on the League Averages page), so the source the 17 pre-1974 rows previously cited does not contain them. No published source containing the full series was identified; `verified` stays `false`. Investigation and retention reasoning below. |

- **Observed rate-limit behavior (2026-07-05 pull):** 180 requests at ~2–2.5 s spacing (1.5 s
  + jitter), zero HTTP rejections. One silent failure mode observed: a valid JSON envelope with
  an empty primary result set (career_2544) — the fetch script now rejects and retries these.
  Full pull ≈ 6 minutes; re-runs skip existing raw files (resumable).
- **Hand-assembled values** (accolades gap-fills, Win Shares, pre-1978 pace): every hand-entered
  value lives in a committed CSV under `data/hand_assembled/` with a `verified` flag and lists
  its verification source here. Methodology v1 §5.3 explicitly requires season-level Win Shares
  as a hand-assembled seed column — this is the documented exception to the "no hand-entered
  performance stats" default, bounded to exactly that column. Verification status as of
  2026-07-11: all 335 WS rows and the 4 tracked pace rows (1974–77) are `verified=true`; the
  17 pre-1974 pace rows are retained as documented unverified estimates (investigation below).
  `data/hand_assembled/` is **committed** (never gitignored): reproducing the seed on a fresh
  clone needs it, since raw API dumps are regenerable but hand-assembled values are not.
- **Roster note:** the pool is 20 players (locked at T3 plan review). Julius Erving was
  deliberately excluded: five ABA seasons (three MVPs, two titles) are invisible to NBA-only
  data, so any NBA-only score would structurally misrepresent him. Moses Malone is included;
  his two pre-prime ABA seasons (age 19–20) are a minor documented limitation. Active players
  (LeBron, Curry, Durant, Giannis, Jokić) are scored on careers through 2025-26.
- **Refresh policy:** live/refreshable ingestion is a later phase (Vision §7). Until then, the
  seed changes only via deliberate, logged re-pulls.

### Per-player Win Shares verification log (2026-07-11)

All 335 season-WS rows in `data/hand_assembled/win_shares.csv` were verified against the Win
Shares column of each player's Basketball-Reference page (manual lookup, per the B-Ref usage
bounds below); drafted values that disagreed were corrected inline before marking
`verified=true`. The 5 previously blank 2025-26 rows were filled from the same pages.

| Player | Source URL | Verified |
|--------|-----------|----------|
| Michael Jordan | https://www.basketball-reference.com/players/j/jordami01.html | 2026-07-11 |
| LeBron James | https://www.basketball-reference.com/players/j/jamesle01.html | 2026-07-11 |
| Kareem Abdul-Jabbar | https://www.basketball-reference.com/players/a/abdulka01.html | 2026-07-11 |
| Bill Russell | https://www.basketball-reference.com/players/r/russebi01.html | 2026-07-11 |
| Wilt Chamberlain | https://www.basketball-reference.com/players/c/chambwi01.html | 2026-07-11 |
| Magic Johnson | https://www.basketball-reference.com/players/j/johnsma02.html | 2026-07-11 |
| Larry Bird | https://www.basketball-reference.com/players/b/birdla01.html | 2026-07-11 |
| Kobe Bryant | https://www.basketball-reference.com/players/b/bryanko01.html | 2026-07-11 |
| Stephen Curry | https://www.basketball-reference.com/players/c/curryst01.html | 2026-07-11 |
| Kevin Durant | https://www.basketball-reference.com/players/d/duranke01.html | 2026-07-11 |
| Shaquille O'Neal | https://www.basketball-reference.com/players/o/onealsh01.html | 2026-07-11 |
| Tim Duncan | https://www.basketball-reference.com/players/d/duncati01.html | 2026-07-11 |
| Hakeem Olajuwon | https://www.basketball-reference.com/players/o/olajuha01.html | 2026-07-11 |
| Oscar Robertson | https://www.basketball-reference.com/players/r/robertos01.html | 2026-07-11 |
| Jerry West | https://www.basketball-reference.com/players/w/westje01.html | 2026-07-11 |
| Moses Malone | https://www.basketball-reference.com/players/m/malonmo01.html | 2026-07-11 |
| Dirk Nowitzki | https://www.basketball-reference.com/players/n/nowitdi01.html | 2026-07-11 |
| Kevin Garnett | https://www.basketball-reference.com/players/g/garneke01.html | 2026-07-11 |
| Giannis Antetokounmpo | https://www.basketball-reference.com/players/a/antetgi01.html | 2026-07-11 |
| Nikola Jokić | https://www.basketball-reference.com/players/j/jokicni01.html | 2026-07-11 |

### Pre-1974 pace: provenance investigation (2026-07-11)

The 21 hand-assembled pace values (1957–77) were drafted from AI model knowledge, to be verified
against B-Ref's League Averages page. The verification pass found the page only tracks pace from
1973-74 onward:

- **1974–77 (4 rows):** all four values match B-Ref's tracked league pace exactly →
  `verified=true`, source corrected to "tracked pace".
- **1957–73 (17 rows):** B-Ref's pace column is blank before 1973-74, so the originally cited
  source does not contain these values. A search for their actual origin found one independently
  corroborated value — 1961-62 = 125.5 matches the regression-based estimate on the archived
  Basketball-Reference blog ("Stray Thoughts on 1962": pace estimated via "a regression to
  estimate turnovers & offensive rebounds") — suggesting the drafted series echoes B-Ref's
  blog-era regression estimates. But no accessible publication of the full series was found:
  the BBR blog survives only as an unbrowsable static archive, and Sports Reference's 2013
  methodology post ("Estimating Pace and Per-Possession Ratings, 1951-1973") is not reachable
  to confirm whether it tabulates league values.
- **Mechanical cross-check:** computing the published pre-1974 estimation method (ElGee 2010 /
  Sports-Reference 2013 lineage — possessions from FGA/FTA/FG with era-constant offensive-
  rebound and turnover rates: ORB% 0.303 / TOV% 0.161 before 1971, 0.319 / 0.158 for 1971–73)
  from this repo's own committed raw LeagueLeaders + LeagueGameLog totals reproduces the tracked
  1974–77 pace within 0.8 possessions, and agrees with the 17 drafted values within ±1.6 for 15
  of the 17 seasons (worst deviations: 1957 −5.4, 1966 +4.2, formula minus draft). The drafted
  values are therefore plausible, but they are not the output of any formula reproducible from
  this repo's data.
- **Resolution:** the 17 values are retained; their source column now reads "unverified
  estimate" and `verified` stays `false`. Reasoning: (a) the era adjustment requires *some*
  pre-1974 pace, and a documented estimate beats a silent gap; (b) the cross-check bounds the
  plausible error at a few percent; (c) every downstream consumer sees the `pace_estimated=true`
  flag on exactly these seasons. Replacing them with the reproducible formula computation above
  is a v2 candidate — it changes scoring inputs, so it takes the strict path (methodology
  update, ADR, `method_version` bump).

---

## Sources

### nba_api (Python package) — PRIMARY
- **Type:** Unofficial community client for public NBA.com endpoints (not an official NBA
  product; endpoint stability not guaranteed)
- **License:** MIT (client library)
- **Data license:** Subject to NBA.com Terms of Use
- **Rate limits:** Undocumented; be conservative — throttle requests in the acquisition script.
  Observed 2026-07-05: 180 calls at ~2–2.5 s spacing drew zero rejections; the only failure mode
  seen was a silently-empty payload for one player (see provenance log).
- **Coverage:** Player stats, team stats, game logs. Coverage and advanced-stat availability vary
  by era (see gaps table below). Verified in the T3 probe: player season totals and game logs
  serve 1956-57 onward; `LeagueDashTeamStats` serves nothing before 1996-97 (not used);
  `TeamYearByYearStats` has zeroed FGA/FTM/REB columns for most of history (not used).
- **Role:** sole automated source for the seed dataset.

### NBA.com official history / award pages — MANUAL REFERENCE
- **Role:** verification source for hand-assembled accolades (MVP, DPOY, All-NBA, championships).
- **Access:** manual lookup only; no scraping.

### Basketball-Reference — MANUAL REFERENCE ONLY, NO AUTOMATED ACCESS
- **Type:** Web resource (Sports Reference LLC)
- **License:** Proprietary. ToS restrict automated access: "Use without license or authorization
  is expressly prohibited."
- **Status:** NOT used for automated ingestion, scraping, or bulk copying. Two bounded manual
  uses, both mandated by methodology v1 and logged in the provenance table above:
  (1) verification source for the hand-assembled season Win Shares column (v1 §5.3 requires WS;
  nba_api cannot serve it) — a human reads the 20 player pages and verifies/corrects typed
  values (completed 2026-07-11, per-player log above); (2) source for the 4 tracked league pace
  values 1974–77 (v1 §12.10) — B-Ref does not publish pace before 1973-74, so the 17 earlier
  values are unverified estimates NOT sourced from B-Ref (provenance investigation above). Plus
  ad-hoc manual spot-verification of individual facts.

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
| PER, WS, BPM | Varies | Some calculated retroactively; availability varies by source. WS is not served by nba_api at all — season WS is a hand-assembled seed column (see provenance log) |
| Team SRS | n/a (computed) | Not an nba_api field; computed per team-season from LeagueGameLog results by `pipeline/srs.py` |
| Pace/possessions | Computed 1977-78+ | Player-level TOV (possession-estimate input) exists from 1977-78; 1974–77 pace is B-Ref's tracked value (verified); 1957–73 pace is a hand-assembled unverified estimate (provenance investigation above). All pre-1978 rows are flagged `pace_estimated` |

These gaps drive the era-conditional rules in the Pandera contracts (see `CLAUDE.md`) and the
era-adjustment requirements in `docs/methodology/v1.md`.
