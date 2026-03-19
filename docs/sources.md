# Data Sources

> Document every data source used in this project: name, license/ToS status,
> rate limits, data coverage, caching strategy, and fallback plan.

---

## Sources

### nba_api (Python package)
- **Type:** Unofficial community client for public NBA.com endpoints (not an official NBA product; endpoint stability not guaranteed)
- **License:** MIT (client library)
- **Data license:** Subject to NBA.com Terms of Use
- **Rate limits:** TODO — document after testing
- **Usage note:** Respect endpoint stability/rate-limit constraints when testing online ingestion.
- **Coverage:** Player stats, team stats, game logs, play-by-play. Coverage varies by era.
- **Caching strategy:** Raw API responses cached locally in `data/raw/` (gitignored). Never required for tests.
- **Fallback:** Committed synthetic fixtures in `tests/fixtures/`

### Basketball-Reference
- **Type:** Web resource (Sports Reference LLC)
- **License:** Proprietary. Terms of Service restrict automated scraping.
- **Status:** NOT used for automated ingestion. Reference only for manual verification.
- **ToS note:** "Use without license or authorization is expressly prohibited" for automated access.

---

## Ingestion Notes (canonical policy in `CLAUDE.md`)

- Canonical ingestion control: `INGEST_MODE=offline|online` (default: `offline`) — see `CLAUDE.md`
- In practice, offline runs use committed fixtures; online runs cache raw responses in `data/raw/` (gitignored)
- Real-source raw data stays out of git; synthetic fixtures live in `tests/fixtures/`
- This file documents source constraints, coverage, caching, and fallback context; canonical ingestion policy lives in `CLAUDE.md`

---

## Known Data Gaps by Era

| Statistic | Available From | Notes |
|-----------|---------------|-------|
| 3-point field goals | 1979-80 season | 3-point line introduced |
| Steals | 1973-74 season | Not tracked before |
| Blocks | 1973-74 season | Not tracked before |
| Turnovers | 1977-78 season | Not tracked before |
| PER, WS, BPM | Varies | Basketball-Reference calculates some retroactively |
| Pace/possessions | Varies | Estimated for older eras |

These gaps explain why era-conditional rules exist in the project's data contracts (see `CLAUDE.md`).