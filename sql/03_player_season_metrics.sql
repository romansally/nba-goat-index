-- 03_player_season_metrics.sql — per-season era-relative metrics (T6)
--
-- Implements v1.md §3 (era adjustment), §4 (SPI, availability), and the
-- §5.1 peak-window bookkeeping, one CTE per methodology step so every column
-- traces to a formula. Objective-layer parameters (production blend, peak_n,
-- peak_min_avail) are injected from config/scoring_v1.yaml by
-- pipeline/transform.py as DuckDB variables — nothing tunable is hardcoded.
--
-- Null discipline (v1.md §9): regular-season metrics are computable for every
-- row; the p_* playoff mirror stays null exactly when the postseason was
-- missed (po_* null propagates — never coalesced to zero).

INSERT INTO mart_player_season_metrics BY NAME
WITH
-- v1.md §3 step 1: player possessions and per-75 rates, using league pace
-- uniformly across all eras. Playoff totals get the identical treatment
-- against the same regular-season league environment (§4, limitation §12.10.2).
per75 AS (
    SELECT
        f.player_id,
        f.season,
        s.season_games,
        s.base_pts,
        s.base_trb,
        s.base_ast,
        s.lg_ts_pct,
        f.mp * s.pace / 48                 AS poss,
        f.pts / (f.mp * s.pace / 48) * 75  AS per75_pts,
        f.trb / (f.mp * s.pace / 48) * 75  AS per75_trb,
        f.ast / (f.mp * s.pace / 48) * 75  AS per75_ast,
        -- §4: AVAIL = min(1, GP / season_games); LEAST caps the one real
        -- gp > season_games case (Jokić 2020, 73 gp vs modal 72).
        LEAST(1.0, f.gp / CAST(s.season_games AS DOUBLE)) AS avail,
        -- §3: True Shooting is already a rate; era adjustment is a direct ratio.
        f.pts / (2.0 * (f.fga + 0.44 * f.fta))            AS ts,
        f.po_mp * s.pace / 48                             AS po_poss,
        f.po_pts / (f.po_mp * s.pace / 48) * 75           AS po_per75_pts,
        f.po_trb / (f.po_mp * s.pace / 48) * 75           AS po_per75_trb,
        f.po_ast / (f.po_mp * s.pace / 48) * 75           AS po_per75_ast,
        f.po_gp
    FROM fact_player_season AS f
    JOIN dim_season AS s USING (season)
),

-- v1.md §3 step 2: divide by the league per-slot baseline. REL = 1.0 means
-- league-average production per possession-slot in that season's environment.
rel AS (
    SELECT
        *,
        per75_pts / base_pts    AS rel_pts,
        per75_trb / base_trb    AS rel_trb,
        per75_ast / base_ast    AS rel_ast,
        ts / lg_ts_pct          AS rel_ts,
        po_per75_pts / base_pts AS p_rel_pts,
        po_per75_trb / base_trb AS p_rel_trb,
        po_per75_ast / base_ast AS p_rel_ast
    FROM per75
),

-- v1.md §4: SPI = blend-weighted sum of REL ratios (default .50/.25/.25,
-- from config). P_SPI is the identical blend over playoff RELs.
spi_rows AS (
    SELECT
        *,
        getvariable('blend_pts') * rel_pts
            + getvariable('blend_trb') * rel_trb
            + getvariable('blend_ast') * rel_ast AS spi,
        getvariable('blend_pts') * p_rel_pts
            + getvariable('blend_trb') * p_rel_trb
            + getvariable('blend_ast') * p_rel_ast AS p_spi
    FROM rel
),

-- v1.md §5.1: a season qualifies for the peak window when AVAIL >= 0.5
-- (config peak_min_avail). If a player somehow has zero qualifying seasons,
-- ALL his seasons become eligible — the deterministic fallback, flagged so
-- run metadata can surface it (never fires on the real seed: min qualifying
-- count is 11).
flagged AS (
    SELECT
        *,
        avail >= getvariable('peak_min_avail') AS qualifies_peak,
        NOT bool_or(avail >= getvariable('peak_min_avail'))
            OVER (PARTITION BY player_id) AS peak_fallback
    FROM spi_rows
),

-- v1.md §5.1: rank peak-eligible seasons by SPI, ties to the earlier season.
-- Ranked over eligible rows only, so peak_rank is null for non-eligible
-- seasons and rank N always means "the player's Nth-best eligible season".
eligible_ranked AS (
    SELECT
        player_id,
        season,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY spi DESC, season ASC
        ) AS peak_rank
    FROM flagged
    WHERE qualifies_peak OR peak_fallback
)

SELECT
    fl.player_id,
    fl.season,
    fl.poss,
    fl.per75_pts,
    fl.per75_trb,
    fl.per75_ast,
    fl.rel_pts,
    fl.rel_trb,
    fl.rel_ast,
    fl.spi,
    fl.avail,
    fl.ts,
    fl.rel_ts,
    fl.po_poss,
    fl.p_rel_pts,
    fl.p_rel_trb,
    fl.p_rel_ast,
    fl.p_spi,
    fl.qualifies_peak,
    fl.peak_fallback,
    er.peak_rank,
    -- §5.1: the peak window is the top-peak_n eligible seasons; players with
    -- fewer than peak_n eligible seasons use all of them (rank never exceeds
    -- the eligible count, so no CASE is needed for the fewer-than-N rule).
    COALESCE(er.peak_rank <= getvariable('peak_n'), FALSE) AS is_peak_window,
    -- Pool-wide season ranking by era-relative production — the "which
    -- seasons stand tallest across eras" cut the T9 report draws on.
    RANK() OVER (ORDER BY fl.spi DESC) AS spi_rank_pool
FROM flagged AS fl
LEFT JOIN eligible_ranked AS er USING (player_id, season);
