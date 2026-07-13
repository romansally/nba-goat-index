-- 04_scoring_components.sql — per-player RAW component inputs (T6)
--
-- Produces the raw (pre-min-max, pre-weight) value behind each v1.md §5
-- component, plus the era-aware accolade rates of §5.5. T7's scoring engine
-- (05_final_goat_scores.sql, driven by pipeline/score.py) applies §6 scaling
-- and the weighting layer; no weights appear here.
--
-- Scope (v1.md §7, §12.9 — one engine, many configurations): every
-- aggregation below runs behind the same filter. Career scope (default)
-- admits all seasons; peak scope restricts to the §5.1 peak window, which
-- recomputes playoff sums, award eligibility, and award wins over window
-- seasons only. pipeline/transform.py injects the peak_scope variable.

INSERT INTO mart_player_component_inputs BY NAME
WITH scoped AS (
    SELECT
        m.*,
        f.gp,
        f.mp,
        f.po_gp,
        f.ws,
        f.team_srs
    FROM mart_player_season_metrics AS m
    JOIN fact_player_season AS f USING (player_id, season)
    WHERE NOT getvariable('peak_scope') OR m.is_peak_window
)

SELECT
    s.player_id,
    p.player_name,
    -- §5.1 Peak: mean SPI of the top-peak_n qualifying seasons (the peak
    -- window flagged in 03; fewer than peak_n eligible seasons -> all of them).
    AVG(s.spi) FILTER (WHERE s.is_peak_window)  AS peak_raw,
    -- §5.1 fallback flag: true iff the window fell back to all seasons
    -- because none met the availability qualifier (surfaced in run metadata).
    bool_or(s.peak_fallback)                    AS peak_fallback_used,
    -- §5.2 Longevity: career volume of era-relative production. Missed games
    -- and missed seasons simply add nothing.
    SUM(s.avail * s.spi)                        AS longevity_raw,
    -- §5.3 Winning/Impact, individual half: career Win Shares per 48 minutes.
    SUM(s.ws) / SUM(s.mp) * 48                  AS ws48,
    -- §5.3 Winning/Impact, team half: games-weighted career team SRS
    -- (traded seasons already carry the GP-weighted SRS from the seed).
    SUM(s.gp * s.team_srs) / SUM(s.gp)          AS srs_w,
    -- §5.4 Playoff: production x depth over playoff runs. SUM skips the
    -- null (missed) postseasons; COALESCE covers a player with no playoff
    -- run at all in scope — "missed postseasons contribute 0", never null.
    COALESCE(SUM(s.p_spi * s.po_gp), 0.0)       AS playoff_raw,
    -- §5.6 Efficiency half: minutes-weighted career REL_TS.
    SUM(s.mp * s.rel_ts) / SUM(s.mp)            AS rel_ts_career,
    -- §5.6 Counting half: minutes-weighted career per-possession average —
    -- a distinct lens from Peak (best-N mean) and Longevity (career sum).
    SUM(s.mp * s.spi) / SUM(s.mp)               AS spi_career
FROM scoped AS s
JOIN dim_player AS p USING (player_id)
GROUP BY s.player_id, p.player_name;


-- v1.md §5.5 Accolades: per-award rates over ELIGIBLE seasons only. v1
-- neither punishes players for awards that did not exist (denominator counts
-- only seasons the award existed) nor credits hypothetical wins (eligibility
-- never inflates the numerator). The era logic is the CASE below plus the
-- award intro seasons injected from config (param_award_intro).
INSERT INTO mart_player_award_rates BY NAME
WITH scoped_seasons AS (
    SELECT
        m.player_id,
        m.season,
        s.asg_held
    FROM mart_player_season_metrics AS m
    JOIN dim_season AS s USING (season)
    WHERE NOT getvariable('peak_scope') OR m.is_peak_window
),

-- Eligible seasons per player x award: played (in-scope) seasons in which
-- the award existed. All-Star additionally requires a game to have been held
-- (1999 lockout: no ASG — excluded from the denominator, §5.5).
eligibility AS (
    SELECT
        ss.player_id,
        ai.award,
        COUNT(*) FILTER (
            WHERE ss.season >= ai.intro_season
            AND CASE WHEN ai.award = 'all_star' THEN ss.asg_held ELSE TRUE END
        ) AS eligible_seasons
    FROM scoped_seasons AS ss
    CROSS JOIN param_award_intro AS ai
    GROUP BY ss.player_id, ai.award
),

-- Weighted wins per player x award, counted over in-scope seasons (awards
-- are season-attributed, §5.5/§7). All-NBA selections carry team points
-- (1st/2nd/3rd = 1.0/0.5/0.25, from config via param_all_nba_points); every
-- other award counts 1 per selection.
-- The WHERE applies the IDENTICAL eligibility predicate as the denominator
-- above: contracts already forbid an award before its intro or an All-Star
-- in a no-ASG season, but the numerator must not depend on that upstream
-- guarantee — an ineligible selection sneaking in must not inflate the rate
-- (defense-in-depth; Codex review, 2026-07-12).
wins AS (
    SELECT
        a.player_id,
        a.award,
        SUM(
            CASE WHEN a.award = 'all_nba' THEN pts.points ELSE 1.0 END
        ) AS weighted_wins
    FROM fact_accolade AS a
    JOIN scoped_seasons AS ss USING (player_id, season)
    JOIN param_award_intro AS ai ON a.award = ai.award
    LEFT JOIN param_all_nba_points AS pts
        ON a.award = 'all_nba' AND a.all_nba_team = pts.all_nba_team
    WHERE a.season >= ai.intro_season
        AND CASE WHEN a.award = 'all_star' THEN ss.asg_held ELSE TRUE END
    GROUP BY a.player_id, a.award
)

SELECT
    e.player_id,
    p.player_name,
    e.award,
    e.eligible_seasons,
    COALESCE(w.weighted_wins, 0.0) AS weighted_wins,
    -- rate_a = weighted_wins_a / eligible_seasons_a. Zero eligible seasons
    -- (e.g. DPOY for the four seed players who retired before 1983) yields
    -- NULL — "award excluded from this player's mix" (§12.7). T7 renormalizes
    -- the remaining award weights; the null is never coerced to zero.
    CASE
        WHEN e.eligible_seasons > 0
        THEN COALESCE(w.weighted_wins, 0.0) / e.eligible_seasons
    END AS rate
FROM eligibility AS e
JOIN dim_player AS p USING (player_id)
LEFT JOIN wins AS w USING (player_id, award);
