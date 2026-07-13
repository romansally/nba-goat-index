-- 06_validation_checks.sql — post-transform structural checks (T6)
--
-- Every SELECT below is a named check that must return ZERO rows; the union
-- is the transform layer's answer to pipeline/contracts.py — contracts guard
-- the input data shape, these guard what the SQL derived from it.
-- pipeline/transform.py raises on any returned row (fail loudly, v1.md §9).
--
-- Complexity budget note (CLAUDE.md Rule 6): this file exceeds 250 lines
-- with documented justification — like pipeline/contracts.py, it is the
-- single declarative registry of every transform-layer check, and its length
-- is named checks (one SELECT each, UNION ALL'd into the one result the
-- runner consumes), not logic. Splitting it would scatter the inventory that
-- `make transform`, the tests, and reviewers treat as a unit.
--
-- Checks are input-relative (counts reconcile against the staged inputs, not
-- hardcoded totals) so the same file validates the real seed AND the
-- synthetic fixture pool. Dataset-specific expectations (which players,
-- which values) live in tests/unit/test_transforms.py instead.
-- Not re-checked here: NOT NULL columns and primary keys — the 01 DDL
-- constraints already make violations fail the INSERT itself.

-- Mart row counts reconcile with their inputs (PRD T6 acceptance criterion 2).
SELECT
    'row_count_dim_player' AS check_name,
    concat('mart=', mart_n, ' input=', input_n) AS detail
FROM (
    SELECT
        (SELECT count(*) FROM dim_player) AS mart_n,
        (SELECT count(*) FROM players_clean) AS input_n
)
WHERE mart_n <> input_n

UNION ALL
SELECT
    'row_count_dim_season',
    concat('mart=', mart_n, ' input=', input_n)
FROM (
    SELECT
        (SELECT count(*) FROM dim_season) AS mart_n,
        (SELECT count(*) FROM league_seasons_clean) AS input_n
)
WHERE mart_n <> input_n

UNION ALL
SELECT
    'row_count_fact_player_season',
    concat('mart=', mart_n, ' input=', input_n)
FROM (
    SELECT
        (SELECT count(*) FROM fact_player_season) AS mart_n,
        (SELECT count(*) FROM player_seasons_clean) AS input_n
)
WHERE mart_n <> input_n

UNION ALL
SELECT
    'row_count_fact_accolade',
    concat('mart=', mart_n, ' input=', input_n)
FROM (
    SELECT
        (SELECT count(*) FROM fact_accolade) AS mart_n,
        (SELECT count(*) FROM accolades_clean) AS input_n
)
WHERE mart_n <> input_n

UNION ALL
SELECT
    'row_count_metrics',
    concat('mart=', mart_n, ' input=', input_n)
FROM (
    SELECT
        (SELECT count(*) FROM mart_player_season_metrics) AS mart_n,
        (SELECT count(*) FROM fact_player_season) AS input_n
)
WHERE mart_n <> input_n

UNION ALL
SELECT
    'row_count_component_inputs',
    concat('mart=', mart_n, ' players=', input_n)
FROM (
    SELECT
        (SELECT count(*) FROM mart_player_component_inputs) AS mart_n,
        (SELECT count(*) FROM dim_player) AS input_n
)
WHERE mart_n <> input_n

UNION ALL
SELECT
    'row_count_award_rates',
    concat('mart=', mart_n, ' players*awards=', input_n)
FROM (
    SELECT
        (SELECT count(*) FROM mart_player_award_rates) AS mart_n,
        (SELECT count(*) FROM dim_player) * (SELECT count(*) FROM param_award_intro) AS input_n
)
WHERE mart_n <> input_n

-- v1.md §3: the True Shooting denominator must be positive on every row
-- (contracts floor fga/fta at 0, which alone would allow a 0/0 season).
UNION ALL
SELECT
    'ts_denominator_positive',
    concat('player ', player_id, ' season ', season)
FROM fact_player_season
WHERE fga + 0.44 * fta <= 0

-- v1.md §9: the playoff mirror is null exactly when the postseason was
-- missed — both directions, matching the seed's all-or-nothing po_* rule.
UNION ALL
SELECT
    'p_spi_null_iff_missed_postseason',
    concat('player ', m.player_id, ' season ', m.season)
FROM mart_player_season_metrics AS m
JOIN fact_player_season AS f USING (player_id, season)
WHERE (m.p_spi IS NULL) <> (f.po_gp IS NULL)
    OR (m.po_poss IS NULL) <> (f.po_gp IS NULL)

-- §10 invariant 4 at the transform layer: NOT NULL constraints cannot catch
-- NaN/inf doubles (a silent 0/0 would sail through), so probe every derived
-- regular-season metric explicitly.
UNION ALL
SELECT
    'metrics_no_nan_or_inf',
    concat('player ', player_id, ' season ', season)
FROM mart_player_season_metrics
WHERE isnan(poss + per75_pts + per75_trb + per75_ast
            + rel_pts + rel_trb + rel_ast + spi + avail + ts + rel_ts)
    OR isinf(poss + per75_pts + per75_trb + per75_ast
             + rel_pts + rel_trb + rel_ast + spi + avail + ts + rel_ts)
    OR (p_spi IS NOT NULL AND (isnan(p_spi) OR isinf(p_spi)))

-- v1.md §4: AVAIL is a share of the schedule, capped at 1; SPI is a
-- non-negative ratio (§5.2 relies on this: decline years still add).
UNION ALL
SELECT
    'avail_in_bounds',
    concat('player ', player_id, ' season ', season, ' avail ', avail)
FROM mart_player_season_metrics
WHERE avail <= 0 OR avail > 1

UNION ALL
SELECT
    'spi_non_negative',
    concat('player ', player_id, ' season ', season, ' spi ', spi)
FROM mart_player_season_metrics
WHERE spi < 0 OR p_spi < 0

-- v1.md §5.1: peak_rank exists exactly for peak-eligible seasons, and the
-- window holds LEAST(peak_n, eligible count) seasons — never zero (the
-- fallback guarantees at least one eligible season per player).
UNION ALL
SELECT
    'peak_rank_null_iff_ineligible',
    concat('player ', player_id, ' season ', season)
FROM mart_player_season_metrics
WHERE (peak_rank IS NULL) <> NOT (qualifies_peak OR peak_fallback)

UNION ALL
SELECT
    'peak_window_size',
    concat('player ', player_id, ' window=', window_n, ' eligible=', eligible_n)
FROM (
    SELECT
        player_id,
        count(*) FILTER (WHERE is_peak_window) AS window_n,
        count(*) FILTER (WHERE qualifies_peak OR peak_fallback) AS eligible_n
    FROM mart_player_season_metrics
    GROUP BY player_id
)
WHERE window_n <> LEAST(getvariable('peak_n'), eligible_n) OR window_n < 1

-- Component inputs: NaN/inf probe (NOT NULL is DDL-enforced).
UNION ALL
SELECT
    'component_inputs_no_nan_or_inf',
    concat('player ', player_id)
FROM mart_player_component_inputs
WHERE isnan(peak_raw + longevity_raw + ws48 + srs_w
            + playoff_raw + rel_ts_career + spi_career)
    OR isinf(peak_raw + longevity_raw + ws48 + srs_w
             + playoff_raw + rel_ts_career + spi_career)

-- v1.md §5.5: rates live in [0, 1] (one selection per award per season, and
-- All-NBA team points max out at 1.0), and rate is null exactly when the
-- award never existed during the player's (in-scope) career (§12.7).
UNION ALL
SELECT
    'award_rate_in_bounds',
    concat('player ', player_id, ' award ', award, ' rate ', rate)
FROM mart_player_award_rates
WHERE rate < 0 OR rate > 1

UNION ALL
SELECT
    'award_rate_null_iff_zero_eligible',
    concat('player ', player_id, ' award ', award)
FROM mart_player_award_rates
WHERE (rate IS NULL) <> (eligible_seasons = 0)

-- Eligibility bounds the denominator, never inflates the numerator (§5.5):
-- wins can only exist inside eligible seasons, which are themselves bounded
-- by the player's in-scope season count.
UNION ALL
SELECT
    'award_wins_require_eligibility',
    concat('player ', player_id, ' award ', award)
FROM mart_player_award_rates
WHERE weighted_wins > 0 AND eligible_seasons = 0

UNION ALL
SELECT
    'award_eligibility_bounded_by_seasons',
    concat('player ', r.player_id, ' award ', r.award)
FROM mart_player_award_rates AS r
JOIN (
    SELECT player_id, count(*) AS n_seasons
    FROM mart_player_season_metrics
    WHERE NOT getvariable('peak_scope') OR is_peak_window
    GROUP BY player_id
) AS s USING (player_id)
WHERE r.eligible_seasons > s.n_seasons

-- Under career scope every accolade row must have been consumed by the award
-- aggregation (every award joins a played season — re-asserted post-join).
-- Under peak scope, selections outside the window are legitimately dropped.
UNION ALL
SELECT
    'accolades_all_consumed',
    concat('player ', a.player_id, ' season ', a.season, ' award ', a.award)
FROM fact_accolade AS a
LEFT JOIN mart_player_season_metrics AS m USING (player_id, season)
WHERE NOT getvariable('peak_scope') AND m.player_id IS NULL

-- Award-key parity, data side (Codex review, 2026-07-12): the rate mart is
-- driven by param_award_intro, so an award present in the facts but missing
-- from the injected config would silently produce zero rate rows while every
-- count check above stayed green. Config-side parity (intro seasons ==
-- contract registry == weight keys) is enforced in pipeline/transform.py.
UNION ALL
SELECT
    'award_key_known_to_config',
    concat('award ', award)
FROM (SELECT DISTINCT award FROM fact_accolade)
WHERE award NOT IN (SELECT award FROM param_award_intro)

-- Every All-NBA selection must carry a team with a configured point value,
-- or its weighted win would silently drop out of the SUM.
UNION ALL
SELECT
    'all_nba_points_mapped',
    concat('player ', player_id, ' season ', season, ' team ', all_nba_team)
FROM fact_accolade
WHERE award = 'all_nba'
    AND (all_nba_team IS NULL
        OR all_nba_team NOT IN (SELECT all_nba_team FROM param_all_nba_points))

-- The award-level reconciliation guarantee (Codex re-review 2026-07-12,
-- expected side completed in the fourth pass 2026-07-13): independently
-- re-derive the COMPLETE scoped player x configured-award grid — not just
-- the groups that have award facts — with both the expected eligibility
-- denominator and the expected weighted wins, then FULL JOIN to the mart and
-- reconcile keys in BOTH directions, eligible_seasons, and weighted_wins.
-- A facts-only expected side missed two shapes: a ghost zero-win row under
-- an unknown key (row count preserved, nothing to compare against) and a
-- corrupted eligible_seasons with a self-consistent rate — the latter
-- directly poisons the rate scoring consumes. A positivity-only check had
-- earlier missed partial win loss (22 silently becoming 21).
-- (An award type absent from param_award_intro is invisible to this grid —
-- that hole is what award_key_known_to_config closes.)
UNION ALL
SELECT
    'accolades_reach_rate_mart',
    concat(
        'player ', coalesce(e.player_id, r.player_id),
        ' award ', coalesce(e.award, r.award),
        ' expected wins ', coalesce(e.expected_wins, -1.0),
        ' elig ', coalesce(e.expected_eligible, -1),
        ' mart wins ', coalesce(r.weighted_wins, -1.0),
        ' elig ', coalesce(r.eligible_seasons, -1)
    )
FROM (
    WITH scoped AS (
        SELECT
            m.player_id,
            m.season,
            s.asg_held
        FROM mart_player_season_metrics AS m
        JOIN dim_season AS s USING (season)
        WHERE NOT getvariable('peak_scope') OR m.is_peak_window
    )

    SELECT
        sc.player_id,
        ai.award,
        COUNT(*) FILTER (
            WHERE sc.season >= ai.intro_season
            AND CASE WHEN ai.award = 'all_star' THEN sc.asg_held ELSE TRUE END
        ) AS expected_eligible,
        COALESCE(SUM(
            CASE
                WHEN a.player_id IS NOT NULL
                    AND sc.season >= ai.intro_season
                    AND CASE
                        WHEN ai.award = 'all_star' THEN sc.asg_held ELSE TRUE
                    END
                THEN CASE
                    WHEN ai.award = 'all_nba' THEN pts.points ELSE 1.0
                END
            END
        ), 0.0) AS expected_wins
    FROM scoped AS sc
    CROSS JOIN param_award_intro AS ai
    LEFT JOIN fact_accolade AS a
        ON a.player_id = sc.player_id
        AND a.season = sc.season
        AND a.award = ai.award
    LEFT JOIN param_all_nba_points AS pts
        ON ai.award = 'all_nba' AND a.all_nba_team = pts.all_nba_team
    GROUP BY sc.player_id, ai.award
) AS e
FULL JOIN mart_player_award_rates AS r
    ON e.player_id = r.player_id AND e.award = r.award
WHERE e.player_id IS NULL -- mart row outside the expected grid (ghost key)
    OR r.player_id IS NULL -- expected grid key missing from the mart
    OR r.eligible_seasons <> e.expected_eligible
    OR abs(r.weighted_wins - e.expected_wins) > 1e-9

-- rate is the value scoring actually consumes, so reconcile it against its
-- own definition too — a corrupt rate with intact weighted_wins would
-- otherwise pass every check above (same shape as the wins gap).
UNION ALL
SELECT
    'award_rate_consistent',
    concat(
        'player ', player_id, ' award ', award, ' rate ', rate,
        ' expected ', weighted_wins / eligible_seasons
    )
FROM mart_player_award_rates
WHERE eligible_seasons > 0
    AND abs(rate - weighted_wins / eligible_seasons) > 1e-9;
