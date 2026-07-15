-- 05_final_goat_scores.sql — the scoring engine (T7)
--
-- The one scoring implementation (v1.md §12.9): applies §6 min–max scaling to
-- T6's raw component inputs, blends the two-signal components (§5.3, §5.6),
-- renormalizes accolade weights over each player's eligible awards (§5.5,
-- §12.7), combines the six components with the weighting layer, and ranks.
-- pipeline/score.py injects every weight as a DuckDB variable — the SQL is
-- identical for career scope, peak scope, and Custom Mode; only the injected
-- weight vector and the upstream scope-restricted marts differ.
--
-- Provenance columns (method_version, git_sha, scope) are stamped by
-- pipeline/score.py after this runs: they are runtime provenance, not
-- scoring math.

-- v1.md §6: MM(x) = 100 × (x − min) / (max − min) across the pool.
-- Degenerate case (every value equal): all players score 50.0.
-- The ratio is computed FIRST: (hi − lo) / (hi − lo) is exactly 1.0 in IEEE
-- float, so the pool max lands on exactly 100.0 and the min on exactly 0.0
-- (multiplying by 100 before dividing rounds twice and can overshoot 100).
CREATE MACRO minmax(x, lo, hi) AS
    CASE WHEN hi = lo THEN 50.0 ELSE 100.0 * ((x - lo) / (hi - lo)) END;

CREATE TABLE mart_final_scores AS
WITH
-- §6 scaling of each single-value raw input (§5.1, §5.2, §5.4) and of the
-- sub-metrics inside the blended components (§5.3, §5.6). The window
-- MIN/MAX OVER () is the pool min/max — scores are pool-relative (§12.2).
scaled AS (
    SELECT
        player_id,
        player_name,
        minmax(peak_raw, MIN(peak_raw) OVER (), MAX(peak_raw) OVER ())
            AS comp_peak,
        minmax(longevity_raw,
            MIN(longevity_raw) OVER (), MAX(longevity_raw) OVER ())
            AS comp_longevity,
        minmax(playoff_raw,
            MIN(playoff_raw) OVER (), MAX(playoff_raw) OVER ())
            AS comp_playoff,
        minmax(ws48, MIN(ws48) OVER (), MAX(ws48) OVER ())
            AS mm_ws48,
        minmax(srs_w, MIN(srs_w) OVER (), MAX(srs_w) OVER ())
            AS mm_srs_w,
        minmax(rel_ts_career,
            MIN(rel_ts_career) OVER (), MAX(rel_ts_career) OVER ())
            AS mm_rel_ts,
        minmax(spi_career,
            MIN(spi_career) OVER (), MAX(spi_career) OVER ())
            AS mm_spi_career
    FROM mart_player_component_inputs
),

-- §5.5: each award's rate is min–maxed across the players with ≥ 1 eligible
-- season for it. A null rate means zero eligible seasons — the award is
-- excluded from that player's mix here (WHERE) and from the anchors
-- (PARTITION BY award sees only eligible players). Null is never coerced
-- to a rate of zero (§12.7 — the rejected "naive treatment").
award_scaled AS (
    SELECT
        player_id,
        award,
        minmax(
            rate,
            MIN(rate) OVER (PARTITION BY award),
            MAX(rate) OVER (PARTITION BY award)
        ) AS mm_rate
    FROM mart_player_award_rates
    WHERE rate IS NOT NULL
),

-- §5.5: Accolades = Σ_eligible(w_a × MM(rate_a)) / Σ_eligible(w_a) — the
-- award weights (from config via param_accolade_weights) renormalized over
-- the awards the player was actually eligible for. For a player eligible
-- for every award the divisor is exactly 1.0; for Bill Russell's missing
-- DPOY it is 0.925 (the §12.7 worked example).
accolades AS (
    SELECT
        s.player_id,
        SUM(w.weight * s.mm_rate) / SUM(w.weight) AS comp_accolades
    FROM award_scaled AS s
    JOIN param_accolade_weights AS w USING (award)
    GROUP BY s.player_id
),

-- §5.3 / §5.6: the blended components are convex combinations of MM-scaled
-- sub-scores (sub-weights injected from config, each pair summing to 1.0) —
-- in [0,100] by construction, not re-stretched (§6). §6 final score: the
-- weighted sum of the six components. Under peak scope pipeline/score.py
-- injects w_longevity = 0.0 and the others renormalized (§7) — same SQL.
combined AS (
    SELECT
        s.player_id,
        s.player_name,
        s.comp_peak,
        getvariable('wib_ws48') * s.mm_ws48
            + getvariable('wib_team_srs') * s.mm_srs_w   AS comp_winning_impact,
        s.comp_playoff,
        a.comp_accolades,
        getvariable('eff_ts_rel') * s.mm_rel_ts
            + getvariable('eff_spi_career') * s.mm_spi_career
                                                         AS comp_efficiency,
        s.comp_longevity
    FROM scaled AS s
    LEFT JOIN accolades AS a USING (player_id)
)

SELECT
    -- §6 ranking: final score desc, ties broken by Peak component desc, then
    -- player_id asc — a total order, so ranks are unique positions 1..N.
    ROW_NUMBER() OVER (
        ORDER BY goat_score DESC, comp_peak DESC, player_id ASC
    ) AS rank,
    player_id,
    player_name,
    goat_score,
    comp_peak,
    comp_winning_impact,
    comp_playoff,
    comp_accolades,
    comp_efficiency,
    comp_longevity
FROM (
    SELECT
        *,
        getvariable('w_peak') * comp_peak
            + getvariable('w_winning_impact') * comp_winning_impact
            + getvariable('w_playoff') * comp_playoff
            + getvariable('w_accolades') * comp_accolades
            + getvariable('w_efficiency') * comp_efficiency
            + getvariable('w_longevity') * comp_longevity AS goat_score
    FROM combined
)
ORDER BY rank;
