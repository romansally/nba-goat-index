-- 01_create_schema.sql — star schema DDL (Tier-1 task T6)
--
-- Authoritative table definitions for the transform layer. Grain and keys are
-- documented per table in docs/data_model.md; COMMENT ON records them in the
-- database itself. Populated by 02_staging.sql (dims + facts) and
-- 03/04 (marts); validated by 06_validation_checks.sql.
--
-- Naming: dim_/fact_ tables are the star schema (ids only — joins go through
-- dims); mart_ tables are analysis-ready outputs and may denormalize
-- player_name for legibility.

-- Grain: one row per seeded player. PK player_id (nba_api person id).
CREATE TABLE dim_player (
    player_id    BIGINT PRIMARY KEY,
    player_name  VARCHAR NOT NULL,
    first_season BIGINT NOT NULL,
    last_season  BIGINT NOT NULL,
    is_active    BOOLEAN NOT NULL
);
COMMENT ON TABLE dim_player IS 'Grain: player. One row per seeded all-time great.';

-- Grain: one row per league season (end-year convention, v1.md §2). PK season.
-- Extends the league reference table with the per-slot baselines of
-- v1.md §3 step 2: baseline_s = (league_team_per_game_s / pace * 75) / 5.
CREATE TABLE dim_season (
    season         BIGINT PRIMARY KEY,
    lg_pts_pg      DOUBLE NOT NULL,
    lg_trb_pg      DOUBLE NOT NULL,
    lg_ast_pg      DOUBLE NOT NULL,
    lg_ts_pct      DOUBLE NOT NULL,
    season_games   BIGINT NOT NULL,
    asg_held       BOOLEAN NOT NULL,
    pace           DOUBLE NOT NULL,
    pace_estimated BOOLEAN NOT NULL,
    base_pts       DOUBLE NOT NULL,
    base_trb       DOUBLE NOT NULL,
    base_ast       DOUBLE NOT NULL
);
COMMENT ON TABLE dim_season IS
    'Grain: season (end-year). League environment + v1.md §3 per-slot baselines.';

-- Grain: one row per player per season (regular season + playoff splits).
-- PK (player_id, season); FKs to dim_player and dim_season.
-- Null semantics preserved from the seed (v1.md §9): era-gated stats
-- (fg3m/fg3a 1980, stl/blk 1974, tov 1978) are null before their intro;
-- the five po_* columns are null together iff the player missed that
-- postseason. Nulls are never zeros.
CREATE TABLE fact_player_season (
    player_id BIGINT NOT NULL REFERENCES dim_player (player_id),
    season    BIGINT NOT NULL REFERENCES dim_season (season),
    team_abbr VARCHAR NOT NULL,
    gp        BIGINT NOT NULL,
    mp        BIGINT NOT NULL,
    pts       BIGINT NOT NULL,
    trb       BIGINT NOT NULL,
    ast       BIGINT NOT NULL,
    fgm       BIGINT NOT NULL,
    fga       BIGINT NOT NULL,
    ftm       BIGINT NOT NULL,
    fta       BIGINT NOT NULL,
    fg3m      BIGINT,
    fg3a      BIGINT,
    stl       BIGINT,
    blk       BIGINT,
    tov       BIGINT,
    po_gp     BIGINT,
    po_mp     BIGINT,
    po_pts    BIGINT,
    po_trb    BIGINT,
    po_ast    BIGINT,
    team_srs  DOUBLE NOT NULL,
    ws        DOUBLE NOT NULL,
    PRIMARY KEY (player_id, season)
);
COMMENT ON TABLE fact_player_season IS
    'Grain: player-season. RS + playoff totals; era/missed-postseason nulls preserved.';

-- Grain: one row per player per season per award won (v1.md §5.5 —
-- season-attributed). PK (player_id, season, award): a player can win each
-- award at most once per season. all_nba_team is 1/2/3 for all_nba, else null.
CREATE TABLE fact_accolade (
    player_id    BIGINT NOT NULL REFERENCES dim_player (player_id),
    season       BIGINT NOT NULL REFERENCES dim_season (season),
    award        VARCHAR NOT NULL,
    all_nba_team BIGINT,
    PRIMARY KEY (player_id, season, award)
);
COMMENT ON TABLE fact_accolade IS
    'Grain: player-season-award. Season-attributed award selections.';

-- Grain: one row per player per season — the v1.md §3–§4 derived metrics.
-- Regular-season metrics are NOT NULL for every row; the p_* playoff mirror
-- is null exactly when the postseason was missed. peak_rank is null for
-- seasons outside the peak-eligible set (v1.md §5.1).
CREATE TABLE mart_player_season_metrics (
    player_id      BIGINT NOT NULL,
    season         BIGINT NOT NULL,
    poss           DOUBLE NOT NULL,
    per75_pts      DOUBLE NOT NULL,
    per75_trb      DOUBLE NOT NULL,
    per75_ast      DOUBLE NOT NULL,
    rel_pts        DOUBLE NOT NULL,
    rel_trb        DOUBLE NOT NULL,
    rel_ast        DOUBLE NOT NULL,
    spi            DOUBLE NOT NULL,
    avail          DOUBLE NOT NULL,
    ts             DOUBLE NOT NULL,
    rel_ts         DOUBLE NOT NULL,
    po_poss        DOUBLE,
    p_rel_pts      DOUBLE,
    p_rel_trb      DOUBLE,
    p_rel_ast      DOUBLE,
    p_spi          DOUBLE,
    qualifies_peak BOOLEAN NOT NULL,
    peak_fallback  BOOLEAN NOT NULL,
    peak_rank      BIGINT,
    is_peak_window BOOLEAN NOT NULL,
    spi_rank_pool  BIGINT NOT NULL,
    PRIMARY KEY (player_id, season)
);
COMMENT ON TABLE mart_player_season_metrics IS
    'Grain: player-season. Era-relative metrics (v1.md §3–§4) + peak window flags (§5.1).';

-- Grain: one row per player — the RAW (pre-min-max, pre-weight) value each
-- v1.md §5 component consumes. T7''s scoring engine applies §6 scaling and
-- the weighting layer; nothing here depends on weights.
CREATE TABLE mart_player_component_inputs (
    player_id          BIGINT PRIMARY KEY,
    player_name        VARCHAR NOT NULL,
    peak_raw           DOUBLE NOT NULL,
    peak_fallback_used BOOLEAN NOT NULL,
    longevity_raw      DOUBLE NOT NULL,
    ws48               DOUBLE NOT NULL,
    srs_w              DOUBLE NOT NULL,
    playoff_raw        DOUBLE NOT NULL,
    rel_ts_career      DOUBLE NOT NULL,
    spi_career         DOUBLE NOT NULL
);
COMMENT ON TABLE mart_player_component_inputs IS
    'Grain: player. Raw component values (v1.md §5.1–§5.6) before §6 scaling.';

-- Grain: one row per player per award type (6 per player). rate is null —
-- meaning "award excluded from this player''s mix" (v1.md §5.5, §12.7) —
-- exactly when eligible_seasons = 0. Null is never coerced to zero.
CREATE TABLE mart_player_award_rates (
    player_id        BIGINT NOT NULL,
    player_name      VARCHAR NOT NULL,
    award            VARCHAR NOT NULL,
    eligible_seasons BIGINT NOT NULL,
    weighted_wins    DOUBLE NOT NULL,
    rate             DOUBLE,
    PRIMARY KEY (player_id, award)
);
COMMENT ON TABLE mart_player_award_rates IS
    'Grain: player-award. Per-eligible-season accolade rates (v1.md §5.5).';
