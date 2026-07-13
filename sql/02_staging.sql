-- 02_staging.sql — stage the cleaned tables into the star schema (T6)
--
-- Inputs are the four cleaned tables registered as views by
-- pipeline/transform.py (players_clean, league_seasons_clean,
-- player_seasons_clean, accolades_clean) — the output of pipeline/clean.py,
-- already contract-validated. Staging adds no business logic beyond the one
-- derivation that belongs to the season dimension: the v1.md §3 step 2
-- per-slot baselines. Everything else is a straight, explicit load.

INSERT INTO dim_player BY NAME
SELECT player_id, player_name, first_season, last_season, is_active
FROM players_clean;

-- v1.md §3 step 2: the league per-75 baseline per player-slot (5 on the
-- floor) for stat s in a season: baseline_s = (lg_team_per_game_s / pace * 75) / 5.
-- REL ratios in 03 divide a player's per-75 rate by these.
INSERT INTO dim_season BY NAME
WITH baselines AS (
    SELECT
        season,
        (lg_pts_pg / pace * 75) / 5 AS base_pts,
        (lg_trb_pg / pace * 75) / 5 AS base_trb,
        (lg_ast_pg / pace * 75) / 5 AS base_ast
    FROM league_seasons_clean
)
SELECT
    ls.season,
    ls.lg_pts_pg,
    ls.lg_trb_pg,
    ls.lg_ast_pg,
    ls.lg_ts_pct,
    ls.season_games,
    ls.asg_held,
    ls.pace,
    ls.pace_estimated,
    b.base_pts,
    b.base_trb,
    b.base_ast
FROM league_seasons_clean AS ls
JOIN baselines AS b USING (season);

INSERT INTO fact_player_season BY NAME
SELECT
    player_id, season, team_abbr,
    gp, mp, pts, trb, ast, fgm, fga, ftm, fta,
    fg3m, fg3a, stl, blk, tov,
    po_gp, po_mp, po_pts, po_trb, po_ast,
    team_srs, ws
FROM player_seasons_clean;

INSERT INTO fact_accolade BY NAME
SELECT player_id, season, award, all_nba_team
FROM accolades_clean;
