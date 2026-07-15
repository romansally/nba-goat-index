"""Unit tests for the T4 cleaning and profiling steps.

Inline synthetic fixtures only — no network, no data/processed/ dependency.
The two committed-seed tests read data/seed/, which is committed and offline
(CLAUDE.md Rule 2). Real player names never appear in synthetic fixtures.
"""

import pandas as pd
import pytest

from pipeline.clean import (
    TABLE_KEYS,
    apply_schema,
    check_name_consistency,
    check_null_patterns,
    check_referential,
    check_unique_key,
    clean,
    load_seed,
)
from pipeline.profile import render

BASE_SEASON = {
    "player_id": 1,
    "season": 1991,
    "team_abbr": "AAA",
    "gp": 70,
    "mp": 2500,
    "pts": 1800,
    "trb": 600,
    "ast": 400,
    "fgm": 700,
    "fga": 1400,
    "ftm": 300,
    "fta": 400,
    "fg3m": 30,
    "fg3a": 100,
    "stl": 120,
    "blk": 60,
    "tov": 180,
    "po_gp": 10,
    "po_mp": 380,
    "po_pts": 250,
    "po_trb": 90,
    "po_ast": 60,
    "team_srs": 5.5,
    "ws": 12.3,
}

PRE_ERA_NULLS = {c: None for c in ["fg3m", "fg3a", "stl", "blk", "tov"]}


def seasons_frame(*overrides: dict) -> pd.DataFrame:
    rows = [{**BASE_SEASON, **o} for o in overrides]
    return apply_schema("player_seasons", pd.DataFrame(rows))


def players_frame(*rows: dict) -> pd.DataFrame:
    base = {
        "player_id": 1,
        "player_name": "PlayerA",
        "first_season": 1991,
        "last_season": 1991,
        "is_active": False,
    }
    return apply_schema("players", pd.DataFrame([{**base, **r} for r in rows]))


class TestSchemaAndNulls:
    def test_era_nulls_and_int64_survive_parquet_round_trip(self, tmp_path):
        # A pre-3PT-era season: era-gated stats and a missed postseason are
        # nulls, and must come back from parquet as Int64 nulls, not floats
        # or zeros (v1.md §9).
        frame = seasons_frame(
            {
                "season": 1965,
                **PRE_ERA_NULLS,
                **{c: None for c in ["po_gp", "po_mp", "po_pts", "po_trb", "po_ast"]},
            }
        )
        path = tmp_path / "roundtrip.parquet"
        frame.to_parquet(path, index=False)
        back = pd.read_parquet(path)
        for col in ["fg3m", "stl", "tov", "po_gp"]:
            assert back[col].dtype == "Int64"
            assert pd.isna(back.loc[0, col])
        assert back.loc[0, "gp"] == 70

    def test_unexpected_column_fails_loudly(self):
        with pytest.raises(ValueError, match="unexpected"):
            apply_schema("players", pd.DataFrame([{"player_id": 1, "bogus": 1}]))

    def test_pre_intro_value_fails(self):
        # Steals recorded in 1965 (tracking began 1973-74): the exact
        # zero-leak the PRD's Key Question warns about.
        bad = seasons_frame({"season": 1965, **PRE_ERA_NULLS, "stl": 100})
        with pytest.raises(ValueError, match="stl.*before"):
            check_null_patterns(bad)

    def test_post_intro_null_fails(self):
        with pytest.raises(ValueError, match="tov.*after"):
            check_null_patterns(seasons_frame({"tov": None}))

    def test_partial_playoff_nulls_fail(self):
        with pytest.raises(ValueError, match="partially-null playoff"):
            check_null_patterns(seasons_frame({"po_pts": None}))

    def test_valid_frame_passes(self):
        check_null_patterns(seasons_frame({}, {"season": 1965, **PRE_ERA_NULLS}))


class TestKeysAndReferences:
    def make_frames(self) -> dict[str, pd.DataFrame]:
        league = {
            "season": 1991,
            "lg_pts_pg": 106.3,
            "lg_trb_pg": 43.5,
            "lg_ast_pg": 25.7,
            "lg_ts_pct": 0.53,
            "season_games": 82,
            "asg_held": True,
            "pace": 97.8,
            "pace_estimated": False,
        }
        accolade = {
            "player_id": 1,
            "season": 1991,
            "award": "mvp",
            "all_nba_team": None,
        }
        return {
            "players": players_frame({}),
            "player_seasons": seasons_frame({}),
            "accolades": apply_schema("accolades", pd.DataFrame([accolade])),
            "league_seasons": apply_schema("league_seasons", pd.DataFrame([league])),
        }

    def test_consistent_frames_pass(self):
        check_referential(self.make_frames())

    def test_duplicate_player_season_key_fails(self):
        with pytest.raises(ValueError, match="duplicate"):
            check_unique_key("player_seasons", seasons_frame({}, {"pts": 900}))

    def test_accolade_without_played_season_fails(self):
        frames = self.make_frames()
        frames["accolades"].loc[0, "season"] = 1992  # award year never played
        with pytest.raises(ValueError, match="without a played player-season"):
            check_referential(frames)

    def test_season_missing_from_league_table_fails(self):
        frames = self.make_frames()
        frames["league_seasons"].loc[0, "season"] = 1990
        with pytest.raises(ValueError, match="missing from league_seasons"):
            check_referential(frames)

    def test_all_nba_first_and_second_same_season_fails(self):
        # (player_id, season, award) is the accolade grain: a differing
        # all_nba_team value must not disambiguate an impossible double win.
        rows = pd.DataFrame(
            [
                {"player_id": 1, "season": 1991, "award": "all_nba", "all_nba_team": 1},
                {"player_id": 1, "season": 1991, "award": "all_nba", "all_nba_team": 2},
            ]
        )
        with pytest.raises(ValueError, match="duplicate"):
            check_unique_key("accolades", apply_schema("accolades", rows))


class TestNameConsistency:
    def hand_ws(self, name: str) -> pd.DataFrame:
        row = {
            "player_name": name,
            "player_id": 1,
            "season": 1991,
            "ws": 12.3,
            "verified": True,
        }
        return pd.DataFrame([row]).astype({"player_name": "string"})

    def test_matching_names_pass(self):
        check_name_consistency(players_frame({}), self.hand_ws("PlayerA"))

    def test_name_drift_fails(self):
        # The real T4 catch was a diacritic ("Jokic" vs "Jokić") — any
        # byte-level drift must fail, not fuzzy-match.
        with pytest.raises(ValueError, match="disagree"):
            check_name_consistency(players_frame({}), self.hand_ws("Playera"))

    def test_unknown_player_id_fails(self):
        hand = self.hand_ws("PlayerA")
        hand.loc[0, "player_id"] = 99
        with pytest.raises(ValueError, match="not in players"):
            check_name_consistency(players_frame({}), hand)


class TestCommittedSeed:
    def test_clean_passes_and_preserves_row_counts(self):
        seed_counts = {name: len(frame) for name, frame in load_seed().items()}
        frames = clean()
        assert {name: len(frame) for name, frame in frames.items()} == seed_counts
        assert frames["player_seasons"]["po_gp"].dtype == "Int64"

    def test_profile_render_is_deterministic(self):
        seed_counts = {name: len(frame) for name, frame in load_seed().items()}
        frames = clean()
        assert render(seed_counts, frames) == render(seed_counts, frames)

    def test_clean_output_key_sorted(self):
        """clean()'s key-sort is a DETERMINISM dependency, not cosmetics
        (see the note in clean.py): DuckDB float aggregation follows input
        row order, so v1.md §10.1 byte-stable scoring relies on this exact
        ordering. If this test fails, determinism downstream is at risk —
        see the shuffled-input test in test_scoring_invariants.py."""
        for name, frame in clean().items():
            expected = frame.sort_values(TABLE_KEYS[name]).reset_index(drop=True)
            pd.testing.assert_frame_equal(frame, expected)
