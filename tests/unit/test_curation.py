"""Unit tests for fetch_seed's offline curation helpers.

Inline synthetic fixtures only — no network, no data/raw/ dependency, so these
run inside `make check` on a fresh clone (CLAUDE.md Rule 2).
"""

import pandas as pd
import pytest

from pipeline.fetch_seed import (
    apply_era_nulls,
    collapse_stints,
    map_awards,
    weighted_srs,
)

API_STATS = {
    "GP": 70,
    "MIN": 2800,
    "PTS": 1800,
    "REB": 700,
    "AST": 350,
    "FGM": 700,
    "FGA": 1500,
    "FTM": 400,
    "FTA": 500,
    "FG3M": 0,
    "FG3A": 0,
    "STL": 0,
    "BLK": 0,
    "TOV": 0,
}


def career_row(season_id: str, team: str, team_id: int, **overrides) -> dict:
    row = {"SEASON_ID": season_id, "TEAM_ABBREVIATION": team, "TEAM_ID": team_id}
    row.update(API_STATS)
    row.update(overrides)
    return row


class TestApplyEraNulls:
    def test_api_zeros_before_intro_become_nulls_not_zeros(self):
        # 1965 (end-year): no 3PT line, no steals/blocks/turnovers tracked —
        # the API sends 0, which must not survive as a real value.
        seasons = collapse_stints(
            pd.DataFrame([career_row("1964-65", "PHL", 1610612755)])
        )
        out = apply_era_nulls(seasons)
        for col in ["fg3m", "fg3a", "stl", "blk", "tov"]:
            assert pd.isna(out.loc[0, col]), f"{col} should be null in 1965"

    def test_post_intro_values_survive(self):
        seasons = collapse_stints(
            pd.DataFrame(
                [career_row("1990-91", "CHI", 1610612741, FG3M=29, STL=223, TOV=202)]
            )
        )
        out = apply_era_nulls(seasons)
        assert out.loc[0, "fg3m"] == 29
        assert out.loc[0, "stl"] == 223
        assert out.loc[0, "tov"] == 202

    def test_boundary_season_is_not_nulled(self):
        # 1973-74 (end-year 1974) is the first steals/blocks season.
        seasons = collapse_stints(
            pd.DataFrame([career_row("1973-74", "LAL", 1610612747, STL=100, BLK=50)])
        )
        out = apply_era_nulls(seasons)
        assert out.loc[0, "stl"] == 100
        assert out.loc[0, "blk"] == 50


class TestCollapseStints:
    def test_traded_season_uses_tot_totals_and_joins_stints(self):
        rows = pd.DataFrame(
            [
                career_row("1964-65", "SFW", 100, GP=38, PTS=1480),
                career_row("1964-65", "PHL", 200, GP=35, PTS=1054),
                career_row("1964-65", "TOT", 0, GP=73, PTS=2534),
            ]
        )
        out = collapse_stints(rows)
        assert len(out) == 1
        assert out.loc[0, "season"] == 1965
        assert out.loc[0, "gp"] == 73  # totals from the TOT row
        assert out.loc[0, "pts"] == 2534
        assert out.loc[0, "team_abbr"] == "SFW/PHL"  # stint order preserved
        assert out.loc[0, "stints"] == [(100, 38), (200, 35)]

    def test_multi_team_season_without_tot_row_fails_loudly(self):
        rows = pd.DataFrame(
            [
                career_row("1964-65", "SFW", 100, GP=38),
                career_row("1964-65", "PHL", 200, GP=35),
            ]
        )
        with pytest.raises(ValueError, match="lacks a TOT row"):
            collapse_stints(rows)

    def test_season_id_converts_to_end_year(self):
        out = collapse_stints(pd.DataFrame([career_row("1999-00", "SAS", 300)]))
        assert out.loc[0, "season"] == 2000


class TestMapAwards:
    def award_row(self, description: str, season: str, team_number=None) -> dict:
        return {
            "DESCRIPTION": description,
            "SEASON": season,
            "ALL_NBA_TEAM_NUMBER": team_number,
        }

    def test_maps_the_six_awards_and_attributes_seasons(self):
        out = map_awards(
            pd.DataFrame(
                [
                    self.award_row("NBA Most Valuable Player", "1961-62"),
                    self.award_row("NBA Champion", "1968-69"),
                    self.award_row("All-NBA", "1958-59", "1"),
                ]
            )
        )
        assert list(out["award"]) == ["mvp", "ring", "all_nba"]
        assert list(out["season"]) == [1962, 1969, 1959]
        assert out["all_nba_team"].tolist() == [pd.NA, pd.NA, 1]

    def test_lookalike_and_junk_awards_are_excluded(self):
        out = map_awards(
            pd.DataFrame(
                [
                    self.award_row(
                        "NBA Sporting News Most Valuable Player of the Year", "1961-62"
                    ),
                    self.award_row("NBA Player of the Week", "1990-91"),
                    self.award_row("Hall of Fame Inductee", "2009-10"),
                    self.award_row("Olympic Gold Medal", "1983-84"),
                ]
            )
        )
        assert out.empty

    def test_all_nba_without_team_number_fails_loudly(self):
        with pytest.raises(ValueError, match="All-NBA"):
            map_awards(pd.DataFrame([self.award_row("All-NBA", "1958-59", None)]))


def test_weighted_srs_weights_stints_by_games_played():
    srs = pd.Series({100: 2.0, 200: -1.0})
    # 30 games at +2.0 and 10 games at -1.0 -> (60 - 10) / 40 = 1.25
    assert weighted_srs([(100, 30), (200, 10)], srs) == pytest.approx(1.25)


# T5 backlog (queued from the Codex review of T3) — build alongside the Pandera
# contracts in pipeline/contracts.py:
# - every accolades (player_id, season) has a matching player_seasons row (offline
#   twin of the curate() orphan guard, run against the committed seed)
# - ws is non-null AND verified=true for every player-season before scoring can run
# - era-boundary null checks (pre-intro stats null; intro season and later not null)
# - an unbalanced-schedule SRS fixture with a hand-solved expected answer
