"""Unit tests for the T5 Pandera contracts (PRD T5 acceptance criterion 3).

Three claims, each tested directly:
  1. The valid synthetic mini-set (the locked hand-worksheet trio, completed
     to full schema) passes every contract, including cross-table rules.
  2. Every designed-bad fixture row fails validation — and fails via the
     specific named check its `violation` label declares, not by accident.
     Some rows trip an incidental second check (documented in
     contracts.iter_invalid_cases); lazy validation reports all failures, so
     asserting the labeled check is present is exact, not approximate.
  3. The committed real seed passes (offline, per CLAUDE.md Rule 2).

Plus regression tests from the T5 Codex review: the fg3a ceiling clears the
real record (Harden 1028, outside the seed pool), a missing column reports a
named failure instead of raising KeyError, and every era-gated column fails
on exactly the intro-season boundary in both directions.
"""

import pandas as pd
import pytest

from pipeline.clean import ERA_INTRO, PO_COLS, clean
from pipeline.contracts import (
    PACE_COMPUTED_FROM,
    ContractViolation,
    iter_invalid_cases,
    load_fixtures,
    validate_all,
)

INVALID_CASES = list(iter_invalid_cases())

# Era-appropriate defaults for an injected season row (see ERA_INTRO).
ERA_DEFAULTS = {"fg3m": 0, "fg3a": 0, "stl": 50, "blk": 20, "tov": 90}


def test_valid_mini_set_passes():
    validate_all(load_fixtures("valid"))


@pytest.mark.parametrize(
    ("label", "frames"),
    [(label, frames) for _, label, frames in INVALID_CASES],
    ids=[case_id for case_id, _, _ in INVALID_CASES],
)
def test_designed_bad_row_fails_its_named_check(label, frames):
    with pytest.raises(ContractViolation) as excinfo:
        validate_all(frames)
    tripped = {check for _, check in excinfo.value.failures}
    assert label in tripped, f"expected {label!r}, got {sorted(tripped)}"


def test_committed_seed_passes_contracts():
    validate_all(clean())


def _frames_with_extra_season(season: int, **overrides) -> dict[str, pd.DataFrame]:
    """Valid mini-set plus one era-correct PlayerA row at `season`, with the
    league row and career span adjusted so only the injected defect can fail."""
    frames = load_fixtures("valid")
    league = frames["league_seasons"]
    if season not in set(league["season"]):
        league_row = {
            "season": season,
            "lg_pts_pg": 100.0,
            "lg_trb_pg": 40.0,
            "lg_ast_pg": 20.0,
            "lg_ts_pct": 0.5,
            "season_games": 82,
            "asg_held": True,
            "pace": 100.0,
            "pace_estimated": season < PACE_COMPUTED_FROM,
        }
        frames["league_seasons"] = pd.concat(
            [league, pd.DataFrame([league_row])], ignore_index=True
        )
    row = {
        "player_id": 1,
        "season": season,
        "team_abbr": "AAA",
        "gp": 50,
        "mp": 2000,
        "trb": 300,
        "ast": 300,
        "fgm": 500,
        "fga": 1200,
        "ftm": 0,
        "fta": 0,
        "team_srs": 1.0,
        "ws": 5.0,
        **{c: None for c in PO_COLS},
        **{
            col: (None if season < intro else ERA_DEFAULTS[col])
            for col, intro in ERA_INTRO.items()
        },
    }
    row.update(overrides)
    row["pts"] = 2 * row["fgm"] + (row["fg3m"] or 0) + row["ftm"]
    frames["player_seasons"] = pd.concat(
        [frames["player_seasons"], pd.DataFrame([row])], ignore_index=True
    )
    players = frames["players"].copy()
    a = players["player_id"] == 1
    players.loc[a, "first_season"] = min(
        int(players.loc[a, "first_season"].iloc[0]), season
    )
    players.loc[a, "last_season"] = max(
        int(players.loc[a, "last_season"].iloc[0]), season
    )
    frames["players"] = players
    return frames


def test_fg3a_at_real_record_passes():
    # Regression (T5 review): the 3PA record is 1,028 (Harden 2018-19) and
    # sits outside the seed pool (seed max 886). A 1000 ceiling would reject
    # a real season the moment the pool expands.
    frames = load_fixtures("valid")
    ps = frames["player_seasons"].copy()
    row = (ps["player_id"] == 1) & (ps["season"] == 2001)
    ps.loc[row, "fg3a"] = 1028
    frames["player_seasons"] = ps
    validate_all(frames)


def test_missing_column_reports_named_failure():
    # A dropped column must surface as a named aggregated failure, not a
    # raw KeyError from the cross-table joins.
    frames = load_fixtures("valid")
    frames["player_seasons"] = frames["player_seasons"].drop(columns="season")
    with pytest.raises(ContractViolation) as excinfo:
        validate_all(frames)
    assert ("player_seasons", "column_in_dataframe") in excinfo.value.failures


@pytest.mark.parametrize(("col", "intro"), sorted(ERA_INTRO.items()))
def test_value_just_before_intro_fails(col, intro):
    frames = _frames_with_extra_season(intro - 1, **{col: 5})
    with pytest.raises(ContractViolation) as excinfo:
        validate_all(frames)
    assert ("player_seasons", f"{col}_era_gate") in excinfo.value.failures


@pytest.mark.parametrize(("col", "intro"), sorted(ERA_INTRO.items()))
def test_null_at_intro_fails(col, intro):
    frames = _frames_with_extra_season(intro, **{col: None})
    with pytest.raises(ContractViolation) as excinfo:
        validate_all(frames)
    assert ("player_seasons", f"{col}_era_gate") in excinfo.value.failures
