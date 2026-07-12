"""Unit tests for the SRS solver (pipeline/srs.py).

The 3-team league below is small enough to solve by hand from the SRS definition
rating_i = avg_margin_i + avg(opponent ratings):

  games: A beat B by 6, B beat C by 6, A beat C by 12
  avg margins: A = +9, B = 0, C = -9
  hand solution: r_A = 6, r_B = 0, r_C = -6
    check A: 9 + (0 + -6)/2 = 6 ✓   B: 0 + (6 + -6)/2 = 0 ✓   C: -9 + (6 + 0)/2 = -6 ✓
"""

import pandas as pd
import pytest

from pipeline.srs import compute_srs


def _both_perspectives(results: list[tuple[str, str, int]]) -> pd.DataFrame:
    rows = []
    for winner, loser, margin in results:
        rows.append({"team": winner, "opp": loser, "margin": margin})
        rows.append({"team": loser, "opp": winner, "margin": -margin})
    return pd.DataFrame(rows)


@pytest.fixture
def three_team_league() -> pd.DataFrame:
    return _both_perspectives([("A", "B", 6), ("B", "C", 6), ("A", "C", 12)])


def test_hand_solved_three_team_league(three_team_league):
    ratings = compute_srs(three_team_league)
    assert ratings["A"] == pytest.approx(6.0)
    assert ratings["B"] == pytest.approx(0.0)
    assert ratings["C"] == pytest.approx(-6.0)


def test_ratings_sum_to_zero(three_team_league):
    assert compute_srs(three_team_league).sum() == pytest.approx(0.0)


def test_two_team_split_series():
    # A beat B by 10, B beat A by 4: only r_A - r_B = 3 is determined; the
    # minimum-norm solution centers the pair at zero.
    games = _both_perspectives([("A", "B", 10), ("B", "A", 4)])
    ratings = compute_srs(games)
    assert ratings["A"] == pytest.approx(1.5)
    assert ratings["B"] == pytest.approx(-1.5)


def test_deterministic_and_sorted(three_team_league):
    first = compute_srs(three_team_league)
    second = compute_srs(three_team_league.sample(frac=1, random_state=7))
    pd.testing.assert_series_equal(first, second)
    assert list(first.index) == sorted(first.index)
