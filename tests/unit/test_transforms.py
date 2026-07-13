"""Tests for the T6 SQL transform layer (pipeline/transform.py + sql/01-04,06).

The core proof is worksheet reproduction: docs/methodology/v1_hand_worksheet.md
hand-computes the locked three-player fixture pool through every v1.md formula,
so the SQL is correct iff it reproduces those numbers. Worksheet intermediates
are rounded to 4 decimals (v1.md §2) — assertions use that tolerance. The
real-seed tests are structural (counts, null patterns, the documented
edge-case players); dataset-specific formula verification stays with the
worksheet, where expected values are hand-derived rather than engine-derived.

The peak-window/scope tests build their own single-player pools in-test
(build_pool) rather than extending the worksheet trio: the trio is LOCKED —
adding players to tests/fixtures/valid changes every min–max anchor
(worksheet lock note). Tests marked "Codex review, 2026-07-12" were added in
the T6 4-Point review to close the gaps it demonstrated.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.clean import apply_schema, clean
from pipeline.contracts import ContractViolation, load_fixtures
from pipeline.transform import (
    ALL_NBA_TEAM_NUMBERS,
    TransformValidationError,
    _check_transforms,
    _execute_transforms,
    _fetch_marts,
    _validate_config,
    load_config,
    run_transforms,
)

TOL = 1e-4  # worksheet intermediates carry 4 decimals

# Hand-worksheet §3–§4 per-season values (player 1 = PlayerA, 2 = B, 3 = C):
# possessions, per-75 rates, REL ratios, SPI, AVAIL, REL_TS, the playoff
# mirror, and both rank columns. None = must be null (missed postseason,
# v1.md §9). Rank ties: A's 1.65 seasons and C's 1.85 seasons are bit-exact
# float ties (verified), so peak_rank breaks to the earlier season (§5.1)
# and spi_rank_pool assigns equal RANKs.
WORKSHEET_SEASONS = {
    (1, 1990): {
        "poss": 6750.0,
        "per75_pts": 27.0,
        "per75_trb": 6.0,
        "per75_ast": 6.0,
        "rel_pts": 1.8,
        "rel_trb": 1.0,
        "rel_ast": 2.0,
        "spi": 1.65,
        "avail": 1.0,
        "rel_ts": 1.2,
        "po_poss": None,
        "p_rel_pts": None,
        "p_rel_trb": None,
        "p_rel_ast": None,
        "p_spi": None,
        "peak_rank": 2,
        "spi_rank_pool": 6,
    },
    (1, 2000): {
        "poss": 6150.0,
        "per75_pts": 27.0,
        "per75_trb": 7.0,
        "per75_ast": 7.0,
        "rel_pts": 1.8,
        "rel_trb": 1.0,
        "rel_ast": 2.0,
        "spi": 1.65,
        "avail": 1.0,
        "rel_ts": 1.2,
        "po_poss": 1500.0,
        "p_rel_pts": 2.0,
        "p_rel_trb": 1.0,
        "p_rel_ast": 2.0,
        "p_spi": 1.75,
        "peak_rank": 3,
        "spi_rank_pool": 6,
    },
    (1, 2001): {
        "poss": 6150.0,
        "per75_pts": 30.0,
        "per75_trb": 7.0,
        "per75_ast": 7.0,
        "rel_pts": 2.0,
        "rel_trb": 1.0,
        "rel_ast": 2.0,
        "spi": 1.75,
        "avail": 1.0,
        "rel_ts": 1.2,
        "po_poss": 750.0,
        "p_rel_pts": 1.6667,
        "p_rel_trb": 1.0,
        "p_rel_ast": 2.0,
        "p_spi": 1.5833,
        "peak_rank": 1,
        "spi_rank_pool": 5,
    },
    (2, 1970): {
        "poss": 9000.0,
        "per75_pts": 28.0,
        "per75_trb": 21.0,
        "per75_ast": 3.0,
        "rel_pts": 2.0,
        "rel_trb": 3.0,
        "rel_ast": 1.0,
        "spi": 2.0,
        "avail": 1.0,
        "rel_ts": 1.0,
        "po_poss": 1575.0,
        "p_rel_pts": 2.0,
        "p_rel_trb": 3.0,
        "p_rel_ast": 1.0,
        "p_spi": 2.0,
        "peak_rank": 1,
        "spi_rank_pool": 1,
    },
    (2, 1971): {
        "poss": 9000.0,
        "per75_pts": 25.2,
        "per75_trb": 19.6,
        "per75_ast": 3.6,
        "rel_pts": 1.8,
        "rel_trb": 2.8,
        "rel_ast": 1.2,
        "spi": 1.9,
        "avail": 1.0,
        "rel_ts": 1.0,
        "po_poss": None,
        "p_spi": None,
        "peak_rank": 2,
        "spi_rank_pool": 2,
    },
    (3, 1999): {
        "poss": 3750.0,
        "per75_pts": 33.0,
        "per75_trb": 7.0,
        "per75_ast": 7.0,
        "rel_pts": 2.2,
        "rel_trb": 1.0,
        "rel_ast": 2.0,
        "spi": 1.85,
        "avail": 1.0,
        "rel_ts": 1.32,
        "po_poss": None,
        "p_spi": None,
        "peak_rank": 1,
        "spi_rank_pool": 3,
    },
    # exact availability boundary: 41/82 = 0.5 qualifies for the peak window
    (3, 2000): {
        "poss": 3075.0,
        "per75_pts": 33.0,
        "per75_trb": 7.0,
        "per75_ast": 7.0,
        "rel_pts": 2.2,
        "rel_trb": 1.0,
        "rel_ast": 2.0,
        "spi": 1.85,
        "avail": 0.5,
        "rel_ts": 1.32,
        "po_poss": 375.0,
        "p_rel_pts": 2.2,
        "p_rel_trb": 1.0,
        "p_rel_ast": 2.0,
        "p_spi": 1.85,
        "peak_rank": 2,
        "spi_rank_pool": 3,
    },
}

# Hand-worksheet §3.2/§4 raw component values.
WORKSHEET_COMPONENTS = {
    1: {
        "peak_raw": 1.6833,
        "longevity_raw": 5.05,
        "ws48": 0.24,
        "srs_w": 6.0,
        "playoff_raw": 50.8333,
        "rel_ts_career": 1.2,
        "spi_career": 1.6835,
    },
    2: {
        "peak_raw": 1.95,
        "longevity_raw": 3.9,
        "ws48": 0.2533,
        "srs_w": 2.5,
        "playoff_raw": 28.0,
        "rel_ts_career": 1.0,
        "spi_career": 1.95,
    },
    3: {
        "peak_raw": 1.85,
        "longevity_raw": 2.775,
        "ws48": 0.2637,
        "srs_w": 4.0,
        "playoff_raw": 9.25,
        "rel_ts_career": 1.32,
        "spi_career": 1.85,
    },
}

# Hand-worksheet accolade rates: (player, award) -> (eligible_seasons, rate).
# rate None = zero eligible seasons -> award excluded (v1.md §12.7), and
# PlayerC's all_star denominator is 1, not 2 (no ASG held in 1999).
WORKSHEET_RATES = {
    (1, "mvp"): (3, 0.3333),
    (1, "ring"): (3, 0.3333),
    (1, "finals_mvp"): (3, 0.3333),
    (1, "all_nba"): (3, 0.8333),
    (1, "dpoy"): (3, 0.0),
    (1, "all_star"): (3, 1.0),
    (2, "mvp"): (2, 0.5),
    (2, "ring"): (2, 0.0),
    (2, "finals_mvp"): (2, 0.0),
    (2, "all_nba"): (2, 1.0),
    (2, "dpoy"): (0, None),
    (2, "all_star"): (2, 1.0),
    (3, "mvp"): (2, 0.5),
    (3, "ring"): (2, 0.0),
    (3, "finals_mvp"): (2, 0.0),
    (3, "all_nba"): (2, 0.75),
    (3, "dpoy"): (2, 0.0),
    (3, "all_star"): (1, 1.0),
}

PLAYER_X = 9990  # the in-test synthetic player (never in fixtures/valid)


def fixture_frames() -> dict[str, pd.DataFrame]:
    return {
        name: apply_schema(name, frame)
        for name, frame in load_fixtures("valid").items()
    }


def build_pool(
    season_rows: list[dict],
    accolades: list[tuple] = (),
    league_overrides: dict[int, dict] | None = None,
) -> dict[str, pd.DataFrame]:
    """Frames for one synthetic player (peak-window and scope tests) —
    contract-valid unless a test deliberately crafts a violation. Rows need
    season/gp/mp/pts (pts even, so the points identity holds with
    fg3m = ftm = 0) plus optional po_* values; the league environment is
    uniform (pace 100, TS .50, 82 games) unless overridden."""
    overrides = league_overrides or {}
    seasons = [row["season"] for row in season_rows]
    league = pd.DataFrame(
        [
            {
                "season": season,
                "lg_pts_pg": 100.0,
                "lg_trb_pg": 40.0,
                "lg_ast_pg": 20.0,
                "lg_ts_pct": 0.5,
                "season_games": 82,
                "asg_held": True,
                "pace": 100.0,
                "pace_estimated": False,
                **overrides.get(season, {}),
            }
            for season in seasons
        ]
    )
    player_seasons = pd.DataFrame(
        [
            {
                "player_id": PLAYER_X,
                "season": row["season"],
                "team_abbr": "XXX",
                "gp": row["gp"],
                "mp": row["mp"],
                "pts": row["pts"],
                "trb": 410,
                "ast": 205,
                "fgm": row["pts"] // 2,
                "fga": row["pts"],
                "ftm": 0,
                "fta": 0,
                "fg3m": 0,
                "fg3a": 0,
                "stl": 50,
                "blk": 40,
                "tov": 100,
                "po_gp": row.get("po_gp"),
                "po_mp": row.get("po_mp"),
                "po_pts": row.get("po_pts"),
                "po_trb": row.get("po_trb"),
                "po_ast": row.get("po_ast"),
                "team_srs": 3.0,
                "ws": 10.0,
            }
            for row in season_rows
        ]
    )
    players = pd.DataFrame(
        [
            {
                "player_id": PLAYER_X,
                "player_name": "Window Tester",
                "first_season": min(seasons),
                "last_season": max(seasons),
                "is_active": False,
            }
        ]
    )
    accolade_frame = pd.DataFrame(
        list(accolades), columns=["player_id", "season", "award", "all_nba_team"]
    )
    frames = {
        "players": players,
        "player_seasons": player_seasons,
        "accolades": accolade_frame,
        "league_seasons": league,
    }
    return {name: apply_schema(name, frame) for name, frame in frames.items()}


@pytest.fixture(scope="module")
def fixture_marts() -> dict[str, pd.DataFrame]:
    return run_transforms(fixture_frames(), load_config())


@pytest.fixture(scope="module")
def seed_frames() -> dict[str, pd.DataFrame]:
    return clean()


@pytest.fixture(scope="module")
def seed_marts(seed_frames) -> dict[str, pd.DataFrame]:
    return run_transforms(seed_frames, load_config())


@pytest.mark.parametrize(("key", "expected"), WORKSHEET_SEASONS.items())
def test_worksheet_season_metrics(fixture_marts, key, expected):
    metrics = fixture_marts["mart_player_season_metrics"]
    row = metrics.set_index(["player_id", "season"]).loc[key]
    for column, value in expected.items():
        if value is None:
            assert pd.isna(row[column]), f"{column} should be null (missed playoffs)"
        else:
            assert row[column] == pytest.approx(value, abs=TOL), column


@pytest.mark.parametrize(("player_id", "expected"), WORKSHEET_COMPONENTS.items())
def test_worksheet_component_inputs(fixture_marts, player_id, expected):
    inputs = fixture_marts["mart_player_component_inputs"]
    row = inputs.set_index("player_id").loc[player_id]
    for column, value in expected.items():
        assert row[column] == pytest.approx(value, abs=TOL), column
    assert not row["peak_fallback_used"]


@pytest.mark.parametrize(("key", "expected"), WORKSHEET_RATES.items())
def test_worksheet_award_rates(fixture_marts, key, expected):
    rates = fixture_marts["mart_player_award_rates"]
    row = rates.set_index(["player_id", "award"]).loc[key]
    eligible, rate = expected
    assert row["eligible_seasons"] == eligible
    if rate is None:
        assert pd.isna(row["rate"]), "zero-eligible award must have null rate"
    else:
        assert row["rate"] == pytest.approx(rate, abs=TOL)


def test_worksheet_peak_windows(fixture_marts):
    """All trio careers have fewer than peak_n qualifying seasons, so every
    season is in the window (v1.md §5.1 fewer-than-N rule) with no fallback."""
    metrics = fixture_marts["mart_player_season_metrics"]
    assert metrics["is_peak_window"].all()
    assert metrics["qualifies_peak"].all()  # includes the exact-0.5 boundary
    assert not metrics["peak_fallback"].any()


def test_peak_scope_equals_career_on_fixture_trio(fixture_marts):
    """Worksheet §7: every trio peak window equals the whole career, so peak
    scope must reproduce the career-scope marts exactly — this pins the scope
    filter mechanism itself (v1.md §7, §12.9)."""
    peak = run_transforms(fixture_frames(), load_config(), scope="peak")
    for table in ("mart_player_component_inputs", "mart_player_award_rates"):
        pd.testing.assert_frame_equal(peak[table], fixture_marts[table])


def test_peak_scope_restricts_playoffs_and_awards():
    """A player with 7 qualifying seasons and playoffs/awards both inside and
    outside the top-5 window: peak scope must actually exclude the
    outside-window contributions — the trio test alone is degenerate because
    its windows equal the whole career (Codex review, 2026-07-12)."""
    rows = [
        {"season": 1990 + i, "gp": 82, "mp": 3280, "pts": 2460 - 82 * i}
        for i in range(7)
    ]
    rows[0] |= {"po_gp": 10, "po_mp": 400, "po_pts": 250, "po_trb": 70, "po_ast": 70}
    rows[6] |= {"po_gp": 8, "po_mp": 320, "po_pts": 160, "po_trb": 56, "po_ast": 56}
    accolades = [
        (PLAYER_X, 1990, "mvp", None),
        (PLAYER_X, 1990, "all_star", None),
        (PLAYER_X, 1996, "mvp", None),
        (PLAYER_X, 1996, "all_star", None),
    ]
    frames = build_pool(rows, accolades)
    config = load_config()
    career = run_transforms(frames, config)
    peak = run_transforms(frames, config, scope="peak")

    metrics = career["mart_player_season_metrics"].set_index("season")
    window = set(metrics.index[metrics["is_peak_window"]])
    assert window == {1990, 1991, 1992, 1993, 1994}  # top-5 SPI (pts descend)

    career_inputs = career["mart_player_component_inputs"].iloc[0]
    peak_inputs = peak["mart_player_component_inputs"].iloc[0]
    run_1990 = metrics.loc[1990, "p_spi"] * 10
    run_1996 = metrics.loc[1996, "p_spi"] * 8
    assert career_inputs["playoff_raw"] == pytest.approx(run_1990 + run_1996)
    assert peak_inputs["playoff_raw"] == pytest.approx(run_1990)  # 1996 out

    career_rates = career["mart_player_award_rates"].set_index("award")
    peak_rates = peak["mart_player_award_rates"].set_index("award")
    for award in ("mvp", "all_star"):
        assert career_rates.loc[award, "weighted_wins"] == 2.0
        assert career_rates.loc[award, "eligible_seasons"] == 7
        assert peak_rates.loc[award, "weighted_wins"] == 1.0  # 1996 win out
        assert peak_rates.loc[award, "eligible_seasons"] == 5


def test_peak_fallback_selects_top_n_by_spi():
    """Zero qualifying seasons (every avail < 0.5): the §5.1 fallback makes
    all seasons eligible, and the window is still the top-peak_n by SPI,
    flagged for run metadata (Codex review, 2026-07-12)."""
    rows = [
        {"season": 1990 + i, "gp": 30, "mp": 1200, "pts": 1230 - 82 * i}
        for i in range(7)
    ]
    marts = run_transforms(build_pool(rows), load_config())
    metrics = marts["mart_player_season_metrics"].set_index("season")
    assert not metrics["qualifies_peak"].any()  # 30/82 < 0.5 everywhere
    assert metrics["peak_fallback"].all()
    window = metrics[metrics["is_peak_window"]]
    assert set(window.index) == {1990, 1991, 1992, 1993, 1994}
    inputs = marts["mart_player_component_inputs"].iloc[0]
    assert inputs["peak_fallback_used"]
    assert inputs["peak_raw"] == pytest.approx(window["spi"].mean())


def test_peak_tiebreak_prefers_earlier_season():
    """Equal-SPI seasons (identical stat lines, identical league environment
    — a true float tie) break to the earlier season (v1.md §5.1); peak_n = 1
    isolates the rule (Codex review, 2026-07-12)."""
    rows = [
        {"season": 1990, "gp": 82, "mp": 3280, "pts": 2460},
        {"season": 1991, "gp": 82, "mp": 3280, "pts": 2460},
    ]
    config = load_config()
    config["peak_n"] = 1
    marts = run_transforms(build_pool(rows), config)
    metrics = marts["mart_player_season_metrics"].set_index("season")
    assert metrics.loc[1990, "spi"] == metrics.loc[1991, "spi"]
    assert metrics["peak_rank"].to_dict() == {1990: 1, 1991: 2}
    assert metrics["is_peak_window"].to_dict() == {1990: True, 1991: False}
    inputs = marts["mart_player_component_inputs"].iloc[0]
    assert inputs["peak_raw"] == metrics.loc[1990, "spi"]


def test_contracts_gate_runs_before_sql():
    """Bug fix (Codex review, 2026-07-12): the transform validates its own
    inputs — an impossible 200-game season must raise ContractViolation
    before any SQL executes, not silently flow through the AVAIL cap and
    corrupt the games-weighted SRS."""
    frames = fixture_frames()
    seasons = frames["player_seasons"].copy()
    seasons.loc[(seasons["player_id"] == 1) & (seasons["season"] == 1990), "gp"] = 200
    frames["player_seasons"] = seasons
    with pytest.raises(ContractViolation):
        run_transforms(frames, load_config())


@pytest.mark.parametrize(
    ("case", "table", "column", "value"),
    [
        ("zero_regular_minutes", "player_seasons", "mp", 0),
        ("zero_playoff_minutes", "player_seasons", "po_mp", 0),
        ("zero_games", "player_seasons", "gp", 0),
        ("zero_league_pace", "league_seasons", "pace", 0.0),
        ("unmapped_all_nba_team", "accolades", "all_nba_team", 4),
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_denominator_guards_reject_contract_violations(case, table, column, value):
    """Every zero/degenerate denominator the SQL could hit is impossible in
    contract-valid data — prove each one is rejected before the SQL runs
    (per-row gp/mp >= 1 also means career GP/MP sums cannot be zero;
    Codex review, 2026-07-12)."""
    frames = fixture_frames()
    frame = frames[table].copy()
    if table == "player_seasons":
        mask = (frame["player_id"] == 1) & (frame["season"] == 2000)
    elif table == "league_seasons":
        mask = frame["season"] == 1990
    else:  # the All-NBA selection in the valid mini-set
        mask = (frame["award"] == "all_nba") & (frame["season"] == 1990)
    frame.loc[mask, column] = value
    frames[table] = frame
    with pytest.raises(ContractViolation):
        run_transforms(frames, load_config())


def test_award_key_parity_enforced():
    """Bug fix (Codex review, 2026-07-12): the rate mart is driven by
    accolade_intro_season, so a missing award key would silently drop that
    award's facts — the transform must refuse to run instead."""
    config = load_config()
    del config["accolade_intro_season"]["mvp"]
    with pytest.raises(ValueError, match="accolade_intro_season"):
        run_transforms(fixture_frames(), config)

    config = load_config()
    del config["accolade_weights"]["mvp"]
    with pytest.raises(ValueError, match="accolade_weights"):
        run_transforms(fixture_frames(), config)


def test_ineligible_win_cannot_inflate_rate():
    """Bug fix (Codex review, 2026-07-12), defense-in-depth: an All-Star
    selection in a no-ASG season is contract-invalid, but even beneath the
    contracts (private seam — the public API has no bypass) the SQL must
    filter the numerator with the same eligibility predicate as the
    denominator — otherwise the illegal 1999 win hides behind the player's
    other eligible season and inflates the rate."""
    rows = [
        {"season": 1999, "gp": 50, "mp": 2000, "pts": 1650},
        {"season": 2000, "gp": 82, "mp": 3280, "pts": 2460},
    ]
    accolades = [
        (PLAYER_X, 1999, "all_star", None),  # illegal: no ASG held in 1999
        (PLAYER_X, 2000, "all_star", None),
    ]
    frames = build_pool(
        rows,
        accolades,
        league_overrides={1999: {"season_games": 50, "asg_held": False}},
    )
    con = _execute_transforms(frames, load_config(), "career")
    _check_transforms(con)
    row = (
        _fetch_marts(con)["mart_player_award_rates"].set_index("award").loc["all_star"]
    )
    assert row["eligible_seasons"] == 1  # 1999 out of the denominator
    assert row["weighted_wins"] == 1.0  # and out of the numerator
    assert row["rate"] == 1.0


def test_stealth_ineligible_win_stays_filtered():
    """Codex re-review (2026-07-12): the sharper variant — with TWO eligible
    seasons and one valid selection, unfiltered numerator logic would produce
    wins 2 / eligible 2 = rate 1.0, which is IN bounds and invisible to every
    range check. The correct output is 1 / 2 = 0.5."""
    rows = [
        {"season": 1999, "gp": 50, "mp": 2000, "pts": 1650},
        {"season": 2000, "gp": 82, "mp": 3280, "pts": 2460},
        {"season": 2001, "gp": 82, "mp": 3280, "pts": 2378},
    ]
    accolades = [
        (PLAYER_X, 1999, "all_star", None),  # illegal: no ASG held in 1999
        (PLAYER_X, 2000, "all_star", None),
    ]
    frames = build_pool(
        rows,
        accolades,
        league_overrides={1999: {"season_games": 50, "asg_held": False}},
    )
    con = _execute_transforms(frames, load_config(), "career")
    _check_transforms(con)
    row = (
        _fetch_marts(con)["mart_player_award_rates"].set_index("award").loc["all_star"]
    )
    assert row["eligible_seasons"] == 2
    assert row["weighted_wins"] == 1.0
    assert row["rate"] == 0.5


def test_seed_counts_reconcile(seed_marts):
    assert len(seed_marts["dim_player"]) == 20
    assert len(seed_marts["dim_season"]) == 70
    assert len(seed_marts["fact_player_season"]) == 335
    assert len(seed_marts["fact_accolade"]) == 668
    assert len(seed_marts["mart_player_season_metrics"]) == 335
    assert len(seed_marts["mart_player_component_inputs"]) == 20
    assert len(seed_marts["mart_player_award_rates"]) == 120


def test_seed_award_wins_match_accolades(seed_frames, seed_marts):
    """Independent pandas aggregation of the accolade facts must equal the
    mart's weighted wins for every (player, award) — joined on award, not
    just player-season, so a silently dropped award type cannot hide
    (Codex review, 2026-07-12)."""
    config = load_config()
    points = {
        number: config["all_nba_team_points"][name]
        for name, number in ALL_NBA_TEAM_NUMBERS.items()
    }
    accolades = seed_frames["accolades"]
    weights = (
        accolades["all_nba_team"]
        .map(points)
        .where(accolades["award"] == "all_nba", 1.0)
        .astype(float)
    )
    expected = weights.groupby([accolades["player_id"], accolades["award"]]).sum()
    rates = seed_marts["mart_player_award_rates"].set_index(["player_id", "award"])
    assert expected.index.isin(rates.index).all()
    for (player_id, award), wins in expected.items():
        assert rates.loc[(player_id, award), "weighted_wins"] == pytest.approx(wins)
    unwon = rates.loc[~rates.index.isin(expected.index), "weighted_wins"]
    assert (unwon == 0).all()


def test_seed_dpoy_excluded_for_pre_1983_retirees(seed_marts):
    """The §12.7 era-fairness machinery on real data: exactly the four seed
    players who retired before DPOY existed (1983) get a null DPOY rate."""
    rates = seed_marts["mart_player_award_rates"]
    dpoy = rates[rates["award"] == "dpoy"]
    excluded = set(dpoy.loc[dpoy["rate"].isna(), "player_name"])
    assert excluded == {
        "Bill Russell",
        "Jerry West",
        "Oscar Robertson",
        "Wilt Chamberlain",
    }
    assert (dpoy.loc[dpoy["rate"].isna(), "eligible_seasons"] == 0).all()


def test_seed_asg_1999_shrinks_all_star_denominator(seed_marts):
    """Tim Duncan played 19 seasons, but 1999 held no All-Star Game, so his
    All-Star eligibility is 18 (v1.md §5.5)."""
    rates = seed_marts["mart_player_award_rates"].set_index(["player_name", "award"])
    assert rates.loc[("Tim Duncan", "all_star"), "eligible_seasons"] == 18


def test_seed_playoff_raw_never_null(seed_marts):
    """SUM over all-null playoff runs must coalesce to 0, never null (§5.4)."""
    inputs = seed_marts["mart_player_component_inputs"]
    assert inputs["playoff_raw"].notna().all()
    assert (inputs["playoff_raw"] > 0).all()  # every seed great made playoffs


def test_seed_documented_edge_cases(seed_marts):
    """Regression sentinels for the T6 investigation findings (qa log):
    Jokić's 73 gp in the 72-game 2020 schedule caps at avail 1.0, and Jerry
    West's one-minute 1967 playoff cameo grades P_SPI ≈ 1.05 off a single
    rebound — the documented, bounded §5.4 small-sample artifact."""
    metrics = seed_marts["mart_player_season_metrics"].set_index(
        ["player_id", "season"]
    )
    assert metrics.loc[(203999, 2020), "avail"] == 1.0
    assert metrics.loc[(78497, 1967), "p_spi"] == pytest.approx(1.0522, abs=TOL)


def test_validation_checks_have_teeth():
    """06_validation_checks.sql must fail loudly on derived-layer corruption:
    a po_gp with no other playoff stats breaks the p_spi-null-iff-missed
    rule. Uses the private seam because the contracts (correctly) intercept
    this corruption first — the point here is the SQL-layer guard beneath
    them, and _check_transforms is the exact raise path run_transforms uses."""
    frames = fixture_frames()
    seasons = frames["player_seasons"].copy()
    mask = (seasons["player_id"] == 1) & (seasons["season"] == 1990)
    seasons.loc[mask, "po_gp"] = 3
    frames["player_seasons"] = seasons
    con = _execute_transforms(frames, load_config(), "career")
    with pytest.raises(
        TransformValidationError, match="p_spi_null_iff_missed_postseason"
    ):
        _check_transforms(con)


def test_bad_production_blend_errors():
    """v1.md §10.3: blends must sum to 1.0 — error, never renormalize."""
    config = load_config()
    config["production_blend"] = {"pts": 0.6, "trb": 0.25, "ast": 0.25}
    with pytest.raises(ValueError, match="production_blend"):
        run_transforms(fixture_frames(), config)


def test_empty_scope_raises():
    """Codex fourth pass (2026-07-13): scope="" must raise, not silently
    fall through a falsey `or` to the config default."""
    with pytest.raises(ValueError, match="scope"):
        run_transforms(fixture_frames(), load_config(), scope="")


def test_reconciliation_catches_partial_award_loss():
    """Codex re-review (2026-07-12): silently dropping ONE selection from an
    aggregate — not the whole award — must trip accolades_reach_rate_mart.
    The pre-fix positivity check (weighted_wins > 0) passed this exact
    corruption; the exact expected-vs-actual reconciliation cannot."""
    con = _execute_transforms(fixture_frames(), load_config(), "career")
    con.execute(
        "UPDATE mart_player_award_rates SET weighted_wins = weighted_wins - 1"
        " WHERE player_id = 1 AND award = 'all_star'"  # PlayerA: 3 wins -> 2
    )
    with pytest.raises(TransformValidationError, match="accolades_reach_rate_mart"):
        _check_transforms(con)


def test_rate_consistency_catches_corrupt_rate():
    """rate is the value scoring consumes: corrupting it while weighted_wins
    stays intact must trip award_rate_consistent — the wins reconciliation
    alone cannot see it (same failure shape as the partial-loss gap)."""
    con = _execute_transforms(fixture_frames(), load_config(), "career")
    con.execute(
        "UPDATE mart_player_award_rates SET rate = 0.9"
        " WHERE player_id = 1 AND award = 'all_star'"  # true rate is 3/3
    )
    with pytest.raises(TransformValidationError, match="award_rate_consistent"):
        _check_transforms(con)


def test_reconciliation_catches_eligibility_corruption():
    """Codex fourth pass (2026-07-13): eligible_seasons is a scoring
    denominator, and corrupting it WITH a self-consistent rate (PlayerA MVP:
    eligible 3 -> 2, rate 1/3 -> 1/2) passed all pre-fix checks —
    award_rate_consistent holds because 1/2 matches wins 1 over eligible 2.
    The grid-based expected side reconciles eligible_seasons exactly."""
    con = _execute_transforms(fixture_frames(), load_config(), "career")
    con.execute(
        "UPDATE mart_player_award_rates SET eligible_seasons = 2, rate = 0.5"
        " WHERE player_id = 1 AND award = 'mvp'"
    )
    with pytest.raises(TransformValidationError, match="accolades_reach_rate_mart"):
        _check_transforms(con)


def test_reconciliation_catches_ghost_zero_win_row():
    """Codex fourth pass (2026-07-13): replacing a legitimate zero-win row
    with an unknown-player zero-win row preserves the total row count and
    passed all pre-fix checks — a facts-only expected side had nothing to
    compare a ghost key against. The complete player x award grid flags both
    the ghost key and the now-missing legitimate key."""
    con = _execute_transforms(fixture_frames(), load_config(), "career")
    con.execute(
        "UPDATE mart_player_award_rates SET player_id = 999999"
        " WHERE player_id = 1 AND award = 'dpoy'"  # a real zero-win row
    )
    with pytest.raises(TransformValidationError, match="accolades_reach_rate_mart"):
        _check_transforms(con)


def test_ts_denominator_check_fires():
    """Direct proof ts_denominator_positive fires (found in the re-review
    open investigation: it was as unproven as the three flagged checks). A
    zero-FGA/zero-FTA season is contract-LEGAL (bounds floor at 0; the points
    identity then forces pts = 0), and DuckDB division yields IEEE NaN, never
    NULL or an error (probed on 1.5.4) — so the row survives the NOT NULL
    DDL and this named check is what makes the degenerate input loud."""
    frames = fixture_frames()
    seasons = frames["player_seasons"].copy()
    mask = (seasons["player_id"] == 1) & (seasons["season"] == 1990)
    seasons.loc[mask, ["pts", "fgm", "fga", "ftm", "fta"]] = 0
    frames["player_seasons"] = seasons
    con = _execute_transforms(frames, load_config(), "career")
    with pytest.raises(TransformValidationError, match="ts_denominator_positive"):
        _check_transforms(con)


def test_unknown_award_check_fires():
    """Direct proof award_key_known_to_config fires on its own (Codex
    re-review): an award type unknown to config reaching the facts (contracts
    reject it — the seam goes beneath them) must be flagged, not silently
    emitted with zero rate rows."""
    frames = fixture_frames()
    extra = pd.DataFrame(
        [{"player_id": 1, "season": 1990, "award": "sixth_man", "all_nba_team": None}]
    )
    frames["accolades"] = apply_schema(
        "accolades", pd.concat([frames["accolades"], extra], ignore_index=True)
    )
    con = _execute_transforms(frames, load_config(), "career")
    with pytest.raises(TransformValidationError, match="award_key_known_to_config"):
        _check_transforms(con)


def test_unmapped_all_nba_team_check_fires():
    """Direct proof all_nba_points_mapped fires on its own (Codex re-review):
    an All-NBA selection with no configured point value would silently drop
    out of the weighted SUM on both sides of the reconciliation — this check
    is what makes that loud."""
    frames = fixture_frames()
    accolades = frames["accolades"].copy()
    mask = (accolades["award"] == "all_nba") & (accolades["season"] == 1990)
    accolades.loc[mask, "all_nba_team"] = 4
    frames["accolades"] = accolades
    con = _execute_transforms(frames, load_config(), "career")
    with pytest.raises(TransformValidationError, match="all_nba_points_mapped"):
        _check_transforms(con)


@pytest.mark.parametrize(
    "points",
    [
        {"first": 1.0, "second": 0.5, "third": 0.0},
        {"first": 1.1, "second": 0.5, "third": 0.25},
        {"first": 0.25, "second": 0.5, "third": 1.0},
        {"first": 1.0, "second": float("nan"), "third": 0.25},
        {"first": float("inf"), "second": 0.5, "third": 0.25},
        {"first": 1.0, "second": 0.5},
        {"first": 1.0, "second": 0.5, "third": 0.25, "fourth": 0.1},
    ],
    ids=["zero", "above_one", "reversed", "nan", "inf", "missing_key", "extra_key"],
)
def test_all_nba_points_domain(points):
    """Codex re-review (2026-07-12): the All-NBA point domain is
    0 < third <= second <= first <= 1 — a zero silently nulls that team's
    credit, a value above 1 breaks the rate-in-[0,1] guarantee, a reversed
    ordering inverts the team semantics, and NaN/Inf must never reach SQL."""
    config = load_config()
    config["all_nba_team_points"] = points
    with pytest.raises(ValueError, match="all_nba_team_points"):
        _validate_config(config)


@pytest.mark.parametrize(
    ("path", "value", "match"),
    [
        (("peak_n",), 2.5, "peak_n"),
        (("peak_n",), True, "peak_n"),
        (("peak_n",), 0, "peak_n"),
        (("peak_min_avail",), 1.5, "peak_min_avail"),
        (("peak_min_avail",), -0.1, "peak_min_avail"),
        (("peak_min_avail",), float("nan"), "peak_min_avail"),
        (("production_blend",), {"pts": 0.75, "trb": 0.25}, "production_blend"),
        (
            ("production_blend",),
            {"pts": 0.5, "trb": 0.25, "ast": 0.25, "stl": 0.0},
            "production_blend",
        ),
        (
            ("production_blend",),
            {"pts": 1.5, "trb": -0.25, "ast": -0.25},  # sums to 1.0, negative
            "production_blend",
        ),
        (
            ("production_blend",),
            {"pts": float("nan"), "trb": 0.5, "ast": 0.5},
            "production_blend",
        ),
        (("accolade_intro_season", "dpoy"), 1990, "accolade_intro_season"),
    ],
)
def test_config_validation_branches(path, value, match):
    """Codex re-review (2026-07-12): every _validate_config branch rejects
    its malformed input — fractional/bool/zero peak_n, out-of-range or NaN
    availability, blend key/value/sign failures, and a drifted award intro
    season (right keys, wrong year — would mis-denominate every DPOY rate)."""
    config = load_config()
    target = config
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError, match=match):
        _validate_config(config)
