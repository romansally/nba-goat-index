"""The six v1.md §10 invariants over the scoring engine (T7), plus the
engine's own fail-loud guards, the pairwise view, an independent pandas
oracle, and the West marginal-impact regression deferred from T6.

Monotonicity perturbations run on in-memory copies of the locked fixture
trio (the on-disk fixtures never change — worksheet lock note) and must keep
the Pandera contracts green: the points identity pts = 2·fgm + fg3m + ftm
forces paired pts/fgm bumps, so pure-SPI cases bump rebounds instead. Every
strict case perturbs a player who is strictly inside the pool range on the
affected sub-metric (v1.md §10.5: min–max is weakly monotone at the pool
max, and at a unique pool min the anchor travels with the riser).

The oracle is a deliberate second implementation of §6 in pandas — kept in
tests only (one scoring implementation in SQL is the production rule). It
re-derives every component and final score from the marts and the raw
config, so a SQL↔methodology divergence on the real seed fails here even
where the three-player trio is too degenerate to expose it.
"""

import copy
import hashlib
import math
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from pipeline.clean import TABLE_KEYS, apply_schema, clean
from pipeline.compare import pairwise
from pipeline.contracts import load_fixtures
from pipeline.score import (
    NEAR_DEGENERATE_ATOL,
    NEAR_DEGENERATE_RTOL,
    WEIGHT_SUM_ATOL,
    ScoringError,
    _connect,
    _effective_weights,
    _guard,
    _noise_thin,
    _score_marts,
    _validate_scoring_config,
    score,
)
from pipeline.transform import _execute_transforms, load_config, run_transforms

WEST, WEST_SEASON = 78497, 1967
PO_COLUMNS = ["po_gp", "po_mp", "po_pts", "po_trb", "po_ast"]
SEVEN_X, SEVEN_Y = 101, 202


def fixture_frames() -> dict[str, pd.DataFrame]:
    return {n: apply_schema(n, f) for n, f in load_fixtures("valid").items()}


def seven_season_frames() -> dict[str, pd.DataFrame]:
    """Two-player pool where player X has SEVEN qualifying seasons — every
    trio career is shorter than peak_n, so only this pool proves top-5 window
    sizing END TO END at the scoring level (Codex T7 review test 7). X's two
    WORST seasons (2006–2007) carry all of his awards and playoff runs:
    career scope must count them, a correct §7 peak window (top-5 by SPI =
    2001–2005) must not. Uniform league rows keep the era rules satisfied;
    Y differs from X on every continuous input so no anchor degenerates."""
    seasons = list(range(2001, 2008))
    league_seasons = pd.DataFrame(
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
            }
            for season in seasons
        ]
    )

    def season_row(player_id, season, pts, fga, trb, ast, srs, ws, po=None):
        return {
            "player_id": player_id,
            "season": season,
            "team_abbr": "XXX" if player_id == SEVEN_X else "YYY",
            "gp": 82,
            "mp": 3280,
            "pts": pts,
            "trb": trb,
            "ast": ast,
            "fgm": pts // 2,
            "fga": fga,
            "ftm": 0,
            "fta": 0,
            "fg3m": 0,
            "fg3a": 0,
            "stl": 50,
            "blk": 40,
            "tov": 100,
            "po_gp": po[0] if po else None,
            "po_mp": po[1] if po else None,
            "po_pts": po[2] if po else None,
            "po_trb": po[3] if po else None,
            "po_ast": po[4] if po else None,
            "team_srs": srs,
            "ws": ws,
        }

    # X: SPI strictly descending with season (pts 2000 -> 1760), so the top-5
    # window is exactly 2001-2005; playoffs only in 2006 and 2007.
    x_rows = [
        season_row(
            SEVEN_X,
            season,
            pts=2000 - 40 * offset,
            fga=2000 - 40 * offset,
            trb=400,
            ast=200,
            srs=3.0,
            ws=10.0,
            po=(10, 400, 300, 100, 50) if season >= 2006 else None,
        )
        for offset, season in enumerate(seasons)
    ]
    y_rows = [
        season_row(
            SEVEN_Y,
            season,
            pts=1000 - 10 * offset,
            fga=1100,
            trb=300,
            ast=150,
            srs=1.0,
            ws=5.0,
            po=(8, 320, 240, 80, 40) if season == 2002 else None,
        )
        for offset, season in enumerate(range(2001, 2004))
    ]
    players = pd.DataFrame(
        [
            {
                "player_id": SEVEN_X,
                "player_name": "SevenSeasonX",
                "first_season": 2001,
                "last_season": 2007,
                "is_active": False,
            },
            {
                "player_id": SEVEN_Y,
                "player_name": "ShortY",
                "first_season": 2001,
                "last_season": 2003,
                "is_active": False,
            },
        ]
    )
    accolades = pd.DataFrame(
        [
            {
                "player_id": SEVEN_X,
                "season": 2006,
                "award": "mvp",
                "all_nba_team": None,
            },
            {
                "player_id": SEVEN_X,
                "season": 2007,
                "award": "mvp",
                "all_nba_team": None,
            },
        ]
    )
    frames = {
        "players": players,
        "player_seasons": pd.DataFrame(x_rows + y_rows),
        "accolades": accolades,
        "league_seasons": league_seasons,
    }
    return {name: apply_schema(name, frame) for name, frame in frames.items()}


@pytest.fixture(scope="module")
def config() -> dict:
    return load_config()


@pytest.fixture(scope="module")
def seed_frames() -> dict[str, pd.DataFrame]:
    return clean()


@pytest.fixture(scope="module")
def trio_career(config) -> pd.DataFrame:
    return score(fixture_frames(), config, "career")


@pytest.fixture(scope="module")
def trio_peak(config) -> pd.DataFrame:
    return score(fixture_frames(), config, "peak")


@pytest.fixture(scope="module")
def seed_career(config, seed_frames) -> pd.DataFrame:
    return score(seed_frames, config, "career")


@pytest.fixture(scope="module")
def seed_peak(config, seed_frames) -> pd.DataFrame:
    return score(seed_frames, config, "peak")


# ---------------------------------------------------------------- invariant 1
@pytest.mark.parametrize("frames_maker", [fixture_frames, clean])
def test_determinism(config, frames_maker):
    """§10.1: same input + same config -> byte-identical output."""
    first = score(frames_maker(), config, "career")
    second = score(frames_maker(), config, "career")
    pd.testing.assert_frame_equal(first, second, check_exact=True)
    assert first.to_csv(index=False) == second.to_csv(index=False)


def test_connections_are_single_threaded(config):
    """Locks the SET threads = 1 half of the §10.1 guarantee on BOTH layers:
    the original T7 wobble came from multi-threaded partial-sum scheduling,
    which within-process repetition alone cannot reliably reproduce."""
    assert (
        int(_connect().execute("SELECT current_setting('threads')").fetchone()[0]) == 1
    )
    con = _execute_transforms(fixture_frames(), config, "career")
    assert int(con.execute("SELECT current_setting('threads')").fetchone()[0]) == 1


def test_cross_process_determinism():
    """Two separate OS processes must byte-agree — the shape of the original
    T7 failure (comp_accolades wobbling ~7e-15 across processes), which
    same-process repetition does not exercise (Codex T7 review test 6)."""
    digests = []
    for _ in range(2):
        subprocess.run(
            [sys.executable, "-m", "pipeline.score"], check=True, capture_output=True
        )
        parquet = Path("data/marts/mart_final_scores.parquet").read_bytes()
        digests.append(hashlib.sha256(parquet).hexdigest())
    assert digests[0] == digests[1]


def test_shuffled_input_order_bounded_and_resortable(config, seed_frames, seed_career):
    """The clean.py key-sort is a real determinism dependency: DuckDB float
    aggregation follows input row order (logically-identical shuffled frames
    drift derived cells ~1e-13 — Codex T7 review). Two assertions close the
    loop with test_clean.py::test_clean_output_key_sorted: shuffled input
    stays semantically identical (same ranks, scores within 1e-9), and
    re-sorting by the clean.py keys restores BYTE-identical output — so
    clean()'s sort is sufficient protection, not luck."""
    shuffled = {
        name: frame.sample(frac=1, random_state=7).reset_index(drop=True)
        for name, frame in seed_frames.items()
    }
    drifted = score(shuffled, config, "career")
    assert list(drifted["player_id"]) == list(seed_career["player_id"])
    # rtol=0: pandas' default relative tolerance would let ~5e-4 drift pass
    # at score magnitude 100 (Codex T7 re-review) — pin drift to 1e-9 absolute.
    pd.testing.assert_frame_equal(
        drifted, seed_career, check_exact=False, atol=1e-9, rtol=0
    )
    resorted = {
        name: frame.sort_values(TABLE_KEYS[name]).reset_index(drop=True)
        for name, frame in shuffled.items()
    }
    pd.testing.assert_frame_equal(
        score(resorted, config, "career"), seed_career, check_exact=True
    )


# ---------------------------------------------------------- invariants 2 + 4
@pytest.mark.parametrize(
    "scored_fixture", ["trio_career", "trio_peak", "seed_career", "seed_peak"]
)
def test_bounds_and_no_nan(scored_fixture, request):
    """§10.2 and §10.4 on both pools, both scopes."""
    scored = request.getfixturevalue(scored_fixture)
    values = scored[
        [c for c in scored.columns if c.startswith("comp_")] + ["goat_score"]
    ]
    assert values.notna().all().all()
    assert ((values >= 0.0) & (values <= 100.0)).all().all()
    assert scored[["rank", "player_id", "player_name", "scope"]].notna().all().all()


# ---------------------------------------------------------------- invariant 3
def _mutate(config, section, key, value):
    mutated = copy.deepcopy(config)
    if value is None:
        del mutated[section][key]
    else:
        mutated[section][key] = value
    return mutated


@pytest.mark.parametrize(
    ("section", "key", "value", "message"),
    [
        ("weights", "peak", 0.26, "sum to exactly 1.0"),
        # 5e-10 drift passed the old 1e-9 tolerance and silently shifted
        # scores at the 10th decimal (Codex T7 review test 4) — the "exactly
        # 1.0" contract is now verified at 1e-12 (float-representation noise
        # of a decimal-exact vector is ~1e-16).
        ("weights", "peak", 0.2500000005, "sum to exactly 1.0"),
        ("weights", "peak", -0.25, ">= 0"),
        ("weights", "peak", float("nan"), ">= 0"),
        ("weights", "longevity", None, "keys must be"),
        ("weights", "bonus", 0.0, "keys must be"),
        ("winning_impact_blend", "ws48", 0.6, "sum to exactly 1.0"),
        ("efficiency_blend", "ts", 0.5, "keys must be"),
        ("accolade_weights", "mvp", 0.31, "sum to exactly 1.0"),
        ("accolade_weights", "dpoy", 0.0, "> 0"),
    ],
)
def test_bad_weight_configs_raise(config, section, key, value, message):
    """§10.3: the engine errors on any malformed weight block — it never
    silently renormalizes."""
    with pytest.raises(ValueError, match=message):
        _validate_scoring_config(_mutate(config, section, key, value))


def test_default_config_is_valid(config):
    _validate_scoring_config(config)


def test_career_weights_pass_through(config):
    assert _effective_weights(config, "career") == {
        c: float(w) for c, w in config["weights"].items()
    }


def test_peak_weights_renormalize_to_one(config):
    """§7: longevity dropped, the rest scaled by 1/(1 − w_longevity)."""
    weights = _effective_weights(config, "peak")
    assert weights["longevity"] == 0.0
    assert abs(sum(weights.values()) - 1.0) <= WEIGHT_SUM_ATOL
    kept = 1.0 - config["weights"]["longevity"]
    assert weights["peak"] == pytest.approx(config["weights"]["peak"] / kept)


def test_full_longevity_weight_cannot_use_peak_scope(config):
    lopsided = copy.deepcopy(config)
    lopsided["weights"] = {c: 0.0 for c in lopsided["weights"]} | {"longevity": 1.0}
    with pytest.raises(ValueError, match="longevity weight < 1.0"):
        _effective_weights(lopsided, "peak")


@pytest.mark.parametrize("scope", ["", "both", "Career"])
def test_invalid_scope_raises(config, scope):
    with pytest.raises(ValueError, match="scope"):
        score(fixture_frames(), config, scope)


@pytest.mark.parametrize("bad", [None, float("nan"), 123, {1: "v1"}, [], "", "   "])
def test_method_version_schema_rejected(config, bad):
    """§10.6: every row echoes method_version verbatim — a None/NaN/mapping
    would stamp garbage provenance on numerically-valid scores (Codex T7
    review test 3), so the schema is enforced before anything runs."""
    mutated = copy.deepcopy(config)
    mutated["method_version"] = bad
    with pytest.raises(ValueError, match="method_version"):
        _validate_scoring_config(mutated)


def test_missing_method_version_rejected(config):
    mutated = copy.deepcopy(config)
    del mutated["method_version"]
    with pytest.raises(ValueError, match="method_version"):
        _validate_scoring_config(mutated)


def test_dpoy_only_accolade_vector_rejected(config):
    """Codex T7 review fix 2c: all weight on DPOY validates by sum alone, but
    gives every pre-1983 player a zero eligible-weight denominator. Accolade
    weights must be strictly positive, rejected at validation time — not
    caught downstream by the NaN guard."""
    mutated = copy.deepcopy(config)
    mutated["accolade_weights"] = dict.fromkeys(mutated["accolade_weights"], 0.0)
    mutated["accolade_weights"]["dpoy"] = 1.0
    with pytest.raises(ValueError, match="> 0"):
        _validate_scoring_config(mutated)


# ---------------------------------------------------------------- invariant 5
def _bump(frames, table, player_id, season, column, delta):
    frame = frames[table]
    mask = (frame["player_id"] == player_id) & (frame["season"] == season)
    assert mask.sum() == 1
    frame.loc[mask, column] += delta


def _add_ring(frames, player_id, season):
    row = pd.DataFrame([{"player_id": player_id, "season": season, "award": "ring"}])
    frames["accolades"] = apply_schema(
        "accolades", pd.concat([frames["accolades"], row], ignore_index=True)
    )


MONOTONICITY_CASES = {
    # component -> (player_id, perturbation). Each player is strictly inside
    # the pool range on the affected sub-metric (worksheet §5 table), so the
    # component must STRICTLY increase (v1.md §10.5).
    "peak": (3, lambda f: _bump(f, "player_seasons", 3, 1999, "trb", 50)),
    "longevity": (2, lambda f: _bump(f, "player_seasons", 2, 1971, "trb", 100)),
    "winning_impact": (
        3,
        lambda f: _bump(f, "player_seasons", 3, 2000, "team_srs", 0.5),
    ),
    "playoff": (2, lambda f: _bump(f, "player_seasons", 2, 1970, "po_trb", 50)),
    "accolades": (3, lambda f: _add_ring(f, 3, 2000)),
    # PlayerA is interior on REL_TS (62.5) but the unique spi_career minimum:
    # the efficiency component must still strictly rise through its TS half.
    # pts and fgm move together to keep the points-identity contract green.
    "efficiency": (
        1,
        lambda f: (
            _bump(f, "player_seasons", 1, 1990, "pts", 2),
            _bump(f, "player_seasons", 1, 1990, "fgm", 1),
        ),
    ),
}


@pytest.mark.parametrize(("component", "case"), MONOTONICITY_CASES.items())
def test_component_monotonicity_strict(config, trio_career, component, case):
    player_id, perturb = case
    frames = fixture_frames()
    perturb(frames)
    bumped = score(frames, config, "career").set_index("player_id")
    base = trio_career.set_index("player_id")
    assert (
        bumped.loc[player_id, f"comp_{component}"]
        > base.loc[player_id, f"comp_{component}"]
    )


def test_component_monotonicity_weak_at_pool_max(config, trio_career):
    """§10.5 boundary: PlayerB is the Peak pool max; raising his SPI moves the
    max anchor with him and his Peak component stays exactly 100."""
    frames = fixture_frames()
    _bump(frames, "player_seasons", 2, 1971, "trb", 100)
    bumped = score(frames, config, "career").set_index("player_id")
    assert bumped.loc[2, "comp_peak"] == 100.0
    assert trio_career.set_index("player_id").loc[2, "comp_peak"] == 100.0


# ---------------------------------------------------------------- invariant 6
@pytest.mark.parametrize("scored_fixture", ["trio_career", "seed_peak"])
def test_version_tags(config, scored_fixture, request):
    """§10.6: method_version echoed verbatim from config; git_sha attached."""
    scored = request.getfixturevalue(scored_fixture)
    assert (scored["method_version"] == config["method_version"]).all()
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert (scored["git_sha"] == head).all()


# ------------------------------------------------------- engine self-guards
def _tamper(scored, mutate):
    broken = scored.copy()
    mutate(broken)
    return broken


@pytest.mark.parametrize(
    ("message", "mutate"),
    [
        ("expected 3", lambda s: s.drop(s.index[0], inplace=True)),
        ("unique positions", lambda s: s.__setitem__("rank", [1, 1, 3])),
        ("NaN", lambda s: s.__setitem__("comp_peak", [float("nan"), 50.0, 50.0])),
        ("outside", lambda s: s.__setitem__("comp_peak", [101.0, 50.0, 50.0])),
    ],
)
def test_guard_raises_on_violation(trio_career, message, mutate):
    """The engine polices its own §10 output before anything consumes it."""
    with pytest.raises(ScoringError, match=message):
        _guard(_tamper(trio_career, mutate), 3)


def test_unknown_award_in_marts_refused(config):
    """An award key with no configured weight would be silently dropped by
    the accolades JOIN — the engine must refuse instead (via the seam;
    transform's parity checks make this unreachable through score())."""
    marts = run_transforms(fixture_frames(), config, "career")
    rates = marts["mart_player_award_rates"].copy()
    rates.loc[rates.index[0], "award"] = "sixth_moty"
    marts["mart_player_award_rates"] = rates
    with pytest.raises(ScoringError, match="no configured weight"):
        _score_marts(marts, config, "career")


def test_incomplete_award_grid_refused(config):
    """A missing player × award grid row would silently renormalize the §5.5
    mix over the wrong eligible set — the engine must refuse instead."""
    marts = run_transforms(fixture_frames(), config, "career")
    marts["mart_player_award_rates"] = marts["mart_player_award_rates"].iloc[:-1]
    with pytest.raises(ScoringError, match="expected 18"):
        _score_marts(marts, config, "career")


# ------------------------------------------- near-degenerate anchor policy
def test_near_constant_ws48_pool_refused(config, seed_frames):
    """Codex T7 review demo 1: ws proportional to mp makes every player's
    WS48 identical up to float noise (raw pool span ~5.6e-17). The old code
    min–max-stretched that noise into a full 0–100 spread (13 of 20 ranks
    changed; an 11.7-point final swing). The engine must refuse to rank on
    noise — contract-valid or not."""
    frames = {name: frame.copy() for name, frame in seed_frames.items()}
    seasons = frames["player_seasons"]
    frames["player_seasons"] = seasons.assign(ws=seasons["mp"] * 0.1 / 48)
    with pytest.raises(ScoringError, match="ws48.*corrupted input"):
        score(frames, config, "career")


def test_uniform_zero_anchors_refused(config, seed_frames):
    """Codex T7 review demo 2: all-zero ws and team_srs are contract-valid
    row by row, but an all-equal CONTINUOUS pool is corruption-shaped (real
    per-possession careers are never bit-identical) — refused, instead of
    silently flowing 50s through the degenerate rule and re-ranking 12 of 20
    players. Award rates keep the §6 exact-tie -> 50 path (trio-exercised)."""
    frames = {name: frame.copy() for name, frame in seed_frames.items()}
    frames["player_seasons"] = frames["player_seasons"].assign(ws=0.0, team_srs=0.0)
    with pytest.raises(ScoringError, match="corrupted input"):
        score(frames, config, "career")


def _tiny_srs_frames(seed_frames, reverse: bool) -> dict[str, pd.DataFrame]:
    """Each player gets a constant career team_srs from ±1e-15 — pure float
    noise straddling zero (the re-review's severe demo). `reverse` flips
    which player gets which tiny value."""
    frames = {name: frame.copy() for name, frame in seed_frames.items()}
    seasons = frames["player_seasons"]
    players = sorted(seasons["player_id"].unique())
    step = 2e-15 / (len(players) - 1)
    tiny = {p: -1e-15 + i * step for i, p in enumerate(players)}
    if reverse:
        tiny = dict(zip(tiny, reversed(list(tiny.values()))))
    frames["player_seasons"] = seasons.assign(team_srs=seasons["player_id"].map(tiny))
    return frames


def test_tiny_symmetric_srs_refused(config, seed_frames):
    """The re-review's SEVERE finding: with both anchors near zero, a purely
    RELATIVE threshold shrinks to nothing — srs_w spanning ±1e-15 passed the
    round-2 guard, and reversing which player got which noise value flipped
    18 of 20 ranks (max final swing 10.0). The absolute floor
    (NEAR_DEGENERATE_ATOL) must refuse this pool."""
    with pytest.raises(ScoringError, match="srs_w.*corrupted input"):
        score(_tiny_srs_frames(seed_frames, reverse=False), config, "career")


def test_tiny_srs_reversal_cannot_change_output(config, seed_frames):
    """Reversing the tiny values must not change scores or ranks. With the
    absolute floor, that guarantee holds the strongest possible way: NEITHER
    ordering scores at all — noise cannot decide anything because noise is
    refused as input."""
    for reverse in (False, True):
        with pytest.raises(ScoringError, match="srs_w"):
            score(_tiny_srs_frames(seed_frames, reverse=reverse), config, "career")


def test_noise_thin_threshold_pinned_exactly():
    """Pins _noise_thin's formula shape (Codex round-3 test 1): the exact
    boundary is inclusive (range == threshold IS noise-thin, so <=, not <),
    one float step above it is ACCEPTED (no longer noise-thin), and BOTH
    terms are load-bearing — dropping atol reopens the straddling-zero
    severe finding, dropping rtol reopens the round-1 large-magnitude
    noise finding."""
    # Float fixed point t = atol + rtol*t — the exact threshold for lo=0.
    t = NEAR_DEGENERATE_ATOL
    for _ in range(100):
        t = NEAR_DEGENERATE_ATOL + NEAR_DEGENERATE_RTOL * t
    assert t == NEAR_DEGENERATE_ATOL + NEAR_DEGENERATE_RTOL * t  # converged
    assert _noise_thin(0.0, t)  # range == threshold exactly: <= must hold
    # one ulp above the threshold: no longer noise-thin -> accepted
    assert not _noise_thin(0.0, math.nextafter(t, math.inf))
    # atol term alone: scale 5e-13 makes the rtol term ~5e-22 — a pure-rtol
    # threshold would pass this straddling-zero noise (the severe finding).
    assert _noise_thin(-5e-13, 5e-13)
    # rtol term alone: at scale ~1.0 a pure-atol threshold (1e-12) would
    # pass this 1e-10 noise range.
    assert _noise_thin(1.0, 1.0 + 1e-10)
    # Above threshold at scale ~1.0: real spread must NOT be refused.
    assert not _noise_thin(1.0, 1.0 + 1e-8)
    # Genuine signal (PlayerA's playoff anchors from the worksheet).
    assert not _noise_thin(9.25, 50.833333333333336)


def test_refusal_precedes_scoring_connection(config, seed_frames, monkeypatch):
    """Codex round-4: mechanically prove the input-domain refusal fires
    BEFORE any scoring SQL connection opens — not merely before output is
    produced. _connect is patched to explode; the corrupted pool must still
    be refused with the anchor-range ScoringError, never reaching DuckDB.
    (The transform layer's own connection is untouched — the guard needs the
    marts it builds.)"""

    def no_connection_allowed():
        raise AssertionError("scoring connection opened before the refusal")

    monkeypatch.setattr("pipeline.score._connect", no_connection_allowed)
    frames = {name: frame.copy() for name, frame in seed_frames.items()}
    frames["player_seasons"] = frames["player_seasons"].assign(ws=0.0, team_srs=0.0)
    with pytest.raises(ScoringError, match="corrupted input"):
        score(frames, config, "career")


def test_peak_scope_ignores_longevity_degeneracy(config):
    """Codex T7 re-review: peak scope drops Longevity (zero weight, absent
    from output), so a degenerate longevity_raw pool must NOT reject a
    peak-scope run — while the identical tamper in career scope must."""
    peak_marts = run_transforms(fixture_frames(), config, "peak")
    peak_marts["mart_player_component_inputs"] = peak_marts[
        "mart_player_component_inputs"
    ].assign(longevity_raw=1.0)
    scored = _score_marts(peak_marts, config, "peak")
    assert len(scored) == 3 and "comp_longevity" not in scored.columns

    career_marts = run_transforms(fixture_frames(), config, "career")
    career_marts["mart_player_component_inputs"] = career_marts[
        "mart_player_component_inputs"
    ].assign(longevity_raw=1.0)
    with pytest.raises(ScoringError, match="longevity_raw"):
        _score_marts(career_marts, config, "career")


# ------------------------------------------------------------- pairwise view
def test_pairwise_worksheet_verdict(trio_career):
    """Worksheet §6: B beats A 57.58 to 49.63; higher side per component."""
    result = pairwise(trio_career, "PlayerB", "PlayerA")
    assert result["verdict"] == "PlayerB"
    assert result["scope"] == "career"
    assert result["margin"] == pytest.approx(7.959, abs=1e-3)
    assert result["final"]["PlayerA"] == pytest.approx(49.63, abs=0.01)
    assert {c: d["higher"] for c, d in result["components"].items()} == {
        "peak": "PlayerB",
        "winning_impact": "PlayerA",
        "playoff": "PlayerA",
        "accolades": "PlayerB",
        "efficiency": "PlayerB",
        "longevity": "PlayerA",
    }


def test_pairwise_accepts_player_ids(trio_career):
    by_id = pairwise(trio_career, 2, 1)
    by_name = pairwise(trio_career, "PlayerB", "PlayerA")
    assert by_id == by_name


def test_pairwise_peak_scope_has_no_longevity(trio_peak):
    result = pairwise(trio_peak, "PlayerC", "PlayerA")
    assert result["scope"] == "peak"
    assert result["verdict"] == "PlayerC"  # worksheet §7: C overtakes A
    assert "longevity" not in result["components"]


def test_pairwise_unknown_player_raises(trio_career):
    with pytest.raises(ValueError, match="matched 0 rows"):
        pairwise(trio_career, "PlayerZ", "PlayerA")


def test_pairwise_rejects_mixed_scope(trio_career, trio_peak):
    """A career/peak hybrid frame is not one engine run — its numbers are not
    comparable and the scope label would be a lie (Codex T7 review test 9)."""
    hybrid = pd.concat([trio_career, trio_peak], ignore_index=True)
    with pytest.raises(ValueError, match="mixes scope"):
        pairwise(hybrid, "PlayerA", "PlayerB")


def test_pairwise_rejects_mixed_method_version(trio_career):
    hybrid = trio_career.copy()
    hybrid.loc[hybrid.index[0], "method_version"] = "v2"
    with pytest.raises(ValueError, match="mixes method_version"):
        pairwise(hybrid, "PlayerA", "PlayerB")


def test_pairwise_rejects_self_comparison(trio_career):
    with pytest.raises(ValueError, match="themself"):
        pairwise(trio_career, "PlayerA", 1)  # name and id, same player


def test_pairwise_component_tie_and_final_tie():
    """Exact ties: component reports 'tie'; the verdict follows rank, which
    encodes the §6 tie-break (peak desc, then player_id asc)."""
    scores = pd.DataFrame(
        {
            "rank": [1, 2],
            "player_id": [7, 9],
            "player_name": ["X", "Y"],
            "goat_score": [50.0, 50.0],
            "comp_peak": [60.0, 60.0],
            "scope": ["career", "career"],
            "method_version": ["v1", "v1"],
        }
    )
    result = pairwise(scores, "Y", "X")
    assert result["verdict"] == "X"
    assert result["margin"] == 0.0
    assert result["components"]["peak"]["higher"] == "tie"


# ------------------------------------------------- real-seed oracle + shape
def _minmax(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(50.0, index=series.index)
    return 100.0 * ((series - lo) / (hi - lo))


def _oracle(marts, config, scope):
    """Independent §6 re-implementation from raw config — no engine imports."""
    inputs = marts["mart_player_component_inputs"].set_index("player_id")
    components = pd.DataFrame(index=inputs.index)
    components["peak"] = _minmax(inputs["peak_raw"])
    components["longevity"] = _minmax(inputs["longevity_raw"])
    components["playoff"] = _minmax(inputs["playoff_raw"])
    wib = config["winning_impact_blend"]
    components["winning_impact"] = wib["ws48"] * _minmax(inputs["ws48"]) + wib[
        "team_srs"
    ] * _minmax(inputs["srs_w"])
    eff = config["efficiency_blend"]
    components["efficiency"] = eff["ts_rel"] * _minmax(inputs["rel_ts_career"]) + eff[
        "spi_career"
    ] * _minmax(inputs["spi_career"])
    rated = marts["mart_player_award_rates"].dropna(subset=["rate"]).copy()
    rated["mm"] = rated.groupby("award")["rate"].transform(_minmax)
    rated["w"] = rated["award"].map(config["accolade_weights"])
    components["accolades"] = rated.groupby("player_id").apply(
        lambda g: (g["w"] * g["mm"]).sum() / g["w"].sum(), include_groups=False
    )
    weights = dict(config["weights"])
    if scope == "peak":
        kept = 1.0 - weights.pop("longevity")
        weights = {c: w / kept for c, w in weights.items()}
        components = components.drop(columns=["longevity"])
    final = sum(w * components[c] for c, w in weights.items())
    return components, final


@pytest.mark.parametrize("scope", ["career", "peak"])
@pytest.mark.parametrize("frames_maker", [fixture_frames, clean, seven_season_frames])
def test_engine_matches_independent_oracle(config, frames_maker, scope):
    frames = frames_maker()
    engine = score(frames, config, scope).set_index("player_id")
    components, final = _oracle(run_transforms(frames, config, scope), config, scope)
    # the engine is rank-ordered, the oracle mart-ordered — align on player_id
    components, final = components.loc[engine.index], final.loc[engine.index]
    for name in components.columns:
        pd.testing.assert_series_equal(
            engine[f"comp_{name}"],
            components[name],
            check_names=False,
            atol=1e-9,
            rtol=0,
        )
    pd.testing.assert_series_equal(
        engine["goat_score"], final, check_names=False, atol=1e-9, rtol=0
    )
    order = (
        components.assign(final=final)
        .sort_values(["final", "peak", "player_id"], ascending=[False, False, True])
        .index
    )
    assert list(engine.index) == list(order)
    assert list(engine["rank"]) == list(range(1, len(engine) + 1))


def test_seed_scores_shape(seed_career):
    assert len(seed_career) == 20
    assert list(seed_career["rank"]) == list(range(1, 21))
    assert seed_career["player_name"].is_unique


def test_seven_season_peak_scope_end_to_end(config):
    """Codex T7 review test 7: nontrivial §7 window restriction proven at the
    SCORING level, not only in the transform suite. X's awards and playoff
    runs all sit in his two worst seasons — outside his top-5-by-SPI window:

    - Playoff flips 100/0 between scopes: career counts X's two runs (raw 33
      vs Y's 12.97); the peak window contains none of them (raw 0 vs 12.97).
    - Accolades: career MVP rate X 2/7 vs Y 0 -> MM 100 vs 0, so X scores
      exactly .30*100 + .70*50 = 65 (every other award is an exact 0-0 tie
      -> degenerate 50); in peak scope X's window MVP rate is 0/5, tying Y's
      0/3 -> the MVP element also degenerates to 50, leaving both players at
      exactly 50 across the whole accolade mix."""
    frames = seven_season_frames()
    career = score(frames, config, "career").set_index("player_id")
    peak = score(frames, config, "peak").set_index("player_id")
    assert career.loc[SEVEN_X, "comp_playoff"] == 100.0
    assert career.loc[SEVEN_Y, "comp_playoff"] == 0.0
    assert peak.loc[SEVEN_X, "comp_playoff"] == 0.0
    assert peak.loc[SEVEN_Y, "comp_playoff"] == 100.0
    assert career.loc[SEVEN_X, "comp_accolades"] == pytest.approx(65.0, abs=1e-9)
    assert career.loc[SEVEN_Y, "comp_accolades"] == pytest.approx(35.0, abs=1e-9)
    assert peak.loc[SEVEN_X, "comp_accolades"] == pytest.approx(50.0, abs=1e-9)
    assert peak.loc[SEVEN_Y, "comp_accolades"] == pytest.approx(50.0, abs=1e-9)
    assert "comp_longevity" not in peak.columns


# ------------------------------------------- West cameo regression (from T6)
def test_west_cameo_marginal_impact(config, seed_frames, seed_career):
    """Deferred from T6 (qa/validation_log.md): remove ONLY Jerry West's 1967
    one-minute playoff cameo and his final score drops by ~0.05217 (the T6
    reviewer's independent derivation) — the documented cameo bound holds
    end-to-end through min–max scaling and weights. West is interior on
    playoff_raw, so no anchor moves: every other row must be bit-identical."""
    frames = {name: frame.copy() for name, frame in seed_frames.items()}
    seasons = frames["player_seasons"]
    mask = (seasons["player_id"] == WEST) & (seasons["season"] == WEST_SEASON)
    assert mask.sum() == 1
    seasons.loc[mask, PO_COLUMNS] = pd.NA
    without_cameo = score(frames, config, "career")

    base_west = seed_career.loc[seed_career["player_id"] == WEST].iloc[0]
    new_west = without_cameo.loc[without_cameo["player_id"] == WEST].iloc[0]
    assert base_west["goat_score"] - new_west["goat_score"] == pytest.approx(
        0.05217, abs=1e-4
    )
    assert base_west["rank"] == new_west["rank"]
    pd.testing.assert_frame_equal(
        seed_career[seed_career["player_id"] != WEST].reset_index(drop=True),
        without_cameo[without_cameo["player_id"] != WEST].reset_index(drop=True),
        check_exact=True,
    )
