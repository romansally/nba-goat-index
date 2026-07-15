"""Golden snapshot guard (T7) — CLAUDE.md Rule 5, PRD T7 acceptance 3.

Two independent references pin the engine, on purpose:

1. The HAND WORKSHEET (docs/methodology/v1_hand_worksheet.md §6–§7), whose
   final scores are typed below as literals. They were hand-computed in T2,
   before any transform or scoring code existed — an engine bug cannot leak
   into them. Tolerance ±0.01 (worksheet §8).
2. The committed golden snapshot tests/golden/v1_scores.json — full-precision
   engine output locked at T7, compared at 1e-9 (the regression pin).

The snapshot also stores a hash of the parsed config: ANY scoring-config
value change (a weight, a blend, an intro season) fails the guard
structurally, not just numerically — regenerating the snapshot without a
method_version bump is a blocked merge (CLAUDE.md Rule 5).
"""

import json

import pytest

from pipeline.clean import apply_schema
from pipeline.contracts import load_fixtures
from pipeline.golden import config_sha256, golden_path, golden_payload
from pipeline.score import score
from pipeline.transform import load_config

# Locked hand-worksheet finals (§6 career, §7 peak) — NOT read from the
# golden file, so a bad regeneration cannot silently drift from the
# hand-derived reference. Order = expected ranking.
WORKSHEET_FINALS = {
    "career": [("PlayerB", 57.58), ("PlayerA", 49.63), ("PlayerC", 45.28)],
    "peak": [("PlayerB", 58.49), ("PlayerC", 50.32), ("PlayerA", 44.03)],
}
# Worksheet §5–§6 career component values (4-decimal intermediates).
WORKSHEET_COMPONENTS = {
    "PlayerA": {
        "peak": 0.0,
        "winning_impact": 50.0,
        "playoff": 100.0,
        "accolades": 52.5,
        "efficiency": 31.25,
        "longevity": 100.0,
    },
    "PlayerB": {
        "peak": 100.0,
        "winning_impact": 28.0864,
        "playoff": 45.0902,
        "accolades": 52.7027,
        "efficiency": 50.0,
        "longevity": 49.4505,
    },
    "PlayerC": {
        "peak": 62.5,
        "winning_impact": 71.4286,
        "playoff": 0.0,
        "accolades": 37.5,
        "efficiency": 81.2405,
        "longevity": 0.0,
    },
}


def fixture_frames():
    return {n: apply_schema(n, f) for n, f in load_fixtures("valid").items()}


@pytest.fixture(scope="module")
def golden() -> dict:
    return json.loads(golden_path(load_config()).read_text())


@pytest.fixture(scope="module")
def live_payload() -> dict:
    """The engine's own golden payload, computed fresh from the live config."""
    return golden_payload(load_config())


@pytest.mark.parametrize("scope", ["career", "peak"])
def test_engine_reproduces_hand_worksheet(scope):
    scored = score(fixture_frames(), load_config(), scope)
    expected = WORKSHEET_FINALS[scope]
    assert list(scored["player_name"]) == [name for name, _ in expected]
    for (name, final), row in zip(expected, scored.itertuples(), strict=True):
        assert row.goat_score == pytest.approx(final, abs=0.01), (name, scope)


def test_engine_reproduces_worksheet_components():
    scored = score(fixture_frames(), load_config(), "career")
    for row in scored.itertuples():
        for component, value in WORKSHEET_COMPONENTS[row.player_name].items():
            got = getattr(row, f"comp_{component}")
            assert got == pytest.approx(value, abs=1e-4), (row.player_name, component)


def test_golden_matches_live_engine(golden, live_payload):
    """The regression pin: every committed value, at full precision."""
    assert live_payload["method_version"] == golden["method_version"]
    assert live_payload["config_sha256"] == golden["config_sha256"]
    assert set(live_payload["scopes"]) == set(golden["scopes"]) == {"career", "peak"}
    for scope in ("career", "peak"):
        live, locked = live_payload["scopes"][scope], golden["scopes"][scope]
        assert len(live) == len(locked)
        for got, want in zip(live, locked, strict=True):
            assert got["rank"] == want["rank"]
            assert got["player_id"] == want["player_id"]
            assert got["player_name"] == want["player_name"]
            assert got["goat_score"] == pytest.approx(want["goat_score"], abs=1e-9)
            assert set(got["components"]) == set(want["components"])
            for component, value in want["components"].items():
                assert got["components"][component] == pytest.approx(value, abs=1e-9), (
                    want["player_name"],
                    scope,
                    component,
                )


def test_golden_config_hash_matches_live_config(golden):
    assert config_sha256(load_config()) == golden["config_sha256"], (
        "config/scoring_v1.yaml values changed since the golden snapshot was "
        "locked — that is a behavior-changing scoring change: follow the v2 "
        "path (methodology doc + ADR + method_version bump + regenerate, "
        "CLAUDE.md Rules 1 and 5)"
    )


def test_golden_file_agrees_with_worksheet(golden):
    """File-level cross-check: the committed snapshot itself must match the
    hand-derived constants, so the two references can never diverge."""
    for scope, expected in WORKSHEET_FINALS.items():
        rows = golden["scopes"][scope]
        assert [r["player_name"] for r in rows] == [name for name, _ in expected]
        for (name, final), row in zip(expected, rows, strict=True):
            assert row["goat_score"] == pytest.approx(final, abs=0.01), (name, scope)


def test_config_hash_is_canonical():
    """Key order must not matter (comment/format edits never break the hash);
    any value change must."""
    config = load_config()
    reordered = dict(reversed(list(config.items())))
    assert config_sha256(reordered) == config_sha256(config)
    tweaked = json.loads(json.dumps(config))
    tweaked["weights"]["peak"] += 0.01
    assert config_sha256(tweaked) != config_sha256(config)


def test_peak_scope_has_no_longevity_component(golden):
    """v1.md §7: Longevity is dropped from the peak mix, not zero-weighted."""
    for row in golden["scopes"]["peak"]:
        assert "longevity" not in row["components"]
    for row in golden["scopes"]["career"]:
        assert "longevity" in row["components"]


def test_golden_filename_matches_method_version(golden):
    """The snapshot path is derived from method_version, so a v2 regeneration
    writes v2_scores.json instead of silently overwriting the locked v1 file
    (Codex T7 review); file content and filename must agree."""
    config = load_config()
    path = golden_path(config)
    assert path.name == f"{config['method_version']}_scores.json"
    assert golden["method_version"] == config["method_version"]
    assert path.name == f"{golden['method_version']}_scores.json"
    assert golden_path({"method_version": "v2"}).name == "v2_scores.json"


@pytest.mark.parametrize(
    "unsafe",
    ["../golden/v1", "v1/x", "/abs/path", "v0", "V1", "v1 ", "", "v01", 1],
)
def test_golden_path_rejects_unsafe_method_version(unsafe):
    """Codex T7 re-review: a path-like method_version ("../golden/v1")
    resolves back onto the existing v1 snapshot, defeating the round-1
    overwrite protection — only a bare v<N> token may name a golden file."""
    with pytest.raises(ValueError, match="method_version must match"):
        golden_path({"method_version": unsafe})


def test_golden_main_v2_writes_new_file_and_preserves_v1(tmp_path, monkeypatch):
    """Codex round-3 test 2: prove the FULL regeneration write path, not just
    the path derivation — a v2 run through pipeline.golden.main() must create
    v2_scores.json and leave the v1 snapshot byte-identical."""
    from pipeline import golden

    committed = golden.golden_path(load_config())
    v1_bytes = committed.read_bytes()
    (tmp_path / "v1_scores.json").write_bytes(v1_bytes)

    config_v2 = json.loads(json.dumps(load_config()))
    config_v2["method_version"] = "v2"
    monkeypatch.setattr(golden, "GOLDEN_DIR", tmp_path)
    monkeypatch.setattr(golden, "load_config", lambda: config_v2)
    golden.main()

    written = json.loads((tmp_path / "v2_scores.json").read_text())
    assert written["method_version"] == "v2"
    assert written["config_sha256"] == config_sha256(config_v2)
    # v1 copies untouched, in the temp dir AND the repo; and since only the
    # version string changed, the v2 SCORES must equal the locked v1 scores.
    assert (tmp_path / "v1_scores.json").read_bytes() == v1_bytes
    assert committed.read_bytes() == v1_bytes
    assert written["scopes"] == json.loads(v1_bytes.decode())["scopes"]


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda c: c["weights"].update({1: 0.0}), "non-string config key"),
        (lambda c: c.update({"method_version": {1: "v1"}}), "non-string config key"),
        (lambda c: c["weights"].update({"peak": float("nan")}), "non-finite"),
        (lambda c: c["weights"].update({"peak": float("inf")}), "non-finite"),
    ],
)
def test_config_hash_rejects_noncanonical(mutate, message):
    """{1: "v1"} and {"1": "v1"} would hash to identical bytes despite the
    engine treating them differently, and NaN/Inf serialize as nonstandard
    JSON — config_sha256 must refuse rather than collide (Codex T7 review)."""
    config = json.loads(json.dumps(load_config()))
    mutate(config)
    with pytest.raises(ValueError, match=message):
        config_sha256(config)
