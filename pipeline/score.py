"""Scoring engine driver (Tier-1 task T7): v1.md §6–§8 over the T6 marts.

The scoring math lives in sql/05_final_goat_scores.sql — one implementation
for career scope, peak scope, and Custom Mode alike (v1.md §12.9). This
driver validates the weighting-layer config (§10.3: error, never
renormalize), computes the effective weight vector for the scope (§7: peak
drops Longevity and redistributes w/(1−w_longevity)), injects weights into
DuckDB, executes the SQL, guards the output fail-loudly (§9), and stamps
provenance (§10.6).

score() is the public path and always runs the full validated chain
(contracts → transforms → scoring); _score_marts is the private seam for
tests that study scoring over pre-built marts, mirroring the transform-layer
seam pattern. Pairwise view: pipeline/compare.py. Golden snapshot
generation: pipeline/golden.py (version-bump-gated, CLAUDE.md Rule 5).

Complexity budget (CLAUDE.md Rule 6): slightly over 250 lines because two
Codex review rounds added defense-in-depth guards (award-grid parity,
anchor-range input domain, provenance schema) whose policy comments are
load-bearing; pairwise and golden tooling were already split out, and
splitting validation from the engine would fragment the one-scoring-path
story this file exists to tell.
"""

import subprocess
from pathlib import Path

import duckdb
import pandas as pd

from pipeline.clean import clean
from pipeline.transform import (
    MARTS_DIR,
    WEIGHT_SUM_ATOL,
    _finite_number,
    load_config,
    run_transforms,
)

SQL_FILE = Path("sql/05_final_goat_scores.sql")

# v1.md §8 weight order — also the component column order in every output.
COMPONENTS = "peak winning_impact playoff accolades efficiency longevity".split()
# Sub-blend config blocks -> (SQL variable prefix, required keys).
SUB_BLENDS = {
    "winning_impact_blend": ("wib", {"ws48", "team_srs"}),
    "efficiency_blend": ("eff", {"ts_rel", "spi_career"}),
}
# v1.md §9 input-domain rule (ADR-0002): a pool range at/below atol+rtol·scale
# carries no signal — min–max would stretch float residue into a 0–100 spread
# (review demos: a 5.6e-17 WS48 span swung 13/20 ranks; ±1e-15 SRS noise beat
# a purely RELATIVE threshold, hence the absolute floor). Real spans sit
# ≥ 0.09 absolute — 8+ orders above both terms.
NEAR_DEGENERATE_ATOL = 1e-12
NEAR_DEGENERATE_RTOL = 1e-9
RAW_COMPONENT_COLUMNS = (
    "peak_raw longevity_raw ws48 srs_w playoff_raw rel_ts_career spi_career".split()
)


class ScoringError(RuntimeError):
    """The engine produced an output that violates a v1.md §10 invariant."""


def _check_unit_weights(name: str, block: dict, positive: bool = False) -> None:
    """A weight block must be finite, non-negative (strictly positive where a
    zero could zero a renormalization denominator), and sum to 1.0 (§10.3)."""
    if any(
        not _finite_number(v) or v < 0 or (positive and v == 0) for v in block.values()
    ):
        bound = "> 0" if positive else ">= 0"
        raise ValueError(f"{name} values must be finite and {bound}: {block}")
    if abs(sum(block.values()) - 1.0) > WEIGHT_SUM_ATOL:
        raise ValueError(
            f"{name} must sum to exactly 1.0 (float64 tolerance "
            f"{WEIGHT_SUM_ATOL}), got {block}"
        )


def _validate_scoring_config(config: dict) -> None:
    """Reject a malformed weighting layer loudly — never silently renormalize.

    Covers the blocks the scoring layer consumes (v1.md §8) plus
    method_version (§10.6); transform._validate_config owns the
    objective-layer parameters T6 consumes."""
    version = config.get("method_version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError(f"method_version must be a non-empty string: {version!r}")
    weights = config["weights"]
    if set(weights) != set(COMPONENTS):
        raise ValueError(
            f"weights keys must be {sorted(COMPONENTS)}: {sorted(weights)}"
        )
    _check_unit_weights("weights", weights)
    for block_name, (_, expected) in SUB_BLENDS.items():
        block = config[block_name]
        if set(block) != expected:
            raise ValueError(
                f"{block_name} keys must be {sorted(expected)}: {sorted(block)}"
            )
        _check_unit_weights(block_name, block)
    # Key parity with the award registry is transform._validate_config's job.
    # Strictly positive: a zero weight could leave a player whose only
    # eligible awards are all zero-weighted with a 0/0 renormalization (§5.5;
    # Codex demo: a DPOY-only vector zeroes every pre-1983 denominator).
    _check_unit_weights("accolade_weights", config["accolade_weights"], positive=True)


def _effective_weights(config: dict, scope: str) -> dict[str, float]:
    """The §6 weight vector for career scope; the §7 renormalization for peak.

    Peak scope drops Longevity and redistributes its weight proportionally
    (w_c / (1 − w_longevity)); Longevity is injected as 0.0 so one SQL serves
    both scopes. The result must still sum to 1.0 — asserted, never repaired."""
    if scope not in ("career", "peak"):
        raise ValueError(f"scope must be 'career' or 'peak', got {scope!r}")
    weights = {c: float(config["weights"][c]) for c in COMPONENTS}
    if scope == "career":
        return weights
    kept = 1.0 - weights["longevity"]
    if kept <= 0:
        raise ValueError("peak scope requires a longevity weight < 1.0")
    renormalized = {c: w / kept for c, w in weights.items() if c != "longevity"}
    renormalized["longevity"] = 0.0
    if abs(sum(renormalized.values()) - 1.0) > WEIGHT_SUM_ATOL:
        raise ScoringError(f"peak-scope weights do not sum to 1.0: {renormalized}")
    return renormalized


def _git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _connect() -> duckdb.DuckDBPyConnection:
    """§10.1 byte-identical determinism: multi-threaded operators combine
    float partial sums in scheduling-dependent order (observed: comp_accolades
    wobbling ~7e-15 across runs). Serial execution is order-deterministic; at
    this data size the cost is nil. Input row order matters the same way —
    clean.py's key-sort is the other half of this guarantee (see its note)."""
    con = duckdb.connect()
    con.execute("SET threads = 1")
    return con


def _noise_thin(lo: float, hi: float) -> bool:
    scale = max(abs(lo), abs(hi))
    return hi - lo <= NEAR_DEGENERATE_ATOL + NEAR_DEGENERATE_RTOL * scale


def _check_anchor_ranges(marts: dict[str, pd.DataFrame], scope: str) -> None:
    """v1.md §9 input-domain rule (ADR-0002): refuse pools whose anchors carry
    no signal instead of stretching float noise across 0–100. Continuous
    inputs cannot legitimately tie pool-wide → zero/noise-thin ranges are
    invalid input. Award rates are discrete: exact ties keep the §6
    degenerate → 50.0 rule (worksheet-exercised); only an unequal-yet-noise-
    thin near-tie is refused. Peak scope exempts the dropped Longevity input."""
    inputs = marts["mart_player_component_inputs"]
    for column in RAW_COMPONENT_COLUMNS:
        if scope == "peak" and column == "longevity_raw":
            continue
        lo, hi = inputs[column].min(), inputs[column].max()
        if _noise_thin(lo, hi):
            raise ScoringError(
                f"{column}: pool range {hi - lo:.3e} is degenerate or within "
                "float noise — corrupted input, not signal (v1.md §9, ADR-0002)"
            )
    rates = marts["mart_player_award_rates"].dropna(subset=["rate"])
    for award, group in rates.groupby("award"):
        lo, hi = group["rate"].min(), group["rate"].max()
        if hi != lo and _noise_thin(lo, hi):
            raise ScoringError(
                f"award {award}: rate range {hi - lo:.3e} is a near-tie within "
                "float noise — corrupted input, not signal (v1.md §9, ADR-0002)"
            )


def _score_marts(
    marts: dict[str, pd.DataFrame], config: dict, scope: str
) -> pd.DataFrame:
    """Private seam: run sql/05 over pre-built marts (callers own validation
    and must pass the SAME scope the marts were transformed under)."""
    weights = _effective_weights(config, scope)
    # The accolades JOIN would silently drop an award key with no configured
    # weight; a missing grid row silently renormalizes the §5.5 mix. Upstream
    # guarantees both — refuse loudly anyway (the T6 silent-award-loss lesson).
    rates = marts["mart_player_award_rates"]
    unknown = set(rates["award"]) - set(config["accolade_weights"])
    if unknown:
        raise ScoringError(f"award(s) with no configured weight: {sorted(unknown)}")
    grid = len(marts["mart_player_component_inputs"]) * len(config["accolade_weights"])
    if len(rates) != grid:
        raise ScoringError(f"award-rate mart has {len(rates)} rows, expected {grid}")
    _check_anchor_ranges(marts, scope)
    con = _connect()
    con.register("mart_player_component_inputs", marts["mart_player_component_inputs"])
    con.register("mart_player_award_rates", marts["mart_player_award_rates"])
    for component, weight in weights.items():
        con.execute(f"SET VARIABLE w_{component} = {weight}")
    for block_name, (prefix, _) in SUB_BLENDS.items():
        for key, value in config[block_name].items():
            con.execute(f"SET VARIABLE {prefix}_{key} = {float(value)}")
    con.register(
        "param_accolade_weights",
        pd.DataFrame(config["accolade_weights"].items(), columns=["award", "weight"]),
    )
    con.execute(SQL_FILE.read_text())
    scored = con.execute("SELECT * FROM mart_final_scores ORDER BY rank").df()
    _guard(scored, len(marts["mart_player_component_inputs"]))
    if scope == "peak":
        # §7: Longevity is dropped from the peak-scope mix, not zero-weighted
        # in the output — a component that cannot count must not be shown.
        scored = scored.drop(columns=["comp_longevity"])
    scored["scope"] = scope
    scored["method_version"] = config["method_version"]
    scored["git_sha"] = _git_sha()
    return scored


def _guard(scored: pd.DataFrame, n_players: int) -> None:
    """Fail loudly on any §10 violation in the engine's own output (§9)."""
    if len(scored) != n_players:
        raise ScoringError(f"scored {len(scored)} players, expected {n_players}")
    if sorted(scored["rank"]) != list(range(1, n_players + 1)):
        raise ScoringError(f"ranks are not unique positions 1..{n_players}")
    values = scored[
        [c for c in scored.columns if c.startswith("comp_")] + ["goat_score"]
    ]
    if values.isna().any().any():
        raise ScoringError(f"NaN in scoring output:\n{values}")
    if ((values < 0.0) | (values > 100.0)).any().any():
        raise ScoringError(f"score outside [0, 100]:\n{values.describe()}")


def score(
    frames: dict[str, pd.DataFrame], config: dict, scope: str | None = None
) -> pd.DataFrame:
    """Clean frames -> validated transforms -> final GOAT scores (one call).

    run_transforms validates config and contracts unconditionally, so scoring
    can never run over unvalidated data; scope=None defers to config (the
    `is None` test keeps a falsey scope loud, as in run_transforms)."""
    _validate_scoring_config(config)
    resolved = config["scope"] if scope is None else scope
    marts = run_transforms(frames, config, resolved)
    return _score_marts(marts, config, resolved)


def main() -> None:
    scored = score(clean(), load_config())
    MARTS_DIR.mkdir(parents=True, exist_ok=True)
    path = MARTS_DIR / "mart_final_scores.parquet"
    scored.to_parquet(path, index=False)
    print(scored[["rank", "player_name", "goat_score"]].to_string(index=False))
    print(f"wrote {path} ({len(scored)} rows)")


if __name__ == "__main__":
    main()
