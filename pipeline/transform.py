"""Run the hand-written DuckDB SQL transforms (Tier-1 task T6).

Thin runner for the SQL layer: validates the config and the input frames
(Pandera contracts — the CLAUDE.md clean → validate → transform order is
enforced in-process, not assumed), registers the four cleaned tables as
DuckDB views, injects the objective-layer parameters from
config/scoring_v1.yaml (the single authoritative source — nothing tunable
is hardcoded in SQL), executes sql/01–04 in order, runs the named checks in
sql/06_validation_checks.sql (raising on any violation), and writes the
star-schema marts to data/marts/ sorted by key for deterministic output.

The scoring math itself (min–max scaling, blends, weights, final ranking)
is NOT here — it lands in T7 as sql/05_final_goat_scores.sql driven by
pipeline/score.py. Offline by design (CLAUDE.md Rule 2).
"""

from __future__ import annotations

import math
from pathlib import Path

import duckdb
import pandas as pd
import yaml

from pipeline.clean import PROCESSED_DIR, SCHEMAS
from pipeline.contracts import AWARD_INTRO, validate_all

CONFIG_PATH = Path("config/scoring_v1.yaml")
SQL_DIR = Path("sql")
MARTS_DIR = Path("data/marts")

TRANSFORM_FILES = [
    "01_create_schema.sql",
    "02_staging.sql",
    "03_player_season_metrics.sql",
    "04_scoring_components.sql",
]
CHECKS_FILE = "06_validation_checks.sql"

# Mart tables and their keys — the ORDER BY that makes outputs deterministic.
MART_KEYS: dict[str, list[str]] = {
    "dim_player": ["player_id"],
    "dim_season": ["season"],
    "fact_player_season": ["player_id", "season"],
    "fact_accolade": ["player_id", "season", "award"],
    "mart_player_season_metrics": ["player_id", "season"],
    "mart_player_component_inputs": ["player_id"],
    "mart_player_award_rates": ["player_id", "award"],
}

ALL_NBA_TEAM_NUMBERS = {"first": 1, "second": 2, "third": 3}

# "Sum to exactly 1.0" (v1.md §10.3) verified in float64 at 1e-12: a
# decimal-exact vector carries only ~1e-16 representation error, so anything
# past 1e-12 is real config drift, not rounding. One constant for every
# weight block — the production blend here and the scoring blocks in
# pipeline/score.py (Codex T7 re-review: production_blend had been left at
# the old 1e-9, admitting a demonstrated 5e-10 drift).
WEIGHT_SUM_ATOL = 1e-12


class TransformValidationError(RuntimeError):
    """A named check in 06_validation_checks.sql returned violations."""


def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text())


def _finite_number(value) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _validate_config(config: dict) -> None:
    """Reject a malformed config loudly (v1.md §10.3: error, never repair).

    The award-key parity check is the guard against silently dropped award
    facts: the rate mart is driven by accolade_intro_season, so a missing key
    would make that award vanish from scoring with every row-count check
    still green (Codex review, 2026-07-12)."""
    blend = config["production_blend"]
    if set(blend) != {"pts", "trb", "ast"}:
        raise ValueError(f"production_blend keys must be pts/trb/ast: {sorted(blend)}")
    if not all(_finite_number(v) and v >= 0 for v in blend.values()):
        raise ValueError(f"production_blend values must be finite and >= 0: {blend}")
    if abs(sum(blend.values()) - 1.0) > WEIGHT_SUM_ATOL:
        raise ValueError(f"production_blend must sum to 1.0, got {blend}")
    peak_n = config["peak_n"]
    if not isinstance(peak_n, int) or isinstance(peak_n, bool) or peak_n < 1:
        raise ValueError(f"peak_n must be a positive integer, got {peak_n!r}")
    peak_min_avail = config["peak_min_avail"]
    if not _finite_number(peak_min_avail) or not 0 <= peak_min_avail <= 1:
        raise ValueError(f"peak_min_avail must be in [0, 1], got {peak_min_avail!r}")
    points = config["all_nba_team_points"]
    if set(points) != set(ALL_NBA_TEAM_NUMBERS):
        raise ValueError(
            f"all_nba_team_points keys must be first/second/third: {sorted(points)}"
        )
    if not all(_finite_number(v) for v in points.values()):
        raise ValueError(f"all_nba_team_points values must be finite: {points}")
    # Domain, not just finiteness (Codex re-review): a zero would silently
    # null that team's contribution to every rate, a value above 1 breaks the
    # award_rate_in_bounds guarantee (rates are bounded by the max point
    # value), and a reversed ordering inverts the 1st > 2nd > 3rd semantics.
    if not 0 < points["third"] <= points["second"] <= points["first"] <= 1:
        raise ValueError(
            "all_nba_team_points must satisfy "
            f"0 < third <= second <= first <= 1, got {points}"
        )
    intro = config["accolade_intro_season"]
    if intro != AWARD_INTRO:
        raise ValueError(
            f"accolade_intro_season {intro} must exactly match the contract "
            f"award registry {AWARD_INTRO} — a missing or drifted award would "
            "silently drop its facts from the rate mart or mis-denominate it"
        )
    if set(config["accolade_weights"]) != set(intro):
        raise ValueError(
            f"accolade_weights keys {sorted(config['accolade_weights'])} must "
            f"match accolade_intro_season keys {sorted(intro)}"
        )


def _set_params(con: duckdb.DuckDBPyConnection, config: dict, scope: str) -> None:
    """Inject the objective-layer config values the SQL reads (v1.md §8)."""
    if scope not in ("career", "peak"):
        raise ValueError(f"scope must be 'career' or 'peak', got {scope!r}")
    blend = config["production_blend"]
    con.execute(f"SET VARIABLE blend_pts = {float(blend['pts'])}")
    con.execute(f"SET VARIABLE blend_trb = {float(blend['trb'])}")
    con.execute(f"SET VARIABLE blend_ast = {float(blend['ast'])}")
    con.execute(f"SET VARIABLE peak_n = {int(config['peak_n'])}")
    con.execute(f"SET VARIABLE peak_min_avail = {float(config['peak_min_avail'])}")
    con.execute(f"SET VARIABLE peak_scope = {str(scope == 'peak').lower()}")
    con.register(
        "param_award_intro",
        pd.DataFrame(
            config["accolade_intro_season"].items(),
            columns=["award", "intro_season"],
        ),
    )
    con.register(
        "param_all_nba_points",
        pd.DataFrame(
            {
                "all_nba_team": [ALL_NBA_TEAM_NUMBERS[k] for k in ALL_NBA_TEAM_NUMBERS],
                "points": [
                    float(config["all_nba_team_points"][k])
                    for k in ALL_NBA_TEAM_NUMBERS
                ],
            }
        ),
    )


def _execute_transforms(
    frames: dict[str, pd.DataFrame], config: dict, scope: str
) -> duckdb.DuckDBPyConnection:
    """Register inputs, inject params, run sql/01–04; return the connection.

    Private seam: run_transforms composes this with _check_transforms and
    _fetch_marts. Tests use the seam directly when they must reach the sql/06
    guard layer with corruption the contracts would intercept, or tamper with
    a mart between build and check to prove a named check fires — the public
    API has no bypass."""
    con = duckdb.connect()
    # v1.md §10.1 byte-identical determinism: multi-threaded operators combine
    # float partial sums in scheduling-dependent order (observed in T7 on the
    # scoring aggregate). Serial execution is order-deterministic; the marts
    # are a few hundred rows, so the cost is nil.
    con.execute("SET threads = 1")
    for name in SCHEMAS:  # the four cleaned tables, e.g. view players_clean
        con.register(f"{name}_clean", frames[name])
    _set_params(con, config, scope)
    for filename in TRANSFORM_FILES:
        con.execute((SQL_DIR / filename).read_text())
    return con


def _check_transforms(con: duckdb.DuckDBPyConnection) -> None:
    """Run the sql/06 named checks; raise on any violation (v1.md §9)."""
    violations = con.execute((SQL_DIR / CHECKS_FILE).read_text()).df()
    if len(violations):
        listing = [f"{row.check_name}: {row.detail}" for row in violations.itertuples()]
        raise TransformValidationError(
            f"{len(listing)} transform check violation(s):\n" + "\n".join(listing)
        )


def _fetch_marts(con: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:
    return {
        table: con.execute(f"SELECT * FROM {table} ORDER BY {', '.join(keys)}").df()
        for table, keys in MART_KEYS.items()
    }


def run_transforms(
    frames: dict[str, pd.DataFrame], config: dict, scope: str | None = None
) -> dict[str, pd.DataFrame]:
    """Execute the SQL layer over cleaned frames; return the marts by name.

    Config validation and the Pandera contracts run unconditionally before
    any SQL (clean → validate → transform, CLAUDE.md data flow): invalid
    inputs raise ContractViolation, malformed config raises ValueError, and
    there is no bypass parameter — tests that need to study the SQL guards
    under corruption use the private seam functions instead.
    """
    _validate_config(config)
    validate_all(frames)
    # `is None`, not `or`: a falsey scope like "" must reach _set_params and
    # raise there, never silently select the config default (Codex, 2026-07-13)
    con = _execute_transforms(
        frames, config, config["scope"] if scope is None else scope
    )
    _check_transforms(con)
    return _fetch_marts(con)


def main() -> None:
    missing = [
        n for n in SCHEMAS if not (PROCESSED_DIR / f"{n}_clean.parquet").exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"missing cleaned parquet for {missing} — run `uv run python -m "
            "pipeline.clean` first (or `make transform`, which chains it)"
        )
    frames = {
        name: pd.read_parquet(PROCESSED_DIR / f"{name}_clean.parquet")
        for name in SCHEMAS
    }
    marts = run_transforms(frames, load_config())
    MARTS_DIR.mkdir(parents=True, exist_ok=True)
    for table, frame in marts.items():
        path = MARTS_DIR / f"{table}.parquet"
        frame.to_parquet(path, index=False)
        print(f"wrote {path} ({len(frame)} rows)")


if __name__ == "__main__":
    main()
