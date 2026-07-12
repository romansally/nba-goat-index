"""Pandera data contracts for the cleaned seed tables (Tier-1 task T5).

Declarative shape rules for all four tables, validated by `make validate`
(offline: runs the cleaning step in-memory, like pipeline/profile.py). Bounds
are calibrated to real extremes so contracts catch corruption without falsely
rejecting real data — see qa/validation_log.md for the full rule inventory and
the extremes behind each bound (Jokić's 73 gp > 72-game 2020 schedule, Wilt's
48.5 min/game overtime seasons, Kareem's record 25.4 ws vs. real negative ws,
Jerry West's 1-minute 1967 playoff cameo).

Every check is named; `validate_all` aggregates Pandera and cross-table
failures into one ContractViolation listing (table, check_name) pairs. The
designed-bad fixtures under tests/fixtures/invalid/ each carry the name of the
check they must trip; `main` re-confirms every one still fails (CLAUDE.md
Rule 3 — if a bad fixture ever passes, the contract has a hole).

Complexity budget note (CLAUDE.md Rule 6): this file exceeds 250 lines with
documented justification — it is the single declarative registry of every
data-shape rule for all four seed tables (the PRD names one contracts file),
and most of its length is named bounds and checks, not logic. Splitting it
would scatter the one contract inventory that reviewers, tests, and
`make validate` treat as a unit.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from pipeline.clean import ERA_INTRO, PO_COLS, clean

FIXTURE_DIR = Path("tests/fixtures")

# Ceilings are NBA single-season records — many held by seed rows themselves
# (Wilt 4029 pts / 2149 trb, Curry 402 fg3m, Jordan 759 po_pts, Magic 303
# po_ast, Kareem 25.4 ws) — or the locked synthetic worksheet rows where those
# are larger (PlayerB: 2520 trb, 3360 fga, 1680 fgm), plus headroom. Floors
# admit the real extremes: negative Win Shares (seed min -0.4) and a 1-game,
# 1-minute, 0-point playoff run. gp/mp/po_mp ceilings are cross-field checks
# (gp_within_schedule, mp_ot_ceiling), not constants.
BOUNDS: dict[str, tuple[float, float | None]] = {
    "gp": (1, None),
    "mp": (1, None),
    "pts": (0, 4400),
    "trb": (0, 2800),
    "ast": (0, 1300),
    "fgm": (0, 1900),
    "fga": (0, 3700),
    "ftm": (0, 1000),
    "fta": (0, 1500),
    "fg3m": (0, 500),
    "fg3a": (0, 1150),  # record is OUTSIDE the seed pool: Harden 1028 (2018-19)
    "stl": (0, 350),
    "blk": (0, 500),
    "tov": (0, 500),
    "po_gp": (1, 28),  # 4 best-of-7 rounds
    "po_mp": (1, None),
    "po_pts": (0, 850),
    "po_trb": (0, 500),
    "po_ast": (0, 350),
    "ws": (-3, 26),
    "team_srs": (-16, 14),
}
LEAGUE_BOUNDS: dict[str, tuple[float, float]] = {
    "lg_pts_pg": (80, 130),
    "lg_trb_pg": (35, 70),
    "lg_ast_pg": (15, 32),
    "lg_ts_pct": (0.40, 0.65),
    "season_games": (50, 82),
    "pace": (85, 135),
}

# Award intro seasons (end-year). Mirrors config/scoring_v1.yaml
# `accolade_intro_season` — duplicated deliberately so data contracts never
# depend on scoring config (a config edit must not loosen validation).
AWARD_INTRO = {
    "mvp": 1956,
    "ring": 1947,
    "finals_mvp": 1969,
    "all_nba": 1947,
    "dpoy": 1983,
    "all_star": 1951,
}
ALL_NBA_THIRD_INTRO = 1989  # 3rd team exists only from 1989 (v1.md §12.10)
PACE_COMPUTED_FROM = 1978  # pace needs TOV, tracked 1977-78+ (docs/sources.md)
OT_ALLOWANCE = 60  # minutes: Wilt 1962 is 48·80+42; a 6-OT game is 48+30

# Overtime makes any mp/gp ratio cap wrong twice over: Wilt's 1962 season
# averaged 48.5 min/game across 80 games, and a 1-game playoff run (they
# exist: West 1967) could legitimately reach the 78-minute 6-OT record. An
# additive allowance handles both.


def _bounded(col: str) -> pa.Check:
    lo, hi = BOUNDS.get(col) or LEAGUE_BOUNDS[col]

    def in_bounds(s: pd.Series) -> pd.Series:
        ok = s >= lo if hi is None else (s >= lo) & (s <= hi)
        return ok.fillna(True).astype(bool)  # nulls are the nullable checks' job

    return pa.Check(in_bounds, name=f"{col}_range")


def _era_gate(col: str, intro: int) -> pa.Check:
    """Null strictly before the stat's tracking-intro season, non-null after —
    both directions (profiling.md finding 6: the boundaries are exact)."""

    def gate(df: pd.DataFrame) -> pd.Series:
        pre = (df["season"] < intro).fillna(False).astype(bool)
        bad = (pre & df[col].notna()) | (~pre & df[col].isna() & df["season"].notna())
        return ~bad

    return pa.Check(gate, name=f"{col}_era_gate")


def _df_check(name: str, fn) -> pa.Check:
    return pa.Check(lambda df, fn=fn: fn(df).fillna(True).astype(bool), name=name)


def _unique(name: str, keys: list[str]) -> pa.Check:
    return pa.Check(lambda df, k=keys: ~df.duplicated(k), name=name)


def _int_col(col: str, nullable: bool = False) -> pa.Column:
    checks = [_bounded(col)] if col in BOUNDS or col in LEAGUE_BOUNDS else []
    return pa.Column("Int64", nullable=nullable, checks=checks)


PLAYER_SEASONS_SCHEMA = pa.DataFrameSchema(
    columns={
        "player_id": pa.Column("Int64"),
        "season": pa.Column("Int64"),
        "team_abbr": pa.Column("string"),
        **{c: _int_col(c) for c in ["gp", "mp", "pts", "trb", "ast"]},
        **{c: _int_col(c) for c in ["fgm", "fga", "ftm", "fta"]},
        **{c: _int_col(c, nullable=True) for c in ERA_INTRO},
        **{c: _int_col(c, nullable=True) for c in PO_COLS},
        "team_srs": pa.Column("float64", checks=[_bounded("team_srs")]),
        "ws": pa.Column("float64", checks=[_bounded("ws")]),
    },
    checks=[
        _unique("ps_key_unique", ["player_id", "season"]),
        *[_era_gate(col, intro) for col, intro in ERA_INTRO.items()],
        _df_check(
            "po_all_or_nothing",
            lambda df: (
                ~(df[PO_COLS].isna().any(axis=1) & ~df[PO_COLS].isna().all(axis=1))
            ),
        ),
        _df_check(
            "pts_identity",  # holds exactly on all 335 seed rows, every era
            lambda df: df["pts"] == 2 * df["fgm"] + df["fg3m"].fillna(0) + df["ftm"],
        ),
        _df_check(
            "shot_chain",
            lambda df: (
                (df["fgm"] <= df["fga"])
                & (df["ftm"] <= df["fta"])
                & (df["fg3m"] <= df["fg3a"])
                & (df["fg3a"] <= df["fga"])
                & (df["fg3m"] <= df["fgm"])
            ),
        ),
        _df_check("mp_ot_ceiling", lambda df: df["mp"] <= 48 * df["gp"] + OT_ALLOWANCE),
        _df_check(
            "po_mp_ot_ceiling",
            lambda df: df["po_mp"] <= 48 * df["po_gp"] + OT_ALLOWANCE,
        ),
    ],
    coerce=True,
    strict=True,
)

ACCOLADES_SCHEMA = pa.DataFrameSchema(
    columns={
        "player_id": pa.Column("Int64"),
        "season": pa.Column("Int64"),
        "award": pa.Column(
            "string",
            checks=[pa.Check(lambda s: s.isin(AWARD_INTRO), name="award_known")],
        ),
        "all_nba_team": pa.Column("Int64", nullable=True),
    },
    checks=[
        _unique("accolade_key_unique", ["player_id", "season", "award"]),
        _df_check(
            "award_after_intro",  # a 1971 DPOY is as impossible as 1965 3PT attempts
            lambda df: ~(df["season"] < df["award"].map(AWARD_INTRO)),
        ),
        _df_check(
            "all_nba_team_null_iff",
            lambda df: (
                (df["award"] == "all_nba") & df["all_nba_team"].isin([1, 2, 3])
                | (df["award"] != "all_nba") & df["all_nba_team"].isna()
            ),
        ),
        _df_check(
            "third_team_intro",
            lambda df: (
                ~((df["all_nba_team"] == 3) & (df["season"] < ALL_NBA_THIRD_INTRO))
            ),
        ),
    ],
    coerce=True,
    strict=True,
)

PLAYERS_SCHEMA = pa.DataFrameSchema(
    columns={
        "player_id": pa.Column("Int64"),
        "player_name": pa.Column(
            "string",
            checks=[pa.Check(lambda s: s.str.len() > 0, name="player_name_nonempty")],
        ),
        "first_season": pa.Column("Int64"),
        "last_season": pa.Column("Int64"),
        # is_active is deliberately type-only: "active iff last_season is the
        # newest league season" is a freshness heuristic, not an invariant.
        "is_active": pa.Column("boolean"),
    },
    checks=[
        _unique("players_key_unique", ["player_id"]),
        _df_check("span_order", lambda df: df["first_season"] <= df["last_season"]),
    ],
    coerce=True,
    strict=True,
)

LEAGUE_SEASONS_SCHEMA = pa.DataFrameSchema(
    columns={
        "season": pa.Column("Int64"),
        **{
            c: pa.Column(
                "Int64" if c == "season_games" else "float64", checks=[_bounded(c)]
            )
            for c in LEAGUE_BOUNDS
        },
        "asg_held": pa.Column("boolean"),
        "pace_estimated": pa.Column("boolean"),
    },
    checks=[
        _unique("league_key_unique", ["season"]),
        _df_check(
            "pace_estimated_iff_pre1978",
            lambda df: df["pace_estimated"] == (df["season"] < PACE_COMPUTED_FROM),
        ),
    ],
    coerce=True,
    strict=True,
)

TABLE_SCHEMAS = {
    "players": PLAYERS_SCHEMA,
    "player_seasons": PLAYER_SEASONS_SCHEMA,
    "accolades": ACCOLADES_SCHEMA,
    "league_seasons": LEAGUE_SEASONS_SCHEMA,
}


class ContractViolation(Exception):
    """One or more contract failures; .failures is sorted (table, check) pairs."""

    def __init__(self, failures: list[tuple[str, str]]):
        self.failures = failures
        lines = "\n".join(f"  {table}: {check}" for table, check in failures)
        super().__init__(f"{len(failures)} contract violation(s):\n{lines}")


# Columns each cross-table rule joins or filters on. If one is missing, the
# joins below would raise instead of reporting; _cross_failures short-circuits
# with the same check name Pandera gives a missing column.
_CROSS_COLUMNS = {
    "player_seasons": ("player_id", "season", "gp"),
    "players": ("player_id", "first_season", "last_season"),
    "accolades": ("player_id", "season", "award"),
    "league_seasons": ("season", "season_games", "asg_held"),
}


def _cross_failures(frames: dict[str, pd.DataFrame]) -> set[tuple[str, str]]:
    """Rules spanning tables — Pandera schemas are single-table by design."""
    missing = {
        (table, "column_in_dataframe")
        for table, cols in _CROSS_COLUMNS.items()
        for col in cols
        if col not in frames[table].columns
    }
    if missing:
        return missing
    ps, pl = frames["player_seasons"], frames["players"]
    ac, lg = frames["accolades"], frames["league_seasons"]
    played = ps[["player_id", "season"]].drop_duplicates()
    schedule = ps.merge(lg[["season", "season_games"]], on="season", how="left")
    span = ps.groupby("player_id")["season"].agg(["min", "max"])
    pl_span = pl.merge(span, on="player_id", how="left")
    asg = ac[ac["award"] == "all_star"].merge(
        lg[["season", "asg_held"]], on="season", how="left"
    )
    checks: dict[tuple[str, str], pd.Series] = {
        ("player_seasons", "ps_season_in_league"): ps["season"].notna()
        & ~ps["season"].isin(lg["season"]),
        ("player_seasons", "ps_player_in_players"): ~ps["player_id"].isin(
            pl["player_id"]
        ),
        # +6 headroom: mid-season trades can exceed any one team's schedule —
        # Walt Bellamy played 88 games in the 82-game 1968-69 season. Covers
        # the seed's real 73 > 72 (Jokić, 2019-20 COVID restart).
        ("player_seasons", "gp_within_schedule"): (
            schedule["gp"] > schedule["season_games"] + 6
        ).fillna(False),
        ("players", "players_have_seasons"): ~pl["player_id"].isin(ps["player_id"]),
        ("players", "players_span_matches_seasons"): pl_span["min"].notna()
        & (
            (pl_span["first_season"] != pl_span["min"])
            | (pl_span["last_season"] != pl_span["max"])
        ),
        ("accolades", "accolade_joins_played_season"): ac.merge(
            played, on=["player_id", "season"], how="left", indicator=True
        )["_merge"]
        == "left_only",
        ("accolades", "all_star_requires_asg"): ~asg["asg_held"].fillna(True),
    }
    return {key for key, bad in checks.items() if bad.astype(bool).any()}


def validate_all(frames: dict[str, pd.DataFrame]) -> None:
    """Validate all four tables plus cross-table rules; raise ContractViolation
    listing every named check that failed (lazy — nothing masks anything)."""
    failures: set[tuple[str, str]] = set()
    for name, schema in TABLE_SCHEMAS.items():
        try:
            schema.validate(frames[name], lazy=True)
        except pa.errors.SchemaErrors as err:
            failures |= {(name, str(check)) for check in err.failure_cases["check"]}
    failures |= _cross_failures(frames)
    if failures:
        raise ContractViolation(sorted(failures))


def load_fixtures(kind: str) -> dict[str, pd.DataFrame]:
    return {
        name: pd.read_csv(FIXTURE_DIR / kind / f"{name}.csv") for name in TABLE_SCHEMAS
    }


def iter_invalid_cases():
    """Yield (case_id, expected_check, frames): the valid mini-set with exactly
    one designed-bad row injected. Some rows trip an incidental second check
    (e.g. a pre-span season also fails players_span_matches_seasons); lazy
    validation reports all, and callers assert the labeled one is present."""
    valid = load_fixtures("valid")
    for table in TABLE_SCHEMAS:
        bad = pd.read_csv(FIXTURE_DIR / "invalid" / f"{table}.csv")
        rows = bad.drop(columns="violation")
        for i, label in enumerate(bad["violation"]):
            frames = dict(valid)
            frames[table] = pd.concat([valid[table], rows.iloc[[i]]], ignore_index=True)
            yield f"{table}:{label}", label, frames


def main() -> None:
    frames = clean()
    validate_all(frames)
    for name, frame in frames.items():
        print(f"{name}: {len(frame)} rows pass contracts")
    validate_all(load_fixtures("valid"))
    print("valid fixture mini-set: pass")
    holes, total = [], 0
    for case_id, label, case_frames in iter_invalid_cases():
        total += 1
        try:
            validate_all(case_frames)
            holes.append(f"{case_id}: passed validation")
        except ContractViolation as exc:
            if label not in {check for _, check in exc.failures}:
                holes.append(f"{case_id}: failed, but not via {label}")
    if holes:  # a bad fixture no longer trips its check — the contract has a hole
        raise SystemExit("contract holes:\n" + "\n".join(f"  {h}" for h in holes))
    print(f"designed-bad fixtures: {total}/{total} still caught")


if __name__ == "__main__":
    main()
