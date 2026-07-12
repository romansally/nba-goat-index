"""Clean the committed seed dataset into typed parquet (Tier-1 task T4).

Reads the four seed CSVs (data/seed/), enforces explicit dtypes and structural
integrity, and writes data/processed/*_clean.parquet — the layer every
downstream step (validate, SQL transforms, scoring) reads. Offline by design
(CLAUDE.md Rule 2).

Cleaning never drops or imputes a row: a violation raises instead (v1.md §9 —
fail loudly, never coerce nulls to zero). Null semantics preserved from the
seed: era-gated stats are null before their intro season; the five po_*
columns are null together when a player missed the postseason.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.fetch_seed import ERA_INTRO

SEED_DIR = Path("data/seed")
HAND_DIR = Path("data/hand_assembled")
PROCESSED_DIR = Path("data/processed")

PO_COLS = ["po_gp", "po_mp", "po_pts", "po_trb", "po_ast"]

# Explicit dtype schema per table (also the authoritative column list — extra
# or missing seed columns fail loudly). Counting stats are nullable Int64:
# era-gated and po_* columns carry real nulls, and pandas' CSV reader erodes
# them to float64, which parquet would otherwise freeze in.
SCHEMAS: dict[str, dict[str, str]] = {
    "players": {
        "player_id": "int64",
        "player_name": "string",
        "first_season": "int64",
        "last_season": "int64",
        "is_active": "boolean",
    },
    "player_seasons": {
        "player_id": "int64",
        "season": "int64",
        "team_abbr": "string",
        **{
            col: "Int64"
            for col in [
                *["gp", "mp", "pts", "trb", "ast", "fgm", "fga", "ftm", "fta"],
                *["fg3m", "fg3a", "stl", "blk", "tov"],
                *PO_COLS,
            ]
        },
        "team_srs": "float64",
        "ws": "float64",
    },
    "accolades": {
        "player_id": "int64",
        "season": "int64",
        "award": "string",
        "all_nba_team": "Int64",
    },
    "league_seasons": {
        "season": "int64",
        "lg_pts_pg": "float64",
        "lg_trb_pg": "float64",
        "lg_ast_pg": "float64",
        "lg_ts_pct": "float64",
        "season_games": "int64",
        "asg_held": "boolean",
        "pace": "float64",
        "pace_estimated": "boolean",
    },
}

TABLE_KEYS = {
    "players": ["player_id"],
    "player_seasons": ["player_id", "season"],
    # A player can win each award at most once per season, so all_nba_team is
    # not part of the grain (the looser 4-column key would have admitted
    # All-NBA 1st + 2nd in the same season — a basketball impossibility).
    "accolades": ["player_id", "season", "award"],
    "league_seasons": ["season"],
}


def load_seed() -> dict[str, pd.DataFrame]:
    return {name: pd.read_csv(SEED_DIR / f"{name}.csv") for name in SCHEMAS}


def apply_schema(name: str, frame: pd.DataFrame) -> pd.DataFrame:
    schema = SCHEMAS[name]
    if set(frame.columns) != set(schema):
        raise ValueError(
            f"{name}: columns {sorted(set(frame.columns) ^ set(schema))} unexpected"
        )
    return frame.astype(schema)[list(schema)]


def check_unique_key(name: str, frame: pd.DataFrame) -> None:
    dupes = frame.duplicated(TABLE_KEYS[name])
    if dupes.any():
        raise ValueError(f"{name}: {dupes.sum()} duplicate {TABLE_KEYS[name]} rows")


def check_referential(frames: dict[str, pd.DataFrame]) -> None:
    ps, ac = frames["player_seasons"], frames["accolades"]
    unknown = set(ps["player_id"]) - set(frames["players"]["player_id"])
    if unknown:
        raise ValueError(
            f"player_seasons has player_ids not in players: {sorted(unknown)}"
        )
    played = set(zip(ps["player_id"], ps["season"], strict=True))
    orphans = {
        k for k in zip(ac["player_id"], ac["season"], strict=True) if k not in played
    }
    if orphans:  # curation excludes the one documented case (v1.md §12.10)
        raise ValueError(f"accolades without a played player-season: {sorted(orphans)}")
    missing = set(ps["season"]) - set(frames["league_seasons"]["season"])
    if missing:
        raise ValueError(f"seasons missing from league_seasons: {sorted(missing)}")


def check_null_patterns(ps: pd.DataFrame) -> None:
    for col, intro in ERA_INTRO.items():
        if ps.loc[ps["season"] < intro, col].notna().any():
            raise ValueError(f"{col}: non-null value before its {intro} intro season")
        if ps.loc[ps["season"] >= intro, col].isna().any():
            raise ValueError(f"{col}: null on or after its {intro} intro season")
    po_null = ps[PO_COLS].isna()
    mixed = po_null.any(axis=1) & ~po_null.all(axis=1)
    if mixed.any():  # null means "no playoff run" — all five or none
        raise ValueError(
            f"partially-null playoff columns on rows {list(ps.index[mixed])}"
        )
    always = [c for c in ps.columns if c not in PO_COLS and c not in ERA_INTRO]
    if ps[always].isna().any().any():
        bad = ps[always].isna().any()
        raise ValueError(f"nulls in always-present columns: {list(bad.index[bad])}")


def check_name_consistency(players: pd.DataFrame, hand_ws: pd.DataFrame) -> None:
    """One canonical name per player_id (players.csv, from nba_api static);
    hand-assembled win_shares.csv must agree exactly. Caught the real
    'Nikola Jokic' vs 'Nikola Jokić' drift — see docs/profiling.md finding 1."""
    merged = hand_ws.merge(players, on="player_id", how="left")
    if merged["player_name_y"].isna().any():
        unknown = sorted(
            merged.loc[merged["player_name_y"].isna(), "player_id"].unique()
        )
        raise ValueError(f"win_shares.csv has player_ids not in players: {unknown}")
    bad = merged[merged["player_name_x"] != merged["player_name_y"]]
    if not bad.empty:
        pairs = sorted(
            set(zip(bad["player_name_x"], bad["player_name_y"], strict=True))
        )
        raise ValueError(f"win_shares.csv names disagree with players.csv: {pairs}")


def clean() -> dict[str, pd.DataFrame]:
    frames = {name: apply_schema(name, df) for name, df in load_seed().items()}
    for name, frame in frames.items():
        check_unique_key(name, frame)
    check_referential(frames)
    check_null_patterns(frames["player_seasons"])
    hand_ws = pd.read_csv(HAND_DIR / "win_shares.csv").astype({"player_name": "string"})
    check_name_consistency(frames["players"], hand_ws)
    return {
        name: frame.sort_values(TABLE_KEYS[name]).reset_index(drop=True)
        for name, frame in frames.items()
    }


def main() -> None:
    frames = clean()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    for name, frame in frames.items():
        path = PROCESSED_DIR / f"{name}_clean.parquet"
        frame.to_parquet(path, index=False)
        print(f"wrote {path} ({len(frame)} rows)")


if __name__ == "__main__":
    main()
