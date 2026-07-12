"""Acquire the committed real seed dataset (Tier-1 task T3).

CLAUDE.md Rule 2: this is the ONLY file in the project that touches the network.
Two phases, both re-runnable:

  --fetch   NETWORK: pull raw nba_api responses into data/raw/ (gitignored).
            Throttled, retried, resumable (existing raw files are skipped).
  --curate  OFFLINE: build the four committed seed CSVs in data/seed/ from
            data/raw/ plus the hand-assembled inputs in data/hand_assembled/.

Endpoints (probed 2026-07-05, see docs/sources.md):
  PlayerProfileV2 / PlayerAwards     per player (ProfileV2 over PlayerCareerStats:
                                     identical season-totals schema, and the
                                     PlayerCareerStats URL for player 2544 is
                                     stuck on a poisoned empty server cache)
  LeagueLeaders                      per season -> league baselines (all players)
  LeagueGameLog                      per season -> SRS margins, schedule lengths

Complexity budget note (CLAUDE.md Rule 6): this file exceeds 250 lines with
documented justification — acquisition is deliberately a single script (the one
network-touching place), and the era-conditional null handling, traded-season
stint collapsing, and league-baseline derivations are verbose but must live
together to keep the seed's provenance auditable in one read.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path

import pandas as pd

from pipeline.srs import compute_srs

RAW_DIR = Path("data/raw")
SEED_DIR = Path("data/seed")
HAND_DIR = Path("data/hand_assembled")

FIRST_SEASON, LAST_SEASON = 1957, 2026  # end-year convention (v1.md §2)

# The 20-player pool locked at T3 plan review (2026-07-05). IDs confirmed against
# nba_api.stats.static.players; a literal so the pool is reviewable in a diff.
ROSTER: dict[str, int] = {
    "Michael Jordan": 893,
    "LeBron James": 2544,
    "Kareem Abdul-Jabbar": 76003,
    "Bill Russell": 78049,
    "Wilt Chamberlain": 76375,
    "Magic Johnson": 77142,
    "Larry Bird": 1449,
    "Kobe Bryant": 977,
    "Stephen Curry": 201939,
    "Kevin Durant": 201142,
    "Shaquille O'Neal": 406,
    "Tim Duncan": 1495,
    "Hakeem Olajuwon": 165,
    "Oscar Robertson": 600015,
    "Jerry West": 78497,
    "Moses Malone": 77449,
    "Dirk Nowitzki": 1717,
    "Kevin Garnett": 708,
    "Giannis Antetokounmpo": 203507,
    "Nikola Jokic": 203999,
}

# First season each era-gated stat was tracked (end-year). Values before the
# intro season become nulls, never zeros (PRD §9; contracts enforce in T5).
ERA_INTRO = {"fg3m": 1980, "fg3a": 1980, "stl": 1974, "blk": 1974, "tov": 1978}

STAT_RENAME = {
    "GP": "gp",
    "MIN": "mp",
    "PTS": "pts",
    "REB": "trb",
    "AST": "ast",
    "FGM": "fgm",
    "FGA": "fga",
    "FTM": "ftm",
    "FTA": "fta",
    "FG3M": "fg3m",
    "FG3A": "fg3a",
    "STL": "stl",
    "BLK": "blk",
    "TOV": "tov",
}
PLAYOFF_COLS = ["gp", "mp", "pts", "trb", "ast"]
COUNT_COLS = [*STAT_RENAME.values(), *[f"po_{c}" for c in PLAYOFF_COLS]]

# Exact PlayerAwards DESCRIPTION strings (probed; exact match keeps lookalikes
# such as "NBA Sporting News Most Valuable Player of the Year" out).
AWARD_MAP = {
    "NBA Most Valuable Player": "mvp",
    "NBA Champion": "ring",
    "NBA Finals Most Valuable Player": "finals_mvp",
    "All-NBA": "all_nba",
    "NBA Defensive Player of the Year": "dpoy",
    "NBA All-Star": "all_star",
}

# Accolades must join a played season (v1.md §5.5 rates, §7 peak scope). One real
# selection cannot: Magic Johnson retired pre-season in 1991-92 (zero games) yet
# was voted into the Feb 1992 All-Star Game. Excluded, documented in v1.md §12.10.
ACCOLADE_EXCLUSIONS = {(77142, 1992, "all_star")}


def season_str(end_year: int) -> str:
    return f"{end_year - 1}-{str(end_year)[-2:]}"


# --------------------------------------------------------------------------- fetch


def fetch_all() -> None:
    """Pull every raw response we don't already have. Network happens only here."""
    from nba_api.stats.endpoints import (
        leaguegamelog,
        leagueleaders,
        playerawards,
        playerprofilev2,
    )

    def get(name: str, factory) -> None:
        path = RAW_DIR / f"{name}.json"
        if path.exists():
            return
        for attempt in range(3):
            time.sleep(1.5 + random.random())  # be conservative: undocumented limits
            try:
                payload = factory().get_json()
                # The API sometimes returns a valid envelope whose data tables
                # are empty (observed for career_2544 on the first pull: zero
                # season rows, only junk result sets populated) — treat it as a
                # failure so it retries instead of poisoning data/raw/. The
                # primary table is the first result set for all four endpoints.
                if not result_sets(json.loads(payload))[0]["rowSet"]:
                    raise ValueError("primary result set is empty")
                path.write_text(payload)
                print(f"fetched {name}")
                return
            except Exception as exc:  # noqa: BLE001 - retry then fail loudly
                wait = 5 * 3**attempt
                print(f"  {name}: attempt {attempt + 1} failed ({exc}); wait {wait}s")
                time.sleep(wait)
        raise RuntimeError(f"giving up on {name} after 3 attempts")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for player, pid in ROSTER.items():
        get(
            f"career_{pid}",
            lambda pid=pid: playerprofilev2.PlayerProfileV2(
                player_id=pid, per_mode36="Totals", timeout=60
            ),
        )
        get(f"awards_{pid}", lambda pid=pid: playerawards.PlayerAwards(pid, timeout=60))
        print(f"player done: {player}")
    for year in range(FIRST_SEASON, LAST_SEASON + 1):
        get(
            f"leaders_{year}",
            lambda y=year: leagueleaders.LeagueLeaders(
                season=season_str(y),
                per_mode48="Totals",
                season_type_all_star="Regular Season",
                stat_category_abbreviation="PTS",
                timeout=60,
            ),
        )
        get(
            f"gamelog_{year}",
            lambda y=year: leaguegamelog.LeagueGameLog(
                season=season_str(y),
                season_type_all_star="Regular Season",
                timeout=60,
            ),
        )
        print(f"league done: {season_str(year)}")


# -------------------------------------------------------------------------- curate


def result_sets(raw: dict) -> list[dict]:
    """Most endpoints wrap data in `resultSets` (a list); LeagueLeaders uses a
    singular `resultSet` object. Normalize to a list."""
    sets = raw["resultSets"] if "resultSets" in raw else raw["resultSet"]
    return sets if isinstance(sets, list) else [sets]


def load_result_set(name: str, result_set: str) -> pd.DataFrame:
    raw = json.loads((RAW_DIR / f"{name}.json").read_text())
    for rs in result_sets(raw):
        if rs["name"] == result_set:
            return pd.DataFrame(rs["rowSet"], columns=rs["headers"])
    raise KeyError(f"{name}: result set {result_set!r} not found")


def collapse_stints(rows: pd.DataFrame) -> pd.DataFrame:
    """One row per season from PlayerProfileV2 season-totals rows.

    Traded seasons arrive as one row per team plus a TOT row: totals come from
    TOT, `team_abbr` joins the stints ("SFW/PHL"), and `stints` keeps per-team
    (TEAM_ID, GP) for GP-weighted SRS (v1.md §5.3).
    """
    out = []
    for season_id, grp in rows.groupby("SEASON_ID", sort=True):
        stints = grp[grp["TEAM_ABBREVIATION"] != "TOT"]
        if len(grp) > 1:
            tot = grp[grp["TEAM_ABBREVIATION"] == "TOT"]
            if len(tot) != 1:
                raise ValueError(f"multi-team season {season_id} lacks a TOT row")
            totals = tot.iloc[0]
        else:
            totals = grp.iloc[0]
        rec: dict = {
            "season": int(season_id[:4]) + 1,
            "team_abbr": "/".join(stints["TEAM_ABBREVIATION"]),
            "stints": list(zip(stints["TEAM_ID"], stints["GP"], strict=True)),
        }
        rec.update({col: totals[api] for api, col in STAT_RENAME.items()})
        out.append(rec)
    return pd.DataFrame(out)


def apply_era_nulls(seasons: pd.DataFrame) -> pd.DataFrame:
    """Era-gated stats are null before their intro season, never zero — even if
    the API sent 0 (PRD §9: zeros where history has no data must become nulls)."""
    seasons = seasons.copy()
    for col, intro in ERA_INTRO.items():
        seasons.loc[seasons["season"] < intro, col] = pd.NA
    return seasons


def map_awards(awards: pd.DataFrame) -> pd.DataFrame:
    """Season-attributed accolades from PlayerAwards rows (v1.md §5.5)."""
    rows = awards[awards["DESCRIPTION"].isin(AWARD_MAP)].copy()
    rows["award"] = rows["DESCRIPTION"].map(AWARD_MAP)
    rows["season"] = rows["SEASON"].str.slice(0, 4).astype(int) + 1
    team_number = pd.to_numeric(rows["ALL_NBA_TEAM_NUMBER"], errors="coerce")
    rows["all_nba_team"] = team_number.where(rows["award"] == "all_nba").astype("Int64")
    bad = rows[(rows["award"] == "all_nba") & ~rows["all_nba_team"].isin([1, 2, 3])]
    if not bad.empty:
        raise ValueError(f"All-NBA rows without a 1/2/3 team number:\n{bad}")
    return rows[["award", "season", "all_nba_team"]]


def weighted_srs(stints: list[tuple], srs_by_team: pd.Series) -> float:
    """GP-weighted SRS across a season's stints (single-team seasons included)."""
    games = sum(gp for _, gp in stints)
    return sum(gp * srs_by_team[tid] for tid, gp in stints) / games


def season_srs(gamelog: pd.DataFrame) -> pd.Series:
    pairs = gamelog.merge(gamelog, on="GAME_ID", suffixes=("", "_opp"))
    pairs = pairs[pairs["TEAM_ID"] != pairs["TEAM_ID_opp"]]
    if len(pairs) != len(gamelog):
        raise ValueError("game log rows do not pair up 2-per-game")
    games = pd.DataFrame(
        {
            "team": pairs["TEAM_ID"],
            "opp": pairs["TEAM_ID_opp"],
            "margin": pairs["PTS"] - pairs["PTS_opp"],
        }
    )
    return compute_srs(games)


def league_row(year: int, leaders: pd.DataFrame, gamelog: pd.DataFrame) -> dict:
    """League baselines for one season (v1.md §2 reference table).

    Baselines are player-attributed sums over LeagueLeaders (all players): PTS
    and AST match published team totals exactly; TRB excludes unattributed team
    rebounds — a uniform, documented definition (docs/sources.md).
    """
    if not leaders["PLAYER_ID"].is_unique:
        raise ValueError(f"{year}: duplicate players in LeagueLeaders")
    team_games = len(gamelog)  # one row per team per game
    per_team = gamelog.groupby("TEAM_ID").size()
    schedule_modes = per_team.value_counts()
    pts, fga, fta = (leaders[c].sum() for c in ["PTS", "FGA", "FTA"])
    row = {
        "season": year,
        "lg_pts_pg": round(pts / team_games, 4),
        "lg_trb_pg": round(leaders["REB"].sum() / team_games, 4),
        "lg_ast_pg": round(leaders["AST"].sum() / team_games, 4),
        "lg_ts_pct": round(pts / (2 * (fga + 0.44 * fta)), 4),
        "season_games": int(
            schedule_modes[schedule_modes == schedule_modes.max()].index.min()
        ),
        "asg_held": year != 1999,  # the only seed-era season with no All-Star Game
    }
    if year >= 1978:  # possessions need OREB (1974+) and TOV (1978+)
        poss = fga - leaders["OREB"].sum() + leaders["TOV"].sum() + 0.44 * fta
        team_min_per_game = leaders["MIN"].sum() / 5 / team_games
        row["pace"] = round(48 * (poss / team_games) / team_min_per_game, 4)
        row["pace_estimated"] = False
    return row


def curate() -> None:
    from nba_api.stats.static import players as static_players

    hand_ws = pd.read_csv(HAND_DIR / "win_shares.csv")
    hand_pace = pd.read_csv(HAND_DIR / "pace_estimates.csv").set_index("season")

    players, season_frames, accolade_frames = [], [], []
    srs_cache = {
        year: season_srs(load_result_set(f"gamelog_{year}", "LeagueGameLog"))
        for year in range(FIRST_SEASON, LAST_SEASON + 1)
    }
    for pid in ROSTER.values():
        static = static_players.find_player_by_id(pid)
        career = collapse_stints(
            load_result_set(f"career_{pid}", "SeasonTotalsRegularSeason")
        )
        playoffs = collapse_stints(
            load_result_set(f"career_{pid}", "SeasonTotalsPostSeason")
        )
        po = playoffs[["season", *PLAYOFF_COLS]].rename(
            columns={c: f"po_{c}" for c in PLAYOFF_COLS}
        )
        seasons = apply_era_nulls(career.merge(po, on="season", how="left"))
        seasons["team_srs"] = [
            round(weighted_srs(stints, srs_cache[season]), 4)
            for stints, season in zip(seasons["stints"], seasons["season"], strict=True)
        ]
        seasons = seasons.drop(columns="stints")
        seasons.insert(0, "player_id", pid)
        season_frames.append(seasons)

        awards = map_awards(load_result_set(f"awards_{pid}", "PlayerAwards"))
        awards.insert(0, "player_id", pid)
        accolade_frames.append(awards)

        players.append(
            {
                "player_id": pid,
                "player_name": static["full_name"],
                "first_season": career["season"].min(),
                "last_season": career["season"].max(),
                "is_active": static["is_active"],
            }
        )

    player_seasons = pd.concat(season_frames).sort_values(["player_id", "season"])
    if player_seasons["season"].min() < FIRST_SEASON:
        raise ValueError("seed contains a season before the league reference table")
    ws_keys = set(zip(hand_ws["player_id"], hand_ws["season"], strict=True))
    api_keys = set(
        zip(player_seasons["player_id"], player_seasons["season"], strict=True)
    )
    if ws_keys != api_keys:  # loud: a drafting gap would otherwise become a silent null
        raise ValueError(
            f"win_shares.csv keys mismatch: missing={sorted(api_keys - ws_keys)[:5]} "
            f"extra={sorted(ws_keys - api_keys)[:5]}"
        )
    player_seasons = player_seasons.merge(
        hand_ws[["player_id", "season", "ws"]], on=["player_id", "season"], how="left"
    )
    player_seasons[COUNT_COLS] = player_seasons[COUNT_COLS].astype("Int64")

    league = pd.DataFrame(
        [
            league_row(
                year,
                load_result_set(f"leaders_{year}", "LeagueLeaders"),
                load_result_set(f"gamelog_{year}", "LeagueGameLog"),
            )
            for year in range(FIRST_SEASON, LAST_SEASON + 1)
        ]
    )
    estimated = league["pace"].isna()
    league.loc[estimated, "pace"] = league.loc[estimated, "season"].map(
        hand_pace["pace"]
    )
    league.loc[estimated, "pace_estimated"] = True
    if league["pace"].isna().any():
        raise ValueError("missing hand-assembled pace estimate for a pre-1978 season")

    accolades = pd.concat(accolade_frames).sort_values(
        ["player_id", "season", "award", "all_nba_team"]
    )
    keys = list(
        zip(
            accolades["player_id"], accolades["season"], accolades["award"], strict=True
        )
    )
    orphans = {k for k in keys if (k[0], k[1]) not in api_keys}
    if orphans != ACCOLADE_EXCLUSIONS:  # loud: an orphan would corrupt accolade rates
        raise ValueError(
            "accolades lacking a player_seasons row do not match the documented "
            f"exclusions: {sorted(orphans ^ ACCOLADE_EXCLUSIONS)}"
        )
    accolades = accolades[[k not in ACCOLADE_EXCLUSIONS for k in keys]]

    SEED_DIR.mkdir(parents=True, exist_ok=True)
    frames = {
        "players": pd.DataFrame(players).sort_values("player_id"),
        "player_seasons": player_seasons,
        "accolades": accolades[["player_id", "season", "award", "all_nba_team"]],
        "league_seasons": league.sort_values("season"),
    }
    for name, frame in frames.items():
        frame.to_csv(SEED_DIR / f"{name}.csv", index=False)
        print(f"wrote data/seed/{name}.csv ({len(frame)} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true", help="network phase only")
    parser.add_argument("--curate", action="store_true", help="offline phase only")
    args = parser.parse_args()
    run_all = not (args.fetch or args.curate)
    if args.fetch or run_all:
        fetch_all()
    if args.curate or run_all:
        curate()
