"""Simple Rating System (SRS) solver.

SRS is a team's average game margin adjusted for strength of schedule:

    rating_i = avg_margin_i + mean(rating of i's opponents, one term per game)

Methodology v1 §5.3 uses games-weighted career team SRS as the team half of the
Winning/Impact component; this module computes per-team SRS from real game results
(one row per team-game) pulled by pipeline/fetch_seed.py. Pure computation — no
network, no I/O.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_srs(games: pd.DataFrame) -> pd.Series:
    """Solve SRS ratings for one season of game results.

    `games` has one row per team-game perspective with columns:
      team   — the team's abbreviation
      opp    — the opponent's abbreviation
      margin — the team's point margin for that game (negative for a loss)

    Returns ratings indexed by team, sorted by team for determinism. The linear
    system r = m + S·r (S averages opponent ratings) is translation-invariant, so
    it is solved with least squares; the minimum-norm solution pins the league
    mean rating to ~0, which is the standard SRS convention.
    """
    teams = np.sort(games["team"].unique())
    index = pd.Index(teams, name="team")
    n = len(teams)
    pos = {t: i for i, t in enumerate(teams)}

    games_played = games.groupby("team").size().reindex(index)
    avg_margin = games.groupby("team")["margin"].mean().reindex(index)

    # S[i, j] = (games i played against j) / (games i played)
    schedule = np.zeros((n, n))
    matchup_counts = games.groupby(["team", "opp"]).size()
    for (team, opp), count in matchup_counts.items():
        schedule[pos[team], pos[opp]] = count / games_played[team]

    system = np.eye(n) - schedule
    ratings, *_ = np.linalg.lstsq(system, avg_margin.to_numpy(), rcond=None)
    return pd.Series(ratings, index=index, name="srs")
