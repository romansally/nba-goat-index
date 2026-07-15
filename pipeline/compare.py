"""Pairwise player comparison (Tier-1 task T7) — v1.md §6.

The pairwise verdict is a derived view of the scored table produced by
pipeline/score.py — no separate math (one engine, one set of numbers,
§12.9). Verdict = higher final score; exact final-score ties resolve by the
§6 ranking tie-break, which the rank column already encodes. The "why" is
the component-by-component table with the higher side named. The
career-vs-peak option is the caller's choice of which scored frame to hand
in: score(frames, config, scope="career" | "peak").
"""

import pandas as pd


def pairwise(scores: pd.DataFrame, player_a: str | int, player_b: str | int) -> dict:
    """Compare two players from one scored frame; accepts id or exact name."""
    # One scored frame = one engine run: a hybrid (e.g. career rows glued to
    # peak rows, or rows from two method versions) would produce a verdict
    # whose numbers are not comparable — refuse it (Codex T7 review).
    for column in ("scope", "method_version"):
        if scores[column].nunique(dropna=False) != 1:
            raise ValueError(
                f"scores frame mixes {column} values "
                f"{sorted(scores[column].astype(str).unique())} — "
                "pairwise needs a single engine run"
            )

    def find(player):
        column = "player_name" if isinstance(player, str) else "player_id"
        match = scores.loc[scores[column] == player]
        if len(match) != 1:
            raise ValueError(f"player {player!r} matched {len(match)} rows")
        return match.iloc[0]

    a, b = find(player_a), find(player_b)
    if a["player_id"] == b["player_id"]:
        raise ValueError(f"cannot compare {a['player_name']!r} with themself")
    winner, loser = (a, b) if a["rank"] < b["rank"] else (b, a)
    components = {}
    for column in scores.columns:
        if not column.startswith("comp_"):
            continue
        if a[column] == b[column]:
            higher = "tie"
        else:
            higher = a["player_name"] if a[column] > b[column] else b["player_name"]
        components[column.removeprefix("comp_")] = {
            a["player_name"]: float(a[column]),
            b["player_name"]: float(b[column]),
            "higher": higher,
        }
    return {
        "scope": scores["scope"].iloc[0],
        "verdict": winner["player_name"],
        "margin": float(winner["goat_score"] - loser["goat_score"]),
        "final": {
            a["player_name"]: float(a["goat_score"]),
            b["player_name"]: float(b["goat_score"]),
        },
        "components": components,
    }
