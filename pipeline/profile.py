"""Profile the cleaned seed dataset into docs/profiling.md (Tier-1 task T4).

Runs the cleaning step in-memory (clean → profile, per the CLAUDE.md data
flow) and renders a deliberately deterministic markdown report: no
timestamps, fixed iteration orders — regenerating on identical input is
byte-identical, so a diff in the committed doc always means the data or the
profiler changed. Findings that informed a cleaning step or a T5 contract
rule are mapped explicitly in the final section (PRD T4 acceptance
criterion 2).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.clean import ERA_INTRO, PO_COLS, clean, load_seed

DOC_PATH = Path("docs/profiling.md")

# Windows follow the tracking-intro boundaries: steals/blocks 1974,
# turnovers 1978, three-pointers 1980 (docs/sources.md gaps table).
ERA_WINDOWS = [
    ("pre-1974", 0, 1973),
    ("1974–77", 1974, 1977),
    ("1978–79", 1978, 1979),
    ("1980+", 1980, 9999),
]


def fmt(value) -> str:
    if pd.isna(value):
        return "—"
    return f"{value:g}" if isinstance(value, float) else str(int(value))


def md_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def row_counts(seed_counts: dict[str, int], frames: dict[str, pd.DataFrame]) -> str:
    rows = [
        [name, str(seed_counts[name]), str(len(frame))]
        for name, frame in frames.items()
    ]
    return md_table(["table", "seed rows (before)", "cleaned rows (after)"], rows)


def null_rates(ps: pd.DataFrame) -> str:
    rows = []
    for col in [*ERA_INTRO, *PO_COLS]:
        cells = []
        for label, lo, hi in ERA_WINDOWS:
            window = ps.loc[ps["season"].between(lo, hi), col]
            cells.append(f"{window.isna().sum()}/{len(window)}")
        total = f"{ps[col].isna().sum()}/{len(ps)}"
        rows.append([f"`{col}`", *cells, total])
    headers = ["column", *[label for label, _, _ in ERA_WINDOWS], "all seasons"]
    return md_table(headers, rows)


def ranges(ps: pd.DataFrame, lg: pd.DataFrame) -> str:
    numeric = [c for c in ps.columns if c not in ("player_id", "team_abbr")]
    rows = [[f"`{col}`", fmt(ps[col].min()), fmt(ps[col].max())] for col in numeric]
    rows += [
        [f"`league_seasons.{col}`", fmt(lg[col].min()), fmt(lg[col].max())]
        for col in ["pace", "lg_pts_pg", "lg_ts_pct"]
    ]
    return md_table(["column", "min", "max"], rows)


def structural_checks(frames: dict[str, pd.DataFrame]) -> str:
    ps = frames["player_seasons"]
    post3pt = ps[ps["season"] >= 1980]
    pts_bad = (
        post3pt["pts"] != 2 * post3pt["fgm"] + post3pt["fg3m"] + post3pt["ftm"]
    ).sum()
    schedule = ps.merge(
        frames["league_seasons"][["season", "season_games"]], on="season"
    )
    over = schedule[schedule["gp"] > schedule["season_games"]]
    over_desc = (
        "; ".join(
            f"player {r.player_id} season {r.season}: gp {r.gp} > modal {r.season_games}"
            for r in over.itertuples()
        )
        or "none"
    )
    checks = [
        ("duplicate primary keys (all four tables)", "0 — enforced, `clean.py` raises"),
        ("accolades without a played player-season", "0 — enforced, `clean.py` raises"),
        (
            "era-gated values outside their tracking window",
            "0 — enforced, `clean.py` raises",
        ),
        ("partially-null playoff column groups", "0 — enforced, `clean.py` raises"),
        (
            "`fgm > fga` / `ftm > fta` / `fg3m > fg3a` / `fg3a > fga`",
            f"{(ps['fgm'] > ps['fga']).sum()} / {(ps['ftm'] > ps['fta']).sum()} / {(ps['fg3m'] > ps['fg3a']).sum()} / {(ps['fg3a'] > ps['fga']).sum()} violations (observed)",
        ),
        (
            "`pts = 2·fgm + fg3m + ftm` (1980+, 3PT tracked)",
            f"{pts_bad} violations (observed)",
        ),
        ("`gp > season_games` (modal schedule length)", over_desc),
        ("max minutes per game (`mp / gp`)", f"{(ps['mp'] / ps['gp']).max():.2f}"),
        (
            "distinct `season_games` values",
            ", ".join(
                str(v)
                for v in sorted(frames["league_seasons"]["season_games"].unique())
            ),
        ),
    ]
    return md_table(["check", "result"], [[c, r] for c, r in checks])


def findings(ps: pd.DataFrame, frames: dict[str, pd.DataFrame]) -> str:
    over = ps.merge(frames["league_seasons"][["season", "season_games"]], on="season")
    n_over = (over["gp"] > over["season_games"]).sum()
    return f"""\
1. **Name inconsistency (→ cleaning step + data fix).** 11 rows of
   `data/hand_assembled/win_shares.csv` spelled the canonical `players.csv` name
   "Nikola Jokić" as ASCII "Nikola Jokic". The join key is `player_id`, so no value was
   affected — but it is exactly the silent drift a name-consistency rule exists to catch. The
   CSV was corrected (provenance row in `docs/sources.md`, 2026-07-11) and
   `clean.py::check_name_consistency` now enforces strict equality, so recurrence fails loudly.
   Current mismatches: 0.
2. **`gp` can exceed the modal schedule (→ T5 contract rule).** {n_over} row(s) exceed
   `season_games` (see structural checks — the 2019-20 COVID restart gave Denver 73 games
   against a modal 72). A naive `gp ≤ season_games` contract would falsely reject real data;
   T5 must bound `gp` with headroom (e.g. `≤ season_games + 3`) or against actual team games.
3. **Minutes bounds must allow overtime (→ T5 contract rule).** Max observed `mp/gp` is
   {(ps["mp"] / ps["gp"]).max():.2f} (Wilt-era seasons averaged above 48 minutes). A
   `mp ≤ 48·gp` rule would fail valid rows; T5 should use an OT-tolerant ceiling.
4. **Negative Win Shares are real (→ T5 contract rule).** Seed `ws` minimum is
   {fmt(ps["ws"].min())}; a `ws ≥ 0` bound would reject a legitimate season. T5's lower bound
   must admit small negatives.
5. **Missed postseasons are all-or-nothing nulls (→ T5 contract rule, cleaning invariant).**
   {int(ps["po_gp"].isna().sum())} of {len(ps)} rows have all five `po_*` columns null
   together — null means "no playoff run" and is never zero-filled (v1.md §9). `clean.py`
   raises on partial nullness; T5 should encode the same all-or-nothing rule.
6. **Era-null boundaries are exact (→ confirms T5 thresholds).** The null-rate table shows
   100% nulls strictly before each tracking intro (steals/blocks 1974, turnovers 1978,
   three-pointers 1980) and 0% from the intro season on — the era-conditional contract rules
   can assert both directions, not just the pre-intro side."""


def render(seed_counts: dict[str, int], frames: dict[str, pd.DataFrame]) -> str:
    ps, lg = frames["player_seasons"], frames["league_seasons"]
    return f"""\
# Seed Data Profile — T4

> Generated by `uv run python -m pipeline.profile` (which runs the cleaning step in-memory).
> Do not edit by hand — regenerate. Output is deterministic: identical input produces a
> byte-identical file, so any diff here means the data or the profiler changed.

## 1. Row counts, before and after cleaning

{row_counts(seed_counts, frames)}

Counts are identical by design: cleaning never drops or imputes rows — every violation
raises instead (`pipeline/clean.py`, per v1.md §9). What cleaning changes is types (nullable
`Int64` counting stats, real booleans, string names) and enforced structure.

## 2. Null rates by field and era (`player_seasons`)

Only the columns below contain nulls; every other column is 100% populated in all eras.
Cells are `nulls/rows` per era window.

{null_rates(ps)}

Era-gated stats (`fg3m`/`fg3a` 1980, `stl`/`blk` 1974, `tov` 1978) are null exactly when the
league did not track them. The `po_*` group is null exactly when the player missed that
postseason.

## 3. Value ranges

{ranges(ps, lg)}

## 4. Duplicate-key and structural checks

Checks marked *enforced* are guaranteed by `clean.py` (it raises, so cleaned output cannot
violate them). Checks marked *observed* are profiled here to inform the T5 contracts.

{structural_checks(frames)}

## 5. Findings → actions (PRD T4 acceptance criterion 2)

{findings(ps, frames)}
"""


def main() -> None:
    seed_counts = {name: len(frame) for name, frame in load_seed().items()}
    DOC_PATH.write_text(render(seed_counts, clean()))
    print(f"wrote {DOC_PATH}")


if __name__ == "__main__":
    main()
