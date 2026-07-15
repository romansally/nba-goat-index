"""Golden snapshot tooling (Tier-1 task T7) — CLAUDE.md Rule 5.

Generates tests/golden/<method_version>_scores.json from the locked fixture
trio: full-precision finals, components, and ranks for both scopes, plus
method_version and a canonical hash of the parsed config. Deliberately no
timestamp or git_sha — regeneration on unchanged inputs must be
byte-identical (git records when and by whom). The filename is DERIVED from
method_version, so a future v2 regeneration lands in v2_scores.json and can
never silently overwrite the locked v1 snapshot (Codex T7 review).

Regenerate with `uv run python -m pipeline.golden` — doing so outside a
method_version bump is a blocked merge (CLAUDE.md Rule 5).
"""

import hashlib
import json
import math
import re
from pathlib import Path

from pipeline.clean import apply_schema
from pipeline.contracts import load_fixtures
from pipeline.score import score
from pipeline.transform import load_config

GOLDEN_DIR = Path("tests/golden")


def golden_path(config: dict) -> Path:
    """Derived filename, gated to a bare vN token: a path-like version such
    as "../golden/v1" would resolve back onto an existing snapshot and defeat
    the overwrite protection this derivation exists for (Codex T7 re-review)."""
    version = config["method_version"]
    if not isinstance(version, str) or not re.fullmatch(r"v[1-9][0-9]*", version):
        raise ValueError(f"method_version must match v<N> (e.g. 'v1'): {version!r}")
    return GOLDEN_DIR / f"{version}_scores.json"


def _require_canonical(node, path: str = "config") -> None:
    """String keys and finite numbers only. json.dumps would coerce {1: "v1"}
    and {"1": "v1"} to identical bytes — a hash collision between configs the
    engine treats differently — and serializes NaN/Inf as nonstandard JSON;
    refuse both (Codex T7 review)."""
    if isinstance(node, dict):
        for key, value in node.items():
            if not isinstance(key, str):
                raise ValueError(f"{path}: non-string config key {key!r}")
            _require_canonical(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            _require_canonical(value, f"{path}[{index}]")
    elif isinstance(node, float) and not math.isfinite(node):
        raise ValueError(f"{path}: non-finite value {node!r}")


def config_sha256(config: dict) -> str:
    """Hash of the PARSED config (canonical JSON): comment and formatting
    edits leave it unchanged; any value change — weights included — breaks
    the golden test until an intentional regeneration (CLAUDE.md Rule 5)."""
    _require_canonical(config)
    canonical = json.dumps(
        config, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def golden_payload(config: dict) -> dict:
    """Golden snapshot content: both scopes over the locked fixture trio."""
    frames = {n: apply_schema(n, f) for n, f in load_fixtures("valid").items()}
    scopes = {}
    for scope in ("career", "peak"):
        scored = score(frames, config, scope)
        scopes[scope] = [
            {
                "rank": int(row["rank"]),
                "player_id": int(row["player_id"]),
                "player_name": row["player_name"],
                "goat_score": float(row["goat_score"]),
                "components": {
                    c.removeprefix("comp_"): float(row[c])
                    for c in scored.columns
                    if c.startswith("comp_")
                },
            }
            for _, row in scored.iterrows()
        ]
    return {
        "method_version": config["method_version"],
        "config_sha256": config_sha256(config),
        "scopes": scopes,
    }


def main() -> None:
    config = load_config()
    path = golden_path(config)
    path.write_text(json.dumps(golden_payload(config), indent=2, sort_keys=True) + "\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
