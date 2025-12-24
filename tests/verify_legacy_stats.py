# tests/verify_legacy_stats.py
"""
Standalone regex verification for Version 27 legacy accolade fields.

Goals:
- Run fast (no network, no full scraper).
- Use mock raw HTML strings that resemble Basketball-Reference "accolades" text.
- Validate the exact regex patterns planned for:
  - finals_mvp_awards
  - all_defensive_selections
  - dpoy_awards
  - scoring_titles
"""

from __future__ import annotations

import re
from typing import Dict


# -------------------------------------------------------------------
# Mock raw HTML snippets (raw text, not BeautifulSoup)
# -------------------------------------------------------------------
MOCK_HTML_SNIPPETS: Dict[str, str] = {
    "Michael Jordan": """
        <div id="meta">
          <h1 itemprop="name"><span>Michael Jordan</span></h1>
          <ul>
            <li>6× Finals MVP</li>
            <li>9x All-Defensive</li>
            <li>1× Defensive Player of the Year</li>
            <li>10× NBA Scoring Champion</li>
          </ul>
        </div>
    """,
    "Hakeem Olajuwon": """
        <div id="meta">
          <h1><span>Hakeem Olajuwon</span></h1>
          <p>Accolades:</p>
          <ul>
            <li>2× Defensive Player of the Year</li>
          </ul>
        </div>
    """,
    "Gary Payton": """
        <div id="meta">
          <h1><span>Gary Payton</span></h1>
          <div class="accolades">
            <span>1× DPOY</span>
          </div>
        </div>
    """,
}


# -------------------------------------------------------------------
# EXACT planned regex patterns (do not change)
# -------------------------------------------------------------------
PATTERNS = {
    "finals_mvp_awards": r"(\d+)\s*[×x]\s*Finals\s+MVP",
    "all_defensive_selections": r"(\d+)\s*[×x]\s*All-Defensive",
    "dpoy_awards": r"(\d+)\s*[×x]\s*(?:Defensive\s+Player|DPOY)",
    "scoring_titles": r"(\d+)\s*[×x]\s*(?:NBA\s+)?Scoring\s+(?:Champion|Champ|Leader)",
}


def extract_legacy_accolades(raw_html: str) -> dict:
    """
    Helper that mirrors the exact extraction style we intend to use:
    - Regex search against raw HTML text
    - Extract the first captured integer if present, else 0
    """

    def _extract_int(pattern: str) -> int:
        m = re.search(pattern, raw_html, flags=re.IGNORECASE | re.DOTALL)
        return int(m.group(1)) if m else 0

    return {
        "finals_mvp_awards": _extract_int(PATTERNS["finals_mvp_awards"]),
        "all_defensive_selections": _extract_int(PATTERNS["all_defensive_selections"]),
        "dpoy_awards": _extract_int(PATTERNS["dpoy_awards"]),
        "scoring_titles": _extract_int(PATTERNS["scoring_titles"]),
    }


def test_michael_jordan() -> None:
    html = MOCK_HTML_SNIPPETS["Michael Jordan"]
    got = extract_legacy_accolades(html)

    assert got["finals_mvp_awards"] == 6, f"Expected 6 Finals MVPs, got {got['finals_mvp_awards']}"
    assert got["all_defensive_selections"] == 9, f"Expected 9 All-Defensive, got {got['all_defensive_selections']}"
    assert got["dpoy_awards"] == 1, f"Expected 1 DPOY, got {got['dpoy_awards']}"
    assert got["scoring_titles"] == 10, f"Expected 10 scoring titles, got {got['scoring_titles']}"


def test_hakeem_olajuwon() -> None:
    html = MOCK_HTML_SNIPPETS["Hakeem Olajuwon"]
    got = extract_legacy_accolades(html)

    assert got["dpoy_awards"] == 2, f"Expected 2 DPOYs, got {got['dpoy_awards']}"


def test_gary_payton() -> None:
    html = MOCK_HTML_SNIPPETS["Gary Payton"]
    got = extract_legacy_accolades(html)

    # This specifically tests the "(?:Defensive Player|DPOY)" variation.
    assert got["dpoy_awards"] == 1, f"Expected 1 DPOY, got {got['dpoy_awards']}"


def run_all_tests() -> None:
    test_michael_jordan()
    test_hakeem_olajuwon()
    test_gary_payton()
    print("✅ verify_legacy_stats.py: All legacy accolade regex tests PASSED.")


if __name__ == "__main__":
    run_all_tests()
