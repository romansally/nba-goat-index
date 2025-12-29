# tests/verify_dpoy_bling_patterns.py
#
# Standalone, fast QA script to verify that DPOY can be detected reliably
# from Basketball-Reference's <ul id="bling"> section, using patterns that
# match "Def. POY" (NOT generic sitewide award nav links).
#
# Run:
#   python tests/verify_dpoy_bling_patterns.py
#
# Notes:
# - This is NOT the full scraper run. It fetches only a few player pages.
# - It prints the bling items and shows which DPOY regex patterns match.

import sys
import re
from bs4 import BeautifulSoup

# Ensure repo root is on path
sys.path.insert(0, ".")

from src.ingestion.basketball_ref_scraper import BasketballReferenceScraper  # noqa: E402


def extract_bling(html: str) -> str:
    """Extract the <ul id="bling">...</ul> block as raw HTML."""
    m = re.search(
        r'<ul[^>]*id="bling"[^>]*>.*?</ul>',
        html,
        flags=re.IGNORECASE | re.DOTALL
    )
    return m.group(0) if m else ""


def main() -> None:
    scraper = BasketballReferenceScraper()

    targets = [
        ("jordami01", "Jordan"),   # single-award year-form: "1987-88 Def. POY"
        ("leonaka01", "Kawhi"),    # multi-award: "2x Def. POY"
        ("olajuha01", "Hakeem"),   # multi-award: "2x Def. POY"
        ("curryst01", "Curry"),    # should have no DPOY in bling
        ("russebi01", "Russell"),  # should have no DPOY in bling
    ]

    # Patterns that reflect how Basketball-Reference renders DPOY in bling.
    dpoy_patterns = [
        # Multi-award form: "2x Def. POY"
        r'(\d{1,2})\s*[×x]\s*Def\.\s*POY',
        # Single-award year form: "1987-88 Def. POY"
        r'\b\d{4}-\d{2}\s*Def\.\s*POY\b',
        # Fallback single form: "Def. POY"
        r'\bDef\.\s*POY\b',
    ]

    for pid, name in targets:
        url = scraper._construct_player_url(pid)
        resp = scraper._make_request(url)
        if not resp:
            print(f"\nFAILED to fetch {name} ({pid})")
            continue

        html = resp.text
        bling = extract_bling(html)

        print("\n" + "=" * 80)
        print(f"{name} ({pid}) bling found? {bool(bling)} len={len(bling)}")

        if not bling:
            print("No bling section found. Cannot test patterns.")
            continue

        soup = BeautifulSoup(bling, "html.parser")
        items = [li.get_text(" ", strip=True) for li in soup.find_all("li")]

        # Print the bling items for human inspection
        for item in items:
            print("-", item)

        # Pattern checks against a compacted bling-text string
        compact = " ".join(items)
        print("\nDPOY pattern checks:")
        for pat in dpoy_patterns:
            m = re.search(pat, compact)
            if m:
                # If pattern has a capture group (e.g., "2x Def. POY"), show it
                group1 = m.group(1) if m.groups() else None
                print(f"  MATCH ✅  pat={pat!r}  group1={group1!r}  matched={m.group(0)!r}")
            else:
                print(f"  no match ❌ pat={pat!r}")

        # Quick inferred count (mirrors your scraper's implied-1 logic conceptually)
        inferred = None
        m_num = re.search(dpoy_patterns[0], compact)
        if m_num and m_num.group(1):
            inferred = int(m_num.group(1))
        elif re.search(dpoy_patterns[1], compact) or re.search(dpoy_patterns[2], compact):
            inferred = 1
        else:
            inferred = 0

        print(f"\nInferred DPOY count from bling: {inferred}")

    print("\nDone.")


if __name__ == "__main__":
    main()
