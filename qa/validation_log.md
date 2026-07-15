# Validation Log

## 2026-07-13 — T7: scoring engine + invariants + golden snapshot

**Result:** `sql/05_final_goat_scores.sql` (min–max scaling, blends, accolade renormalization,
weighted final, §6 ranking) driven by `pipeline/score.py` (config validation, scope weight
vector, provenance, fail-loud output guard) with `pipeline/compare.py` as the §6 pairwise
view. Full gate green: 207 tests at the round-1 commit (152 at T6 close; this line
originally said 205 — see the process note in the post-review section), 235 after the
review round. `make check` offline.

### The two-reference correctness proof

- **Hand worksheet (independent):** the engine reproduces every locked
  `docs/methodology/v1_hand_worksheet.md` final — career B 57.58 / A 49.63 / C 45.28, peak
  B 58.49 / C 50.32 / A 44.03 — at the worksheet's ±0.01, plus all 18 career component values
  at 1e-4, from constants typed into `tests/unit/test_golden.py`, never read from the golden
  file. The worksheet was hand-computed in T2, before any engine code existed.
- **Golden snapshot (regression pin):** `tests/golden/v1_scores.json` locks full-precision
  finals, components, and ranks for BOTH scopes, plus `method_version` and a sha256 of the
  **parsed** config (canonical JSON — comment edits don't break it; any value change fails the
  guard structurally). No timestamp/git_sha inside: regeneration on unchanged inputs is
  byte-identical (verified — two `--write-golden` runs, one hash), so git records when/who.
- **Independent oracle:** a second, pandas-only implementation of §6 (tests-only — production
  keeps one scoring implementation, in SQL) matches every engine component and final at 1e-9
  on the fixture trio AND the real seed, both scopes — including the §12.7 DPOY
  renormalization for Wilt/West/Russell/Oscar.
- **What the trio references cannot pin (corrected in review round 2 — the original wording
  here overclaimed):** every trio career is shorter than `peak_n`, so the worksheet and the
  golden payload are numerically blind to top-5 **window sizing** (a `peak_n` 5→6 semantic
  change leaves both untouched). That behavior is pinned by the seven-season transform tests
  and, since round 2, an end-to-end seven-season scoring test; a `peak_n` **config** edit is
  caught structurally by the golden config hash. Credit belongs to those mechanisms, not to
  the double-pin alone.

### Bugs found by the invariants during T7 (both fixed pre-commit)

1. **Determinism (§10.1) caught real cross-process wobble:** `comp_accolades` varied ~7e-15
   between runs — DuckDB's multi-threaded operators combine float partial sums in
   scheduling-dependent order. Fixed with `SET threads = 1` on the scoring connection and,
   as defense-in-depth for the same risk class, the transform connection (cost nil at a few
   hundred rows). Six post-fix cross-process runs hash identically.
2. **Bounds (§10.2) caught a rounding-order defect:** the first minmax macro computed
   `(100·(x−lo))/(hi−lo)`, which double-rounds and pushed PlayerA's playoff component to
   100.00000000000001; the engine's own guard refused the output. Fixed to
   `100·((x−lo)/(hi−lo))` — the §6 formula shape — which is exact at both anchors
   ((hi−lo)/(hi−lo) = 1.0 in IEEE).

### West marginal-impact regression (T6 deferral, closed)

`test_west_cameo_marginal_impact`: removing only the 1967 one-minute playoff cameo moves
West's final career score by **0.052173** — matching the T6 reviewer's independent derivation
(0.05217) at 1e-4 — leaves his rank at 15, and leaves all 19 other rows **bit-identical**
(West is interior on `playoff_raw`; no min–max anchor moves). The documented cameo bound
holds end-to-end through scaling and weights.

### Real-data findings from the T7 investigation

1. **Min–max is stable on this pool:** all 7 raw component inputs have 20 distinct values;
   the tightest anchor gap is 0.44% of range (playoff min: Moses 111.00 vs Oscar 112.59).
   Award-rate anchor ties are exact ties (12 × DPOY 0.0, 3 × FMVP 0.0, 2 × ring 1/21) —
   deterministic under MM. No final-score ties (smallest gap: career 0.021, peak 0.120), no
   2-decimal rounding collisions in either scope.
2. **Trial career ranking:** LeBron 69.10 · Kareem 61.14 · Jokić 59.43 · Magic 58.27 ·
   Wilt 54.67 · Jordan 52.91 · … · Russell 27.42 (#16) · Moses 15.91 (#20). Two structural
   observations for T9's honest Spearman interpretation (documented pool-relative behavior,
   v1.md §12.2 — the iron rule forbids tuning): LeBron is a runaway max anchor on longevity
   (33.8% of range above #2 Kareem) and playoff volume (38.6% above #2 Duncan), compressing
   mid-pool spread there; and the 50/25/25 SPI blend rewards elite rebound/assist
   per-possession production (Jokić anchors peak AND spi_career; Russell is the pool min on
   peak, longevity, and both efficiency halves — three 0.00 components against a 74.07
   accolade score). The golden snapshot locks the fixture trio only; the real ranking stays
   open for T9's reporting.
3. **Peak-scope ranking** (career → peak): Jokić 78.15 #1, Magic 72.82, Jordan 72.70;
   PlayerC-style short-career peaks rise exactly as §7 intends. Renormalized weights sum to
   1.0 in float64 exactly.
4. **v1.md §10.5 wording imprecision fixed (clarity edit, no version bump):** strictness was
   claimed for any player "not at the pool maximum", but a *unique pool minimum* also moves
   with a rising min-holder (MM stays 0 — weak, not strict). The invariant statement now says
   "strictly inside the pool range"; scoring behavior is untouched (no code reads that
   sentence), and the tests were always scoped to interior players per the section's own
   final sentence.

### Negative proof (verified once and reverted)

1. **Any weight change breaks the golden (PRD T7 acceptance 3):** flipping
   `weights.peak 0.25→0.26` / `longevity 0.10→0.09` (sum still 1.0, so config validation
   stays green) failed 4 guards — worksheet reproduction in both scopes, the live-engine pin,
   and the config hash. Reverted → 8/8 green.
2. **A tampered golden file cannot pass:** editing one stored final (57.58→57.68) failed both
   the live-engine pin and the golden-vs-worksheet cross-check. Restored by regeneration to
   the identical byte hash.
3. **The engine polices its own output:** `test_guard_raises_on_violation` proves the §10
   guard raises on a dropped row, duplicated rank, NaN component, and out-of-bounds score.

### Post-review fixes (Codex 4-Point review, 2026-07-13 — changes requested)

The review independently confirmed the core math ("no other default-v1 formula, ranking,
award-rate, or worksheet discrepancy was found" across the full pool, both scopes) and found
four guardrail/validation gaps, each demonstrated with reproducible numbers, all fixed:

- **Blocking (confirmed): near-degenerate anchors silently amplified noise or masked
  corruption.** Two demonstrated escapes: ws set proportional to minutes gave a WS48 pool
  span of 5.55e-17 — pure float noise — which min–max stretched into a full 0/50/75/100
  spread (13 of 20 ranks changed, largest final swing 11.6873); uniformly zeroed ws +
  team_srs (contract-valid row by row) flowed 50s through the exact-degenerate rule and
  changed 12 of 20 ranks. Fixed with a **documented near-equality policy** in
  `pipeline/score.py::_check_anchor_ranges`: the seven continuous component inputs must show
  a pool range above 1e-9 RELATIVE to their magnitude (an all-equal continuous pool is
  corruption-shaped — real careers are never bit-identical), and award rates — where exact
  ties are legitimate and fixture-exercised — refuse only the unequal-yet-noise-thin
  near-tie. The §6 exact-tie → 50 scaling rule is unchanged; no valid pool's scores change
  (real spans sit ≥ 9% of magnitude — 8 orders above the threshold). Tests:
  `test_near_constant_ws48_pool_refused`, `test_uniform_zero_anchors_refused`.
- **Confirmed: weight/config validation escapes.** (a) The 1e-9 sum tolerance admitted a
  demonstrated 5e-10 drift (peak weight 0.2500000005) that shifted scores at the 10th
  decimal while v1.md said "exactly" — tightened to **1e-12** and the spec/code relationship
  documented in v1.md §10.3 (decimal-exact vectors carry ~1e-16 representation error;
  equal-sixths Custom vectors still validate). (b) `method_version` had no schema check and
  a None produced valid scores with null provenance — now validated as a non-empty string
  before anything runs (parametrized rejection: None/NaN/int/mapping/list/empty/blank/
  missing). (c) A DPOY-only accolade vector gave every pre-1983 player a 0/0
  renormalization denominator, caught only downstream by the NaN guard — accolade weights
  are now **strictly positive at validation time** (v1.md §10.3 note).
- **Confirmed: config-hash schema-shape collision.** `{1: "v1"}` and `{"1": "v1"}` hashed
  identically while producing different provenance. `config_sha256` (now in
  `pipeline/golden.py`) enforces string-only keys and finite numbers recursively and
  serializes with `allow_nan=False` — noncanonical configs raise instead of colliding.
  Tests: parametrized `test_config_hash_rejects_noncanonical`.
- **Doc-only (confirmed): the "double-pin has no gap" framing overclaimed** — corrected
  above and in `docs/data_model.md`: peak-window sizing is credited to the seven-season
  tests, config drift to the hash.

Also fixed from the review's list: `tests/golden/` filename is now **derived** from
`method_version` (`pipeline/golden.py::golden_path` — a v2 regeneration writes
`v2_scores.json`, it cannot overwrite the locked v1 file; parity test added).
`pipeline/compare.py` rejects mixed-scope frames, mixed-method_version frames, and
self-comparison (a career/peak hybrid would produce a verdict whose numbers are not
comparable). The golden tooling moved from `score.py` to `pipeline/golden.py` (score.py
stays within the Rule 6 complexity budget with the new guards; regeneration is now
`python -m pipeline.golden`) — the regenerated snapshot is byte-identical to the committed
one, confirming the move changed nothing.

**Determinism hardening (review caveat + own follow-through):** the original test only
repeated within one process. Now: a **cross-process test** (two `python -m pipeline.score`
subprocesses must hash-agree — the shape of the original wobble), direct assertions that
BOTH layers' DuckDB connections run single-threaded, and the input-order dependency made
explicit — the review measured ~1.42e-13 cell drift under shuffled logically-identical
input rows, currently protected only by `clean()`'s key-sort. That sort is now documented
in `pipeline/clean.py` as a determinism dependency, pinned by
`test_clean_output_key_sorted`, and `test_shuffled_input_order_bounded_and_resortable`
proves the loop closes: shuffled input keeps ranks with drift ≤ 1e-9, and re-sorting by the
clean keys restores byte-identical output.

**Independently verified by the reviewer (logged as evidence, no change needed):**

1. **Min–max anchor exactness:** on PlayerA's playoff element (hi = x = 50.833333333333336,
   lo = 9.25) the pre-fix form `(100·(x−lo))/(hi−lo)` yields 100.00000000000001; the shipped
   form `100·((x−lo)/(hi−lo))` yields exactly 100.0.
2. **Accolade renormalization, all 20 real players, both scopes, to 1.421e-14:** career —
   16 players use denominator 1.0 and four (Wilt/Russell/West/Oscar) exclude DPOY at 0.925;
   peak — 15 use 1.0, two use 0.925, and three exclude DPOY + Finals MVP at 0.775 (their
   top-5 windows predate the 1969 Finals MVP).

**Process note (recurring count drift, T6's 16-vs-20 pattern):** the round-1 entry above
logged "205 tests" written before the final gate run added two guard tests (actual: 207).
Corrected in place; from this round on, logged counts are updated only AFTER the final gate
run of a round. **Round-2 final gate: 235 passed** (207 → 235, +28 this round).
End-to-end scoring test additions this round: seven-season peak-scope pool
(the trio cannot exercise window sizing), near-degenerate refusals, method_version schema,
tolerance rejection, canonical-hash rejections, golden filename parity, pairwise
homogeneity/self-compare rejections, cross-process + shuffled-input determinism.

### Post-re-review fixes (Codex 4-Point re-review, 2026-07-13 — one severe + follow-through)

The re-review confirmed all six round-2 fixes work as tested and stated explicitly that no
other default-real-data formula, accolade, ranking, golden-value, compare, or determinism
discrepancy exists beyond its findings. All findings fixed:

- **Severe (confirmed): the near-degenerate threshold failed for anchors straddling zero.**
  A purely relative threshold (`range ≤ 1e-9 · max(|lo|, |hi|)`) shrinks to nothing when
  both anchors are tiny: per-player srs_w spanning ±1e-15 (contract-valid) passed the guard,
  and reversing which player received which noise value flipped 18 of 20 ranks (max final
  swing 10.0). Fixed with an absolute floor: `range ≤ 1e-12 + 1e-9 · max(|lo|, |hi|)`
  (`NEAR_DEGENERATE_ATOL`). Tests: `test_tiny_symmetric_srs_refused` and
  `test_tiny_srs_reversal_cannot_change_output` — with refusal, the reversal guarantee holds
  the strongest way: neither ordering scores at all.
- **METHODOLOGY DECISION (owner call, not resolved as a side effect):** the reviewer
  correctly flagged that refusing degenerate/near-degenerate continuous pools changes
  observable behavior vs v1.md §6's literal "every player scores 50.0" for a class of
  contract-valid inputs. Three options were laid out with tradeoffs (preserve-§6 + metadata;
  refuse + ADR without bump; full v2 bump); the owner chose **refuse + ADR-0002, no
  method_version bump**. Rationale (full record: `docs/adr/0002-continuous-anchor-input-
  domain.md`): it is a domain restriction — no in-domain input's output changes, proven
  mechanically by the golden snapshot remaining byte-identical; §9 already establishes
  fail-loud input refusal as v1-native; no committed artifact ever exercised continuous
  degenerate-50. v1.md amended accordingly: §6's degenerate rule is scoped to award-rate
  elements (the only case the worksheet exercises) and §9 (retitled "Missing data and
  input-domain policy") carries the anchor-range rule with the exact threshold.
- **Confirmed: the guard wrongly policed `longevity_raw` under peak scope**, where §7 drops
  the component (zero weight, absent from output). The guard is now scope-aware. Test:
  `test_peak_scope_ignores_longevity_degeneracy` (peak-scope degenerate longevity scores;
  the identical tamper under career scope is refused).
- **Confirmed: `production_blend` still validated at the old 1e-9** in `transform.py` while
  the scoring blocks were tightened to 1e-12 — a demonstrated pts = 0.5000000005 (sum
  1.0000000005) passed the public path and moved a fixture final by 1.8e-8. Fixed:
  `WEIGHT_SUM_ATOL = 1e-12` now lives in `transform.py` as the single shared constant
  (score.py imports it — no drift between layers is possible). Test: new
  `test_config_validation_branches` case.
- **Confirmed: golden filename derivation was defeatable** by a path-like version —
  `method_version="../golden/v1"` resolved onto the existing v1 snapshot, bypassing the
  round-2 overwrite protection. `golden_path` now requires a bare `v<N>` token
  (`^v[1-9][0-9]*$`). Test: parametrized `test_golden_path_rejects_unsafe_method_version`
  (traversal, subpath, absolute path, v0, V1, trailing space, empty, zero-padded, non-str).
- **Test bug (confirmed):** the shuffled-input assertion used `atol=1e-9` without `rtol=0`,
  so pandas' default relative tolerance would have admitted ~5e-4 drift at score magnitude
  100. Fixed: `rtol=0`.

**Round-3 final gate: 248 passed** (235 → 248, +13 this round), `make check` offline,
golden snapshot byte-identical through every change in the round — the boundary condition
of the ADR-0002 decision, verified after each edit.

### Post-round-3 fixes (Codex 4-Point review round 3, 2026-07-13 — governance + docs)

Round-3 verdict: **all six round-2 technical fixes PASS with strong verification**; two
findings requiring attention, both resolved:

- **GOVERNANCE (the important one — owner decision, second of the tier):** Codex correctly
  objected that ADR-0002 could not override CLAUDE.md Rule 1 — Rule 1 as written required a
  v2 bump for ANY defined-behavior change, and the uniform-zero input genuinely changed from
  "scores returned" to "raises"; the ADR's "domain restriction" category existed nowhere in
  the constitution. The owner was given (a) exact amendment wording, (b) an exploit-resistance
  analysis, (c) an honest amendment-vs-bump assessment, and **approved amending Rule 1
  itself**: a new input-domain tightening clause permitting bump-free refusal changes only
  under four mechanically-verified conditions (refuse-only whole-run abort with no accepted
  output changed; input-decidable predicate; reference artifacts untouched with golden
  byte-identical sans regeneration; methodology doc amended citing the ADR) — widening back
  is explicitly v2. Recorded in **ADR-0003** (the constitutional amendment, with a
  condition-by-condition compliance checklist for the first application) and **ADR-0002
  revised** to invoke the clause as its authority, with the process history kept honestly
  (originally shipped as an implicit exception; objected to; constitution amended; ADR
  re-grounded). Two clauses were tightened from the reviewed draft and flagged to the owner:
  condition 3 scoped to the named reference artifacts (the literal draft would have forbidden
  code changes, unsatisfiable by any real diff), and Rule 1's "typos/clarity only" sentence
  cross-referenced to condition 4 (which would otherwise contradict it). Evidence for this
  application verified empirically: relative to commit 09fef29 (the commit immediately
  preceding the guard — the condition-3 baseline, defined in ADR-0003), zero diffs under
  `data/seed/`, `tests/fixtures/`, `tests/golden/`; gate green; golden byte-identical
  (SHA-256 `50944af1…8342` at 09fef29 and now).
- **Doc contradiction (confirmed): v1.md §12.2 claimed the final score is min–max scaled**
  and anchors 0/100 — §6 correctly defines it as an un-restretched weighted sum, and real
  output proves it (career finals span ≈16–69, peak ≈23–78; no player at 0 or 100). §12.2
  now describes element-level anchoring with blended components and finals as convex
  combinations that inherit pool-relativity without anchoring the scale.
- **Stale degenerate-50 wording:** `docs/data_model.md` now states the refuse behavior for
  continuous inputs (with the peak-scope Longevity exemption) and scopes 50.0 to award-rate
  elements; the hand worksheet's two degenerate-rule mentions are scoped the same way (prose
  only — no locked value touched).
- **Tests added (Codex round-3 list):** `test_noise_thin_threshold_pinned_exactly` — pins
  the guard formula at the exact float fixed-point boundary (t = atol + rtol·t) inclusively,
  one ulp above it exclusively, and each term's necessity separately (dropping atol reopens
  the straddling-zero severe finding; dropping rtol reopens the round-1 large-magnitude
  case); `test_golden_main_v2_writes_new_file_and_preserves_v1` — runs the FULL
  `pipeline.golden.main()` write path with method_version="v2" against a temp dir: creates
  v2_scores.json, leaves both v1 copies byte-identical, and the v2 scores equal the locked
  v1 scores exactly (only the version string and config hash differ).

**Round-4 final gate: 250 passed** (248 → 250, +2 this round), `make check` offline, golden
byte-identical, reference artifacts untouched relative to the 09fef29 baseline (0 diffs
under seed/fixtures/golden vs that commit — the ADR-0003 condition-3 evidence, checked
directly; vs `main` the golden file is necessarily an addition, since T7 created it).

### Post-round-4 fixes (Codex 4-Point review round 4, 2026-07-13/14 — evidentiary precision)

Round-4 verdict: no severe runtime/scoring bug; the reviewer independently confirmed
09fef29 and the current code produce byte-identical real-seed output (the no-bump
decision's mechanical foundation). Four precision findings, all fixed:

- **Confirmed: ADR-0003's condition-3 claim was imprecise** — "no diff under tests/golden/
  across the entire change" is false against `main`, where the cumulative T7 diff ADDS
  v1_scores.json (the file was born in 09fef29). Fixed: the condition-3 evidence now names
  its baseline explicitly — commit 09fef29, the commit immediately preceding the guard —
  with the golden SHA-256 (`50944af1…8342`, identical then and now, re-verified via
  `git show 09fef29:… | shasum`) and zero diff lines under all three reference paths vs
  that baseline. ADR-0003 gained a "Baseline for condition 3" section defining "unchanged"
  as relative-to-the-immediately-preceding-commit and explaining why commit-boundary
  baselining cannot launder reference changes (the baseline is fixed by history, not
  chooseable; same-change edits always diff against it; prior-commit edits face their own
  gates — Rule 5 for goldens, visible review for seed/fixtures). The same imprecision was
  fixed in ADR-0002's compliance item 3 and both QA-log claims above. **[Superseded in the
  round-5 remediation below: the single-commit baseline was itself exploitable — "fixed by
  history" is false because commit boundaries are author-controlled. The definition is now
  the merge base against main.]**
- **Confirmed: v1.md §9 still used the round-3-rejected framing** ("a domain restriction,
  not a scoring change") — contradicting what the Rule 1 amendment establishes. Replaced
  with the precise statement: it changes the accepted input domain without changing any
  score for an input that remains accepted — a behavior-affecting change shipping bump-free
  under the Rule 1 clause.
- **Confirmed: ADR-0002's metadata alternative was self-contradictory** (claimed noise-thin
  pools would score AND that ULP-apart inputs would refuse). Rewritten to describe the
  alternative actually presented to the owner — §6-literal for exact ties, refusal only for
  near-ties, plus diagnostics — with the pure metadata-only variant rejected a fortiori.
- **Confirmed (minor): the `_noise_thin` boundary test's docstring inverted the semantics**
  — "one float step above [the threshold] is refused" where the assertion proves it is
  ACCEPTED (no longer noise-thin). Docstring and inline comment corrected to match what the
  test proves.
- **Test added (round-4 request):** `test_refusal_precedes_scoring_connection` — patches
  `pipeline.score._connect` to raise if ever called, submits the uniform-zero pool, and
  asserts the anchor-range ScoringError still fires: refusal mechanically precedes any
  scoring SQL connection, not merely any output. (The transform layer's own connection is
  untouched — the guard consumes the marts it builds.)

**Round-5 final gate: 251 passed** (250 → 251, +1 this round), `make check` offline, golden
byte-identical (SHA re-verified against the 09fef29 baseline).

### Post-round-5 fixes (Codex 4-Point review round 5, 2026-07-14 — a working exploit)

Round-5 verdict: everything passes except one finding — the most important of the tier:
**Codex constructed a working exploit against ADR-0003's anti-laundering defense.** The
claim "the baseline is fixed by history, not chooseable" is false — commit boundaries are
author-controlled. The demonstrated two-step attack: commit A quietly modifies the seed so
a future guard won't reject it (passes contracts and the gate; touches no golden, so
Rule 5 never fires); commit B introduces the guard. "The commit immediately before the
tightening" is now commit A, so a per-commit zero-diff check passes cleanly while the seed
actually moved — and merging both together hides the maneuver entirely. Fixed:

- **Condition 3 redefined on the merge base.** "The change" is now the ENTIRE reviewed
  merge diff relative to the target branch's merge base (`git merge-base main HEAD`),
  never any single commit — commits A and B both land inside that diff no matter how the
  work is split or ordered, so the attack produces a visible seed diff by construction. A
  prior artifact edit counts as separate only if independently reviewed and merged to the
  target branch BEFORE the change began. Reworded in CLAUDE.md Rule 1 condition 3 itself
  and in ADR-0003's verbatim quote (kept in sync — a third constitutional wording change,
  flagged to the owner like the previous two: leaving the constitution on the old wording
  while fixing only the ADR would have left the exploit alive by interpretation).
  ADR-0003's baseline section now records the exploit and the corrected reasoning;
  ADR-0002's compliance item 3 matches.
- **Created-within-the-change allowance (the founding case, made explicit):**
  `tests/golden/v1_scores.json` cannot show "zero diff from main" — main carries only
  `.gitkeep`; the file was born in this change at 09fef29. The allowance is narrow: no
  prior version existed (nothing to launder), bytes pinned from the creation commit
  through the tip (blob OID equality, 09fef29 vs working tree — SHA-256 `50944af1…8342`
  throughout), and the created artifact still faces its own independent gates (the
  worksheet constants in test_golden.py; Rule 5 thereafter).
- **Mechanical enforcement added:** `tests/unit/test_reference_artifacts.py` runs in every
  `make check` and recomputes the condition-3 evidence against the merge base — seed +
  fixtures zero-diff, merge-base goldens blob-identical, created goldens pinned from their
  most recent add-commit. On main itself it degrades to "no uncommitted reference edits."
  **[Superseded in the round-6 remediation below: "most recent add-commit" was itself a
  bypass — delete/re-add reset the pin — along with two more holes in this first version
  of the enforcement.]**
  Negative proofs, verified once and reverted: a one-byte seed append failed
  `test_seed_and_fixtures_match_merge_base`; a one-digit golden edit failed
  `test_created_goldens_pinned_from_creation` — the tampered CREATED golden was caught by
  the creation-pin rule itself, demonstrating the carve-out is not a hole.

**Round-6 final gate: 254 passed** (251 → 254, +3 this round), `make check` offline, golden
byte-identical, all three reference-artifact guards green against merge base 05a3947.

### Post-round-6 fixes (Codex 4-Point review round 6, 2026-07-14 — enforcement bypasses)

Round-6 verdict: the merge-base fix genuinely closes the round-5 exploit and the branch
passes cleanly under it — but the re-break challenge succeeded against the ENFORCEMENT
mechanism itself: three bypasses in the first version of
`tests/unit/test_reference_artifacts.py`, all severe as workflow defects because ADR-0003
claims mechanical enforcement that these sequences defeated. All closed:

- **Severe (confirmed): delete/re-add reset the creation pin.** The check pinned created
  goldens to their "most recent add commit," so create → delete → re-add-with-different-
  bytes passed cleanly — contradicting "no prior version, pinned from creation." Fixed:
  `check_created_goldens` now inspects the full `merge_base..HEAD` history — exactly ONE
  add, ZERO deletes — and additionally requires the path to have NO history reachable from
  the merge base at all (a file that existed on main and was removed before the merge base
  has a prior version to launder; it must not qualify as "genuinely new"). Regression
  tests build both attack shapes in throwaway git repos:
  `test_delete_readd_laundering_fails`, `test_removed_before_merge_base_is_not_genuinely_new`.
- **Severe (confirmed): non-JSON goldens bypassed every check.** Both golden-detection
  helpers filtered on `.json`, while Rule 1 protects everything under `tests/golden/`.
  Fixed: every tracked file under `tests/golden/` is protected regardless of extension,
  with an explicit metadata allowlist (`.gitkeep` only). A deleted merge-base golden is
  now also a named violation (previously a raw subprocess error). Regression:
  `test_non_json_golden_is_protected` (tampered `.csv` golden refused).
- **Lower severity (confirmed): criss-cross histories yield multiple merge bases** and
  `git merge-base` silently picks one, potentially hiding drift relative to the unchecked
  candidate. Fixed: `resolve_merge_base` uses `merge-base --all`, requires exactly one
  result, fails loudly otherwise. Regression: `test_multiple_merge_bases_rejected`
  (constructs a real criss-cross: main and a side branch each merge the other's pre-merge
  tip).
- **Shallow repositories now rejected outright** (`rev-parse --is-shallow-repository` must
  be false) instead of documented as unsupported — the creation-pin and merge-base checks
  are meaningless without full history. Regression: `test_shallow_repository_rejected`
  (real `--depth 1` clone).

CLAUDE.md needed NO wording change this round: condition 3's text ("no prior version …
pinned from its creation commit through the tip") already stated the correct requirement —
the enforcement code caught up to the constitution. ADR-0003's enforcement descriptions
updated to the hardened semantics; the round-6 entry's "most recent add-commit" claim
carries a supersession marker.

**Round-7 final gate: 259 passed** (254 → 259, +5: two laundering regressions, non-JSON
protection, criss-cross rejection, shallow rejection), `make check` offline, golden
byte-identical, all guards green against merge base 05a3947.

### Post-round-7 fixes (Codex 4-Point review round 7, 2026-07-14 — the index exploit)

Round-7 verdict: all round-6 fixes confirmed solid (5/5 regression tests pass) and the
branch stays clean — but a new, FUNDAMENTAL exploit was found and reproduced by the
reviewer in a throwaway repo: **the guard checked working-tree bytes, while `git commit`
records INDEX bytes, and the two can be made to differ.** Stage malicious bytes
(`git add`), restore the safe content to the working file without re-staging: every
working-tree check passes, and the next commit records the malicious staged version
anyway. The same gap covered goldens (only the working-tree hash was checked, never the
staged entry) and generalized to untracked files under protected paths never being
examined at all. All closed:

- **Seed/fixtures now checked in BOTH places:** `git diff <merge-base>` (working tree)
  AND `git diff --cached <merge-base>` (index) must be empty for the protected trees.
  Regression: `test_staged_seed_bytes_caught_despite_clean_working_tree` reproduces the
  reviewer's exact sequence — stage malicious seed bytes, restore safe working bytes —
  and asserts the index leg fires.
- **Goldens verified as (mode, blob) pairs in BOTH the staged index entry and the working
  tree** against the expected entry (merge base for pre-existing goldens; the sole
  creation commit for created ones). Mode is part of the comparison: a regular-file→
  symlink swap with identical apparent content is a 100644→120000 mode change and is
  rejected; only plain regular files are valid reference artifacts (executable bits are
  likewise refused). Regressions: `test_staged_golden_bytes_caught_despite_pinned_
  working_tree`, `test_symlink_mode_swap_rejected` (symlink to a decoy file with
  byte-identical apparent content).
- **Untracked files under protected trees refused outright** (`git ls-files --others
  --exclude-standard`) — an untracked artifact could otherwise be staged the moment after
  a passing check. New standing guard test on the real repo plus regression
  `test_untracked_file_under_protected_tree_caught`.
- A golden missing from the INDEX (unstaged/removed) is now its own named violation,
  alongside the existing working-tree-missing case.

Guard suite: 8 → 13 tests (4 standing guards on the real repo + 9 bypass regressions on
throwaway histories). CLAUDE.md again needed no wording change — condition 3 forbids
modification "in the same change" without qualifying where the bytes live; the
enforcement now checks everywhere a commit can source bytes from.

**Round-8 final gate: 264 passed** (259 → 264, +5: the untracked standing guard on the
real repo plus four new bypass regressions), `make check` offline, golden byte-identical,
all guards green against merge base 05a3947.

### Post-round-8 fixes (Codex 4-Point review round 8, 2026-07-14 — the tip-visibility gap)

Round-8 verdict: scoring/methodology fully clean (re-confirmed); one severe enforcement
gap, one smaller gap, one optional boundary note. All addressed:

- **Severe (confirmed, reviewer-reproduced): created-then-deleted goldens evaded every
  guard.** `check_created_goldens` iterated only paths tracked at the tip; a golden
  created mid-branch and deleted before the tip was in neither "tracked at merge base"
  nor "tracked now," so its history was never inspected — directly contradicting
  condition 3's "pinned from creation through the tip." Fixed exactly as prescribed: the
  candidate set is now the UNION of merge-base tree paths, current index paths, and every
  golden path touched anywhere in `merge_base..HEAD` (`git log --format= --name-only -z
  --no-renames`, so renames surface as explicit delete/add pairs and nothing hides in
  formatting); the existing rules — genuinely new, exactly one add, zero deletes, final
  index+worktree presence via `_verify_golden` — now apply to the full path set. The
  violation message states the rule plainly: a created reference artifact must appear
  exactly once and SURVIVE to the tip. Regression:
  `test_created_then_deleted_golden_is_rejected` (add → commit → delete → commit, no
  re-add: refused).
- **Smaller (confirmed): `--exclude-standard` let a `.gitignore` entry hide an untracked
  protected file** from the untracked check, contradicting "any untracked file." Fixed:
  the check now runs without exclusions — ignored or not, an untracked file under a
  protected tree is refused. Verified first that the real repo is clean under the
  stricter rule (zero untracked files under all three trees, no exclusions applied).
  Regression: `test_ignored_untracked_protected_file_is_rejected` (gitignored smuggled
  files under all three protected trees; the violation lists every one).
- **Boundary note — DECIDED: addressed, not just documented.** Default `git hash-object`
  applies clean filters, so a `.gitattributes` filter could normalize physically tampered
  working bytes back to the expected blob. Since the pipeline reads PHYSICAL bytes, raw
  physical equality is the invariant that matters: golden hashing now uses
  `--no-filters`. Consequence accepted and documented: an autocrlf-mutated checkout fails
  loudly, which is correct behavior for byte-exact reference data. The residual — the
  seed/fixtures legs ride on `git diff`, which respects filters — stays inside the
  round-8 threat boundary (local-run distortion; cannot reach main unseen) and is
  documented in the guard's docstring rather than rebuilt. Optional regression test
  implemented: `test_clean_filter_physical_difference_rejected`, which first PROVES the
  attack premise (the filtering hash equals the committed blob for physically different
  bytes) and then asserts the --no-filters check refuses it.

Guard suite: 13 → 16 tests. CLAUDE.md unchanged again — condition 3's "pinned from
creation through the tip" already forbade created-then-deleted artifacts; enforcement
caught up to the constitution for the third consecutive round.

**Round-9 final gate: 267 passed** (264 → 267, +3 regressions), `make check` offline,
golden byte-identical, all guards green against merge base 05a3947.

### Post-round-9 fixes (Codex 4-Point review round 9, 2026-07-14 — history simplification)

Round-9 verdict: one severe finding; scoring/methodology not re-flagged. Addressed:

- **Severe (confirmed, premise reproduced in-test): default `git log` history
  simplification let a merged side branch's golden history evade every guard.** All four
  path-limited history queries (`_goldens_touched`, the pre-merge-base prior-version
  lookup, and the adds/deletes queries in `check_created_goldens`) walked only one
  TREESAME parent at each merge — when a merge's result tree matches a parent for the
  queried path, git prunes the other parent's history entirely. A golden created and
  deleted on a side branch, merged with an ordinary `--no-ff` merge, left NO trace in any
  query: the round-8 exploit transported through a normal merge workflow, no adversarial
  git config required. Fixed exactly as prescribed: `--full-history` added to all four
  queries, forcing the walk over every commit reachable in the range. Regressions:
  - `test_merged_side_branch_created_then_deleted_golden_is_rejected` — create+delete a
    golden on a side branch, `--no-ff` merge into a feature whose first parent never had
    the path; the test first PROVES the premise (the simplified path-history of
    `merge_base..HEAD` is empty — the side branch is invisible without the flag), then
    asserts `check_created_goldens` refuses ("survive to the tip").
  - `test_prior_version_hidden_in_merged_ancestry_is_not_genuinely_new` — add+delete a
    golden on a side branch BEFORE the merge base, merge into main, reintroduce the path
    with different bytes inside the change; the `--full-history` prior-version lookup
    refuses ("not genuinely new").

  Both regressions verified against the unfixed code (flag stripped in a scratch copy):
  each fails with DID NOT RAISE — they genuinely pin the fix.

Guard suite: 16 → 18 tests. CLAUDE.md unchanged for the fourth consecutive round —
condition 3's "no prior version" and "pinned from creation through the tip" already
forbade both shapes; enforcement caught up to the constitution again.

**Round-10 final gate: 269 passed** (267 → 269, +2 regressions), `make check` offline,
golden byte-identical, all guards green against merge base 05a3947.

### Post-round-10 fixes (Codex 4-Point review round 10, 2026-07-14 — the committed-HEAD gap)

Round-10 verdict: one severe finding — the guard verified the working tree and the index
but never the COMMITTED HEAD tree, the mirror image of round 7's index gap: `git commit`
records the index, but a MERGE records HEAD's tree, so malicious bytes committed at HEAD
behind a safe index/worktree restoration passed every leg while a merge would still carry
the malicious committed bytes. All three prescribed fixes applied:

- **Fix 1 — `check_seed_and_fixtures` grew a third leg:** `git diff <merge-base> HEAD --
  data/seed tests/fixtures` must be empty, alongside the existing working-tree and
  `--cached` legs. Regression:
  `test_committed_head_seed_tamper_rejected_despite_safe_index_and_worktree` — commits
  malicious seed bytes, restores safe bytes to index AND worktree, first PROVES both
  pre-round-10 legs are clean, then asserts the committed-HEAD leg refuses.
- **Fix 2 — `_verify_golden` now requires the HEAD tree entry (mode + blob) to equal the
  expected entry**, alongside the existing staged-index and working-bytes checks.
  Regression: `test_committed_head_golden_tamper_rejected_despite_safe_index_and_worktree`
  — commits a tampered golden, restores the pinned bytes to index and worktree, first
  proves the staged entry equals the merge-base entry, then asserts refusal.
- **Fix 3 — creation-pin bypass, same root cause:** "one add, zero deletes" never
  inspected MODIFY commits, so create → modify-to-malicious → restore left tip, index,
  and worktree all equal to the creation entry while an intermediate commit carried
  different bytes. Fixed as prescribed: every commit on
  `git rev-list --ancestry-path <creation>..HEAD` must record exactly the creation entry
  (rev-list is not path-limited, so round-9's history-simplification trap does not apply
  here). Regression: `test_created_golden_modify_then_restore_is_rejected`.

All three regressions verified against the unfixed code (the three fixes reversed in a
scratch copy): each fails with DID NOT RAISE — they genuinely pin the fixes.

Guard suite: 18 → 21 tests. CLAUDE.md unchanged for the fifth consecutive round —
condition 3's "byte-identical" and "pinned from creation through the tip" already forbade
all three shapes; enforcement caught up to the constitution again.

**Process decision (owner, 2026-07-14):** round 11 is the FINAL review pass. It asks one
specific question — "the guard now checks HEAD, index, worktree, and full commit-ancestry
for created artifacts: are there any other places a commit's tree could diverge from what
the guard inspects?" — and any finding it produces is documented here as a known
limitation rather than fixed, and the branch merges regardless. Ten rounds of adversarial
hardening on a solo project's own governance mechanism is past reasonable diminishing
returns; the committed-HEAD gap was the last structurally distinct surface (worktree,
index, HEAD, ancestry), and everything found after round 7 was a variant of an
already-covered concept.

**Round-11 final gate: 272 passed** (269 → 272, +3 regressions), `make check` offline,
golden byte-identical, all guards green against merge base 05a3947.

### Round 11 (final pass, 2026-07-14): exhaustion confirmed — documented known limitations

Round-11 verdict: **exhaustion confirmed, T7 cleared for merge.** The final-pass question
("the guard now checks HEAD, index, worktree, and full commit-ancestry for created
artifacts — are there any other places a commit's tree could diverge from what the guard
inspects?") produced five enumerable divergence surfaces. Per the round-10 process
decision, they are documented here as known limitations — each is out of scope for this
guard's job, not an unaddressed bypass. None can place tampered reference bytes onto
`main` without appearing in the reviewed merge diff.

1. **Check-to-merge timing gap.** The guard runs when `make check` runs; nothing binds
   "the commit that passed the gate" to "the ref that gets merged" — commits made after a
   passing local gate are unchecked until the next run. Out of scope: the gate is a local
   invariant check, not a merge-time enforcement service. The residual is closed by
   policy (merge only a just-gated tip) today and mechanically by post-Tier-1 CI running
   the same gate on the exact merge ref; late commits are visible in the reviewed merge
   diff.
2. **Future merge-result differences.** The guard verifies the branch tip; the merge
   commit `main` will record is a NEW tree computed at merge time — conflict resolutions
   or content-level merging can make it differ from the verified tip. Out of scope: a
   tree cannot be inspected before it exists. The squash-merge policy keeps the merged
   tree equal to the reviewed tip's diff, and `main` must itself pass `make check`
   post-merge (main always green), where the guard re-verifies reference state.
3. **Locally controlled git state.** Every query the guard makes goes through the local
   `git` the developer controls — replace refs, `GIT_*` redirection,
   skip-worktree/assume-unchanged bits, a doctored `git` on PATH, or editing the guard
   file itself. Out of scope: a self-audit tool cannot attest the environment it runs in
   (shallow clones, the one detectable shape, ARE refused). This is the round-8
   documented threat boundary: local-run distortion, incapable of reaching `main` unseen
   because the merge diff exposes any actually-committed tampering.
4. **Checkout-transform boundary.** Git may transform bytes between the object database
   and the working tree (`.gitattributes` text/eol, clean/smudge filters). Golden
   verification pins RAW PHYSICAL bytes via `hash-object --no-filters` (round-8), but the
   seed/fixtures legs ride on `git diff`, which respects filters — physically different
   working bytes can appear "unchanged" to those two legs (the committed-HEAD leg
   compares commit trees and is immune). Out of scope: same local-run boundary as (3);
   committed blobs — what a merge propagates and a fresh clone materializes — are fully
   verified, per the round-8 decision recorded in the guard's docstring.
5. **`.gitkeep` allowlist.** `GOLDEN_ALLOWLIST` exempts the `.gitkeep` basename anywhere
   under `tests/golden/`, so a file with that exact name escapes golden verification.
   Out of scope: `.gitkeep` is directory-existence metadata that no pipeline or test ever
   reads as reference data; weaponizing it would require source code that READS a
   `.gitkeep`, which is a reviewable code change. The exemption is one named, greppable
   constant.

## 2026-07-12 — T6: star schema + DuckDB SQL transforms

**Result:** `make transform` runs clean → contract validation → SQL transforms →
`06_validation_checks.sql` end-to-end offline (`pipeline/transform.py` runs the Pandera
contracts on its input frames itself — the CLAUDE.md validate-before-transform order is
enforced in-process, not assumed). Every mart row count reconciles with its staged input
(PRD T6 acceptance criterion 2):

| mart | rows | reconciles against |
|---|---|---|
| dim_player | 20 | players (20) |
| dim_season | 70 | league_seasons (70) |
| fact_player_season | 335 | player_seasons (335) |
| fact_accolade | 668 | accolades (668) |
| mart_player_season_metrics | 335 | fact_player_season (335) |
| mart_player_component_inputs | 20 | dim_player (20) |
| mart_player_award_rates | 120 | 20 players × 6 awards |

Reconciliation is enforced, not just observed: the count checks in `06_validation_checks.sql`
compare marts against the staged inputs on every run and `pipeline/transform.py` raises on any
mismatch. All 24 named checks return zero rows on the real seed and on the fixture trio
(20 at initial commit — an earlier revision of this entry miscounted them as 16 — plus 3
added in the first review round and 1 in the re-review round below).

### Worksheet reproduction (the correctness proof)

`tests/unit/test_transforms.py` reproduces every hand-computed intermediate in
`docs/methodology/v1_hand_worksheet.md` — per-season REL/SPI/AVAIL/REL_TS, playoff P_SPI with
missed-postseason nulls, all raw component values, and all 18 award-rate cells (including
PlayerB's null DPOY rate and PlayerC's 1999-shrunk All-Star denominator) — at the worksheet's
4-decimal tolerance. Peak scope on the trio provably equals career scope (worksheet §7),
pinning the §12.9 scope-filter mechanism.

### Real-data findings from the T6 investigation

1. **Jerry West's 1967 one-minute playoff cameo grades P_SPI 1.052 off a single rebound**
   (1 gp, 1 mp, 0 pts, 1 trb → ~2.6 possessions → REL_trb ≈ 4.2). Bounded by construction:
   `p_spi × po_gp` adds ~1.05 of his 206.56 career playoff raw (~0.5%, ≈0.05 final points).
   v1.md §5.4 has no minutes qualifier — implemented exactly as specified, documented in
   `docs/data_model.md`, pinned by a regression sentinel test. A qualifier would be a v2
   (behavior-changing) decision.
2. **DPOY zero-eligibility is real, ×4:** Wilt, West, Russell, Oscar retired before 1983.
   Their DPOY rate is emitted as null with `eligible_seasons = 0` (never 0.0), the §12.7
   exclude-and-renormalize input. A test asserts the exact four-player set.
3. **Peak-window degenerate rules never fire on the seed** (minimum qualifying-season count is
   11, Jokić) but are implemented and exercised by the fixture trio (all ≤ 3 qualifying
   seasons → fewer-than-N rule; `peak_fallback` flag ships for run metadata).
4. **No zero-playoff careers in the seed** (minimum 8 runs), but `playoff_raw` coalesces a
   would-be-null SUM to 0 per §5.4; a check asserts non-null.
5. **Low-minute regular seasons don't distort REL ratios:** pool SPI spans [0.795, 2.092];
   the smallest seasons (Moses Malone 1993: 104 mp; Curry 2020: 139 mp) yield in-range SPI
   and ≤ 0.2 Longevity contribution after the AVAIL multiplier. Verified, no guard needed.
6. **TS denominator hole closed:** contracts floor `fga`/`fta` at 0, which alone would admit a
   0/0 season; `ts_denominator_positive` now requires `fga + 0.44·fta > 0` on every row.

### Negative proof (both directions, verified once and reverted)

1. **Checks raise on real violations:** temporarily tightening `avail_in_bounds` to
   `avail > 0.99` made `pipeline.transform` fail loudly, listing the named check and the
   violating player-seasons.
2. **Tests guard the checks:** temporarily neutering `p_spi_null_iff_missed_postseason`
   (`WHERE FALSE`) made `test_validation_checks_have_teeth` fail — the test that corrupts a
   fixture row (a `po_gp` without the other po_* columns) and asserts the named check catches
   it. That test is permanent, so silently weakening this check now breaks `make check`.

### Post-review fixes (Codex 4-Point review, 2026-07-12)

Three severe bugs, all demonstrated empirically by the reviewer with reproducible steps,
all fixed with tests that fail against the pre-fix code:

- **Bug (confirmed): transform never validated its inputs.** `make transform` chained
  clean → SQL directly; an impossible 200-game season sailed through (`LEAST(1, …)` capped
  AVAIL and the bad GP corrupted games-weighted SRS) despite Pandera rejecting it in
  isolation. Fixed: `run_transforms` now calls `validate_all(frames)` before any SQL —
  clean → validate → transform is enforced in-process for every caller. A `validate=False`
  escape hatch initially existed for tests that must reach the sql/06 guard layer with
  corruption contracts would intercept first — **superseded in the re-review round below:
  the public parameter was removed entirely in favor of a private test seam.**
  Tests: `test_contracts_gate_runs_before_sql` plus five parametrized denominator-guard
  cases (zero mp/po_mp/gp, zero pace, unmapped All-NBA team).
- **Bug (confirmed): award facts could be silently discarded.** The rate mart is driven by
  `accolade_intro_season`; deleting `mvp` from config left 3 real MVP facts in
  `fact_accolade`, emitted zero MVP rate rows, and every existing check stayed green.
  Fixed three ways: `pipeline/transform.py` enforces exact award parity (config intro
  seasons == the contract award registry, keys AND seasons, and == `accolade_weights` keys);
  sql/06 gained `award_key_known_to_config` and `all_nba_points_mapped` (data-side parity);
  and `accolades_reach_rate_mart` joins facts to the rate mart on `(player_id, award)` —
  not just player-season — requiring positive weighted wins for every in-scope eligible
  fact (**strengthened to exact two-way reconciliation in the re-review round below**).
  Tests: `test_award_key_parity_enforced`, `test_seed_award_wins_match_accolades`
  (independent pandas aggregation compared to the mart, all 120 cells).
- **Bug (confirmed): award numerator lacked the denominator's eligibility filter.** The
  wins CTE counted any selection joined to a played in-scope season; an illegal 1999
  All-Star selection would inflate `weighted_wins` for a player with other eligible seasons
  (the `weighted_wins > 0 AND eligible_seasons = 0` guard misses that case). Fixed: the
  wins CTE applies the identical intro-season + `asg_held` predicate as the eligibility
  CTE — defense-in-depth beneath the contracts, which already forbid such rows.
  Test: `test_ineligible_win_cannot_inflate_rate` (bypasses contracts deliberately).

Minor fixes from the same review: check-count claim corrected (20, not 16 — now 23);
`peak_n` validated as a positive integer (no silent `int()` truncation); `peak_min_avail`
validated finite in [0, 1]; `production_blend` validated for exact keys, finiteness, and
non-negativity; `all_nba_team_points` validated for exact keys, finiteness, and
non-negativity (NaN can no longer pass). Review test suggestions added: nontrivial
peak-scope restriction (7-season player, awards/playoffs inside AND outside the window —
the trio-only test was degenerate), fallback window ordering, peak tie-break at
`peak_n = 1`, and the full per-season worksheet grid (poss, per-75, REL, playoff REL,
both rank columns). Transform suite: 37 → 49 tests.

### Post-re-review fixes (Codex 4-Point re-review, 2026-07-12)

The re-review confirmed all three original bugs genuinely fixed, then found the new guards
had gaps of the same shape — proven by adversarial inputs, all now closed:

- **Moderate (confirmed): the reconciliation check only tested positivity.** Reducing a
  win aggregate by one (partial silent loss — the original Bug 2 shape) passed all checks
  because `weighted_wins > 0` still held. Fixed: `accolades_reach_rate_mart` is now an
  exact two-way reconciliation — an independent re-aggregation of the in-scope eligible
  facts FULL-JOINed to the mart, flagging any mismatch beyond 1e-9, missing mart rows
  (coalesced to a −1 sentinel so absence can't masquerade as zero), and phantom mart wins
  with no backing facts. Test: `test_reconciliation_catches_partial_award_loss` tampers a
  3-win aggregate to 2 mid-pipeline and asserts the named check fires.
- **Own finding, same shape (open investigation): `rate` itself was unreconciled.** The
  wins reconciliation cannot see a corrupt `rate` with intact `weighted_wins` — and rate
  is the value scoring actually consumes. Added `award_rate_consistent`
  (`rate = weighted_wins / eligible_seasons` wherever eligible > 0; check count 23 → 24)
  plus tamper test `test_rate_consistency_catches_corrupt_rate`.
- **Moderate (confirmed): All-NBA point config domain was inconsistent across layers.**
  Empirically reproduced before fixing: `third = 1.1` and `third = 0.0` both passed
  validation — a zero silently nulls every third-team credit, a value above 1 breaks the
  rate ≤ 1 guarantee that `award_rate_in_bounds` asserts. Fixed:
  `0 < third ≤ second ≤ first ≤ 1` enforced in `_validate_config`; seven parametrized
  domain tests (zero, above-one, reversed ordering, NaN, Inf, missing/extra key).
- **Moderate (confirmed): the new SQL guards had no direct negative tests.** Added direct
  proofs that each fires on its own, via the private seam:
  `test_unknown_award_check_fires` (unknown award type → `award_key_known_to_config`),
  `test_unmapped_all_nba_team_check_fires` (team 4 → `all_nba_points_mapped`), the two
  tamper tests above, and — found in the open investigation, same gap in an *older*
  guard — `test_ts_denominator_check_fires` (a contract-legal 0-FGA/0-FTA season, where
  the points identity forces pts = 0, produces IEEE NaN True Shooting in DuckDB, which
  survives the NOT NULL DDL; the named check makes it loud).
- **`validate=False` removed (optional item, taken):** `run_transforms` now validates
  config and contracts unconditionally with no bypass parameter. Corruption tests use the
  private seam (`_execute_transforms` / `_check_transforms` / `_fetch_marts`) — the same
  functions `run_transforms` composes, so the tested paths are the production paths.
- **Stealth ineligible-win case added:** with two eligible seasons and one valid
  selection, unfiltered numerator logic yields 2/2 = 1.0 — in bounds, invisible to every
  range check; `test_stealth_ineligible_win_stays_filtered` asserts 1/2 = 0.5.
- **Process:** sql/06 (300+ lines) now carries the CLAUDE.md Rule 6 complexity-budget
  justification in its header, contracts.py-style; `docs/data_model.md` no longer claims
  the West final-score effect is regression-tested (only the per-season P_SPI is — the
  final-score assertion is the T7 item below). Config branch coverage: 11 parametrized
  `_validate_config` cases. Transform suite 49 → 73 tests.

### Open investigation (re-review round — full-codebase sweep)

Beyond the reviewer's items, the sweep examined the SQL files, runner, config validation,
tests, and docs for anything wrong, fragile, or methodology-inconsistent. Findings:

1. **`rate` unreconciled** — found and fixed (above).
2. **`ts_denominator_positive` was unreachable-unproven** — probed DuckDB 1.5.4 division
   semantics (`1.0/0.0 = inf`, `0.0/0.0 = NaN`, integer division likewise; never NULL,
   never an error), which confirms the degenerate row *reaches* the check rather than
   dying at the DDL — proven reachable with a direct test (above).
3. **Examined and found sound, with reasoning:** the reconciliation check re-derives
   expected wins from the facts side, so it cannot catch a bug replicated identically in
   both aggregations — that residual risk is covered by the *independent pandas*
   aggregation in `test_seed_award_wins_match_accolades` and the hand-worksheet rate
   cells; `python -m pipeline.transform` run directly could read stale processed parquet
   (mitigated: `make transform` chains clean first, the frames that are transformed are
   the frames that are validated, and T8's orchestrator owns sequencing); float ties in
   peak ranking are deterministic via the season tiebreak, and the two worksheet tie
   pairs were verified bit-exact before their ranks were pinned; FK insert order in
   sql/02 (dims before facts) is correct for DuckDB's enforced REFERENCES; the f-string
   `SET VARIABLE` interpolation only ever renders values `_validate_config` has already
   proven finite numeric. Nothing further found — the investigation was done, and this
   list is its complete yield.

### Post-fourth-pass fixes (Codex review, 2026-07-13 — conditional clearance)

The fourth pass confirmed the re-review remediation and found one blocking gap plus one
minor, both reproduced against the pre-fix code before fixing (probe output on record):

- **Blocking (confirmed): the reconciliation's expected side only covered keys with award
  facts.** Two demonstrated escapes: a ghost zero-win row under an unknown key (total row
  count preserved — nothing on the expected side to compare against) and a corrupted
  `eligible_seasons` with a self-consistent rate (PlayerA MVP 3 → 2 with rate 1/3 → 1/2 —
  `award_rate_consistent` holds by construction, and eligibility was reconciled nowhere).
  Both passed all 24 checks. Fixed: `accolades_reach_rate_mart`'s expected side is now the
  COMPLETE scoped player × configured-award grid (scoped seasons CROSS JOIN the award
  registry, facts LEFT-JOINed on), carrying independently re-derived `eligible_seasons`
  AND `weighted_wins`; the FULL JOIN reconciles keys in both directions plus both values.
  Tests: `test_reconciliation_catches_eligibility_corruption`,
  `test_reconciliation_catches_ghost_zero_win_row`.
- **Minor (confirmed): `scope=""` silently selected career scope** through the falsey
  `scope or config["scope"]` fallback. Fixed with an explicit `is None` test so any
  non-None scope reaches `_set_params` and its career/peak validation. Test:
  `test_empty_scope_raises`.

Named checks remain 24 (the reconciliation check was strengthened in place). Transform
suite 73 → 76 tests; full gate 149 → 152. Codex confirmed no other blocking issue beyond
these two items.

**Deferred to T7 (logged, not implemented):** a West marginal-impact regression — once the
scoring engine exists, remove only the 1967 playoff cameo and assert the default final
score decreases by ~0.05 (reviewer's independent derivation: 0.05217), confirming the
documented cameo bound holds end-to-end through min–max scaling and weights.

## 2026-07-12 — T5: Pandera contracts + designed-bad fixtures

**Result:** `make validate` passes on the committed real seed (players 20 · player_seasons 335 ·
accolades 668 · league_seasons 70). The valid synthetic mini-set (the locked hand-worksheet
trio) passes. All 32 designed-bad fixture rows fail via the specific named check each is
labeled with — confirmed both by `tests/unit/test_contracts.py` (parametrized, one test per
row) and by `make validate` itself, which re-runs the hole check on every invocation.

### Real-data extremes that shaped the contracts

Naive versions of five rules would have falsely rejected committed real data:

| naive rule | real data that breaks it | contract actually written |
|---|---|---|
| `gp ≤ season_games` | Jokić 2019-20: 73 gp vs modal 72 (COVID restart); Walt Bellamy precedent: 88 gp in the 82-game 1968-69 via mid-season trade | `gp ≤ season_games + 6` (`gp_within_schedule`) |
| `mp ≤ 48·gp` | Wilt 1961-62: 3882 mp in 80 gp (48.5/game — overtime) | `mp ≤ 48·gp + 60` (`mp_ot_ceiling`); additive, not a ratio, so 1-game samples can absorb a multi-OT game (record: 78 min, 6 OT) |
| `po_mp/po_gp ≤ 48` | seed playoff max is 48.75 min/game | same additive rule (`po_mp_ot_ceiling`) |
| `ws ≥ 0` | seed minimum is −0.4 (negative Win Shares are real) | `ws ∈ [−3, 26]` (ceiling: Kareem's record 25.4, in the seed) |
| playoff stats ≥ 1 | Jerry West 1967: po_gp 1, po_mp 1, po_pts/trb/ast 0 (a real 1-minute playoff cameo) | floors exactly `po_gp ≥ 1`, `po_mp ≥ 1`, counting stats ≥ 0 |

### Observed → enforced promotions

Rules profiling.md reported as "observed, 0 violations" are now hard contracts:
the points identity `pts = 2·fgm + fg3m + ftm` (fg3m as 0 where era-null; exact on all 335
rows, every era — the strongest single corruption catch) and the shot chain
`fgm ≤ fga · ftm ≤ fta · fg3m ≤ fg3a · fg3a ≤ fga · fg3m ≤ fgm`.

### Rules found in T5 investigation (not in profiling.md)

- **Accolade era gates** — the awards-side analog of the stat era gates: award season ≥ its
  intro season (a 1971 DPOY is as impossible as 1965 3PT attempts), no `all_star` where
  `asg_held` is false (1999), All-NBA 3rd team only from 1989, `all_nba_team` null **iff**
  award ≠ all_nba and in {1, 2, 3} when present.
- **Tighter accolade grain** — `(player_id, season, award)` is unique across all 668 rows;
  the previous 4-column key (with `all_nba_team`) would have admitted All-NBA 1st + 2nd in
  the same season. Contract enforces the tighter key; `clean.py::TABLE_KEYS` aligned.
- **Bidirectional player referential integrity** — every player_seasons row joins players
  AND every players row has ≥ 1 season (the reverse direction was previously unchecked).
- **Career span integrity** — `first_season`/`last_season` must equal the min/max of the
  player's actual season rows.
- **League regularities** — `pace_estimated` true exactly for pre-1978 seasons (pace needs
  turnovers, tracked 1977-78+); value ranges for pace and all league baselines.

### Deliberate non-rules (considered and rejected, with reasoning)

- **`is_active` vs last_season** — today, exactly the five players with `last_season = 2026`
  are active, but "active iff last season is the newest" is a data-freshness heuristic, not
  an invariant (a retirement immediately after a final season, or an active player missing a
  full year, breaks it either direction). Typed boolean only.
- **league_seasons contiguity** — the seed is contiguous 1957–2026, but the method only
  requires lookup coverage (every player-season joins a league season, which IS contracted);
  a contiguity rule would falsely reject the sparse 6-season fixture league table.
- **`po_gp ≤ gp`** — regular season and playoffs are independent samples; an injury-year
  return can legitimately have more playoff than regular-season games.

### Bound calibration note

Ceilings are NBA single-season records — many of which the seed itself holds (Wilt's 4029
pts / 2149 trb, Curry's 402 fg3m, Jordan's 759 playoff pts, Magic's 303 playoff ast, Kareem's
25.4 ws) — or the locked worksheet fixture values where those are larger (PlayerB's 2520 trb,
3360 fga, 1680 fgm), plus headroom. They exist to catch unit-scale corruption, not to encode
typicality.

### Negative proof

Verified once and reverted: temporarily raising `OT_ALLOWANCE` to 10 000 makes the
`mp_ot_ceiling` bad-fixture test fail and `make validate` exit with a "contract holes"
error — the guardrails are not trivially passing.

### Post-review fixes (Codex 4-Point review, 2026-07-12)

- **Bug (confirmed): `fg3a` ceiling was 1000, below the real record** — Harden's 1,028
  three-point attempts (2018-19). The seed max (Curry's 886) masked it: the record holder is
  outside the 20-player pool, unlike every other ceiling's record holder. Raised to 1150; a
  full re-audit of all other ceilings against records *outside* the pool found no second
  case (ast 1164 Stockton < 1300 · stl 301 Robertson < 350 · blk 456 Eaton < 500 · tov 464
  Harden < 500). Regression test: a synthetic season with fg3a = 1028 must pass.
- **Missing-column handling**: `_cross_failures` now short-circuits with the same
  `column_in_dataframe` failure name Pandera reports, instead of raising KeyError from a
  join (was fail-loud either way, but outside the aggregated-failure interface). Tested.
- **Coverage added**: designed-bad fixture rows for the two previously unproven rules,
  `players_have_seasons` and `po_mp_ot_ceiling` (30 → 32 rows); a direct `clean.py` test of
  the tightened accolade grain; and parameterized both-direction boundary tests for every
  era-gated column at intro−1/intro (off-by-one guard across the whole `ERA_INTRO` set).
