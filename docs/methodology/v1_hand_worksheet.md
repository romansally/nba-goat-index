# Methodology v1 — Hand-Computation Worksheet

**Purpose:** proves `docs/methodology/v1.md` acceptance criterion 5 — a reader can hand-compute
a final score from the doc alone. Every number below was computed by hand from the formulas in
v1.md, using 4-decimal intermediates. **T7's golden snapshot must reproduce the final scores
within ±0.01.**

**Lock note:** these three synthetic players are the core of the valid fixture mini-set
(`tests/fixtures/`, T5). T5 may add designed-bad rows (which never get scored), but changing
these players' inputs or adding players to the *valid scoring pool* changes every min–max anchor
— after T7 commits the golden snapshot, that is a version-bump event.

The trio deliberately exercises: multi-era span (PlayerA), a pre-3PT-era dominant-in-one-category
big (PlayerB), a short career with a lockout season and an exact-boundary availability season
(PlayerC), the fewer-than-N Peak rule (all three), award-ineligibility renormalization (B has no
DPOY-eligible seasons), the no-ASG-in-1999 eligibility exclusion (C), and the degenerate
min–max = 50 rule (DPOY, All-Star).

---

## 1. Synthetic league reference table

| season | pace | lg_pts_pg | lg_trb_pg | lg_ast_pg | lg_TS | season_games | asg_held |
|--------|------|-----------|-----------|-----------|-------|--------------|----------|
| 1970   | 120  | 112       | 56        | 24        | .500  | 80           | yes      |
| 1971   | 120  | 112       | 56        | 24        | .500  | 80           | yes      |
| 1990   | 100  | 100       | 40        | 20        | .500  | 82           | yes      |
| 1999   | 90   | 90        | 42        | 21        | .500  | 50           | **no**   |
| 2000   | 90   | 90        | 42        | 21        | .500  | 82           | yes      |
| 2001   | 90   | 90        | 42        | 21        | .500  | 82           | yes      |

Per-slot baselines (v1.md §3): `baseline_s = lg_pg_s × 15 / pace`

| season(s)      | base_pts | base_trb | base_ast |
|----------------|----------|----------|----------|
| 1970, 1971     | 14.0     | 7.0      | 3.0      |
| 1990           | 15.0     | 6.0      | 3.0      |
| 1999–2001      | 15.0     | 7.0      | 3.5      |

## 2. Player inputs

Regular season (FTA = 0 for all synthetic rows, so TS = PTS / (2 × FGA)):

| player | season | GP | MP   | PTS  | TRB  | AST | FGA  | WS | team_SRS |
|--------|--------|----|------|------|------|-----|------|----|----------|
| A      | 1990   | 82 | 3240 | 2430 | 540  | 540 | 2025 | 15 | 5.0      |
| A      | 2000   | 82 | 3280 | 2214 | 574  | 574 | 1845 | 16 | 6.0      |
| A      | 2001   | 82 | 3280 | 2460 | 574  | 574 | 2050 | 18 | 7.0      |
| B      | 1970   | 80 | 3600 | 3360 | 2520 | 360 | 3360 | 20 | 3.0      |
| B      | 1971   | 80 | 3600 | 3024 | 2352 | 432 | 3024 | 18 | 2.0      |
| C      | 1999   | 50 | 2000 | 1650 | 350  | 350 | 1250 | 12 | 4.0      |
| C      | 2000   | 41 | 1640 | 1353 | 287  | 287 | 1025 | 8  | 4.0      |

Playoffs (missed: A 1990, B 1971, C 1999):

| player | season | P_GP | P_MP | P_PTS | P_TRB | P_AST |
|--------|--------|------|------|-------|-------|-------|
| A      | 2000   | 20   | 800  | 600   | 140   | 140   |
| A      | 2001   | 10   | 400  | 250   | 70    | 70    |
| B      | 1970   | 14   | 630  | 588   | 441   | 63    |
| C      | 2000   | 5    | 200  | 165   | 35    | 35    |

Accolades (season-attributed):

| player | season | awards                                  |
|--------|--------|-----------------------------------------|
| A      | 1990   | All-NBA 2nd, All-Star                   |
| A      | 2000   | Ring, Finals MVP, All-NBA 1st, All-Star |
| A      | 2001   | MVP, All-NBA 1st, All-Star              |
| B      | 1970   | MVP, All-NBA 1st, All-Star              |
| B      | 1971   | All-NBA 1st, All-Star                   |
| C      | 1999   | MVP, All-NBA 1st  *(no ASG held)*       |
| C      | 2000   | All-NBA 2nd, All-Star                   |

---

## 3. PlayerA — full hand computation

### 3.1 Per-75 rates, REL ratios, SPI (v1.md §3–§4)

Possessions = MP × pace / 48; per-75 divisor = possessions / 75.

**1990:** poss = 3240 × 100/48 = 6750 → divisor 90.
per75: PTS 2430/90 = 27.0 · TRB 540/90 = 6.0 · AST 540/90 = 6.0
REL: pts 27/15 = **1.8** · trb 6/6 = **1.0** · ast 6/3 = **2.0**
SPI = .5(1.8) + .25(1.0) + .25(2.0) = **1.6500** · AVAIL = 82/82 = 1.0
TS = 2430/4050 = .6000 → REL_TS = **1.2000**

**2000:** poss = 3280 × 90/48 = 6150 → divisor 82.
per75: PTS 27.0 · TRB 7.0 · AST 7.0 → REL: 1.8 · 1.0 · 2.0
SPI = **1.6500** · AVAIL = 1.0 · TS = 2214/3690 = .6000 → REL_TS = **1.2000**

**2001:** divisor 82. per75: PTS 2460/82 = 30.0 · TRB 7.0 · AST 7.0 → REL: 2.0 · 1.0 · 2.0
SPI = .5(2.0) + .25(1.0) + .25(2.0) = **1.7500** · AVAIL = 1.0
TS = 2460/4100 = .6000 → REL_TS = **1.2000**

### 3.2 Component raw values

- **Peak** (§5.1): all 3 seasons qualify (AVAIL 1.0 ≥ 0.5); fewer than 5 → mean of all:
  (1.65 + 1.65 + 1.75)/3 = **1.6833**
- **Longevity** (§5.2): Σ AVAIL × SPI = 1.65 + 1.65 + 1.75 = **5.0500**
- **Winning/Impact** (§5.3): WS48 = (15+16+18)/(3240+3280+3280) × 48 = 49/9800 × 48 = **0.2400**;
  SRS_w = (82×5 + 82×6 + 82×7)/246 = **6.0000**
- **Playoff** (§5.4):
  2000: P_poss = 800 × 90/48 = 1500 → divisor 20 → per75 30.0/7.0/7.0 → REL 2.0/1.0/2.0 →
  P_SPI = 1.7500 → run = 1.75 × 20 = 35.0000
  2001: divisor 10 → per75 25.0/7.0/7.0 → REL 1.6667/1.0/2.0 → P_SPI = 1.5833 → run = 15.8333
  Raw = 35.0 + 15.8333 = **50.8333**
- **Accolades** (§5.5), career = 3 seasons, all awards existed 1990–2001, ASG held all 3:
  MVP 1/3 = .3333 · ring 1/3 = .3333 · FMVP 1/3 = .3333 ·
  All-NBA (1.0+1.0+0.5)/3 = .8333 · DPOY 0/3 = 0 · All-Star 3/3 = 1.0
- **Efficiency** (§5.6): REL_TS_career = 1.2000 (all seasons 1.2, minutes-weighted);
  SPI_career = (3240×1.65 + 3280×1.65 + 3280×1.75)/9800 = 16498/9800 = **1.6835**

## 4. PlayerB and PlayerC — compact computation

**PlayerB** (divisor 120 both seasons: poss = 3600 × 120/48 = 9000):
1970: per75 28.0/21.0/3.0 → REL 2.0/**3.0**/1.0 → SPI **2.0000**; TS = .5 → REL_TS 1.0
1971: per75 25.2/19.6/3.6 → REL 1.8/2.8/1.2 → SPI **1.9000**; REL_TS 1.0
Peak = mean(2.0, 1.9) = **1.9500** · Longevity = **3.9000**
WS48 = 38/7200 × 48 = **0.2533** · SRS_w = **2.5000**
Playoff 1970: P_poss = 630×120/48 = 1575 → divisor 21 → per75 28/21/3 → P_SPI 2.0 →
raw = 2.0 × 14 = **28.0000**
Accolade rates (2 seasons): MVP .5 · ring 0 · FMVP 0 (eligible: 1969 intro ≤ 1970) ·
All-NBA 2/2 = 1.0 · **DPOY: 0 eligible seasons → excluded** · All-Star 2/2 = 1.0
REL_TS_career = **1.0000** · SPI_career = **1.9500**

**PlayerC** (1999 divisor 50: poss = 2000×90/48 = 3750; 2000 divisor 41: poss = 3075):
1999: per75 33.0/7.0/7.0 → REL 2.2/1.0/2.0 → SPI **1.8500**; AVAIL = 50/50 = 1.0;
TS = 1650/2500 = .66 → REL_TS **1.3200**
2000: per75 33.0/7.0/7.0 → SPI **1.8500**; AVAIL = 41/82 = **0.5 (exact boundary — qualifies)**;
REL_TS 1.3200
Peak = **1.8500** · Longevity = 1×1.85 + 0.5×1.85 = **2.7750**
WS48 = 20/3640 × 48 = **0.2637** · SRS_w = **4.0000**
Playoff 2000: divisor 5 → per75 33/7/7 → P_SPI 1.85 → raw = 1.85 × 5 = **9.2500**
Accolade rates: MVP 1/2 = .5 · ring 0 · FMVP 0 · All-NBA (1.0+0.5)/2 = .75 · DPOY 0/2 = 0 ·
**All-Star 1/1 = 1.0 (1999 excluded — no ASG held)**
REL_TS_career = **1.3200** · SPI_career = **1.8500**

## 5. Min–max scaling across the pool (v1.md §6)

`MM(x) = 100 × (x − min)/(max − min)`; degenerate (max = min, **award-rate elements** —
v1.md §6 as amended per ADR-0002: continuous inputs cannot legitimately tie pool-wide and
are refused instead) → 50.0 for all.

| Element          | A raw   | B raw   | C raw   | A MM    | B MM    | C MM    |
|------------------|---------|---------|---------|---------|---------|---------|
| Peak             | 1.6833  | 1.9500  | 1.8500  | 0.00    | 100.00  | 62.50   |
| Longevity        | 5.0500  | 3.9000  | 2.7750  | 100.00  | 49.4505 | 0.00    |
| WS48             | 0.2400  | 0.2533  | 0.2637  | 0.00    | 56.1727 | 100.00  |
| SRS_w            | 6.0000  | 2.5000  | 4.0000  | 100.00  | 0.00    | 42.8571 |
| Playoff          | 50.8333 | 28.0000 | 9.2500  | 100.00  | 45.0902 | 0.00    |
| rate: MVP        | 0.3333  | 0.5000  | 0.5000  | 0.00    | 100.00  | 100.00  |
| rate: ring       | 0.3333  | 0.0000  | 0.0000  | 100.00  | 0.00    | 0.00    |
| rate: FMVP       | 0.3333  | 0.0000  | 0.0000  | 100.00  | 0.00    | 0.00    |
| rate: All-NBA    | 0.8333  | 1.0000  | 0.7500  | 33.3333 | 100.00  | 0.00    |
| rate: DPOY       | 0.0000  | —       | 0.0000  | 50.00*  | excl.   | 50.00*  |
| rate: All-Star   | 1.0000  | 1.0000  | 1.0000  | 50.00*  | 50.00*  | 50.00*  |
| REL_TS_career    | 1.2000  | 1.0000  | 1.3200  | 62.50   | 0.00    | 100.00  |
| SPI_career       | 1.6835  | 1.9500  | 1.8500  | 0.00    | 100.00  | 62.4809 |

\* degenerate min–max rule (all eligible values equal — an award-rate element, where exact
pool-wide ties are legitimate; v1.md §6/ADR-0002) → 50.0.

**Blended components:**

- Winning/Impact = .5 MM(WS48) + .5 MM(SRS_w): A = **50.0000** · B = **28.0864** · C = **71.4286**
- Efficiency = .5 MM(REL_TS) + .5 MM(SPI_career): A = **31.2500** · B = **50.0000** · C = **81.2405**
- Accolades = Σ w_a MM(rate_a) / Σ w_a over eligible awards:
  A = .30(0) + .25(100) + .15(100) + .15(33.3333) + .075(50) + .075(50)
    = 0 + 25 + 15 + 5 + 3.75 + 3.75 = **52.5000**
  B (DPOY excluded → ÷ 0.925) = [.30(100) + .25(0) + .15(0) + .15(100) + .075(50)] / 0.925
    = 48.75 / 0.925 = **52.7027**
  C = .30(100) + 0 + 0 + .15(0) + .075(50) + .075(50) = 30 + 3.75 = **37.5000**

## 6. Final scores — career scope (default weights .25/.20/.18/.15/.12/.10)

| Component (weight)     | A        | B        | C        |
|------------------------|----------|----------|----------|
| Peak (.25)             | 0.0000   | 25.0000  | 15.6250  |
| Winning/Impact (.20)   | 10.0000  | 5.6173   | 14.2857  |
| Playoff (.18)          | 18.0000  | 8.1162   | 0.0000   |
| Accolades (.15)        | 7.8750   | 7.9054   | 5.6250   |
| Efficiency (.12)       | 3.7500   | 6.0000   | 9.7489   |
| Longevity (.10)        | 10.0000  | 4.9451   | 0.0000   |
| **GOAT score**         | **49.63** | **57.58** | **45.28** |

**Career ranking: 1. PlayerB (57.58) · 2. PlayerA (49.63) · 3. PlayerC (45.28)**

## 7. Peak scope (v1.md §7) — bonus verification of the toggle

Every player's top-5 qualifying window here equals his whole career (all careers ≤ 3 qualifying
seasons), so all recomputed raws and min–max values are unchanged; only the weights change:
Longevity dropped, others ÷ 0.90.

- A: (49.6250 − 10.0000) / 0.90 = 39.6250 / 0.90 = **44.03**
- B: (57.5840 − 4.9451) / 0.90 = 52.6389 / 0.90 = **58.49**
- C: (45.2846 − 0.0000) / 0.90 = **50.32**

**Peak ranking: 1. PlayerB (58.49) · 2. PlayerC (50.32) · 3. PlayerA (44.03)** — PlayerC (the
short-career peak monster) overtakes PlayerA when longevity stops counting, exactly the
career-vs-peak behavior questions.md Q6 analyzes.

## 8. Rounding and golden tolerance

Intermediates above are rounded to 4 decimals; final scores to 2 (half away from zero). The
engine computes in full float64, so T7's golden test compares engine outputs to this worksheet
at **±0.01 absolute tolerance** on final scores (and the golden JSON itself is generated by the
engine at full precision).
