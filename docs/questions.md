# Analytical Questions — Tier-1 Report

The locked acceptance criteria for `docs/report.md` (T9). Each names the output that answers
it. These constrain `docs/methodology/v1.md` (T2): the pipeline must be able to produce
per-component scores, era tags, editable weights, and a working peak/career scope toggle.

1. **Who ranks in the top 10 of the v1 GOAT Index, and which component (Peak, Longevity,
   Winning/Impact, Playoff, Accolades, Efficiency/Advanced) drives each player's score?**
   Answer: `results/goat_scores_v1.csv` ranking table + a component-stacked bar chart for the
   top 10.

2. **Does the Winning/Impact proxy treat pre-1997 legends (who lack on/off data) comparably to
   post-1997 players, or does any era show scores that trace to a method gap rather than
   genuine greatness?** Answer: a Winning/Impact component comparison across eras, called out
   explicitly for any legend whose score looks era-penalized rather than merit-based.

3. **For 3 marquee pairwise matchups (Jordan vs. LeBron, Bird vs. Magic, Wilt vs. Kareem), who
   wins and why?** Answer: pairwise compare output — verdict + component-by-component
   breakdown, higher/lower highlighted per component.

4. **How sensitive is the top-10 ranking to the default weighting — does any player's rank
   swing sharply under a plausible alternate weighting?** Answer: a small weight-perturbation
   table/note (full sensitivity analysis is Phase 5; Tier-1 does a light spot-check only).

5. **How closely does the v1 ranking track a published consensus expert list, and where's the
   biggest deviation — method flaw or legitimate disagreement?** Answer: Spearman correlation
   coefficient + honest analysis of the biggest deviations, stating for each whether it reveals
   a method gap to fix or a defensible difference to keep.

6. **Which players' ranks shift most when comparing career-overall vs. peak-N-seasons scope,
   and what does that reveal about longevity-monsters vs. peak-prime superstars?** Answer: a
   career-vs-peak rank-delta table/chart highlighting the largest movers.

---

*Locked. T2 (methodology v1) reads these alongside the Vision doc before defining components,
era tags, weights, and the peak/career scope toggle.*
