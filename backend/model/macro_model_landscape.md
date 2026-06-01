# Macroeconomic Model Landscape
_For the China Macro Monitor project. Last updated: 2026-05-25._

---

## Overview: the family tree

```
Standard DSGE
├── RANK  (Representative Agent New Keynesian)  ← simplest, most common
├── TANK  (Two Agent New Keynesian)             ← one step toward realism
└── HANK  (Heterogeneous Agent New Keynesian)  ← what we want
         └── built on top of Bewley / Aiyagari incomplete-markets models
```

---

## 1. RANK — Representative Agent New Keynesian

**What it is:** The workhorse of central bank macro. One "representative" household stands in for the entire economy. Monetary policy works by changing the interest rate, which shifts the household's incentive to consume today vs. tomorrow (intertemporal substitution).

**Core equations:** IS curve + Phillips curve + monetary policy rule (Taylor rule). Three equations, closed-form solutions.

**Why it's limited for our use case:** There is no heterogeneity. Every household responds identically to a policy shock. You cannot ask "does this policy help low-income households more than wealthy ones?" You cannot model China's savings puzzle (why do Chinese households save ~35% of income?) because the representative agent's savings rate is just calibrated as a parameter, not derived from behavior.

**Canonical papers (well-known in the field; not all DOIs verified in this session):**
- Clarida, Gali, Gertler (1999) — "The Science of Monetary Policy: A New Keynesian Perspective." *Journal of Economic Literature* 37(4)
- Woodford (2003) — *Interest and Prices* (textbook, Princeton UP)
- Gali (2015) — *Monetary Policy, Inflation and the Business Cycle* (textbook, Princeton UP, 2nd ed.)

**Full DSGE implementations of RANK (estimated, not just calibrated):**
- Smets & Wouters (2003, 2007) — the benchmark estimated DSGE. The 2007 AER paper ("Shocks and Frictions in US Business Cycles") is the one everyone uses. Code is in the Macroeconomic Model Data Base.
- Christiano, Eichenbaum, Evans — CEE model. Also in MMB.

**Software:**
- [Dynare](https://www.dynare.org) — MATLAB/Octave. The standard tool for RANK/DSGE. Version 7.0 (2026). Most published DSGE papers ship a `.mod` file you run directly in Dynare.
- [Macroeconomic Model Data Base (MMB)](https://www.macromodelbase.com) — 160+ DSGE models in a common Dynare-based framework. Lets you compare models and run the same policy shock across all of them. No confirmed China-specific model in the database.

---

## 2. TANK — Two Agent New Keynesian

**What it is:** A minimal compromise. Split the economy into two types: "Ricardian" households (can save and invest) and "hand-to-mouth" households (consume all income immediately, no savings). Much cheaper to solve than HANK but captures the first-order distributional effect.

**Why it's useful:** For a policy question like "does a transfer to low-income households raise consumption more than a tax cut for the wealthy?", TANK gives a tractable answer. It also shows up in many central bank models as a practical workaround.

**Limitation:** The fraction of hand-to-mouth households is set by the modeler, not derived from data. It is fundamentally a shortcut.

**Key paper:**
- Bilbiie (2008) — "Limited Asset Markets Participation, Monetary Policy and (Inverted) Aggregate Demand Logic." *Journal of Economic Theory.* _(DOI not verified in this session — look it up before citing)_

---

## 3. HANK — Heterogeneous Agent New Keynesian

**What it is:** The full model. Combines:
- A Bewley/Aiyagari incomplete-markets model of household savings (many households, each facing idiosyncratic income risk, holding liquid and illiquid assets)
- A New Keynesian general equilibrium block (firms with price stickiness, monetary policy, aggregate shocks)

Every household in the model makes its own consumption/savings decision given its current wealth, income draw, and expectations. Aggregate consumption is the sum of all these individual decisions. This means the model has a well-defined wealth distribution that changes over time and responds to policy.

**Why this is what we want for China:**
- China's savings puzzle is fundamentally about household behavior under income uncertainty and weak social safety nets. HANK models this directly.
- The transmission of monetary or fiscal policy depends on where wealth is concentrated. In China, wealth is highly concentrated in housing. HANK can capture this.
- A question like "does a consumption voucher program raise total consumption?" requires knowing who holds liquid vs. illiquid wealth — exactly what HANK tracks.

**The indirect channel:** The key HANK insight (from KMV 2018) is that most of monetary policy's effect on consumption comes not from the direct intertemporal substitution channel (as in RANK) but from the indirect channel via labor income. Higher rates → lower investment → lower labor demand → lower wages → lower consumption. This completely changes how you evaluate policy.

### The canonical HANK paper

**Kaplan, Moll, Violante (2018)**
"Monetary Policy According to HANK"
*American Economic Review* 108(3), pp. 697–743
DOI: `10.1257/aer.20160042`
PDF: https://benjaminmoll.com/wp-content/uploads/2019/07/HANK.pdf

### The computational breakthrough that made HANK tractable

**Auclert, Bardóczy, Rognlie, Straub (2021)**
"Using the Sequence-Space Jacobian to Solve and Estimate Heterogeneous-Agent Models"
*Econometrica* 89(5), pp. 2375–2408
DOI: `10.3982/ECTA17434`

Before SSJ, solving a HANK model required global solution methods (very slow — hours per solve). The SSJ method linearizes the model in sequence space, making it orders of magnitude faster. This is what makes HANK practical for policy simulations.

---

## 4. Bewley / Aiyagari — the household block underlying HANK

HANK's household sector is built on these models. You need to understand them to understand HANK.

**Core idea:** Households face uninsurable idiosyncratic income shocks (you might lose your job, your income fluctuates) and cannot perfectly smooth consumption. They self-insure by saving. In equilibrium, the wealth distribution emerges endogenously from these individual decisions.

**Canonical papers:**
- Aiyagari (1994) — "Uninsured Idiosyncratic Risk and Aggregate Saving." *Quarterly Journal of Economics* 109(3). _(Well-known; DOI not verified this session)_
- Huggett (1993) — "The risk-free rate in heterogeneous-agent incomplete-insurance economies." *Journal of Economic Dynamics and Control.* _(Same caveat)_

---

## 5. Software and implementations

### SSJ toolkit
**GitHub:** https://github.com/shade-econ/sequence-jacobian
**Install:** `pip install sequence-jacobian`
**What it ships:** Working HANK notebook (`notebooks/hank.ipynb`) you can run immediately. Implements the Auclert et al. (2021) method.
**Status:** v1.0.0 (2022). Modest commit activity since then. Still the standard reference implementation.

### econpizza
**GitHub:** https://github.com/gboehl/econpizza
**Install:** `pip install econpizza`
**Docs:** https://econpizza.readthedocs.io
**What it does:** Solves fully **nonlinear** HANK models using JAX and automatic differentiation. Based on Boehl, "HANK on Speed: Robust Nonlinear Solutions using Automatic Differentiation" (*Journal of Economic Theory*).
**Status:** v0.6.9 (May 2025). Actively maintained. 111 stars.
**Why it matters over SSJ:** SSJ linearizes around the steady state — fine for small shocks. econpizza handles large, nonlinear shocks (relevant if we want to model large policy shifts like a major fiscal stimulus or a housing price crash).

### HARK toolkit
**GitHub:** https://github.com/econ-ark/HARK
**Install:** `pip install econ-ark`
**Docs:** https://docs.econ-ark.org
**What it does:** Toolkit for building the household block of a HANK model from scratch. Good for constructing and calibrating the Bewley/Aiyagari component. Not a full HANK out of the box — you build the GE block on top.
**Status:** v0.17.2 (May 2026). Actively maintained.

### KMV replication code
**GitHub:** https://github.com/ikarib/HANK
MATLAB port of the original Kaplan-Moll-Violante code. Useful for understanding the canonical HANK model directly.

### Dynare (for RANK/DSGE only)
**Website:** https://www.dynare.org
**Platform:** MATLAB or GNU Octave (no native Python)
**When to use:** If you want to run an existing published DSGE `.mod` file or use MMB. Not useful for HANK — Dynare does not support the heterogeneous-agent block.

---

## 6. What exists for China specifically

Very little. The honest state of the literature:

| Paper | Model type | China relevance | Code available |
|---|---|---|---|
| Yang, Zhang, Hou (2023). *PLOS ONE*. DOI `10.1371/journal.pone.0288976` | Two-country HANK | Section 7 applies to US-China trade/tariffs | Not confirmed |
| Qi, Yu, Liu, Ren (2023). *PLOS ONE*. DOI `10.1371/journal.pone.0289712` | Heterogeneous savers/borrowers DSGE | Chinese housing wealth → consumption | Not confirmed |
| Zhang, X. (2024). *PLOS ONE*. DOI `10.1371/journal.pone.0308663` | DSGE with search-and-matching | Public sector employment rigidity in China | Yes — Figshare `10.6084/m9.figshare.26299456` |
| Dai, Minford, Zhou (2015). *Applied Economics*. CEPR DP 10238 | Standard DSGE | Explicitly a "DSGE model of China" | Not confirmed public |
| Chen, Funke, Paetz (2012). BOFIT DP | DSGE with non-market policy tools | Monetary policy tools in mainland China | Not confirmed |

**The gap:** No published, code-available HANK model calibrated to China's economy exists as of 2026-05-25. Building one means taking the SSJ or econpizza HANK notebook and calibrating to Chinese data (China Household Finance Survey for wealth distribution, NBS/PBOC for aggregate moments).

---

## 7. What we would need to build a China HANK

1. **Household block calibration data:** China Household Finance Survey (CHFS) — wealth distribution, liquid vs. illiquid asset holdings by income decile
2. **Aggregate moment targets:** NBS (consumption/GDP ratio, savings rate, investment share), PBOC (credit growth, policy rate)
3. **Income process:** Estimate idiosyncratic income shock persistence and variance from CHFS or CFPS (China Family Panel Studies)
4. **SOE / dual-sector extension:** China's economy has a large state sector that doesn't behave like a competitive firm. This requires adding a second production block.
5. **Housing block:** Housing is ~70% of household wealth in China. A China HANK without a housing sector will misprice wealth effects.
6. **Software:** Start with `econpizza` for nonlinear solution; `econ-ark/HARK` for household block construction.