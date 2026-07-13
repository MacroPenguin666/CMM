# The U.S. AI Value Chain — Investment-Grade Dossier
### Single-pass research output | Data as of 26 June 2026

> **Scope & method note.** This is a *condensed single-pass* execution of the deep-research skill, not the full multi-week artifact (which would profile every company to the §4 template depth). Figures are dated and sourced; public-company numbers come from filings/earnings, private-company numbers from press and research-firm estimates (Sacra, Value Add VC, The Information, Bloomberg, CNBC) and are **run-rate, not audited** — these conflict across sources, so ranges are given and discrepancies flagged. Per the AI domain pack, semiconductors are treated as **one layer** (full chip-stack depth belongs to the chips agent) and **power/minerals inputs** are covered at the procurement level (depth belongs to the minerals agent). This is decision-grade analysis with bull/bear cases — **not investment advice**. Confidence tags: **[H]/[M]/[L]**.

---

## 1. Executive summary

The U.S. AI value chain in mid-2026 is a demand explosion colliding with two physical constraints and one financial question.

- **The demand is real and accelerating.** NVIDIA's data-center revenue hit **$75.2B in a single quarter** (Q1 FY27, ended Apr 2026, +92% YoY) and it guided the next quarter to ~$91B [H]. The four largest hyperscalers guided to **~$725B of 2026 capex**, up ~77% from ~$410B in 2025, almost entirely AI [H]. Cloud backlogs are enormous (Google Cloud >$460B; Microsoft commercial RPO $627B) [H].
- **The binding constraint has moved from chips to power and memory.** "Time-to-power" is now the gating factor for data-center delivery; ~30% of new U.S. capacity is shifting to bring-your-own-power, and gas/nuclear deals are proliferating [H]. Simultaneously, an **HBM memory shortage** (3-supplier oligopoly diverting wafers to high-bandwidth memory) has roughly doubled DRAM prices and is now a top-three input bottleneck — and a driver of the capex-cost inflation hyperscalers cited [H].
- **The financial question is whether end-demand ROI justifies the build.** J.P. Morgan estimates the industry needs **~$650B of new annual revenue** to earn a 10% return on the infrastructure being built, versus an estimated **$50–150B of current AI-attributable revenue** — a 4–13x gap [M]. An MIT study found 95% of enterprise GenAI pilots showed no measurable P&L return [M]. Layered on top is a **>$800B web of circular/vendor financing** (chipmaker → AI lab → cloud → back to chipmaker) and a live **GPU-depreciation accounting dispute** (Burry: ~$176B of understated depreciation 2026–28) [M].

**Where value and risk concentrate.** Value capture today is concentrated upstream — NVIDIA (compute), the HBM oligopoly (SK Hynix/Samsung/Micron), TSMC (fabrication), and power producers with firm capacity. The model labs (OpenAI, Anthropic) show historic revenue velocity but negative margins and heavy compute dependence. The application layer is where ROI must ultimately appear, and it is the least proven. Systemic risk concentrates in three single points of failure (NVIDIA's accelerator dominance, TSMC's leading-edge monopoly, the HBM oligopoly) and in the circular-financing structure.

**Headline contested calls (see §6).** (a) Capex is rational *if* inference demand compounds as bulls expect; the bear case is a 4–13x revenue gap and a depreciation/financing reckoning. (b) The circular financing is defensible as performance-contingent (UBS) but fragile if OpenAI's funding chain breaks. (c) Pricing power is most durable at the compute/HBM/fab chokepoints and least durable at the model and commodity-application layers.

---

## 2. Value-chain map (MECE)

| Layer | What it does | Leading players (exemplars, non-exhaustive) |
|---|---|---|
| **0. Inputs & enablers** | Power generation, grid, cooling, capital | Constellation, Vistra, GE Vernova, Oklo/TerraPower (SMR); Vertiv, Eaton, Schneider; private credit / vendor finance |
| **1. Semiconductors & hardware** *(one layer here)* | Accelerators, custom silicon, memory, networking | NVIDIA, AMD, Broadcom, Marvell; **foreign chokepoints:** TSMC, ASML, SK Hynix, Samsung, Micron, Arm |
| **2. Compute infrastructure** | Hyperscale cloud, neoclouds, data centers | AWS, Azure, Google Cloud, Oracle/OCI; CoreWeave, Lambda, Crusoe, Nebius; Equinix, Digital Realty |
| **3. Foundation models / labs** | Frontier model training & serving | OpenAI, Anthropic, Google DeepMind, Meta (Llama), xAI |
| **4. Data, tooling & MLOps** | Data platforms, labeling, orchestration | Databricks, Snowflake, Scale AI |
| **5. Applications & software** | Horizontal + vertical + consumer AI | Microsoft Copilot, Palantir, Salesforce, ServiceNow; Cursor/Anysphere, Cognition, GitHub Copilot |

---

## 3. Layer-by-layer deep dives (with compact company profiles)

*(Per the one-pass scope, §4-template company profiles are folded into each layer; the most material names get role / moat / financials / valuation / risk treatment.)*

### Layer 0 — Inputs & enablers (power is the new oil)
**Dynamics.** AI data centers generate ~$10–12M of revenue per MW annually, so accelerating a campus even two years ahead of a grid connection is worth tens of billions — making "time-to-power" the central development constraint, not an afterthought [H]. ~30% of planned U.S. capacity is moving to bring-your-own-power; natural gas powers ~75% of behind-the-meter equipment (~23 GW); co-location at existing gas plants (the Calpine/CyrusOne template) bypasses 5–10-year interconnection queues [H]. Data centers were ~40% of PJM capacity costs in the last auction, making rising consumer electricity bills a live political risk [M].

- **Constellation Energy (CEG)** — Role: largest U.S. nuclear fleet; supplies firm carbon-free power via 20-year PPAs (e.g., the Three Mile Island/Crane restart for Microsoft). Moat: irreplaceable baseload nuclear capacity. Risk: premium valuation (~22–49x forward earnings depending on metric), stock down ~25% from its Oct-2025 high of $413 after 2026 EPS guidance ($11–12) missed; a PJM transmission bottleneck could delay full Crane deliverability to ~2031 [M].
- **Vistra (VST)** — Same nuclear/PPA thematic at a lower multiple (~10.6x EV/EBITDA); signed multi-plant deals with Meta (2,600+ MW) [M].
- **SMR developers (Oklo, TerraPower)** — Meta contracted up to 6.6 GW of nuclear (incl. new Natrium SMRs) through 2035; **none are yet commercially operational in the West**, so this is option value, not delivered capacity [M].

### Layer 1 — Semiconductors & hardware (the keystone)
**Dynamics.** This layer captures the most value and holds the hardest chokepoints. NVIDIA dominates accelerators; the HBM oligopoly and advanced packaging (TSMC CoWoS) gate supply; custom silicon is rising but not displacing NVIDIA.

- **NVIDIA (NVDA)** — Role: dominant AI accelerator + networking. Financials: FY26 revenue **$215.9B (+65%)**; Q1 FY27 revenue **$81.6B**, data center **$75.2B (+92% YoY)**, GM ~75%, net income FY26 ~$117B; Q2 FY27 guide ~$91B [H]. Moat: CUDA software lock-in + networking (InfiniBand/Spectrum-X) + annual cadence (Blackwell → Vera Rubin late 2026 → Rubin Ultra 2027; Rubin targets ~10x lower inference cost) [H]. Risks: customer concentration (hyperscalers ~50% of data-center revenue), China export-control loss (no China data-center compute in guidance), rising custom-silicon competition, and its own role as financier of customers (>$40B committed to AI startups). Supply commitments rose to **$95.2B** (Q4 FY26), a leading indicator of forward demand — and forward risk [H].
- **Broadcom (AVGO) & Marvell (MRVL)** — Role: custom AI ASICs (XPUs) for hyperscalers. Broadcom AI revenue +106% with a stated ~$100B revenue ambition; Marvell AI-ASIC revenue ~$9–11B in 2026 (~¼ of Broadcom's scale) [M]. Significance: the credible custom-silicon alternative that constrains NVIDIA's long-run pricing power.
- **AMD (AMD)** — Role: #2 merchant GPU (MI350/MI355 vs Blackwell); landed a large OpenAI commitment with warrants (OpenAI poised to become a major AMD holder) — itself a node in the circular-financing web [M].
- **Memory / HBM (foreign chokepoint)** — **SK Hynix** (HBM share ~50–57%; Q1 2026 revenue ~$35.5B, operating profit ~$27.8B; ~$29B Nasdaq listing planned ~July 2026), **Samsung** (~35–40%), **Micron** (~5–10%; crossed ~$1T market cap; $20B 2026 capex). HBM is **sold out through 2026** with shortages flagged into 2027+; DRAM prices ~doubled since early 2025; the wafer diversion to HBM is crowding out consumer electronics (smartphones −12.9%, PCs −11.3% in 2026) [H].
- **Foundry/equipment (foreign chokepoints)** — TSMC (leading-edge monopoly; ~30% revenue growth guided for 2026; Taiwan concentration = the system's largest SPOF), ASML (sole EUV supplier), Arm (IP). *Depth deferred to the chips agent.*

### Layer 2 — Compute infrastructure (the buyers)
**Dynamics.** The hyperscalers are the demand engine and the capex risk. Combined 2026 capex **~$725B** (up ~77% YoY); Goldman models ~$5.3T cumulative FY25–30 for the four [H]. Cloud is growing fast — **Google Cloud +63% YoY, Azure +40%, AWS +28%** in Q1 2026 — with backlogs building, which bulls cite as proof demand is real [H]. Free cash flow is compressing (Amazon's FCF projected to turn negative in 2026) [M].

- **Microsoft (MSFT)** — Azure +40%; AI revenue run-rate ~$37B (+123%); commercial RPO **$627B**; CY2026 capex guided ~$190B; "capacity constrained" through 2026; ~$25B of the capex raise attributed to memory/component cost inflation [H].
- **Alphabet (GOOGL)** — Google Cloud +63%; backlog >$460B (roughly doubled); ~$185–190B capex; vertically integrated via TPUs [H].
- **Amazon (AMZN)** — AWS +28%; Trainium/chip business ~$20B run-rate; ~$200B 2026 capex (largest single spender) [H].
- **Meta (META)** — No public cloud to monetize; $125–145B capex (raised mid-year on component costs); shares fell ~9% on the raise — the first real market pushback on the spending curve [H].
- **Oracle (ORCL)** — The aggressive neocloud-style bet: anchor of Stargate; ~$300B OpenAI cloud deal from 2027 (~$30B/yr); raising ~$50B of debt to fund the build — the clearest case of balance-sheet risk tied to a single customer's solvency [M].
- **CoreWeave (CRWV)** — Pure-play GPU neocloud (IPO'd Mar 2025; ~$19B). The stress test of the financing model: short-seller Jim Chanos argued its ~$3.4B adjusted EBITDA is dwarfed by ~$1.2B interest on ~$20B of GPU assets even at a generous 10-year GPU life; the stock fell from $187 (Jun 2025) to $72 (mid-Dec 2025), −61% [M]. NVIDIA holds ~7% and agreed to buy ~$6.3B of its capacity — circularity in miniature.

### Layer 3 — Foundation models / labs (historic velocity, negative margins)
**Dynamics.** Revenue growth here is unprecedented, but so is cash burn, and the labs are existentially dependent on Layer-1/2 supply and on continued private capital.

- **OpenAI** — Revenue run-rate ~$20B (end-2025) → **~$24–25B (Q1 2026)**; raised **$122B at an $852B post-money valuation** (closed Mar 31 2026; anchored by Amazon, NVIDIA, SoftBank, Microsoft); filed a confidential **S-1 on 8 Jun 2026**. Economics: ~33% gross margin (inference-constrained), projected ~$27B cash burn in 2026, cash-flow-positive not before 2030 [M]. Compute commitments are staggering: ~$250B Azure, $38B AWS + a $50B Amazon investment + 2 GW Trainium, ~$300B Oracle, 10 GW of NVIDIA systems; Altman has cited ~$1.4T of data-center commitments over 8 years [M]. This is the demand epicenter and the financing hub.
- **Anthropic** *(reported objectively; press-sourced)* — Run-rate ~$10B (end-2025) → **~$47B (May 2026)** per company/press disclosures; **$65B Series H at a $965B valuation** (late May 2026), with strategic participation from Samsung/SK Hynix/Micron and $5B from Amazon; confidential **S-1 filed 1 Jun 2026**; WSJ reported it expects a 130% surge to its first operating profit [M]. Compute: a reported $1.25B/month deal with xAI/SpaceX through May 2029, ~3.5 GW of Google+Broadcom TPU from 2027, 1 GW of NVIDIA, up to $25B from Amazon. Overhangs flagged by analysts: a gross-vs-net revenue-accounting question, a U.S. Department of War supply-chain-risk/litigation matter, and a recent export-control action affecting its most advanced tier [M]. *(Caveat: these are press figures, not audited; the run-rate snapshots imply annualized-growth optics that should be read with care.)*
- **xAI / SpaceX** — Merged Feb 2026 (combined ~$1.25T at merger); pursuing a mega-IPO reportedly targeting ~$2T and a >$75B raise — treat as pending/uncertain (sources conflict) [L].
- **Google DeepMind & Meta (Llama)** — Captive labs inside hyperscalers; Gemini/TPU vertical integration is a structural cost advantage; Llama anchors the open-weight price floor.

### Layer 4 — Data, tooling & MLOps (picks-and-shovels)
**Dynamics.** The "winners don't compete with the labs" layer — they make models usable. Lighter coverage in this pass.
- **Databricks** — Controls the enterprise data layer; Mosaic ML gives in-house training; positioned as platform-of-record for RAG/fine-tuning [M].
- **Scale AI (~$13B)** — Data labeling/RLHF supply; note Meta's large 2025 investment reshaped its competitive and neutrality position [M].
- **Snowflake** — Data-cloud incumbent racing to add AI/eval tooling [L].

### Layer 5 — Applications & software (where ROI must appear)
**Dynamics.** The AI coding category is the breakout, but it is also where commoditization risk is highest. AI coding tools generated ~$12.8B in 2026 (2x 2024) [M].
- **Palantir (PLTR)** — Q1 2026 revenue **$1.63B (+85% YoY)**, FY26 guide ~$7.65B (+71%), GAAP margin ~53%, Rule-of-40 ~145%, NDR 139% — exceptional fundamentals. But the stock trades at ~120–150x trailing P/E and >40x P/S, sits ~33% below its Nov-2025 high of $207, faces European contract losses (France, UK NHS), and is a named Burry short — the purest "how much AI growth will investors pay for" test [M].
- **Cursor / Anysphere** — ~$2B ARR by Feb 2026 (fastest 0→$2B in B2B software history); raising at ~$50B (from $29.3B in Nov 2025); projecting ~$6B ARR by end-2026; SpaceX holds a $60B acquisition option; NVIDIA is a strategic investor [M]. Risk: coding assistance is commoditizing into every IDE/cloud.
- **Cognition (Devin)** — Agent-first; ~$26B valuation (May 2026); revenue $37M (May 2025) → $492M (May 2026) [M].
- **Microsoft Copilot / GitHub Copilot** — Distribution-advantaged incumbent; GitHub Copilot ~4.7M paid subscribers, ~37% of the coding-tools market, ~90% Fortune-100 adoption [M].

---

## 4. Cross-cutting analysis

### 4.1 Capital-flow / circular-financing map
2026 analyses put circular/vendor-financing arrangements at **>$800B** [M]. The loop: **chipmaker → AI lab → cloud → back to chipmaker**, with the same names on multiple sides. NVIDIA invests in OpenAI/xAI/Mistral and the neoclouds, which buy NVIDIA chips; OpenAI commits hundreds of billions to Oracle/Azure/AWS, which buy NVIDIA; NVIDIA had committed **>$40B of equity to AI companies by May 2026 (>$30B to OpenAI)** [M]. The **bull framing (UBS, Janus Henderson):** the OpenAI–NVIDIA arrangement is ~13% of NVIDIA's projected 2026 revenue (~$272B consensus), reinvestment is *performance-contingent* (unlike fixed dot-com-era telecom commitments), and balance sheets are far healthier than 2000 [M]. The **bear framing:** it manufactures the appearance of demand and magnifies losses; the Feb-2026 episode — when a WSJ report that NVIDIA's $100B OpenAI investment had "stalled" coincided with Oracle raising $50B of debt — showed how fast the market connects the chain (OpenAI can't pay Oracle → Oracle stops buying NVIDIA) [M]. **Watch-item:** OpenAI's funding chain is the system's load-bearing wall; much of the "demand" depends on its continued ability to raise.

### 4.2 The binding constraint
It has shifted **from chips → power and HBM memory** [H]. Chips are constrained but available with advance commitment; **power** (generation + interconnection) and **HBM/advanced packaging** are the true gates. This reorders who captures scarcity rents: firm-power owners and the memory oligopoly gain pricing power; it also means the buildout's pace is set by grid and fab/packaging timelines, not order books.

### 4.3 Concentration & single points of failure
Three hard SPOFs: **NVIDIA** (accelerator dominance + CUDA), **TSMC** (leading-edge fabrication, geographically concentrated in Taiwan), and the **HBM oligopoly** (SK Hynix/Samsung/Micron >95% of DRAM). Add **hyperscaler demand concentration** (a handful of buyers drive most Layer-1 revenue) and **OpenAI counterparty concentration** (a single lab underwrites much of Oracle's and others' backlog). Any one breaking transmits through the whole chain.

### 4.4 Demand durability & ROI (the decisive debate)
- **Unit price is collapsing:** cost per million tokens fell from ~$10 to ~$2.50 in a year (Ramp/Artefact), or ~$1.90 → ~$0.65 (−65%, Yipit) [M].
- **But total spend is rising (Jevons):** token consumption reached ~24,000B/month by May 2026 (~19x in 18 months); agentic workflows consume 5–30x more tokens per task than chatbots [M]. Goldman models a ~24x consumption increase by 2030.
- **The gap:** J.P. Morgan estimates ~$650B of new annual revenue is needed for a 10% return on the build; current AI-attributable revenue is ~$50–150B → a **4–13x gap**; Bain projects $2T of annual AI revenue required by 2030 [M].
- **Realized ROI is weak so far:** MIT NANDA found 95% of GenAI pilots showed no measurable P&L return; S&P (42% abandoned most projects), IBM (25% delivering expected ROI), Morgan Stanley (only 21% of S&P 500 citing meaningful ROI) corroborate [M]. Real cost strain is visible: Uber burned its entire 2026 AI budget in four months; Microsoft canceled most direct Claude Code licenses over cost; one firm reportedly spent $500M in a month ("tokenmaxxing") [M].
- **Counter-signal:** cloud backlogs and growth rates suggest the demand is genuine even if monetization lags; inference is currently priced below cost (a "false floor" that should normalize upward) [M].

### 4.5 Regulation & geopolitics
Export controls have zeroed out NVIDIA's China data-center revenue in guidance [H]; a recent export action also constrained the most advanced model tier of at least one U.S. lab [M]. Antitrust scrutiny of circular AI mega-deals is rising; the HBM/fab chokepoints sit in Korea/Taiwan, exposing the U.S. chain to East-Asia geopolitical risk. *(Mineral/material and full chip-export dimensions belong to the sibling agents.)*

---

## 5. Contested questions, adjudicated

1. **Is the capex rational or a bubble?** *Both halves are evidenced.* Bull: backlogs, growth rates, and Jevons-style consumption growth are real; performance-contingent financing limits downside. Bear: a 4–13x revenue gap, 95% pilot-failure, FCF compression, and a depreciation/financing structure that amplifies any demand miss. **View [M]:** the *infrastructure* is likely under-built for a 5-year horizon **if** inference demand compounds as observed, but the *equity valuations and the financing chain* are priced for near-perfect execution, so the risk is a sharp repricing rather than a demand collapse. Falsifier of the bull case: two consecutive quarters of decelerating hyperscaler cloud growth *with* flat backlogs.
2. **GPU depreciation — flattered earnings?** Burry estimates ~$176B of understated depreciation (2026–28), with Oracle (~27%) and Meta (~21%) most exposed by 2028; Amazon already cut server useful lives in Feb 2025 [M]. Rebuttal: depreciation is non-cash (FCF unaffected) and 3–6-year lives are GAAP-defensible if old GPUs find inference second-life. **View [M]:** the cash impact is overstated by Burry but the *earnings-quality* point is valid — if true life is ~3 years, much "growth capex" is actually sustaining capex, structurally lowering FCF and raising impairment risk.
3. **Is the circular financing fragile?** **View [M]:** defensible while OpenAI can raise, fragile if it cannot — the chain's weakest link is a single private company's funding access. The S-1 filings (OpenAI, Anthropic) are partly mechanisms to fund these very commitments.
4. **Will inference economics commoditize the model layer?** Token prices are deflating and open-weight models set a floor; labs subsidize inference. **View [M]:** model-layer gross margins are structurally pressured; durable value accrues to (a) compute/HBM/fab chokepoints, (b) distribution-advantaged incumbents (hyperscalers, Microsoft), and (c) proprietary-data/workflow applications — less to pure model access.

---

## 6. Scenarios & stress test (24-month horizon)

| | **Bull (~30%)** | **Base (~45%)** | **Bear (~25%)** |
|---|---|---|---|
| Trigger | Inference demand compounds; ROI cases land; power/HBM scale | Growth continues but constraints + margin pressure bite unevenly | Capex outruns ROI; a financing/depreciation reckoning; demand disappoints |
| Winners | NVIDIA, HBM trio, TSMC, power owners, Palantir, leading labs | Chokepoint owners (NVIDIA, HBM, TSMC, power); diversified hyperscalers | Firm-power owners, low-cost incumbents, short-sellers |
| Losers | Shorts, sub-scale neoclouds | Pure-play neoclouds, commodity apps, high-multiple names | Levered neoclouds (CoreWeave-type), Oracle (single-customer debt), high-multiple apps (Palantir), the labs |
| Leading indicators | Backlog growth + accelerating cloud rev + falling $/token *with* rising usage | Stable backlogs, gradual margin compression | Decelerating cloud growth + flat backlogs + a neocloud credit event or GPU impairment |

**The single most informative tripwire:** a credit event or covenant breach at a GPU-backed neocloud, or a large GPU impairment/useful-life cut — either would validate the bear chain and reprice the whole stack.

---

## 7. Investment implications by layer (analysis, not advice)

- **Most defensible pricing power:** Layer 1 chokepoints (NVIDIA/CUDA, HBM oligopoly, TSMC) and Layer 0 firm power. These capture scarcity rents regardless of which lab or app wins.
- **Highest operating leverage with proven economics:** profitable application incumbents (Palantir) and distribution-advantaged hyperscalers — but valuations already embed the growth.
- **Highest risk-for-reward:** model labs (historic growth, negative margins, funding-dependent) and levered neoclouds (the depreciation/credit stress point).
- **The macro watch:** the ~$650B revenue gap and the OpenAI funding chain are the two variables that most determine whether the whole structure holds.

---

## 8. Comparison tables

**8a. Compute & hardware (public; latest reported, dated)**
| Company | Key metric | Valuation signal | Source date |
|---|---|---|---|
| NVIDIA | Q1 FY27 rev $81.6B; DC $75.2B (+92%) | ~75% GM; FY26 NI ~$117B | May 2026 |
| Broadcom | AI rev +106%; ~$100B ambition | custom-silicon scale leader | Jun 2026 |
| AMD | MI350/MI355; OpenAI warrant deal | #2 merchant GPU | 2026 |
| SK Hynix | Q1'26 rev ~$35.5B; OP ~$27.8B; HBM ~50–57% | ~$29B Nasdaq listing ~Jul'26 | Apr 2026 |
| Micron | $20B 2026 capex; HBM ~5–10% | crossed ~$1T mcap | Apr 2026 |

**8b. Hyperscalers (2026 capex guidance, post-Q1 revisions)**
| Company | 2026 capex | Cloud growth (Q1'26) | Backlog signal |
|---|---|---|---|
| Amazon | ~$200B | AWS +28% | Trainium ~$20B run-rate |
| Microsoft | ~$190B (CY) | Azure +40% | RPO $627B |
| Alphabet | ~$185–190B | Cloud +63% | backlog >$460B |
| Meta | $125–145B | (no public cloud) | shares −9% on raise |
| *Combined* | *~$725B (+~77% YoY)* | | Goldman: ~$5.3T FY25–30 |

**8c. Frontier labs (private; press-sourced run-rate — not audited)**
| Lab | Run-rate revenue | Valuation | IPO status |
|---|---|---|---|
| OpenAI | ~$24–25B (Q1'26) | $852B (Mar'26) | S-1 filed 8 Jun 2026 |
| Anthropic | ~$47B (May'26, reported) | $965B (May'26) | S-1 filed 1 Jun 2026 |
| xAI/SpaceX | n/d | ~$1.25T (merger) | mega-IPO pending (~$2T target) |

**8d. Applications (mix)**
| Company | Revenue | Valuation | Note |
|---|---|---|---|
| Palantir | Q1'26 $1.63B (+85%); FY ~$7.65B | ~$330B mcap; ~120–150x P/E | −33% from $207 high; Burry short |
| Cursor/Anysphere | ~$2B ARR (Feb'26) | ~$50B (raising) | SpaceX $60B option |
| Cognition/Devin | $492M (May'26) | ~$26B | agent-first |

---

## 9. Source list (publisher · date · URL)

1. NVIDIA 8-K Q4/FY2026 & Q1 FY2027; 10-Q — SEC · Feb–May 2026 · sec.gov/Archives/edgar/data/0001045810/
2. "Nvidia smashes Q4 2026…" — Fortune · 25 Feb 2026 · fortune.com/2026/02/25/nvidia-nvda-earnings-q4-results-jensen-huang/
3. "Hyperscalers Hit $700 Billion in 2026 AI Spending" — Yahoo Finance · 1 May 2026 · finance.yahoo.com/sectors/technology/articles/hyperscalers-hit-700-billion-2026-111243744.html
4. "Big Tech's AI spending… $725 billion" — Tom's Hardware · 30 Apr 2026 · tomshardware.com/tech-industry/big-tech/big-techs-ai-spending-plans-reach-725-billion
5. "Meta, Microsoft, Amazon, Alphabet… shocking amount" (Goldman $5.3T) — Yahoo Finance · Jun 2026 · finance.yahoo.com/sectors/technology/article/meta-microsoft-amazon-and-alphabet-are-about-to-spend-a-shocking-amount-of-money-to-dominate-the-ai-era-115359575.html
6. "Tech AI spending… cash taking big hit" — CNBC · 6 Feb 2026 · cnbc.com/2026/02/06/google-microsoft-meta-amazon-ai-cash.html
7. "AI Capex 2026: $300B… where it's going" (power constraint) — NextWaves · 5 May 2026 · nextwavesinsight.com/hyperscaler-ai-capex-microsoft-google-amazon-meta-2026/
8. OpenAI revenue/valuation/funding — Sacra · Jun 2026 · sacra.com/c/openai/
9. "OpenAI raises $122 billion…" — OpenAI · 31 Mar 2026 · openai.com/index/accelerating-the-next-phase-ai/
10. "OpenAI IPO: $850B Valuation, $25B Revenue" (S-1 8 Jun) — Tech-Insider · Jun 2026 · tech-insider.org/openai-ipo-850-billion-valuation-2026/
11. "AI Circular Financing: The Nvidia-OpenAI-Oracle Money Loop" — Alatirok · May 2026 · alatirok.com/ai-circular-financing-explained/
12. "Should recent AI financing deals be a cause for concern?" (UBS CIO) — UBS · Oct 2025 · ubs.com/global/en/wealthmanagement/insights/…/latest-10102025.html
13. "AI Circular Deals…" — Bloomberg · 11 Mar 2026 · bloomberg.com/graphics/2026-ai-circular-deals/
14. "OpenAI/Nvidia/Oracle $100B 'stall'" — tech-ish · 3 Feb 2026 · tech-ish.com/2026/02/03/nvidia-openai-oracle-circular-financing-loop/
15. "Data Center Power & Energy News 2026" — iRecruit · Jun 2026 · irecruit.co/insights/data-center-power-and-energy-news-2026
16. "Constellation Energy… nuclear AI power deals" — useLuminix · 30 Apr 2026 · useluminix.com/reports/company-overviews/constellation-energy-company-overview-…-2026
17. "Meta inks nuclear deals up to 6.6 GW…" — Utility Dive · 9 Jan 2026 · utilitydive.com/news/meta-nuclear-deal-oklo-vistra-terrapower-ai-data-centers/809215/
18. "'Big Short' Burry accuses hyperscalers…" — CNBC · 11 Nov 2025 · cnbc.com/2025/11/11/big-short-investor-michael-burry-accuses-ai-hyperscalers…
19. "Are AI Chip 'Useful Lives' Creating Useless Earnings?" — Levelheaded Investing · 4 Dec 2025 · levelheadedinvesting.com/p/are-ai-chips-useful-lives-creating-useless-earnings
20. "The Melting Ice Cube… Burry/Chanos/CoreWeave" — OuterSpeak · 14 Feb 2026 · outerspeak.substack.com/p/the-melting-ice-cube-michael-burry
21. "This Obviously is an AI Bubble. The Math Says So" (J.P. Morgan $650B; Bain $2T) — Anomaly Investments · Jun 2026 · anomalyinvestments.substack.com/p/this-obviously-is-an-ai-bubble-the
22. "The AI Token Pricing Crisis…" — Investing.com · 22 May 2026 · investing.com/analysis/the-ai-token-pricing-crisis-behind-openai-and-anthropics-revenue-race-200680777
23. "Enterprise AI ROI Sticker Shock" (MIT NANDA 95%) — AI Consulting Network · May 2026 · theaiconsultingnetwork.com/blog/enterprise-ai-roi-sticker-shock-cre-investors-2026
24. "The AI economy could crash on mounting chip costs" — Fortune · 30 May 2026 · fortune.com/2026/05/30/ai-chip-token-bubble-economy-nvidia-microsoft-hyperscalers-2/
25. "Memory Chip Shortage 2026: HBM Takes 23% of DRAM Wafers" — Tech-Insider · Jun 2026 · tech-insider.org/memory-chip-shortage-2026-ai-consumer-electronics/
26. "Samsung & SK hynix warn… shortages to 2027+" — Tom's Hardware · 30 Apr 2026 · tomshardware.com/tech-industry/artificial-intelligence/samsung-and-sk-hynix-warn…
27. "SK Hynix posts record Q1 profit" — CNBC · 23 Apr 2026 · cnbc.com/2026/04/23/sk-hynix-earnings-ai-memory-shortage-hbm-demand.html
28. "Broadcom AI Revenue Surges 106%" — Tech-Insider · Jun 2026 · tech-insider.org/broadcom-ai-revenue-custom-chips-2026/
29. "Anthropic says it hit a $30 billion revenue run rate" — VentureBeat · 8 May 2026 · venturebeat.com/technology/anthropic-says-it-hit-a-30-billion-revenue-run-rate-after-crazy-80x-growth
30. "Anthropic raises $65 billion, nears $1T…" — TechCrunch · 28 May 2026 · techcrunch.com/2026/05/28/anthropic-raises-65-billion-nears-1t-valuation-ahead-of-ipo/
31. "Anthropic tops OpenAI as most valuable AI startup" — CNBC · 28 May 2026 · cnbc.com/2026/05/28/anthropic-open-ai-startup-value.html
32. "What Anthropic Becoming Top Private AI Firm Means" — Investing.com · May 2026 · investing.com/analysis/…-200681008
33. "Anthropic Financial Forecast" (S-1 1 Jun; overhangs) — FutureSearch · Jun 2026 · futuresearch.ai/anthropic-financial-forecast/
34. "Palantir's Valuation Tests…" — Investing.com · May 2026 · investing.com/analysis/palantirs-valuation-tests-how-much-ai-growth-investors-will-pay-for-200680966
35. "Palantir (PLTR) Stock Analysis" — Simply Wall St · Jun 2026 · simplywall.st/stocks/us/software/nasdaq-pltr/palantir-technologies
36. "Cursor $2B at $50B valuation…" — The Next Web · 6 May 2026 · thenextweb.com/news/cursor-anysphere-2-billion-funding-50-billion-valuation-ai-coding
37. "Cognition's $26B Raise…" — TechTimes · 29 May 2026 · techtimes.com/articles/317354/20260529/…
38. "AI Company Rankings 2026" (Perplexity/CoreWeave/Scale valuations) — TLDL · 2 May 2026 · tldl.io/resources/ai-companies-landscape-2026
39. "HBM Supply Crisis 2026" (Micron/SK Hynix capex) — EnkiAI · 8 Apr 2026 · enkiai.com/data-center/hbm-supply-crisis-2026-the-bottleneck-redefining-ai/
40. "Is AI really getting cheaper? The token cost illusion" — Artefact · 1 Apr 2026 · artefact.com/blog/is-ai-really-getting-cheaper-the-token-cost-illusion/

---

## 10. Appendix — assumptions, confidence, open questions

**Estimation / data-quality notes.** Private-company revenue figures are *run-rate snapshots* from press/research firms, not audited statements; they conflict across sources (e.g., OpenAI Q1 2026 run-rate appears as $20B–$25B; Anthropic's $47B May figure is a reported run-rate with annualized-growth optics). Hyperscaler 2026 capex was revised **upward** through Q1-2026 earnings (Feb estimates of ~$630–690B → ~$725B by late April), so vintage matters. Where a figure is a single-source estimate it is tagged [M]/[L].

**Confidence summary.** High [H]: NVIDIA financials, hyperscaler capex/cloud-growth magnitudes, power-as-constraint, HBM shortage. Medium [M]: private-lab revenue/valuations, circular-financing totals, depreciation-impact estimates, ROI-gap math. Low [L]: xAI/SpaceX IPO specifics, some application-layer projections.

**Top open questions for the full-depth pass.**
1. OpenAI's audited economics (the S-1 will be the single most important disclosure for the whole chain).
2. Neocloud credit structures and covenants (CoreWeave et al.) — the most likely first crack.
3. The true blended useful life of deployed GPUs (resolves the depreciation debate).
4. Hard data on AI-attributable enterprise revenue (to size the ROI gap precisely).
5. Data-center REIT and power-interconnection timelines by region (gates the buildout).
6. Deeper Layer-4 coverage (Databricks/Snowflake/Scale economics), under-developed in this pass.
