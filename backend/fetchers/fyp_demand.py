"""
15th-FYP Domestic Demand cockpit — Part V of the outline (第五篇 建设强大国内市场,
chapters 15–17). Static registry of the ten 节-level sections with their
plan-text capture (ground truth: the stored outline in policy_docs, gov.cn
content_7062633), plus helpers that pull related policy documents and the
macro series backing the sidebar charts.

Served by /api/fyp/demand (backend/api.py). Mirrors fyp_tech.py: plan text and
section metadata are Python constants; live rows come from data/cmm.db.
"""

import sqlite3

DOC_URL = "https://www.gov.cn/yaowen/liebiao/202603/content_7062633.htm"

# Macro series (macro_series table, Global Macro Database) used by the sidebars.
SERIES_VARS = ["hcons_GDP", "inv_GDP", "unemp", "govexp_GDP"]

HUB = {
    "name_en": "Expanding domestic demand",
    "name_cn": "扩大内需 · 战略基点",
    "objective": "居民消费率明显提高 — a marked rise in household consumption as a "
                 "share of GDP, with domestic demand the persistent main engine of "
                 "growth (Chapter 3, the plan's 2030 objectives).",
    "preamble": [
        "Expanding domestic demand is the strategic basis (战略基点) of the new "
        "development pattern: build a strong domestic market and a reliable "
        "internal circulation (国内大循环).",
        "Couple improving livelihoods with promoting consumption, and investing "
        "in things with investing in people (惠民生和促消费、投资于物和投资于人紧密结合).",
        "Let new demand lead new supply and new supply create new demand — a "
        "positive consumption–investment and supply–demand interplay at a higher "
        "dynamic equilibrium.",
    ],
    "doc_like": ["扩大内需", "提振消费", "扩大消费"],
}

# The ten 节 of Part V. `chapter` groups the nodes (15 consumption / 16
# investment / 17 unified market); `points` follow the stored Chinese text
# section by section.
SECTIONS = [
    {
        "id": "cap", "chapter": 15, "glyph": "收",
        "name_en": "Consumption capacity", "name_cn": "夯实居民消费基础",
        "points": [
            "Stabilise and expand employment: support firms that keep and add "
            "jobs, cultivate new occupations, open job space in the digital, "
            "green and silver economies.",
            "Raise incomes: steadily lift minimum wages, improve the operating "
            "environment for small businesses and the self-employed, promote "
            "healthy property and stock markets.",
            "Widen the social-safety net: bring gig and new-form workers into "
            "employee insurance, index minimum-living (低保) standards to "
            "per-capita consumption spending, raise government spending on "
            "livelihood support.",
        ],
        "doc_like": ["就业", "工资", "社会保障"],
    },
    {
        "id": "services", "chapter": 15, "glyph": "服",
        "name_en": "Service consumption", "name_cn": "释放服务消费潜力",
        "points": [
            "Expand service consumption through wider market access and blended "
            "business formats.",
            "Convenience: community embedded services and 15-minute "
            "convenient-living circles (一刻钟便民生活圈), refreshed commercial "
            "districts, better elderly-care, childcare and domestic services.",
            "Development-type consumption: regulated education and training, a "
            "bigger health-consumption market, imported international quality "
            "services.",
            "Experience: culture, sports and tourism — easier approval of live "
            "shows and sports events, an ice-snow tourism plan, cruises, yachts, "
            "RV camping and low-altitude consumption; immersive interactive "
            "consumption scenes blending commerce, tourism, culture, sports and "
            "health.",
        ],
        "doc_like": ["服务消费", "养老", "托育", "文旅"],
    },
    {
        "id": "goods", "chapter": 15, "glyph": "购",
        "name_en": "Goods consumption", "name_cn": "推动商品消费扩容升级",
        "points": [
            "Housing: city-by-city property policy to release rigid and upgrade "
            "demand; aging-friendly and smart retrofits of older homes.",
            "Autos: shift from purchase control to usage management — charging/"
            "battery-swap and parking infrastructure, aftermarket modification "
            "and rental markets.",
            "Renewal: trade-in and recycling systems for cars, electronics, "
            "appliances and furniture; a regulated second-hand market.",
            "Digital and green: a digital-consumption drive, smart products with "
            "smart-home interconnection standards, flexible customised "
            "production; promote green low-carbon products.",
            "Premium: heritage brands (老字号) and national trend brands "
            "(国货潮牌), derivative merchandise, the first-launch economy "
            "(首发经济).",
        ],
        "doc_like": ["以旧换新", "汽车消费", "消费品", "家电"],
    },
    {
        "id": "env", "chapter": 15, "glyph": "境",
        "name_en": "Consumption environment", "name_cn": "持续改善消费环境",
        "points": [
            "Clear unreasonable restrictions on consumption; put incentives "
            "directly in consumers' hands; expand consumer finance; build "
            "flagship new consumption scenes.",
            "Inbound consumption: departure-tax-refund stores, international "
            "consumption-center cities, \"Buy in China\" (购在中国).",
            "Time to spend: enforce paid annual leave, encourage flexible "
            "staggered vacations, pilot spring and autumn school breaks.",
            "Protection and measurement: stronger consumer-rights channels, "
            "rules for live-stream selling and prepaid schemes, comprehensive "
            "consumption statistics (全口径消费统计).",
        ],
        "doc_like": ["消费环境", "入境消费", "离境退税", "消费者权益"],
    },
    {
        "id": "gov_inv", "chapter": 16, "glyph": "政",
        "name_en": "Government investment returns", "name_cn": "提高政府投资效益",
        "points": [
            "Reorient public investment toward livelihood, short-boards and new "
            "momentum; pair \"hard investment\" with \"soft construction\"; a "
            "batch of landmark national projects.",
            "Invest in people: 一老一小 (elderly + child) services, primary "
            "healthcare, senior-high and quality higher-education expansion, "
            "vocational training — raise the livelihood share of government "
            "investment.",
            "Back new-type infrastructure and intangible-asset investment; "
            "whole-process management to avoid overbuilding and low-return "
            "projects.",
            "Tools: clearer central/local division of investment, pilot "
            "full-scope government investment plans, special-bond reform "
            "(自审自发, larger construction share), new policy-based financial "
            "instruments, streamlined investment approvals.",
        ],
        "doc_like": ["政府投资", "专项债", "中央预算内投资"],
    },
    {
        "id": "priv_inv", "chapter": 16, "glyph": "民",
        "name_en": "Private investment", "name_cn": "激发民间投资活力",
        "points": [
            "Raise the private-investment share of total investment on equal "
            "treatment, rights protection and policy synergy.",
            "A long-term mechanism for private participation in major projects — "
            "railways, nuclear power, hydropower, water supply — with higher "
            "private equity stakes where conditions allow.",
            "Encourage private investment in tech innovation and industrial "
            "upgrading; open emerging-sector application scenarios to private "
            "firms.",
            "Equal access to land and financing; government investment funds as "
            "guides with a full invest–finance–manage–exit (投融管退) cycle.",
        ],
        "doc_like": ["民间投资", "民营"],
    },
    {
        "id": "loop", "chapter": 16, "glyph": "循",
        "name_en": "Investment–consumption loop", "name_cn": "促进投资消费良性循环",
        "points": [
            "Work the投资-消费 joints: investment lifts consumption capacity and "
            "willingness, consumption upgrading steers where investment goes.",
            "Back industries with strong job and income spillovers; invest in "
            "consumption infrastructure and scene upgrades — health, elderly "
            "care, sports, leisure.",
            "Add consumption functions to transport hubs and industrial "
            "clusters; upgrade county-level commerce; fix logistics, "
            "warehousing and rural-delivery short-boards.",
        ],
        "doc_like": ["县域商业", "消费基础设施", "扩大内需"],
    },
    {
        "id": "base", "chapter": 17, "glyph": "制",
        "name_en": "Base market institutions", "name_cn": "完善统一大市场基础制度规则",
        "points": [
            "Equal, lasting property-rights protection across ownership types — "
            "same liability, same crime, same punishment (同责同罪同罚); IP "
            "protection with trade-secret rule guides.",
            "Market access: a dynamically revised negative list, strictly one "
            "national list (全国一张清单).",
            "Unified disclosure and credit systems: quality/safety/environment "
            "disclosure rules, the national credit-information platform, "
            "incentive–penalty–repair mechanisms; a better bankruptcy regime "
            "and a simple-exit mechanism for all business entities.",
            "Align statistics and taxes with a single market: activity-location "
            "statistics, revenue sharing between headquarters and branches, "
            "production and consumption locations; enact the National Unified "
            "Market Construction Regulation (全国统一大市场建设条例).",
        ],
        "doc_like": ["统一大市场", "市场准入", "负面清单", "社会信用"],
    },
    {
        "id": "fair", "chapter": 17, "glyph": "竞",
        "name_en": "Fair competition", "name_cn": "维护公平竞争市场秩序",
        "points": [
            "Make fair-competition review binding; remove barriers in factor "
            "access, qualifications, tendering and government procurement.",
            "Discipline local promotion: encouraged/prohibited lists for "
            "investment attraction with disclosure, a dynamic list of "
            "unified-market violations with accountability.",
            "Stronger antitrust and anti-unfair-competition enforcement with "
            "key-sector guides.",
            "One standards system: wider mandatory national standards, "
            "emerging-industry standards, internationalisation; shared testing "
            "and certification with mutual recognition; unified enforcement "
            "with consistent penalty benchmarks.",
        ],
        "doc_like": ["公平竞争", "反垄断", "招标投标", "市场监管"],
    },
    {
        "id": "infra", "chapter": 17, "glyph": "通",
        "name_en": "Market infrastructure", "name_cn": "促进市场设施高标准联通",
        "points": [
            "A modern circulation system: national logistics-hub network, "
            "backbone commodity corridors anchored on strategic fulcrum cities, "
            "commodity resource-allocation hubs, unified circulation rules and "
            "standards — cut whole-economy logistics costs.",
            "Multimodal transport: shared facilities, standards and data; "
            "single-document and single-container (一单制/一箱制) rules; "
            "container rail–water transfer.",
            "Trusted information and platforms: whole-chain information trust "
            "mechanisms; unified public-resource trading platforms (tendering, "
            "government/SOE procurement) with AI and big-data supervision and "
            "full-process transparency.",
        ],
        "doc_like": ["物流", "多式联运", "流通"],
    },
]

CHAPTERS = {
    15: {"name_en": "Boosting consumption", "name_cn": "大力提振消费"},
    16: {"name_en": "Effective investment", "name_cn": "扩大有效投资"},
    17: {"name_en": "Unified national market", "name_cn": "纵深推进全国统一大市场建设"},
}


# ---------------------------------------------------------------------------
# v2 — implementing policies (milestones), concrete goals (targets), and
# status-vs-data. Every number below was verified against the cited source
# on 2026-07-14; static readings are annual official figures that change at
# most yearly — update them when the next official release lands.
# ---------------------------------------------------------------------------

_XFGH_URL = "https://www.gov.cn/zhengce/content/202607/content_7075216.htm"   # 扩大消费"十五五"规划批复
_TZXF_URL = "https://www.gov.cn/zhengce/202503/content_7013808.htm"           # 提振消费专项行动方案

# Curated implementing policies per section (and hub). Newest first.
MILESTONES = {
    "hub": [
        {"date": "2026-07", "title_en": "Expanding Consumption 15th-FYP sub-plan approved",
         "title_cn": "《扩大消费“十五五”规划》(国函〔2026〕66号)",
         "note": "28 task packages + 9 special columns; sets the hard 2030 goal of ~60 tn CNY "
                 "retail sales (2025: 50.1 tn) and services-share growth.", "url": _XFGH_URL},
        {"date": "2025-03", "title_en": "Special Action Plan on Boosting Consumption",
         "title_cn": "中办国办《提振消费专项行动方案》",
         "note": "8 areas / 30 measures — income growth, consumption capacity, service and "
                 "big-ticket consumption, environment, and restriction clean-up.", "url": _TZXF_URL},
    ],
    "cap": [
        {"date": "2025-07", "title_en": "National childcare subsidy scheme",
         "title_cn": "育儿补贴制度实施方案",
         "note": "3,600 CNY per child per year until age 3, retroactive to 2025-01; "
                 "~90 bn CNY central budget in year one — first nationwide cash transfer to households.",
         "url": "https://www.gov.cn/zhengce/202507/content_7034531.htm"},
        {"date": "2025-07", "title_en": "Gig-worker injury insurance pilot expanded",
         "title_cn": "新就业形态人员职业伤害保障试点扩围",
         "note": "9 ministries: +10 provinces and major platforms from 2025-07; nationwide "
                 "coverage (31 provinces) planned for 2026 — the plan's 织密扎牢社会保障网 in practice.",
         "url": "https://www.gov.cn/zhengce/zhengceku/202507/content_7031656.htm"},
        {"date": "2025-03", "title_en": "Boosting-consumption plan: income actions",
         "title_cn": "提振消费专项行动方案 — 城乡居民增收促进行动",
         "note": "Wage growth, minimum-wage adjustment mechanism, property/stock market "
                 "stabilisation as consumption preconditions.", "url": _TZXF_URL},
    ],
    "services": [
        {"date": "2025-09", "title_en": "Policy package on expanding service consumption",
         "title_cn": "商务部等9部门《关于扩大服务消费的若干政策措施》",
         "note": "Follow-up package: opening-up of services, new scenes, 一老一小 focus.",
         "url": "https://www.gov.cn/zhengce/zhengceku/202509/content_7040952.htm"},
        {"date": "2025-04", "title_en": "Service Consumption Quality Action — 2025 work plan",
         "title_cn": "服务消费提质惠民行动2025年工作方案",
         "note": "9 ministries, 48 measures across dining, lodging, health, culture, tourism, "
                 "sports; new formats incl. tourist trains, air tours, micro-dramas.",
         "url": "https://www.gov.cn/lianbo/bumen/202504/content_7019127.htm"},
        {"date": "2026-07", "title_en": "Consumption sub-plan service columns",
         "title_cn": "扩大消费“十五五”规划 — 养老/托育/文旅/健康专栏",
         "note": "Dedicated columns on elderly care, childcare, culture & tourism and health "
                 "consumption with named delivery programs.", "url": _XFGH_URL},
    ],
    "goods": [
        {"date": "2025-12", "title_en": "2026 trade-in program notice + first tranche",
         "title_cn": "关于2026年实施“两新”政策的通知 (发改环资〔2025〕1745号)",
         "note": "2026 consumer trade-in funded with 250 bn CNY of super-long special bonds "
                 "(first 62.5 bn tranche pre-allocated 2025-12); 9:1 central/local cost share.",
         "url": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202512/t20251230_1402851.html"},
        {"date": "2025-01", "title_en": "2025 trade-in expansion",
         "title_cn": "2025年加力扩围实施“两新”政策 (发改环资〔2025〕13号)",
         "note": "300 bn CNY super-long special bonds for consumer trade-ins (double 2024's "
                 "150 bn); coverage extended to phones/tablets/smartwatches, 12 appliance classes.",
         "url": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202501/t20250108_1395564.html"},
        {"date": "2026-06", "title_en": "Full-chain auto-consumption reform",
         "title_cn": "全链条扩大汽车消费 (国新办发布会)",
         "note": "Purchase-management → usage-management shift: used-car circulation, "
                 "modification, rental and aftermarket opening.",
         "url": "http://www.mofcom.gov.cn/zcjd/gnmy/art/2026/art_027a967d7c4243f5a15d5f1337fd698e.html"},
    ],
    "env": [
        {"date": "2025-04", "title_en": "Departure tax refund overhaul",
         "title_cn": "商务部等6部门优化离境退税政策 + 税务总局“即买即退”全国推广",
         "note": "Refund threshold cut 500→200 CNY, cash cap doubled to 20,000 CNY, "
                 "refund-at-purchase nationwide — anchor of 购在中国 inbound consumption.",
         "url": "https://www.gov.cn/zhengce/zhengceku/202504/content_7021194.htm"},
        {"date": "2025-03", "title_en": "Consumption-environment and restriction clean-up actions",
         "title_cn": "提振消费方案 — 消费环境改善提升 / 限制措施清理优化行动",
         "note": "Paid-leave enforcement, staggered vacations and spring/autumn school-break "
                 "pilots; systematic removal of unreasonable consumption restrictions.",
         "url": _TZXF_URL},
    ],
    "gov_inv": [
        {"date": "2026-03", "title_en": "2026 super-long special bonds: 1.3 tn CNY",
         "title_cn": "2026年超长期特别国债安排 (预算报告)",
         "note": "800 bn for major strategies/security (两重, 1,459 projects), 200 bn equipment "
                 "renewal, 250 bn consumer trade-ins; issuance Apr–Oct 2026.",
         "url": "https://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen/202603/t20260316_3985331.htm"},
        {"date": "2025-09", "title_en": "New policy-based financial instrument: 500 bn CNY",
         "title_cn": "新型政策性金融工具",
         "note": "500 bn CNY project-capital injections, fully deployed by 2025-10-31: 2,300+ "
                 "projects, ~7 tn CNY total investment — incl. consumer infrastructure, AI, urban renewal.",
         "url": "http://finance.people.com.cn/n1/2025/1102/c1004-40594850.html"},
        {"date": "2024-12", "title_en": "Special-bond reform: self-review-and-issue pilot",
         "title_cn": "国办《关于优化完善地方政府专项债券管理机制的意见》",
         "note": "自审自发 pilot in 10 provinces + Xiong'an — province-level approval replaces "
                 "central review; expanded 2026-06. Direct delivery of the plan's Ch-16 reform.",
         "url": "https://www.gov.cn/zhengce/content/202412/content_6994502.htm"},
    ],
    "priv_inv": [
        {"date": "2025-05", "title_en": "Private Economy Promotion Law in force",
         "title_cn": "中华人民共和国民营经济促进法 (2025-05-20施行)",
         "note": "First basic law for the private economy: equal access, rights protection, "
                 "payment-arrears remedies; NDRC published first implementation-case batch 2026-05.",
         "url": "http://www.npc.gov.cn/npc/c2/c30834/202504/t20250430_445088.html"},
        {"date": "2025-04", "title_en": "Energy sector opened wider to private capital",
         "title_cn": "国家能源局促进能源领域民营经济发展十条举措",
         "note": "Private stakes in new nuclear projects raised beyond the old ~10% — up to 20% "
                 "in recent approvals; ~20 private firms now hold stakes in new reactors.",
         "url": "http://www.news.cn/20250429/274f49e90b864abf99af12f705a0eaf3/c.html"},
    ],
    "loop": [
        {"date": "2025-10", "title_en": "Policy-tool money into consumption infrastructure",
         "title_cn": "新型政策性金融工具 — 消费基础设施投向",
         "note": "The 500 bn instrument explicitly lists consumer infrastructure among its "
                 "priority destinations — investment steered toward consumption capacity.",
         "url": "https://jrj.sh.gov.cn/ZXYW178/20251021/933a0f4f29b041e48bb666b92540e64c.html"},
        {"date": "2025-01", "title_en": "County commerce action — final year",
         "title_cn": "县域商业建设行动 (三年行动收官)",
         "note": "County/township/village commercial networks + rural delivery 补短板 — the "
                 "plan's county-commerce and rural-logistics point in delivery.",
         "url": "https://www.mofcom.gov.cn/zwgk/jgdt/art/2025/art_f43b016416ef4dd49435a0edb36429f4.html"},
    ],
    "base": [
        {"date": "2025-04", "title_en": "Market-access negative list, 2025 edition",
         "title_cn": "市场准入负面清单（2025年版）",
         "note": "106 items, down from 117 (2022) and 151 (2018, first edition) — one national "
                 "list (全国一张清单) discipline; 469 national measures (from 486).",
         "url": "https://www.ndrc.gov.cn/xxgk/zcfb/ghxwj/202504/t20250424_1397358.html"},
        {"date": "2025-01", "title_en": "Unified-market construction guideline",
         "title_cn": "《全国统一大市场建设指引（试行）》(发改体改〔2024〕1742号)",
         "note": "First operational guideline: what localities must do, may do, must not do.",
         "url": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202501/t20250107_1395496.html"},
        {"date": None, "title_en": "Unified Market Construction Regulation — pending",
         "title_cn": "全国统一大市场建设条例 — 制定中",
         "note": "The plan's own Ch-17 commitment (制定条例); not yet enacted as of 2026-07 — "
                 "the key open institutional milestone.", "url": None},
    ],
    "fair": [
        {"date": "2025-06", "title_en": "Anti-Unfair-Competition Law revised (anti-内卷)",
         "title_cn": "反不正当竞争法修订 (2025-10-15施行)",
         "note": "Bans platforms forcing below-cost pricing on merchants — first statutory tool "
                 "aimed at 内卷式 price wars; part of the 综合整治\"内卷式\"竞争 campaign.",
         "url": "https://www.gov.cn/yaowen/liebiao/202506/content_7029689.htm"},
        {"date": "2025-04", "title_en": "Fair-competition review: implementation rules",
         "title_cn": "公平竞争审查条例实施办法 (总局令第99号, 2025-04-20施行)",
         "note": "48 articles; the 条例's 19 standards operationalised into 66 prohibited "
                 "situations for policy-drafting review.",
         "url": "https://www.gov.cn/gongbao/2025/issue_12006/202504/content_7021457.html"},
        {"date": "2024-08", "title_en": "Fair Competition Review Regulation in force",
         "title_cn": "公平竞争审查条例 (2024-08-01施行)",
         "note": "All policy measures affecting market activity must pass fair-competition "
                 "review before issuance — the base institution for Ch-17 §2.",
         "url": "http://xzfg.moj.gov.cn/front/law/detail?LawID=1725"},
    ],
    "infra": [
        {"date": "2025-11", "title_en": "Logistics data interconnection plan",
         "title_cn": "推动物流数据开放互联实施方案 (发改数据〔2025〕1387号)",
         "note": "Opens and links logistics data across modes/platforms to cut costs.",
         "url": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202511/t20251110_1401472.html"},
        {"date": "2024-11", "title_en": "Action plan on cutting whole-economy logistics costs",
         "title_cn": "中办国办《有效降低全社会物流成本行动方案》",
         "note": "Hard goal: logistics cost/GDP ratio down to ~13.5% by 2027 (2023: 14.4%).",
         "url": "https://www.gov.cn/zhengce/202411/content_6989622.htm"},
        {"date": "2023-08", "title_en": "Multimodal single-document/single-container opinions",
         "title_cn": "关于加快推进多式联运“一单制”“一箱制”发展的意见",
         "note": "8 ministries; predates the plan but is the operative instrument for its "
                 "一单制/一箱制 point — 2024: container multimodal volume +15.6%, rail–water +16.5%.",
         "url": "https://www.gov.cn/zhengce/zhengceku/202308/content_6899866.htm"},
    ],
}

# Concrete goals joined to data. history = [[period, value], ...] ascending.
# target_kind: min_level (reach at least), max_level (get below), trend (direction only, goal_dir).
TARGETS = [
    {"id": "retail_total", "sections": ["hub", "goods"],
     "name_en": "Total retail sales of consumer goods", "name_cn": "社会消费品零售总额",
     "baseline_period": "2025", "baseline": 50.12, "target": 60.0, "target_period": "2030",
     "target_kind": "min_level", "unit": "tn CNY", "binding": "hard goal (规划, ~60万亿)",
     "source_url": _XFGH_URL,
     "history": [["2021", 44.08], ["2022", 43.97], ["2023", 47.15], ["2024", 48.79], ["2025", 50.12]],
     "history_source": "NBS annual releases"},
    {"id": "hcons_rate", "sections": ["hub"],
     "name_en": "Household consumption share of GDP", "name_cn": "居民消费率",
     "baseline_period": "2024", "baseline": 41.0, "target": None, "target_period": "2030",
     "target_kind": "trend", "goal_dir": "up", "flat_eps": 0.3,
     "unit": "% of GDP", "binding": "qualitative (明显提高, 纲要第三章)",
     "source_url": DOC_URL, "series": "hcons_GDP",
     "history_source": "Global Macro Database (annual)"},
    {"id": "services_share", "sections": ["services"],
     "name_en": "Services share of household consumption spending", "name_cn": "人均服务性消费支出占比",
     "baseline_period": "2024", "baseline": 46.1, "target": None, "target_period": "2030",
     "target_kind": "trend", "goal_dir": "up", "flat_eps": 0.2,
     "unit": "% of consumption spending", "binding": "qualitative (稳步提高, 规划)",
     "source_url": _XFGH_URL,
     "history": [["2020", 42.6], ["2024", 46.1], ["2025", 46.1]],
     "history_source": "NBS residents' income & spending releases"},
    {"id": "priv_share", "sections": ["priv_inv"],
     "name_en": "Private share of fixed-asset investment", "name_cn": "民间投资比重",
     "baseline_period": "2024", "baseline": 50.1, "target": None, "target_period": "2030",
     "target_kind": "trend", "goal_dir": "up", "flat_eps": 0.2,
     "unit": "% of FAI", "binding": "qualitative (提高民间投资比重, 纲要第十六章)",
     "source_url": "https://www.stats.gov.cn/sj/zxfb/202601/t20260119_1962326.html",
     "history": [["2024", 50.1], ["2025", 49.7]],
     "history_source": "NBS FAI releases (2025 share computed: private FAI −6.4% vs total −3.8%)"},
    {"id": "logistics_ratio", "sections": ["infra"],
     "name_en": "Logistics cost as share of GDP", "name_cn": "社会物流总费用与GDP比率",
     "baseline_period": "2024", "baseline": 14.1, "target": 13.5, "target_period": "2027",
     "target_kind": "max_level", "unit": "% of GDP", "binding": "hard goal (行动方案, ~13.5%)",
     "source_url": "https://www.chinanews.com.cn/cj/2026/02-07/10567994.shtml",
     "history": [["2012", 18.0], ["2023", 14.4], ["2024", 14.1], ["2025", 13.9]],
     "history_source": "中国物流与采购联合会 annual readings"},
    {"id": "neg_list", "sections": ["base"],
     "name_en": "Market-access negative list: item count", "name_cn": "市场准入负面清单事项数",
     "baseline_period": "2018", "baseline": 151, "target": None, "target_period": "2030",
     "target_kind": "trend", "goal_dir": "down", "flat_eps": 2,
     "unit": "items", "binding": "institutional (动态修订, 纲要第十七章)",
     "source_url": "https://www.ndrc.gov.cn",
     "history": [["2018", 151], ["2022", 117], ["2025", 106]],
     "history_source": "NDRC negative-list editions (Q&A, 2025)"},
    {"id": "unemp_ctrl", "sections": ["cap"],
     "name_en": "Surveyed urban unemployment rate", "name_cn": "城镇调查失业率",
     "baseline_period": "2026-03", "baseline": 5.4, "target": 5.5, "target_period": "2026",
     "target_kind": "max_level", "unit": "%", "binding": "annual control target (~5.5%, 2026 GWR)",
     "source_url": "https://www.stats.gov.cn/sj/zxfb/202606/t20260616_1963954.html",
     "history": [["2026-03", 5.4], ["2026-05", 5.1], ["2026-06", 5.0]],
     "history_source": "NBS monthly releases"},
]

# Static status chips per section: verified one-off official readings.
FACTS = {
    "hub": [
        {"label": "Consumption contribution to growth, 14th-FYP avg", "value": "58.8%",
         "sub": "+10pp vs 13th FYP (NDRC/MOFCOM Q&A, 2026-07)"},
        {"label": "GDP 2025", "value": "+5.0%", "sub": "140.2 tn CNY, target met"},
    ],
    "cap": [
        {"label": "Income vs growth, 2025", "value": "+5.0% = +5.0%",
         "sub": "real disposable income grew in sync with GDP (NBS)"},
        {"label": "New urban jobs target 2026", "value": "12M+", "sub": "GWR 2026"},
    ],
    "services": [
        {"label": "Services spending growth 2020–25", "value": "8.5%/yr",
         "sub": "share up 3.5pp since 2020 — but flat at 46.1% in 2025"},
    ],
    "goods": [
        {"label": "Trade-in bond funding", "value": "150→300→250 bn",
         "sub": "CNY, 2024 → 2025 → 2026 (super-long special bonds)"},
    ],
    "env": [
        {"label": "Tax-refund shoppers, yr 1 of reform", "value": "+367%",
         "sub": "refunded sales +90% YoY (State Council briefing, 2026-04)"},
    ],
    "gov_inv": [
        {"label": "Policy-tool capital deployed", "value": "500 bn CNY",
         "sub": "2,300+ projects, ~7 tn total investment (by 2025-10-31)"},
        {"label": "2026 super-long bonds", "value": "1.3 tn CNY",
         "sub": "800 bn 两重 · 200 bn equipment · 250 bn trade-ins"},
    ],
    "priv_inv": [
        {"label": "Private FAI, 2025", "value": "−6.4%",
         "sub": "vs total FAI −3.8% — share still falling (NBS)"},
        {"label": "Nuclear openings", "value": "up to 20%",
         "sub": "private stakes in new reactors, ~20 firms (2024–25 approvals)"},
    ],
    "loop": [
        {"label": "Consumer-infrastructure funding", "value": "in 500 bn tool",
         "sub": "named priority of the new policy-based instrument (2025-10)"},
    ],
    "base": [
        {"label": "统一大市场建设条例", "value": "pending",
         "sub": "the plan's key Ch-17 deliverable — not yet enacted (2026-07)"},
        {"label": "Negative-list violations publicised", "value": "115 cases",
         "sub": "7 batches since 2018 (NDRC Q&A, 2025)"},
    ],
    "fair": [
        {"label": "Review standards operationalised", "value": "66 situations",
         "sub": "实施办法 (2025-04) turns 19 standards into 66 prohibitions"},
        {"label": "AUCL revision", "value": "in force 2025-10",
         "sub": "bans platform-forced below-cost pricing (anti-内卷)"},
    ],
    "infra": [
        {"label": "Container multimodal volume, 2024", "value": "+15.6%",
         "sub": "rail–water container transfer +16.5% (MOT)"},
    ],
}

# Live status series per section: (source_table, indicator) → monthly chart.
STATUS_SERIES = {
    "goods": [
        {"src": "BRU_Consumption_Retail_Sales_Total_Retail_Sales",
         "label": "Retail sales, % YoY", "color": "#e8a838"},
        {"src": "BRU_Consumption_Car_Sales_Weekly_Car_Sales_%YoY_16-week_moving_average",
         "label": "Car sales, % YoY (16-wk MA)", "color": "#4a9eff"},
    ],
    "services": [
        {"src": "BRU_Consumption_Box_Office_Daily_Box_Office_%_YoY_365-day_moving_average",
         "label": "Box office, % YoY (365-d MA)", "color": "#e8a838"},
        {"src": "BRU_Consumption_Flights_YoY",
         "label": "Flights operated, % YoY", "color": "#4a9eff"},
    ],
    "gov_inv": [
        {"src": "BRU_Investment_Fixed_Breakdown_Infra",
         "label": "FAI: infrastructure, % YoY ytd", "color": "#4a9eff"},
        {"src": "BRU_Investment_Fixed_Breakdown_Total",
         "label": "FAI: total, % YoY ytd", "color": "#9b6dff"},
    ],
    "priv_inv": [
        {"src": "BRU_Investment_Fixed_Breakdown_Total",
         "label": "FAI: total, % YoY ytd", "color": "#4a9eff"},
        {"src": "BRU_Investment_Fixed_Breakdown_Property",
         "label": "FAI: property, % YoY ytd", "color": "#d4483b"},
    ],
    "loop": [
        {"src": "BRU_Consumption_Retail_Sales_Total_Retail_Sales",
         "label": "Retail sales, % YoY", "color": "#e8a838"},
        {"src": "BRU_Investment_Fixed_Breakdown_Total",
         "label": "FAI: total, % YoY ytd", "color": "#4a9eff"},
    ],
    "fair": [
        {"src": "BRU_Inflation_CEPIHS2_China_PPI_YoY",
         "label": "PPI, % YoY (price-war symptom — proxy)", "color": "#d4483b"},
    ],
}

STATUS_CAPTIONS = {
    "goods": "Bruegel China dashboard, monthly — the trade-in impulse faded through H1 2026.",
    "services": "High-frequency service-consumption proxies (Bruegel, monthly last-obs).",
    "gov_inv": "NBS via Bruegel — infrastructure holds up while total FAI contracts.",
    "priv_inv": "Total and property FAI trends frame the private-investment squeeze.",
    "loop": "The loop itself: consumption and investment momentum, same axis.",
    "fair": "Persistent PPI deflation is the overcapacity/price-war symptom the campaign targets.",
}


def target_reading(t: dict, latest: dict | None) -> dict:
    """Classify a target vs its latest reading: on_track / off_track / mixed / met / n/a."""
    if not latest:
        return {"status": "n/a", "note": "no reading yet"}
    v, period = latest["value"], str(latest["period"])
    base_y, tgt_y = int(str(t["baseline_period"])[:4]), int(str(t["target_period"])[:4])
    kind = t["target_kind"]
    if kind == "trend":
        if period[:4] <= str(t["baseline_period"])[:4]:
            return {"status": "n/a",
                    "note": f"latest reading ({period}) is the baseline — no post-baseline data yet"}
        delta = v - t["baseline"]
        if abs(delta) <= t.get("flat_eps", 0):
            return {"status": "mixed",
                    "note": f"flat at {v:.4g} {t['unit']} since {t['baseline_period']} — goal is a clear "
                            + ("rise" if t["goal_dir"] == "up" else "fall")}
        good = (delta > 0) == (t["goal_dir"] == "up")
        word = "up" if delta > 0 else "down"
        return {"status": "on_track" if good else "off_track",
                "note": f"{word} {abs(delta):.4g} from {t['baseline']:g} ({t['baseline_period']}) "
                        f"to {v:.4g} ({period})"}
    # min_level / max_level with a numeric target
    if kind == "min_level":
        done, gap = v >= t["target"], (v - t["baseline"]) / (t["target"] - t["baseline"])
    else:
        done, gap = v <= t["target"], (t["baseline"] - v) / (t["baseline"] - t["target"])
    if done:
        return {"status": "met",
                "note": f"within target: {v:.4g} vs {t['target']:g} {t['unit']} ({period})"}
    year = int(period[:4])
    elapsed = 0.0 if tgt_y == base_y else max(0.0, min(1.0, (year - base_y) / (tgt_y - base_y)))
    if elapsed == 0:
        note = "baseline-year reading only — no in-plan data yet"
        if kind == "min_level" and t["baseline"] > 0 and tgt_y > base_y:
            pace = ((t["target"] / t["baseline"]) ** (1 / (tgt_y - base_y)) - 1) * 100
            note += f"; requires ≈{pace:.1f}%/yr through {t['target_period']}"
        return {"status": "n/a", "note": note}
    frac = gap / elapsed if elapsed else 0
    status = "on_track" if frac >= 0.8 else ("mixed" if frac >= 0.4 else "off_track")
    return {"status": status,
            "note": f"{max(0, gap) * 100:.0f}% of the gap closed with {elapsed * 100:.0f}% of "
                    f"{t['baseline_period']}→{t['target_period']} elapsed"}


def _monthly_last(rows: list[dict]) -> list[dict]:
    """Ascending [{date,value}] → one point per month (last observation)."""
    out: dict[str, dict] = {}
    for r in rows:
        out[r["date"][:7]] = {"date": r["date"][:7], "value": r["value"]}
    return [out[k] for k in sorted(out)]


def related_docs(conn: sqlite3.Connection, likes: list[str], limit: int = 6) -> list[dict]:
    """Latest full-text policy_docs whose title matches any keyword (url is UNIQUE)."""
    if not likes:
        return []
    where = " OR ".join("title LIKE ?" for _ in likes)
    rows = conn.execute(
        f"SELECT title, url, published, ministry, instrument_type FROM policy_docs "
        f"WHERE fetch_status='ok' AND ({where}) "
        f"ORDER BY published DESC LIMIT ?",
        [f"%{k}%" for k in likes] + [limit],
    ).fetchall()
    return [{"title": t, "url": u, "published": p, "ministry": m,
             "instrument_type": i} for t, u, p, m, i in rows]


def _resolve_targets(section_id: str, macro: dict) -> list[dict]:
    """Targets for a section, each joined with its history, latest and reading."""
    out = []
    for t in TARGETS:
        if section_id not in t["sections"]:
            continue
        if "series" in t:  # live GMD series
            hist = [{"period": str(r["year"]), "value": r["value"]}
                    for r in macro.get(t["series"], [])]
        else:
            hist = [{"period": p, "value": v} for p, v in t["history"]]
        latest = hist[-1] if hist else None
        out.append({**{k: v for k, v in t.items() if k not in ("history", "series")},
                    "history": hist, "latest": latest,
                    "reading": target_reading(t, latest)})
    return out


def _status_block(conn: sqlite3.Connection, section_id: str) -> dict | None:
    """Live monthly status series (Bruegel) for a section, trimmed and downsampled."""
    from backend.fetchers.bruegel import get_bruegel_series
    specs = STATUS_SERIES.get(section_id)
    if not specs:
        return None
    series = []
    for sp in specs:
        try:
            rows = sorted(get_bruegel_series(conn, sp["src"], limit=5000),
                          key=lambda r: r["date"])
        except sqlite3.OperationalError:   # bruegel_series not fetched yet
            rows = []
        rows = _monthly_last([r for r in rows
                              if r["date"] >= "2019-01" and r["value"] is not None])
        series.append({"label": sp["label"], "color": sp["color"], "data": rows})
    if not any(s["data"] for s in series):
        return None
    return {"caption": STATUS_CAPTIONS.get(section_id, ""), "series": series}


def build_payload(conn: sqlite3.Connection) -> dict:
    """Assemble the /api/fyp/demand response from the registry + cmm.db."""
    from backend.fetchers.macro import get_macro_series

    outline = conn.execute(
        "SELECT published, text_len FROM policy_docs WHERE url = ?", (DOC_URL,)
    ).fetchone()
    # end_year caps off the GMD's post-2025 projections so all lines stop
    # at observed data (hcons_GDP ends 2024).
    macro = {v: get_macro_series(conn, v, start_year=1980, end_year=2025)
             for v in SERIES_VARS}
    return {
        "hub": {**{k: v for k, v in HUB.items() if k != "doc_like"},
                "docs": related_docs(conn, HUB["doc_like"]),
                "milestones": MILESTONES.get("hub", []),
                "targets": _resolve_targets("hub", macro),
                "facts": FACTS.get("hub", [])},
        "chapters": CHAPTERS,
        "sections": [
            {**{k: v for k, v in s.items() if k != "doc_like"},
             "docs": related_docs(conn, s["doc_like"]),
             "milestones": MILESTONES.get(s["id"], []),
             "targets": _resolve_targets(s["id"], macro),
             "facts": FACTS.get(s["id"], []),
             "status": _status_block(conn, s["id"])}
            for s in SECTIONS
        ],
        "series": macro,
        "doc_url": DOC_URL,
        "doc_published": outline[0] if outline else None,
        "doc_text_len": outline[1] if outline else None,
    }
