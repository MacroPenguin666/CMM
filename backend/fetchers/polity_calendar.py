"""
China Political Calendar — curated registry of Party and State calendar fixtures.

No live public feed exists for China's political calendar, so this is a curated,
manually-maintained list (like fiscal_reference / PSC membership) rather than an
auto-refreshed fetcher. Every dated entry is sourced (see `source` field); entries
without an officially announced date are marked `confirmed: False` and carry a
`date_note` explaining the projection basis instead of a hard date.

category: "party" (CCP internal organs — Congress, Plenum, Politburo, CCDI) or
          "state" (government/legislative — NPC, CPPCC, State Council/CEWC).
"""

from datetime import date

EVENTS = [
    # --- Party Congresses (every 5 years) ---
    {
        "id": "congress-17", "category": "party", "type": "congress",
        "name": "17th National Congress", "name_cn": "第十七次全国代表大会",
        "date_start": "2007-10-15", "date_end": "2007-10-21", "confirmed": True,
        "summary": "Elected Hu Jintao to a second term as General Secretary.",
        "source": "https://en.wikipedia.org/wiki/17th_National_Congress_of_the_Chinese_Communist_Party",
    },
    {
        "id": "congress-18", "category": "party", "type": "congress",
        "name": "18th National Congress", "name_cn": "第十八次全国代表大会",
        "date_start": "2012-11-08", "date_end": "2012-11-14", "confirmed": True,
        "summary": "Xi Jinping elected General Secretary; leadership transition from Hu Jintao.",
        "source": "https://en.wikipedia.org/wiki/18th_National_Congress_of_the_Chinese_Communist_Party",
    },
    {
        "id": "congress-19", "category": "party", "type": "congress",
        "name": "19th National Congress", "name_cn": "第十九次全国代表大会",
        "date_start": "2017-10-18", "date_end": "2017-10-24", "confirmed": True,
        "summary": "Xi Jinping Thought written into the Party constitution; Xi's second term begins.",
        "source": "https://en.wikipedia.org/wiki/19th_National_Congress_of_the_Chinese_Communist_Party",
    },
    {
        "id": "congress-20", "category": "party", "type": "congress",
        "name": "20th National Congress", "name_cn": "第二十次全国代表大会",
        "date_start": "2022-10-16", "date_end": "2022-10-22", "confirmed": True,
        "summary": "Xi Jinping elected to an unprecedented third term as General Secretary.",
        "source": "https://english.www.gov.cn/news/topnews/202210/16/content_WS634b6e01c6d0a757729e142e_5.html",
    },
    {
        "id": "congress-21", "category": "party", "type": "congress",
        "name": "21st National Congress", "name_cn": "第二十一次全国代表大会",
        "confirmed": False, "date_note": "Expected 2027 (5-year cadence); no month/day announced yet.",
        "summary": "Next quinquennial Party Congress — will set the leadership slate for 2027–2032.",
        "source": "https://en.wikipedia.org/wiki/21st_National_Congress_of_the_Chinese_Communist_Party",
    },

    # --- 20th Central Committee Plenums ---
    {
        "id": "plenum-20-1", "category": "party", "type": "plenum",
        "name": "1st Plenum, 20th CC", "name_cn": "二十届一中全会",
        "date_start": "2022-10-22", "date_end": "2022-10-23", "confirmed": True,
        "summary": "Elected the Politburo, Standing Committee, and Xi Jinping as General Secretary.",
        "source": "https://en.wikipedia.org/wiki/1st_plenary_session_of_the_20th_Central_Committee_of_the_Chinese_Communist_Party",
    },
    {
        "id": "plenum-20-2", "category": "party", "type": "plenum",
        "name": "2nd Plenum, 20th CC", "name_cn": "二十届二中全会",
        "date_start": "2023-02-26", "date_end": "2023-02-28", "confirmed": True,
        "summary": "Adopted the Party/state institutional reform plan submitted to the March 2023 NPC.",
        "source": "https://news.cgtn.com/news/2023-03-01/Commuique-of-the-2nd-plenary-session-of-the-20th-CPC-Central-Committee-1hPeH9O8E7K/index.html",
    },
    {
        "id": "plenum-20-3", "category": "party", "type": "plenum",
        "name": "3rd Plenum, 20th CC", "name_cn": "二十届三中全会",
        "date_start": "2024-07-15", "date_end": "2024-07-18", "confirmed": True,
        "summary": "Adopted the \"Resolution on Further Deepening Reform Comprehensively to Advance Chinese "
                   "Modernization\" — economic reform roadmap to 2029, incl. retirement-age reform.",
        "source": "http://en.cppcc.gov.cn/2024-07/19/c_1006186.htm",
    },
    {
        "id": "plenum-20-4", "category": "party", "type": "plenum",
        "name": "4th Plenum, 20th CC", "name_cn": "二十届四中全会",
        "date_start": "2025-10-20", "date_end": "2025-10-23", "confirmed": True,
        "summary": "Adopted the CC's Recommendations for the 15th Five-Year Plan (2026–2030).",
        "source": "https://www.fmprc.gov.cn/eng/xw/zyxw/202510/t20251023_11739505.html",
    },
    {
        "id": "plenum-20-5", "category": "party", "type": "plenum",
        "name": "5th Plenum, 20th CC", "name_cn": "二十届五中全会",
        "confirmed": False,
        "date_note": "Unscheduled — not yet announced. Historical pattern (~7 plenums per term, "
                     "final one shortly before the next Congress) suggests further plenums before 2027.",
        "summary": "Next Central Committee plenum — no confirmed date or agenda.",
        "source": "https://en.wikipedia.org/wiki/20th_Central_Committee_of_the_Chinese_Communist_Party",
    },

    # --- CCDI Plenary Sessions (annual, January) ---
    {
        "id": "ccdi-20-3", "category": "party", "type": "ccdi",
        "name": "3rd Plenary Session, 20th CCDI", "name_cn": "二十届中央纪委三次全会",
        "date_start": "2024-01-08", "date_end": "2024-01-10", "confirmed": True,
        "summary": "Annual discipline-inspection plenary; set anti-corruption priorities for 2024.",
        "source": "https://news.cgtn.com/news/2024-01-10/CCDI-plenary-session-adopts-communique-1qfMsnVShYQ/p.html",
    },
    {
        "id": "ccdi-20-4", "category": "party", "type": "ccdi",
        "name": "4th Plenary Session, 20th CCDI", "name_cn": "二十届中央纪委四次全会",
        "date_start": "2025-01-06", "date_end": "2025-01-08", "confirmed": True,
        "summary": "Annual discipline-inspection plenary; set anti-corruption priorities for 2025.",
        "source": "https://english.www.gov.cn/",
    },
    {
        "id": "ccdi-20-5", "category": "party", "type": "ccdi",
        "name": "5th Plenary Session, 20th CCDI", "name_cn": "二十届中央纪委五次全会",
        "date_start": "2026-01-12", "date_end": "2026-01-14", "confirmed": True,
        "summary": "Annual discipline-inspection plenary; set anti-corruption priorities for 2026.",
        "source": "https://english.www.gov.cn/news/202601/14/content_WS69675b7dc6d00ca5f9a0890d.html",
    },
    {
        "id": "ccdi-20-6", "category": "party", "type": "ccdi",
        "name": "6th Plenary Session, 20th CCDI", "name_cn": "二十届中央纪委六次全会",
        "date_start": "2027-01-10", "confirmed": False,
        "date_note": "Estimated from the annual January cadence (prior sessions: Jan 6–14); not yet announced.",
        "summary": "Next annual discipline-inspection plenary (estimated date).",
        "source": "https://en.wikipedia.org/wiki/Central_Commission_for_Discipline_Inspection",
    },

    # --- Politburo quarterly economic-situation meetings (confirmed cadence: Apr / Jul / Dec) ---
    {
        "id": "pb-econ-2026-04", "category": "party", "type": "politburo",
        "name": "Politburo meeting — Q1 economic review", "name_cn": "中央政治局会议",
        "date_start": "2026-04-24", "date_end": "2026-04-24", "confirmed": True,
        "summary": "Quarterly Politburo meeting analyzing the economic situation.",
        "source": "https://triviumchina.com/2025/07/15/on-our-radar-when-the-politburo-speaks/",
    },
    {
        "id": "pb-econ-2026-07", "category": "party", "type": "politburo",
        "name": "Politburo meeting — H1 economic review", "name_cn": "中央政治局会议",
        "date_start": "2026-07-25", "confirmed": False,
        "date_note": "Estimated from the standing Apr/Jul/Dec cadence (Apr 2026 session was Apr 24); not yet announced.",
        "summary": "Quarterly Politburo meeting analyzing the economic situation (estimated date).",
        "source": "https://triviumchina.com/2025/07/15/on-our-radar-when-the-politburo-speaks/",
    },
    {
        "id": "pb-econ-2026-12", "category": "party", "type": "politburo",
        "name": "Politburo meeting — year-end economic review", "name_cn": "中央政治局会议",
        "date_start": "2026-12-03", "confirmed": False,
        "date_note": "Estimated — precedes the Central Economic Work Conference by about a week; not yet announced.",
        "summary": "Quarterly Politburo meeting analyzing the economic situation, precedes CEWC (estimated date).",
        "source": "https://sinocism.com/p/december-politburo-meeting-and-the",
    },

    # --- Two Sessions (NPC + CPPCC, annual, early March) ---
    {
        "id": "two-sessions-2023", "category": "state", "type": "two_sessions",
        "name": "Two Sessions 2023", "name_cn": "2023年全国两会",
        "date_start": "2023-03-04", "date_end": "2023-03-13", "confirmed": True,
        "summary": "CPPCC + NPC annual sessions; Li Qiang confirmed as Premier.",
        "source": "https://npcobserver.com/2023/03/04/npc-2023-agenda-and-daily-schedule/",
    },
    {
        "id": "two-sessions-2024", "category": "state", "type": "two_sessions",
        "name": "Two Sessions 2024", "name_cn": "2024年全国两会",
        "date_start": "2024-03-04", "date_end": "2024-03-11", "confirmed": True,
        "summary": "CPPCC + NPC annual sessions; ~5% GDP growth target set.",
        "source": "https://npcobserver.com/2024/03/04/china-npc-2024-agenda-daily-schedule/",
    },
    {
        "id": "two-sessions-2025", "category": "state", "type": "two_sessions",
        "name": "Two Sessions 2025", "name_cn": "2025年全国两会",
        "date_start": "2025-03-04", "date_end": "2025-03-11", "confirmed": True,
        "summary": "CPPCC + NPC annual sessions.",
        "source": "https://npcobserver.com/2025/03/04/china-npc-2025-agenda-daily-schedule/",
    },
    {
        "id": "two-sessions-2026", "category": "state", "type": "two_sessions",
        "name": "Two Sessions 2026", "name_cn": "2026年全国两会",
        "date_start": "2026-03-04", "date_end": "2026-03-12", "confirmed": True,
        "summary": "CPPCC + NPC annual sessions; adopted the 15th Five-Year Plan outline.",
        "source": "https://npcobserver.com/2026/03/04/china-npc-2026-agenda-daily-schedule/",
    },
    {
        "id": "two-sessions-2027", "category": "state", "type": "two_sessions",
        "name": "Two Sessions 2027", "name_cn": "2027年全国两会",
        "date_start": "2027-03-04", "confirmed": False,
        "date_note": "Estimated from the recurring CPPCC Mar 4 / NPC Mar 5 pattern; not yet announced.",
        "summary": "Next annual CPPCC + NPC sessions (estimated date).",
        "source": "https://npcobserver.com/",
    },

    # --- Central Economic Work Conference (annual, mid-December) ---
    {
        "id": "cewc-2023", "category": "state", "type": "cewc",
        "name": "Central Economic Work Conference 2023", "name_cn": "2023年中央经济工作会议",
        "date_start": "2023-12-11", "date_end": "2023-12-12", "confirmed": True,
        "summary": "Set the economic policy agenda for 2024.",
        "source": "https://english.www.gov.cn/news/202312/12/content_WS657860aec6d0868f4e8e21c2.html",
    },
    {
        "id": "cewc-2024", "category": "state", "type": "cewc",
        "name": "Central Economic Work Conference 2024", "name_cn": "2024年中央经济工作会议",
        "date_start": "2024-12-11", "date_end": "2024-12-12", "confirmed": True,
        "summary": "Set the economic policy agenda for 2025.",
        "source": "http://english.www.gov.cn/news/202412/12/content_WS675ae633c6d0868f4e8ede69.html",
    },
    {
        "id": "cewc-2025", "category": "state", "type": "cewc",
        "name": "Central Economic Work Conference 2025", "name_cn": "2025年中央经济工作会议",
        "date_start": "2025-12-10", "date_end": "2025-12-11", "confirmed": True,
        "summary": "Set the economic policy agenda for 2026, first year of the 15th FYP.",
        "source": "https://english.www.gov.cn/news/202512/11/content_WS693a9c0dc6d00ca5f9a08098.html",
    },
    {
        "id": "cewc-2026", "category": "state", "type": "cewc",
        "name": "Central Economic Work Conference 2026", "name_cn": "2026年中央经济工作会议",
        "date_start": "2026-12-10", "confirmed": False,
        "date_note": "Estimated from the recurring Dec 10–12 pattern; not yet announced.",
        "summary": "Next annual economic policy agenda-setting conference (estimated date).",
        "source": "https://www.china-briefing.com/news/2025-central-economic-work-conference/",
    },
]

# Event-type display metadata: color + short label, keyed by `type`.
TYPE_META = {
    "congress":     {"label": "Party Congress",       "color": "#d4483b"},
    "plenum":       {"label": "CC Plenum",             "color": "#e8a838"},
    "ccdi":         {"label": "CCDI Plenary",          "color": "#9d7cf4"},
    "politburo":    {"label": "Politburo Meeting",     "color": "#c47e1a"},
    "two_sessions": {"label": "Two Sessions",          "color": "#4a9eff"},
    "cewc":         {"label": "Central Econ. Work Conf.", "color": "#3eb370"},
}


def _sort_key(ev: dict) -> str:
    return ev.get("date_start") or "9999-99-99"


def get_calendar_data(today: str | None = None) -> dict:
    """Return the full curated calendar, split into past/upcoming relative to `today`."""
    today = today or date.today().isoformat()
    events = sorted(EVENTS, key=_sort_key)

    past, upcoming, unscheduled = [], [], []
    for ev in events:
        e = {**ev, "type_label": TYPE_META.get(ev["type"], {}).get("label", ev["type"]),
             "type_color": TYPE_META.get(ev["type"], {}).get("color", "#888")}
        if not e.get("date_start"):
            unscheduled.append(e)
        elif e["date_start"] > today:
            upcoming.append(e)
        else:
            past.append(e)

    past.sort(key=_sort_key, reverse=True)
    upcoming.sort(key=_sort_key)

    return {
        "today": today,
        "past": past,
        "upcoming": upcoming,
        "unscheduled": unscheduled,
        "next": upcoming[0] if upcoming else None,
        "type_meta": TYPE_META,
    }
