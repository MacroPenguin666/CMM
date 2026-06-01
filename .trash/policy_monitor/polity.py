"""
China Political Structure — hierarchy, meeting calendar, decision-making process.
Static data + live Xinhua scraper for recent meeting announcements.
"""

import logging
import re
from datetime import date
from urllib.parse import urljoin

import requests
from lxml import html as lhtml

log = logging.getLogger("polity")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# Political Hierarchy
# ---------------------------------------------------------------------------

STRUCTURE = {
    "id": "prc",
    "name": "People's Republic of China",
    "name_cn": "中华人民共和国",
    "type": "root",
    "desc": "Governed by the Chinese Communist Party as the sole ruling party since 1949.",
    "children": [
        {
            "id": "ccp",
            "name": "Chinese Communist Party (CCP)",
            "name_cn": "中国共产党",
            "type": "party",
            "desc": "Ruling party — holds supreme political authority over state and military. ~98 million members.",
            "children": [
                {
                    "id": "npc_ccp",
                    "name": "National Party Congress",
                    "name_cn": "全国代表大会",
                    "type": "congress",
                    "desc": "Highest party organ, ~2,300 delegates. Meets every 5 years. Elects the Central Committee.",
                    "meeting_freq": "Every 5 years",
                    "children": [
                        {
                            "id": "cc",
                            "name": "Central Committee (CC)",
                            "name_cn": "中央委员会",
                            "type": "committee",
                            "desc": "~205 full members + ~171 alternates. Meets in Plenums (~1/year). Elects Politburo and PSC.",
                            "meeting_freq": "~1 Plenum per year",
                            "children": [
                                {
                                    "id": "psc",
                                    "name": "Politburo Standing Committee (PSC)",
                                    "name_cn": "政治局常委会",
                                    "type": "psc",
                                    "desc": "7 members — the supreme decision-making body of China. Meets weekly.",
                                    "meeting_freq": "Weekly",
                                    "members": [
                                        {"rank": 1, "name": "Xi Jinping", "name_cn": "习近平",
                                         "roles": ["General Secretary", "President", "CMC Chairman"]},
                                        {"rank": 2, "name": "Li Qiang", "name_cn": "李强",
                                         "roles": ["Premier of State Council"]},
                                        {"rank": 3, "name": "Zhao Leji", "name_cn": "赵乐际",
                                         "roles": ["Chairman, NPC Standing Committee"]},
                                        {"rank": 4, "name": "Wang Huning", "name_cn": "王沪宁",
                                         "roles": ["Chairman, CPPCC"]},
                                        {"rank": 5, "name": "Cai Qi", "name_cn": "蔡奇",
                                         "roles": ["Director, CCP Central Secretariat"]},
                                        {"rank": 6, "name": "Ding Xuexiang", "name_cn": "丁薛祥",
                                         "roles": ["Executive Vice Premier"]},
                                        {"rank": 7, "name": "Li Xi", "name_cn": "李希",
                                         "roles": ["Secretary, CCDI"]},
                                    ],
                                    "children": [],
                                },
                                {
                                    "id": "politburo",
                                    "name": "Politburo",
                                    "name_cn": "政治局",
                                    "type": "politburo",
                                    "desc": "24 members (incl. PSC). Sets major policy direction. Meets roughly monthly.",
                                    "meeting_freq": "~Monthly",
                                    "children": [],
                                },
                                {
                                    "id": "cmc",
                                    "name": "Central Military Commission (CMC)",
                                    "name_cn": "中央军事委员会",
                                    "type": "military_cmd",
                                    "desc": "Controls the PLA. Chaired by Xi Jinping. Party CMC = State CMC in practice.",
                                    "members_note": "Chairman: Xi Jinping. Vice Chairmen: Zhang Youxia, He Weidong.",
                                    "children": [],
                                },
                                {
                                    "id": "ccdi",
                                    "name": "CCDI",
                                    "name_cn": "中央纪律检查委员会",
                                    "type": "discipline",
                                    "desc": "Central Commission for Discipline Inspection. Anti-corruption body. Secretary: Li Xi (PSC #7).",
                                    "children": [],
                                },
                                {
                                    "id": "secretariat",
                                    "name": "Central Secretariat",
                                    "name_cn": "中央书记处",
                                    "type": "secretariat",
                                    "desc": "Day-to-day party administration. Director: Cai Qi (PSC #5).",
                                    "children": [],
                                },
                            ],
                        },
                    ],
                },
            ],
        },
        {
            "id": "state",
            "name": "State (Government)",
            "name_cn": "国家机构",
            "type": "state_gov",
            "desc": "Formal state apparatus — in practice subordinate to the CCP.",
            "children": [
                {
                    "id": "npc",
                    "name": "National People's Congress (NPC)",
                    "name_cn": "全国人民代表大会",
                    "type": "legislature",
                    "desc": "Nominal highest state organ. ~3,000 delegates. Annual plenary in March. Largely rubber-stamps party decisions.",
                    "meeting_freq": "Annual plenary (March) + SC sessions every 2 months",
                    "children": [
                        {
                            "id": "npc_sc",
                            "name": "NPC Standing Committee",
                            "name_cn": "全国人大常委会",
                            "type": "committee",
                            "desc": "~170 members. Legislates between NPC plenaries. Chairman: Zhao Leji (PSC #3).",
                            "meeting_freq": "Every 2 months",
                            "children": [],
                        },
                    ],
                },
                {
                    "id": "state_council",
                    "name": "State Council",
                    "name_cn": "国务院",
                    "type": "cabinet",
                    "desc": "Executive branch / cabinet. Premier: Li Qiang. Implements party policy through 26 ministries and agencies.",
                    "children": [
                        {
                            "id": "premier",
                            "name": "Premier — Li Qiang",
                            "name_cn": "国务院总理",
                            "type": "official",
                            "desc": "Heads the State Council. Chairs executive meetings. Appointed March 2023.",
                            "children": [],
                        },
                        {
                            "id": "vice_premiers",
                            "name": "Vice Premiers (4)",
                            "name_cn": "副总理",
                            "type": "officials",
                            "desc": "Ding Xuexiang (PSC #6, Executive VP), He Lifeng (finance/economy), Liu Guozhong, Li Shulei.",
                            "children": [],
                        },
                        {
                            "id": "ministries",
                            "name": "Ministries & Agencies (26+)",
                            "name_cn": "部委机构",
                            "type": "ministries",
                            "desc": "Incl. Finance (MOF), Commerce (MOFCOM), Foreign Affairs (MFA), NDRC, MIIT, SAMR, PBOC, etc.",
                            "children": [],
                        },
                    ],
                },
                {
                    "id": "cppcc",
                    "name": "CPPCC",
                    "name_cn": "中国人民政治协商会议",
                    "type": "advisory",
                    "desc": "Chinese People's Political Consultative Conference. Advisory/consultative body, not legislative. Chairman: Wang Huning (PSC #4).",
                    "meeting_freq": "Annual (March, precedes NPC)",
                    "children": [],
                },
                {
                    "id": "court",
                    "name": "Supreme People's Court",
                    "name_cn": "最高人民法院",
                    "type": "judiciary",
                    "desc": "Highest judicial body. Operates under party supervision; judicial independence is limited.",
                    "children": [],
                },
                {
                    "id": "procuratorate",
                    "name": "Supreme People's Procuratorate",
                    "name_cn": "最高人民检察院",
                    "type": "judiciary",
                    "desc": "National prosecution authority. Coordinates with CCDI on corruption and discipline cases.",
                    "children": [],
                },
            ],
        },
        {
            "id": "pla",
            "name": "People's Liberation Army (PLA)",
            "name_cn": "中国人民解放军",
            "type": "military",
            "desc": "Party army — loyal to CCP, not the state. ~2 million active personnel. Commanded by the CMC, not the State Council.",
            "children": [
                {"id": "pla_gf", "name": "PLA Ground Force", "name_cn": "陆军", "type": "service",
                 "desc": "Largest branch by personnel.", "children": []},
                {"id": "plan", "name": "PLA Navy (PLAN)", "name_cn": "海军", "type": "service",
                 "desc": "World's largest navy by hull count.", "children": []},
                {"id": "plaaf", "name": "PLA Air Force (PLAAF)", "name_cn": "空军", "type": "service",
                 "desc": "3rd largest air force globally.", "children": []},
                {"id": "plarf", "name": "PLA Rocket Force (PLARF)", "name_cn": "火箭军", "type": "service",
                 "desc": "Nuclear and conventional ballistic/cruise missiles.", "children": []},
                {"id": "ssf", "name": "Strategic Support Force", "name_cn": "战略支援部队", "type": "service",
                 "desc": "Space, cyber, electronic warfare, and psychological operations.", "children": []},
            ],
        },
    ],
}

# ---------------------------------------------------------------------------
# Meeting Calendar
# ---------------------------------------------------------------------------

CALENDAR = [
    # ---- Party Congresses ----
    {"id": "pc19", "type": "party_congress", "name": "19th Party Congress",
     "name_cn": "十九大", "start": "2017-10-18", "end": "2017-10-24",
     "significance": "Xi Jinping re-elected; 'Xi Jinping Thought' enshrined in CCP constitution", "source": "static"},
    {"id": "pc20", "type": "party_congress", "name": "20th Party Congress",
     "name_cn": "二十大", "start": "2022-10-16", "end": "2022-10-22",
     "significance": "Xi Jinping begins unprecedented 3rd term as General Secretary; new PSC elected", "source": "static"},
    {"id": "pc21", "type": "party_congress", "name": "21st Party Congress (expected)",
     "name_cn": "二十一大", "start": "2027-10-01", "end": None, "approximate": True,
     "significance": "Leadership transition due; Xi's continuation beyond 2027 is the central question", "source": "static"},

    # ---- Central Committee Plenums (20th CC) ----
    {"id": "plenum20_1", "type": "plenum", "name": "20th CC 1st Plenum",
     "name_cn": "二十届一中全会", "start": "2022-10-22", "end": "2022-10-23",
     "significance": "Elected new Politburo, PSC, and CMC leadership", "source": "static"},
    {"id": "plenum20_2", "type": "plenum", "name": "20th CC 2nd Plenum",
     "name_cn": "二十届二中全会", "start": "2023-02-26", "end": "2023-02-28",
     "significance": "Approved State Council restructuring; nominated Xi Jinping for 3rd presidential term", "source": "static"},
    {"id": "plenum20_3", "type": "plenum", "name": "20th CC 3rd Plenum",
     "name_cn": "二十届三中全会", "start": "2024-07-15", "end": "2024-07-18",
     "significance": "Major reform package — 'further deepening reform comprehensively'; economic restructuring mandate", "source": "static"},
    {"id": "plenum20_4", "type": "plenum", "name": "20th CC 4th Plenum (expected)",
     "name_cn": "二十届四中全会", "start": "2025-10-01", "end": None, "approximate": True,
     "significance": "Expected governance / rule of law focus", "source": "static"},
    {"id": "plenum20_5", "type": "plenum", "name": "20th CC 5th Plenum (expected)",
     "name_cn": "二十届五中全会", "start": "2026-10-01", "end": None, "approximate": True,
     "significance": "Expected to adopt 15th Five-Year Plan (2026–2030)", "source": "static"},
    {"id": "plenum20_6", "type": "plenum", "name": "20th CC 6th Plenum (expected)",
     "name_cn": "二十届六中全会", "start": "2027-02-01", "end": None, "approximate": True,
     "significance": "Preparatory plenum before 21st Party Congress", "source": "static"},

    # ---- NPC Plenaries (14th NPC) ----
    {"id": "npc14_1", "type": "npc", "name": "14th NPC 1st Session",
     "name_cn": "十四届全国人大一次会议", "start": "2023-03-05", "end": "2023-03-13",
     "significance": "Xi elected for 3rd presidential term; Li Qiang confirmed as Premier; State Council restructured", "source": "static"},
    {"id": "npc14_2", "type": "npc", "name": "14th NPC 2nd Session",
     "name_cn": "十四届全国人大二次会议", "start": "2024-03-05", "end": "2024-03-11",
     "significance": "GDP growth target set at ~5%; fiscal stimulus discussed; tech self-reliance budget increases", "source": "static"},
    {"id": "npc14_3", "type": "npc", "name": "14th NPC 3rd Session",
     "name_cn": "十四届全国人大三次会议", "start": "2025-03-05", "end": "2025-03-11",
     "significance": "Annual budget and government work report for 2025", "source": "static"},
    {"id": "npc14_4", "type": "npc", "name": "14th NPC 4th Session (expected)",
     "name_cn": "十四届全国人大四次会议", "start": "2026-03-05", "end": "2026-03-11", "approximate": True,
     "significance": "Annual session — budget, government work report, 15th FYP discussion", "source": "static"},

    # ---- CPPCC Plenaries (14th CPPCC) ----
    {"id": "cppcc14_1", "type": "cppcc", "name": "14th CPPCC 1st Session",
     "name_cn": "十四届全国政协一次会议", "start": "2023-03-04", "end": "2023-03-11",
     "significance": "New CPPCC leadership; Wang Huning elected Chairman", "source": "static"},
    {"id": "cppcc14_2", "type": "cppcc", "name": "14th CPPCC 2nd Session",
     "name_cn": "十四届全国政协二次会议", "start": "2024-03-04", "end": "2024-03-10",
     "significance": "Annual consultative session 2024", "source": "static"},
    {"id": "cppcc14_3", "type": "cppcc", "name": "14th CPPCC 3rd Session",
     "name_cn": "十四届全国政协三次会议", "start": "2025-03-04", "end": "2025-03-10",
     "significance": "Annual consultative session 2025", "source": "static"},
    {"id": "cppcc14_4", "type": "cppcc", "name": "14th CPPCC 4th Session (expected)",
     "name_cn": "十四届全国政协四次会议", "start": "2026-03-03", "end": "2026-03-09", "approximate": True,
     "significance": "Expected March 2026 — precedes NPC session", "source": "static"},

    # ---- Central Economic Work Conference ----
    {"id": "cewc22", "type": "cewc", "name": "CEWC 2022",
     "name_cn": "2022年中央经济工作会议", "start": "2022-12-15", "end": "2022-12-16",
     "significance": "Post-COVID reopening pivot; economic recovery set as top priority", "source": "static"},
    {"id": "cewc23", "type": "cewc", "name": "CEWC 2023",
     "name_cn": "2023年中央经济工作会议", "start": "2023-12-11", "end": "2023-12-12",
     "significance": "Fiscal expansion; tech self-sufficiency; housing market stabilization", "source": "static"},
    {"id": "cewc24", "type": "cewc", "name": "CEWC 2024",
     "name_cn": "2024年中央经济工作会议", "start": "2024-12-11", "end": "2024-12-12",
     "significance": "Consumption stimulus push; AI + advanced manufacturing; ~5% growth target confirmed", "source": "static"},
    {"id": "cewc25", "type": "cewc", "name": "CEWC 2025 (expected)",
     "name_cn": "2025年中央经济工作会议", "start": "2025-12-10", "end": None, "approximate": True,
     "significance": "Expected December 2025 — will set 2026 economic priorities", "source": "static"},

    # ---- Politburo (recurring pattern) ----
    {"id": "pb_monthly", "type": "politburo_regular", "name": "Politburo Meetings (monthly)",
     "name_cn": "政治局会议", "start": None, "end": None, "recurring": True,
     "significance": "~Monthly. Major policy decisions, economic reviews, study sessions on key topics.", "source": "static"},
    {"id": "psc_weekly", "type": "psc_regular", "name": "PSC Meetings (weekly)",
     "name_cn": "政治局常委会会议", "start": None, "end": None, "recurring": True,
     "significance": "Weekly (not publicly announced). Supreme decision-making forum.", "source": "static"},
]

# ---------------------------------------------------------------------------
# Decision-Making Process
# ---------------------------------------------------------------------------

DECISION_PROCESS = [
    {
        "step": 1,
        "name": "PSC Deliberation",
        "body": "Politburo Standing Committee",
        "body_cn": "政治局常委会",
        "type": "psc",
        "desc": (
            "The 7-member PSC sets strategic direction. Meetings are closed and results "
            "only announced afterward. Decisions are formally consensus-based, but Xi Jinping's "
            "authority is dominant. This is where China's most consequential choices are made."
        ),
        "examples": ["Pandemic policy (zero-COVID to reopening)", "Major diplomatic stances", "Leadership personnel decisions"],
    },
    {
        "step": 2,
        "name": "Politburo Endorsement",
        "body": "Politburo",
        "body_cn": "政治局",
        "type": "politburo",
        "desc": (
            "The full 24-member Politburo is briefed and formally endorses major decisions. "
            "Monthly meetings often include 'collective study sessions' on key themes, building "
            "political consensus across the wider leadership tier."
        ),
        "examples": ["Economic policy frameworks", "Five-Year Plan priorities", "External relations strategy"],
    },
    {
        "step": 3,
        "name": "Party Document / Directive",
        "body": "Central Committee / Secretariat",
        "body_cn": "中央委员会 / 中央书记处",
        "type": "committee",
        "desc": (
            "Decisions are formalized as Party documents — 'Decisions', 'Opinions', or 'Circulars'. "
            "These carry more binding authority than state law. CC Plenums adopt major 'Decisions' "
            "that set policy for years. The Secretariat drafts and circulates documents within the party system."
        ),
        "examples": ["CC Plenum 'Decisions'", "Joint 'Opinions on...' documents", "Central No.1 Documents (annual rural policy)"],
    },
    {
        "step": 4,
        "name": "State Legislation / Regulation",
        "body": "State Council / NPC",
        "body_cn": "国务院 / 全国人民代表大会",
        "type": "legislature",
        "desc": (
            "The State Council translates party directives into administrative regulations or submits "
            "legislation to the NPC. The NPC rubber-stamps proposals with near-unanimous votes. "
            "The NPC Standing Committee handles routine legislation between annual sessions."
        ),
        "examples": ["State Council executive meeting decisions", "NPC plenary laws", "Administrative regulations and 'opinions'"],
    },
    {
        "step": 5,
        "name": "Ministry / Local Implementation",
        "body": "Ministries, Provincial Governments",
        "body_cn": "各部委、地方政府",
        "type": "ministries",
        "desc": (
            "Line ministries and provincial governments implement policy. Local leaders retain discretion "
            "in speed and emphasis, creating variation across regions. Performance metrics tied to party "
            "promotion incentives drive compliance. Key ministries: NDRC, MOF, MIIT, MOFCOM, PBOC."
        ),
        "examples": ["NDRC implementing industrial policy", "Provincial GDP targets", "SAMR/CSRC regulatory enforcement"],
    },
    {
        "step": 6,
        "name": "Feedback & Adjustment",
        "body": "Internal reporting, CCDI, media",
        "body_cn": "内部反馈渠道",
        "type": "feedback",
        "desc": (
            "Policy outcomes feed back via internal party reports, local government statistics, CCDI "
            "inspection tours, and state media signals. Reversals are rare; adjustments are framed as "
            "'refinements'. Xinhua and People's Daily editorials often signal upcoming policy shifts before "
            "formal announcements."
        ),
        "examples": ["Internal 'neibu' reports to leadership", "CCDI central inspection tours", "Xinhua editorial signals"],
    },
]

# ---------------------------------------------------------------------------
# Meeting news scraper — Xinhua English politics page
# ---------------------------------------------------------------------------

_MEETING_KEYWORDS = [
    "politburo", "standing committee", "plenum", "plenary session",
    "national congress", "NPC session", "CPPCC", "economic work conference",
    "central committee",
]


def scrape_meeting_news(max_items: int = 20) -> list[dict]:
    """
    Scrape recent political meeting headlines from Xinhua English.
    Returns list of {title, link, date, body}.
    """
    results: list[dict] = []
    urls = [
        "http://www.news.cn/english/politics/index.htm",
        "http://www.news.cn/english/china/index.htm",
    ]
    seen: set[str] = set()

    for url in urls:
        if len(results) >= max_items:
            break
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            tree = lhtml.fromstring(resp.content)
            for a in tree.xpath("//a[@href]"):
                title = (a.text_content() or "").strip()
                href = a.get("href", "")
                if len(title) < 20 or title in seen:
                    continue
                title_lower = title.lower()
                if not any(kw.lower() in title_lower for kw in _MEETING_KEYWORDS):
                    continue
                seen.add(title)
                link = urljoin(url, href)
                parent_text = ""
                try:
                    parent_text = a.getparent().text_content()
                except Exception:
                    pass
                date_str = _extract_date(parent_text) or _extract_date(href)
                results.append({
                    "title": title,
                    "link": link,
                    "date": date_str,
                    "body": _classify_body(title_lower),
                    "source": "xinhua",
                })
                if len(results) >= max_items:
                    break
        except Exception as e:
            log.warning(f"Meeting scraper failed for {url}: {e}")

    return results


_DATE_RE = re.compile(r'(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})')
_HREF_DATE = re.compile(r'[/_](\d{4})(\d{2})(\d{2})[/_]')


def _extract_date(text: str) -> str:
    m = _DATE_RE.search(text)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    m = _HREF_DATE.search(text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


def _classify_body(title_lower: str) -> str:
    if "standing committee" in title_lower and "npc" not in title_lower:
        return "PSC"
    if "politburo" in title_lower:
        return "Politburo"
    if "plenum" in title_lower or "plenary" in title_lower:
        return "Central Committee"
    if "national congress" in title_lower:
        return "Party Congress"
    if "npc" in title_lower or "people's congress" in title_lower:
        return "NPC"
    if "cppcc" in title_lower:
        return "CPPCC"
    if "economic work conference" in title_lower:
        return "CEWC"
    return "Party"


# ---------------------------------------------------------------------------
# API helper
# ---------------------------------------------------------------------------

def get_polity_data() -> dict:
    today = date.today().isoformat()
    calendar_with_status = []
    for ev in CALENDAR:
        ev_copy = dict(ev)
        if ev.get("recurring"):
            ev_copy["status"] = "recurring"
        elif not ev.get("start"):
            ev_copy["status"] = "recurring"
        elif ev["start"] <= today:
            ev_copy["status"] = "past"
        else:
            ev_copy["status"] = "upcoming"
        calendar_with_status.append(ev_copy)

    return {
        "structure": STRUCTURE,
        "calendar": sorted(
            [e for e in calendar_with_status if not e.get("recurring")],
            key=lambda e: e.get("start") or "9999",
        ),
        "recurring": [e for e in calendar_with_status if e.get("recurring")],
        "decision_process": DECISION_PROCESS,
    }
