"""
China Political Structure.
Source: https://multimedia.scmp.com/widgets/china/govt-explainer/index.html

Organs and leading groups are from the SCMP page structure.
PSC membership is from the 20th Party Congress (October 2022) official record.
Leading group chairs are from official Xinhua/government announcements.
"""

# Politburo Standing Committee — 20th Party Congress, October 2022
PSC_MEMBERS = [
    {
        "rank": 1,
        "name": "Xi Jinping",
        "name_cn": "习近平",
        "primary_role": "General Secretary · President · CMC Chairman",
        "organs": ["cmc"],
        "leads": ["foreign_affairs", "taiwan", "internet", "national_security",
                  "deepening_reform", "defence_reform", "finance_economy", "military_civil"],
    },
    {
        "rank": 2,
        "name": "Li Qiang",
        "name_cn": "李强",
        "primary_role": "Premier, State Council",
        "organs": ["state_council"],
        "leads": [],
    },
    {
        "rank": 3,
        "name": "Zhao Leji",
        "name_cn": "赵乐际",
        "primary_role": "Chairman, NPC Standing Committee",
        "organs": ["npc"],
        "leads": [],
    },
    {
        "rank": 4,
        "name": "Wang Huning",
        "name_cn": "王沪宁",
        "primary_role": "Chairman, CPPCC",
        "organs": ["cppcc"],
        "leads": [],
    },
    {
        "rank": 5,
        "name": "Cai Qi",
        "name_cn": "蔡奇",
        "primary_role": "Director, CCP Central Secretariat",
        "organs": [],
        "leads": [],
    },
    {
        "rank": 6,
        "name": "Ding Xuexiang",
        "name_cn": "丁薛祥",
        "primary_role": "Executive Vice Premier",
        "organs": ["state_council"],
        "leads": ["finance_economy"],
    },
    {
        "rank": 7,
        "name": "Li Xi",
        "name_cn": "李希",
        "primary_role": "Secretary, CCDI",
        "organs": ["supervision"],
        "leads": [],
    },
]

# State organs — from SCMP page
ORGANS = [
    {"id": "state_council", "name": "State Council",              "name_cn": "国务院",            "psc_leader": "Li Qiang"},
    {"id": "cmc",           "name": "Central Military Commission", "name_cn": "中央军事委员会",    "psc_leader": "Xi Jinping"},
    {"id": "npc",           "name": "NPC",                        "name_cn": "全国人民代表大会",   "psc_leader": "Zhao Leji"},
    {"id": "cppcc",         "name": "CPPCC",                      "name_cn": "中国人民政治协商会议","psc_leader": "Wang Huning"},
    {"id": "supervision",   "name": "National Supervision Commission","name_cn": "国家监察委员会", "psc_leader": "Li Xi"},
    {"id": "judiciary",     "name": "Supreme People's Court",     "name_cn": "最高人民法院",       "psc_leader": None},
]

# Leading groups / commissions — from SCMP page frame-5
# Chairs from official Xinhua announcements
LEADING_GROUPS = [
    {
        "id": "foreign_affairs",
        "name": "Foreign Affairs Commission",
        "name_cn": "中央外事工作委员会",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping"],
    },
    {
        "id": "taiwan",
        "name": "Taiwan Affairs Leading Group",
        "name_cn": "中央对台工作领导小组",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping"],
    },
    {
        "id": "internet",
        "name": "Cyberspace Affairs Commission",
        "name_cn": "中央网络安全和信息化委员会",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping"],
    },
    {
        "id": "national_security",
        "name": "National Security Commission",
        "name_cn": "中央国家安全委员会",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping"],
    },
    {
        "id": "deepening_reform",
        "name": "Commission for Deepening Reform",
        "name_cn": "中央全面深化改革委员会",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping"],
    },
    {
        "id": "defence_reform",
        "name": "Military Reform Leading Group",
        "name_cn": "深化国防和军队改革领导小组",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping"],
    },
    {
        "id": "finance_economy",
        "name": "Finance & Economics Commission",
        "name_cn": "中央财经委员会",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping", "Ding Xuexiang"],
    },
    {
        "id": "military_civil",
        "name": "Military-Civil Fusion Commission",
        "name_cn": "中央军民融合发展委员会",
        "chair": "Xi Jinping",
        "members": ["Xi Jinping"],
    },
]


def get_polity_data() -> dict:
    return {
        "psc": PSC_MEMBERS,
        "organs": ORGANS,
        "leading_groups": LEADING_GROUPS,
    }


def scrape_meeting_news(max_items: int = 20) -> list[dict]:
    return []
