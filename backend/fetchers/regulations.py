"""
Regulations data module — MOFCOM active laws + NPC bills under revision.

DB: data/regulations.db
Tables: mofcom_docs, npc_bills, npc_bill_events
"""

import logging
import sqlite3
from datetime import datetime, timezone

from pathlib import Path
import sqlite3 as _sqlite3

from backend.storage import get_conn as _storage_get_conn

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS mofcom_docs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    doc_type        TEXT,
    hierarchy_level INTEGER,
    topic           TEXT,
    issue_date      TEXT,
    effective_date  TEXT,
    expiration_date TEXT,
    url             TEXT,
    status          TEXT DEFAULT 'active',
    fetched_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_mofcom_hier  ON mofcom_docs(hierarchy_level);
CREATE INDEX IF NOT EXISTS idx_mofcom_topic ON mofcom_docs(topic);

CREATE TABLE IF NOT EXISTS npc_bills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id         TEXT UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    title_cn        TEXT,
    status          TEXT,
    topic           TEXT,
    category        TEXT,
    date_introduced TEXT,
    url             TEXT,
    fetched_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_npc_status ON npc_bills(status);
CREATE INDEX IF NOT EXISTS idx_npc_topic  ON npc_bills(topic);

CREATE TABLE IF NOT EXISTS npc_bill_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id     TEXT NOT NULL,
    event_type  TEXT,
    event_date  TEXT,
    description TEXT,
    FOREIGN KEY (bill_id) REFERENCES npc_bills(bill_id)
);
CREATE INDEX IF NOT EXISTS idx_events_bill ON npc_bill_events(bill_id);
"""

# Topic keywords (first match wins; case-insensitive substring search in title)
# Both Chinese and English keywords — NPC Observer titles are in English, MOFCOM in Chinese
TOPIC_MAP = {
    "Trade & Customs":        ["tariff", "trade law", "export control", "customs law",
                               "government procurement", "bid invitation", "bidding",
                               "贸易", "进出口", "关税", "出口管制", "进口税", "海关", "采购"],
    "Foreign Affairs":        ["foreign investment", "foreign relations", "foreign state immunity",
                               "foreign trade", "market access", "anti-transnational",
                               "overseas chinese", "exit-entry",
                               "外资", "外商投资", "市场准入", "外商", "对外关系", "外贸", "华侨"],
    "Finance & Tax":          ["financial stability", "banking supervision", "bank law",
                               "commercial banks", "insurance law", "securities",
                               "value-added tax", "consumption tax", "tax collection",
                               "environmental protection tax", "accounting law", "budget",
                               "certified public accountants", "pricing law",
                               "金融", "银行", "证券", "外汇", "资本", "基金", "税", "会计", "预算"],
    "Digital & Telecom":      ["e-commerce", "digital economy", "cybersecurity", "cybercrime",
                               "telecommunications", "radio and television", "radio spectrum",
                               "space law", "internet", "data",
                               "电子商务", "电商", "数字", "互联网", "网络", "电信", "广播"],
    "Intellectual Property":  ["intellectual property", "patent", "trademark", "copyright",
                               "trademarks law",
                               "知识产权", "专利", "商标", "版权", "著作权"],
    "Competition & Trade Remedies": ["anti-dumping", "anti-unfair competition", "monopoly",
                               "anti-monopoly", "sanctions", "countervailing", "anti-money laundering",
                               "反倾销", "制裁", "反补贴", "保障措施", "反垄断", "竞争"],
    "Energy & Resources":     ["energy law", "atomic energy", "nuclear", "mineral resources",
                               "electricity", "renewable energy", "petroleum", "coal",
                               "能源", "矿产", "石油", "资源", "煤炭", "电力", "矿业", "原子能"],
    "Environment & Land":     ["ecological", "environmental code", "national parks",
                               "qinghai-tibet", "nature", "conservation law",
                               "antarctic", "natural protected", "natural disasters",
                               "flood control", "cultivated land", "farmland",
                               "marine environmental", "cultural relics",
                               "national reserves", "earthquake",
                               "生态", "环境", "保护", "碳", "耕地", "湿地"],
    "Water & Agriculture":    ["water law", "fisheries", "agriculture law", "food security",
                               "rural collective", "rural", "irrigation", "meteorology",
                               "水法", "渔业", "农业", "粮食", "农村", "水利"],
    "Transport & Logistics":  ["transport", "railway", "civil aviation", "maritime",
                               "road traffic", "logistics", "aviation law",
                               "运输", "铁路", "航空", "航海", "道路", "物流"],
    "Education & Science":    ["education", "preschool education", "degrees law",
                               "academic degrees", "science and technology", "teachers",
                               "law on publicity", "rule of law", "historical cultural",
                               "intangible cultural", "cultural industry",
                               "教育", "科技", "学位", "文化", "遗产"],
    "Public Health":          ["public health emergency", "infectious diseases",
                               "border health", "quarantine", "medical devices",
                               "pharmacists", "drugs", "healthcare security",
                               "卫生", "疾病", "医疗", "药品"],
    "Social Policy":          ["social assistance", "social credit", "social insurance",
                               "childcare", "eldercare", "volunteer", "charity law",
                               "reward and protect", "good samaritans", "veterans",
                               "household registration", "disability", "barrier-free",
                               "社会", "慈善", "养老", "残疾", "志愿"],
    "Labor & Employment":     ["labor law", "employment", "workers", "occupational",
                               "industrial workforce", "fire and rescue personnel",
                               "labor policy", "劳动", "就业", "工人", "消防"],
    "Corporate & Insolvency": ["company law", "enterprise bankruptcy", "state-owned assets",
                               "private economy", "industry associations", "chambers of commerce",
                               "法院", "公司", "破产", "国有资产", "民营"],
    "National Security":      ["national security", "counterespionage", "state secrets",
                               "national defense", "militia", "officers in active service",
                               "patriotic", "national reserves security",
                               "国家安全", "保密", "国防", "反间谍"],
    "Legal System":           ["civil procedure", "administrative reconsideration",
                               "arbitration", "legislation law", "state compensation",
                               "procuratorial", "public interest litigation", "oversight",
                               "supervision law", "food safety", "statistics law",
                               "accounting", "notarization", "delegates",
                               "urban residents", "villagers committees", "organic law",
                               "criminal law", "criminal procedure", "civil compulsory",
                               "detention centers", "prisons law", "people's police",
                               "public security administration", "emergency response",
                               "code of conduct", "recording and review", "retirement ages",
                               "ethnic unity", "standard spoken", "finance law",
                               "national development plans", "metrology", "product quality",
                               "law on the people", "law on the state",
                               "法律", "仲裁", "监察", "诉讼", "司法", "立法"],
    "Hazardous Materials":    ["hazardous chemicals", "safety law", "atomic", "radiation",
                               "electromagnetic radiation", "dangerous",
                               "危险化学品", "放射性", "安全生产"],
    "Real Estate & Housing":  ["real property registration", "housing", "real estate",
                               "urban planning", "land registration",
                               "不动产", "房地产", "城市规划"],
}

# Map doc_type keywords to hierarchy levels
HIERARCHY_MAP = [
    (["行政法规", "条例"], 3),
    (["部门规章", "规章", "办法", "规定"], 4),
    (["规范性文件", "决定", "命令"], 5),
    (["通知", "意见", "公告", "通告", "公示"], 6),
]

HIERARCHY_LABELS = {
    3: "行政法规 (Administrative Regulations)",
    4: "部门规章 (Ministerial Rules)",
    5: "规范性文件 (Normative Documents)",
    6: "通知 / 意见 (Notices & Opinions)",
}

# NPC bill status display labels
STATUS_LABELS = {
    "passed":       "Passed",
    "pending":      "Pending",
    "shelved":      "Shelved",
    "consultation": "Public Comment",
    "other":        "Other",
}


_MIGRATIONS = [
    "ALTER TABLE npc_bills ADD COLUMN title_cn TEXT",
    "ALTER TABLE npc_bills ADD COLUMN category TEXT",
]


def get_regulations_db() -> sqlite3.Connection:
    conn = _storage_get_conn()
    conn.executescript(SCHEMA)
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass  # column already exists
    conn.row_factory = sqlite3.Row
    return conn


def detect_topic(title: str) -> str:
    # Normalise various dash/apostrophe variants before matching
    title_lower = title.lower().replace("–", "-").replace("'", "'").replace("’", "'")
    for topic, keywords in TOPIC_MAP.items():
        for kw in keywords:
            if kw.lower() in title_lower:
                return topic
    return "Other"


def infer_hierarchy(doc_type: str) -> int:
    if not doc_type:
        return 6
    for keywords, level in HIERARCHY_MAP:
        if any(kw in doc_type for kw in keywords):
            return level
    return 6


def store_mofcom_docs(conn: sqlite3.Connection, docs: list[dict]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for doc in docs:
        doc.setdefault("fetched_at", now)
        doc.setdefault("status", "active")
        doc.setdefault("effective_date", None)
        doc.setdefault("expiration_date", None)
        doc["topic"] = detect_topic(doc.get("title", ""))
        doc["hierarchy_level"] = infer_hierarchy(doc.get("doc_type", ""))
        try:
            conn.execute(
                """INSERT OR REPLACE INTO mofcom_docs
                   (doc_id, title, doc_type, hierarchy_level, topic, issue_date,
                    effective_date, expiration_date, url, status, fetched_at)
                   VALUES (:doc_id, :title, :doc_type, :hierarchy_level, :topic,
                           :issue_date, :effective_date, :expiration_date, :url,
                           :status, :fetched_at)""",
                doc,
            )
            inserted += conn.execute("SELECT changes()").fetchone()[0]
        except Exception as e:
            log.warning("mofcom_docs insert error: %s", e)
    conn.commit()
    return inserted


def store_npc_bill(conn: sqlite3.Connection, bill: dict, events: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    bill.setdefault("fetched_at", now)
    bill.setdefault("title_cn", None)
    bill.setdefault("category", None)
    bill["topic"] = detect_topic(bill.get("title", ""))
    # Infer date_introduced from earliest event if not set
    if not bill.get("date_introduced") and events:
        dates = [e["event_date"] for e in events if e.get("event_date")]
        if dates:
            bill["date_introduced"] = min(dates)
    conn.execute(
        """INSERT OR REPLACE INTO npc_bills
           (bill_id, title, title_cn, status, topic, category, date_introduced, url, fetched_at)
           VALUES (:bill_id, :title, :title_cn, :status, :topic, :category, :date_introduced, :url, :fetched_at)""",
        bill,
    )
    conn.execute("DELETE FROM npc_bill_events WHERE bill_id = ?", (bill["bill_id"],))
    for ev in events:
        ev["bill_id"] = bill["bill_id"]
        conn.execute(
            """INSERT INTO npc_bill_events (bill_id, event_type, event_date, description)
               VALUES (:bill_id, :event_type, :event_date, :description)""",
            ev,
        )
    conn.commit()


def get_mofcom_docs(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """SELECT doc_id, title, doc_type, hierarchy_level, topic,
                  issue_date, effective_date, url, status
           FROM mofcom_docs
           WHERE status = 'active'
           ORDER BY hierarchy_level ASC, topic ASC, issue_date DESC"""
    ).fetchall()
    return [dict(r) for r in rows]


def get_npc_bills(conn: sqlite3.Connection) -> list[dict]:
    bills = conn.execute(
        """SELECT bill_id, title, title_cn, status, topic, category, date_introduced, url
           FROM npc_bills
           ORDER BY topic ASC, date_introduced ASC"""
    ).fetchall()
    result = []
    for b in bills:
        bill = dict(b)
        events = conn.execute(
            """SELECT event_type, event_date, description
               FROM npc_bill_events
               WHERE bill_id = ?
               ORDER BY event_date ASC""",
            (bill["bill_id"],),
        ).fetchall()
        bill["events"] = [dict(e) for e in events]
        result.append(bill)
    return result


def get_regulations_stats(conn: sqlite3.Connection) -> dict:
    mofcom_count = conn.execute(
        "SELECT COUNT(*) FROM mofcom_docs WHERE status='active'"
    ).fetchone()[0]
    npc_count = conn.execute(
        "SELECT COUNT(*) FROM npc_bills WHERE status NOT IN ('passed', 'shelved')"
    ).fetchone()[0]
    return {"mofcom_active": mofcom_count, "npc_under_reform": npc_count}
