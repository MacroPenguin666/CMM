"""Tests for backend.fetchers.fyp_demand (registry sanity + payload assembly)."""

import sqlite3

import pytest

from backend.fetchers.fyp_demand import (
    CHAPTERS, DOC_URL, FACTS, HUB, MILESTONES, SECTIONS, SERIES_VARS,
    STATUS_SERIES, TARGETS, build_payload, related_docs, target_reading,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute("""CREATE TABLE policy_docs (
        id INTEGER PRIMARY KEY, ministry TEXT, title TEXT, url TEXT UNIQUE,
        published TEXT, instrument_type TEXT, fetch_status TEXT,
        text_len INTEGER)""")
    c.execute("""CREATE TABLE macro_series (
        variable TEXT, year INTEGER, value REAL)""")
    docs = [
        ("gov", "关于汽车消费以旧换新的通知", "http://x/1", "2026-05-01", "通知", "ok", 100),
        ("ndrc", "全国统一大市场建设指引", "http://x/2", "2026-04-01", "意见", "ok", 100),
        ("mofcom", "以旧换新问答", "http://x/3", "2026-06-01", "答问", "pending", 0),
        ("gov", "十五五规划纲要", DOC_URL, "2026-03-13", "规划", "ok", 60175),
    ]
    c.executemany("INSERT INTO policy_docs (ministry,title,url,published,"
                  "instrument_type,fetch_status,text_len) VALUES (?,?,?,?,?,?,?)", docs)
    c.executemany("INSERT INTO macro_series VALUES (?,?,?)",
                  [("hcons_GDP", 2023, 40.6), ("hcons_GDP", 2024, 41.0),
                   ("inv_GDP", 2024, 40.1), ("unemp", 2024, 5.1),
                   ("govexp_GDP", 2024, 33.0), ("inv_GDP", 2030, 40.6)])
    c.execute("CREATE TABLE bruegel_series (indicator TEXT, date TEXT, value REAL)")
    c.executemany(
        "INSERT INTO bruegel_series VALUES (?,?,?)",
        [("BRU_Consumption_Retail_Sales_Total_Retail_Sales", "2018-06-30", 9.0),  # pre-2019: trimmed
         ("BRU_Consumption_Retail_Sales_Total_Retail_Sales", "2026-04-30", 0.2),
         ("BRU_Consumption_Retail_Sales_Total_Retail_Sales", "2026-05-31", -0.6),
         # two obs in one month → downsampled to the last one
         ("BRU_Consumption_Car_Sales_Weekly_Car_Sales_%YoY_16-week_moving_average", "2026-05-07", 2.0),
         ("BRU_Consumption_Car_Sales_Weekly_Car_Sales_%YoY_16-week_moving_average", "2026-05-14", 3.0)])
    yield c
    c.close()


def test_registry_shape():
    assert len(SECTIONS) == 10
    assert {s["chapter"] for s in SECTIONS} == set(CHAPTERS) == {15, 16, 17}
    assert len({s["id"] for s in SECTIONS}) == 10
    for s in SECTIONS:
        assert s["points"] and s["doc_like"] and s["name_cn"] and s["glyph"]
    assert HUB["doc_like"] and HUB["preamble"]


def test_related_docs_matches_and_filters(conn):
    docs = related_docs(conn, ["以旧换新"])
    # pending row excluded, ok row returned
    assert [d["url"] for d in docs] == ["http://x/1"]
    assert docs[0]["ministry"] == "gov"
    assert related_docs(conn, ["不存在的关键词"]) == []
    assert related_docs(conn, []) == []


def test_related_docs_ordering_and_limit(conn):
    docs = related_docs(conn, ["消费", "统一大市场"], limit=1)
    assert len(docs) == 1
    assert docs[0]["published"] == "2026-05-01"  # newest first


def test_build_payload_shape(conn):
    p = build_payload(conn)
    assert len(p["sections"]) == 10
    assert set(p["series"]) == set(SERIES_VARS)
    # post-2025 projections capped off
    assert all(r["year"] <= 2025 for r in p["series"]["inv_GDP"])
    assert p["series"]["hcons_GDP"][-1] == {"year": 2024, "value": 41.0}
    assert p["doc_url"] == DOC_URL
    assert p["doc_published"] == "2026-03-13" and p["doc_text_len"] == 60175
    for s in p["sections"]:
        assert "docs" in s and "points" in s and "doc_like" not in s
        assert isinstance(s["milestones"], list) and isinstance(s["facts"], list)
        for t in s["targets"]:
            assert t["reading"]["status"] in ("met", "on_track", "mixed", "off_track", "n/a")
            assert t["history"] and t["latest"]
    assert "doc_like" not in p["hub"] and isinstance(p["hub"]["docs"], list)
    assert any(t["id"] == "retail_total" for t in p["hub"]["targets"])
    # status: goods has bruegel rows — trimmed to ≥2019 and downsampled monthly
    goods = next(s for s in p["sections"] if s["id"] == "goods")
    retail = goods["status"]["series"][0]["data"]
    assert retail == [{"date": "2026-04", "value": 0.2}, {"date": "2026-05", "value": -0.6}]
    cars = goods["status"]["series"][1]["data"]
    assert cars == [{"date": "2026-05", "value": 3.0}]  # last obs of the month
    # sections whose bruegel indicators are absent get status None, not a crash
    fair = next(s for s in p["sections"] if s["id"] == "fair")
    assert fair["status"] is None


# ---------------------------------------------------------------------------
# v2: milestones / targets / status registries + reading logic
# ---------------------------------------------------------------------------

def test_milestones_registry():
    ids = {s["id"] for s in SECTIONS}
    assert set(MILESTONES) == ids | {"hub"}
    for sec, ms in MILESTONES.items():
        assert len(ms) >= 2, sec
        for m in ms:
            assert m["title_en"] and m["title_cn"] and m["note"], (sec, m)
            # the one deliberately open milestone (条例 pending) has no date/url
            if m["date"] is not None:
                assert m["url"] and m["url"].startswith("http"), (sec, m)


def test_targets_registry():
    ids = {s["id"] for s in SECTIONS} | {"hub"}
    for t in TARGETS:
        assert set(t["sections"]) <= ids
        assert t["source_url"].startswith("http")
        assert t["target_kind"] in ("min_level", "max_level", "trend")
        if t["target_kind"] == "trend":
            assert t["goal_dir"] in ("up", "down") and "flat_eps" in t
        else:
            assert t["target"] is not None
        if "history" in t:
            periods = [p for p, _ in t["history"]]
            assert periods == sorted(periods) and len(periods) >= 2
        else:
            assert t.get("series") in SERIES_VARS


def test_status_series_registry():
    ids = {s["id"] for s in SECTIONS}
    assert set(STATUS_SERIES) <= ids
    for specs in STATUS_SERIES.values():
        for sp in specs:
            assert sp["src"].startswith("BRU_") and sp["label"] and sp["color"]


def test_facts_registry():
    ids = {s["id"] for s in SECTIONS} | {"hub"}
    assert set(FACTS) <= ids
    for facts in FACTS.values():
        for f in facts:
            assert f["label"] and f["value"]


def _t(kind, baseline, target, base_p="2025", tgt_p="2030", **kw):
    return {"target_kind": kind, "baseline": baseline, "target": target,
            "baseline_period": base_p, "target_period": tgt_p,
            "unit": "u", **kw}


def test_reading_min_level():
    t = _t("min_level", 50.0, 60.0)
    assert target_reading(t, None)["status"] == "n/a"
    # baseline-year reading only → n/a with required pace
    r = target_reading(t, {"period": "2025", "value": 50.0})
    assert r["status"] == "n/a" and "%/yr" in r["note"]
    # 2027: 40% elapsed; 4/10 gap closed → frac 1.0 → on track
    assert target_reading(t, {"period": "2027", "value": 54.0})["status"] == "on_track"
    # 2029: 80% elapsed; 1/10 closed → frac 0.125 → off track
    assert target_reading(t, {"period": "2029", "value": 51.0})["status"] == "off_track"
    assert target_reading(t, {"period": "2030", "value": 61.0})["status"] == "met"


def test_reading_max_level():
    t = _t("max_level", 14.1, 13.5, base_p="2024", tgt_p="2027")
    # 2025: 33% elapsed, 33% closed → on track
    assert target_reading(t, {"period": "2025", "value": 13.9})["status"] == "on_track"
    assert target_reading(t, {"period": "2026", "value": 13.4})["status"] == "met"
    # moving the wrong way → gap negative → off track
    assert target_reading(t, {"period": "2026", "value": 14.5})["status"] == "off_track"


def test_reading_trend():
    t = _t("trend", 50.1, None, base_p="2024", goal_dir="up", flat_eps=0.2)
    # baseline-period reading → n/a
    assert target_reading(t, {"period": "2024", "value": 50.1})["status"] == "n/a"
    assert target_reading(t, {"period": "2025", "value": 49.7})["status"] == "off_track"
    assert target_reading(t, {"period": "2025", "value": 50.2})["status"] == "mixed"   # within eps
    assert target_reading(t, {"period": "2025", "value": 51.0})["status"] == "on_track"
    down = _t("trend", 151, None, base_p="2018", goal_dir="down", flat_eps=2)
    assert target_reading(down, {"period": "2025", "value": 106})["status"] == "on_track"
