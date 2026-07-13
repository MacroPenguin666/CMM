"""
Tests for the multi-material commodities pipeline: materials registry sanity,
USGS MCS CSV parsing (both data-release formats), copper static-tail merge,
and trade helpers. Fixtures under tests/fixtures/ are row subsets of the real
USGS data-release CSVs (MCS 2026 + MCS 2025 world, saved 2026-07-09).
"""

import re
from pathlib import Path

import pytest

from backend.fetchers import commodities as C
from backend.fetchers.materials import CATEGORIES, MATERIALS

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def usgs(monkeypatch=None):
    """Parsed fixture CSVs, with network _get patched out."""
    orig = C._get

    def fake_get(url, timeout=60):
        if url == C.MCS2026_URL:
            return (FIXTURES / "mcs2026_sample.csv").read_bytes()
        if url == C.MCS2025_URL:
            return (FIXTURES / "mcs2025_world_sample.csv").read_bytes()
        raise AssertionError(f"unexpected network call: {url}")

    C._get = fake_get
    try:
        yield C.fetch_mcs2026(), C.fetch_mcs2025()
    finally:
        C._get = orig


# ---------------------------------------------------------------- registry

def test_registry_categories_and_fields():
    for slug, spec in MATERIALS.items():
        assert re.fullmatch(r"[a-z0-9-]+", slug), slug
        assert spec["category"] in CATEGORIES, slug
        assert spec["name"] and spec["symbol"] and spec["uses"], slug
        assert spec["hs"], f"{slug} has no HS codes"
        for code, label in spec["hs"]:
            assert re.fullmatch(r"\d{4}|\d{6}", code), f"{slug}: bad HS code {code}"
            assert label
        # Materials without USGS production rules must explain why.
        if not spec.get("usgs"):
            assert spec.get("prod_note"), f"{slug} lacks both usgs rules and prod_note"


def test_registry_usgs_rules_shape():
    for slug, spec in MATERIALS.items():
        for key, rule in (spec.get("usgs") or {}).items():
            assert rule.get("label"), f"{slug}.{key}"
            for edition in ("mcs2026", "mcs2025"):
                r = rule.get(edition)
                if r is not None:
                    assert isinstance(r, tuple) and len(r) == 2, f"{slug}.{key}.{edition}"


def test_trade_code_order_primary_first_no_dups():
    order = C.trade_code_order()
    assert len(order) == len(set(order))
    primaries = {spec["hs"][0][0] for spec in MATERIALS.values()}
    n_prim = len(primaries)
    assert set(order[:n_prim]) == primaries  # all primary codes come first
    assert "7403" in order and "2603" in order


# ------------------------------------------------------------- value cleaning

@pytest.mark.parametrize("raw,expected", [
    ("1,234", 1234.0), ("5,510", 5510.0), ("W", None), ("", None),
    ("NA", None), ("—", None), ("<1", 1.0), ("120e", 120.0), (None, None),
])
def test_clean_value(raw, expected):
    assert C._clean_value(raw) == expected


def test_rename_collapses_world_and_other():
    assert C._rename("World total (rounded)") == "World total"
    assert C._rename("World total (ilmenite, rounded)") == "World total"
    assert C._rename("Other Countries") == "Other countries"
    assert C._rename("other countries (rounded)") == "Other countries"
    assert C._rename("Congo (Kinshasa)") == "DR Congo"
    assert C._rename("Korea, Republic of") == "South Korea"


# ------------------------------------------------------------- USGS parsing

def test_copper_production_with_static_tail(usgs):
    m26, m25 = usgs
    stages = C.build_production(MATERIALS["copper"], m26, m25)
    stages = C._merge_copper_static(stages)
    mine = stages["mine"]
    assert mine["years"] == [2020, 2021, 2022, 2023, 2024, 2025]
    chile = dict(zip(mine["years"], mine["countries"]["Chile"]))
    assert chile[2020] == 5730          # static tail
    assert chile[2024] == 5510          # MCS 2026
    assert mine["world"][0] == 20600
    refinery = stages["refinery"]
    assert refinery["years"][0] == 2020
    assert dict(zip(refinery["years"], refinery["countries"]["China"]))[2020] == 10000
    assert refinery["world"][-1] is not None


def test_lithium_production(usgs):
    m26, m25 = usgs
    stages = C.build_production(MATERIALS["lithium"], m26, m25)
    mine = stages["mine"]
    assert mine["years"] == [2023, 2024, 2025]
    assert mine["countries"]["Australia"][0] == 91700  # MCS 2025 file, 2023
    assert mine["unit"] == "metric tons"
    assert "World total" not in mine["countries"]      # world split out
    assert mine["world"] is not None


def test_titanium_typo_prefix_and_multi_stage(usgs):
    """MCS 2025 spells 'ilmentite'; the registry prefix must still match."""
    m26, m25 = usgs
    stages = C.build_production(MATERIALS["titanium"], m26, m25)
    assert 2023 in stages["mine"]["years"]              # typo'd rows matched
    assert set(stages) == {"mine", "mine2", "refinery"}
    assert stages["refinery"]["label"].startswith("Titanium sponge")


def test_boron_has_no_world_total(usgs):
    m26, m25 = usgs
    stages = C.build_production(MATERIALS["boron"], m26, m25)
    assert stages["mine"]["countries"]
    assert stages["mine"]["world"] is None


def test_germanium_no_stages_but_noted(usgs):
    m26, m25 = usgs
    assert C.build_production(MATERIALS["germanium"], m26, m25) == {}
    assert MATERIALS["germanium"]["prod_note"]


# ----------------------------------------------------------------- trade

def test_mark_partial():
    by_year = {
        "2022": {"exports": {"total": 100}, "imports": {"total": 90}},
        "2023": {"exports": {"total": 105}, "imports": {"total": 95}},
        "2024": {"exports": {"total": 50}, "imports": {"total": 94}},  # partial
    }
    assert C._mark_partial(by_year) == [2024]


def test_mark_partial_price_crash_not_partial():
    """A >20% value drop with volumes holding up is a price move, not
    missing filings (e.g. cobalt 2023)."""
    by_year = {
        "2022": {"exports": {"total": 100, "totalW": 10}},
        "2023": {"exports": {"total": 55, "totalW": 11}},   # price crash
        "2024": {"exports": {"total": 30, "totalW": 4}},    # genuinely partial
    }
    assert C._mark_partial(by_year) == [2024]


def test_migrate_legacy_trade_carries_commodities():
    legacy = {"trade": {"commodities": {"7403": {"label": "x", "by_year": {}}}}}
    out = C.migrate_legacy_trade(legacy)
    assert "7403" in out["commodities"]
    assert C.migrate_legacy_trade({}) == {"source": "UN Comtrade",
                                          "unit": "USD (netWgt in kg)",
                                          "commodities": {}}
