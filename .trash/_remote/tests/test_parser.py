"""
Tests for parser.py.

Uses a synthetic HTML fixture (tests/fixtures/sample_table.html) that matches
the expected site structure. When real HTML is captured via --debug-browser,
replace the fixture and update parser.py selectors — these tests will catch
any regressions.

The FakePage class mimics the scrapling Page API surface used by parser.py:
  - page.css(selector) → list of FakeElement
  - element.css("::text").get("") → text content
  - element.attrib → dict of HTML attributes
  - element.css("::text") → FakeTextResult supporting .get("")

Uses lxml.html for parsing — no scrapling dependency in tests.
"""
import pytest
from pathlib import Path

# ── FakePage: minimal scrapling Page API shim backed by lxml ─────────────────

try:
    import lxml.html
    import lxml.cssselect
    _LXML_AVAILABLE = True
except ImportError:
    _LXML_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _LXML_AVAILABLE,
    reason="lxml required for parser tests (pip install lxml)"
)

_FIXTURE = Path(__file__).parent / "fixtures" / "sample_table.html"


class FakeTextResult:
    def __init__(self, text: str) -> None:
        self._text = text

    def get(self, default: str = "") -> str:
        return self._text if self._text is not None else default


class FakeElement:
    def __init__(self, el) -> None:
        self._el = el

    def css(self, selector: str) -> "list[FakeElement] | FakeTextResult":
        if selector == "::text":
            text = self._el.text_content()
            return FakeTextResult(text)
        try:
            matches = self._el.cssselect(selector)
            return [FakeElement(m) for m in matches]
        except Exception:
            return []

    @property
    def attrib(self):
        return dict(self._el.attrib)

    def __getitem__(self, idx):
        # Allows cells[i] access pattern used in parser._extract_row
        children = [FakeElement(c) for c in self._el if c.tag == "td"]
        return children[idx]

    def __len__(self):
        return len([c for c in self._el if c.tag == "td"])


class FakePage:
    """Wraps an lxml-parsed HTML document, exposing the scrapling Page CSS API."""

    def __init__(self, html: str) -> None:
        self._root = lxml.html.fromstring(html)

    def css(self, selector: str) -> "list[FakeElement]":
        try:
            matches = self._root.cssselect(selector)
            return [FakeElement(m) for m in matches]
        except Exception:
            return []


@pytest.fixture(scope="module")
def fixture_html() -> str:
    assert _FIXTURE.exists(), (
        f"Fixture not found: {_FIXTURE}. "
        "Run --debug-browser to capture real HTML and save it there."
    )
    return _FIXTURE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def page(fixture_html) -> FakePage:
    return FakePage(fixture_html)


# ── Import parser internals ───────────────────────────────────────────────────

from customs_scraper.parser import (
    parse_results_page,
    has_next_page,
    get_total_row_count,
    _extract_row,
    _parse_number,
    _parse_int,
    _clean_hs_code,
    COLUMN_MAP,
)

# ── Unit tests: pure helper functions (no HTML / scrapling needed) ────────────

class TestParseNumber:
    def test_integer_string(self):
        assert _parse_number("1234") == 1234.0

    def test_float_with_comma_separators(self):
        assert _parse_number("1,234,567.89") == pytest.approx(1234567.89)

    def test_empty_string(self):
        assert _parse_number("") is None

    def test_none(self):
        assert _parse_number(None) is None

    def test_zero(self):
        assert _parse_number("0") == 0.0

    def test_negative(self):
        assert _parse_number("-500.5") == pytest.approx(-500.5)

    def test_non_numeric_garbage(self):
        # Returns None when no digits present
        assert _parse_number("N/A") is None

    def test_usd_symbol(self):
        assert _parse_number("$1,000.00") == pytest.approx(1000.0)


class TestParseInt:
    def test_plain_integer(self):
        assert _parse_int("42") == 42

    def test_with_prefix(self):
        assert _parse_int("Total: 3 records") == 3

    def test_empty(self):
        assert _parse_int("") is None

    def test_none(self):
        assert _parse_int(None) is None


class TestCleanHsCode:
    def test_strips_spaces(self):
        assert _clean_hs_code("8471 3000") == "84713000"

    def test_no_change_needed(self):
        assert _clean_hs_code("84713000") == "84713000"

    def test_internal_whitespace(self):
        assert _clean_hs_code("84\t71\n30\r00") == "84713000"


# ── Unit tests: _extract_row with fake cells ──────────────────────────────────

class FakeCell:
    """Minimal cell mock for _extract_row tests."""
    def __init__(self, text: str, attrs: dict | None = None) -> None:
        self._text = text
        self._attrs = attrs or {}

    def css(self, selector: str):
        if selector == "::text":
            return FakeTextResult(self._text)
        return []

    @property
    def attrib(self):
        return self._attrs


def _make_cells(overrides: dict | None = None) -> list:
    defaults = {
        0: "84713000",
        1: "Laptops",
        2: "502",
        3: "United States",
        4: "1,234,567.89",
        5: "8,888,888.00",
        6: "5000",
        7: "PCS",
    }
    if overrides:
        defaults.update(overrides)
    return [FakeCell(defaults[i]) for i in range(len(COLUMN_MAP))]


class TestExtractRow:
    def test_normal_row(self):
        cells = _make_cells()
        row = _extract_row(cells, 2024, 1)
        assert row is not None
        assert row["year"] == 2024
        assert row["month"] == 1
        assert row["hs8_code"] == "84713000"
        assert row["hs_description"] == "Laptops"
        assert row["country_code"] == "502"
        assert row["country_name"] == "United States"
        assert row["export_value_usd"] == pytest.approx(1234567.89)
        assert row["export_value_cny"] == pytest.approx(8888888.0)
        assert row["export_qty"] == 5000.0
        assert row["export_qty_unit"] == "PCS"

    def test_empty_value_fields_become_none(self):
        cells = _make_cells({4: "", 5: "", 7: ""})
        row = _extract_row(cells, 2024, 1)
        assert row is not None
        assert row["export_value_usd"] is None
        assert row["export_value_cny"] is None
        assert row["export_qty_unit"] is None

    def test_empty_description_becomes_none(self):
        cells = _make_cells({1: ""})
        row = _extract_row(cells, 2024, 1)
        assert row["hs_description"] is None

    def test_empty_row_returns_none(self):
        cells = _make_cells({0: "", 4: ""})
        result = _extract_row(cells, 2024, 1)
        assert result is None

    def test_hs_code_whitespace_stripped(self):
        cells = _make_cells({0: "8471 3000"})
        row = _extract_row(cells, 2024, 1)
        assert row["hs8_code"] == "84713000"


# ── Integration tests: parse_results_page against HTML fixture ────────────────

class TestParseResultsPage:
    def test_returns_correct_row_count(self, page):
        rows = parse_results_page(page, 2024, 1)
        assert len(rows) == 3

    def test_year_and_month_injected(self, page):
        rows = parse_results_page(page, 2024, 3)
        assert all(r["year"] == 2024 for r in rows)
        assert all(r["month"] == 3 for r in rows)

    def test_hs_code_parsed(self, page):
        rows = parse_results_page(page, 2024, 1)
        assert all(r["hs8_code"] == "84713000" for r in rows)

    def test_country_codes_distinct(self, page):
        rows = parse_results_page(page, 2024, 1)
        country_codes = {r["country_code"] for r in rows}
        assert "502" in country_codes
        assert "101" in country_codes
        assert "304" in country_codes

    def test_numeric_values_parsed(self, page):
        rows = parse_results_page(page, 2024, 1)
        us_row = next(r for r in rows if r["country_code"] == "502")
        assert us_row["export_value_usd"] == pytest.approx(1234567.89)
        assert us_row["export_qty"] == 5000.0

    def test_empty_values_become_none(self, page):
        rows = parse_results_page(page, 2024, 1)
        jp_row = next(r for r in rows if r["country_code"] == "304")
        assert jp_row["export_value_usd"] is None
        assert jp_row["export_value_cny"] is None

    def test_no_rows_on_empty_page(self):
        empty_page = FakePage("<html><body><table class='data-table'><tbody></tbody></table></body></html>")
        rows = parse_results_page(empty_page, 2024, 1)
        assert rows == []

    def test_missing_table_returns_empty(self):
        bare_page = FakePage("<html><body><p>No results</p></body></html>")
        rows = parse_results_page(bare_page, 2024, 1)
        assert rows == []


# ── Integration tests: has_next_page ─────────────────────────────────────────

class TestHasNextPage:
    def test_next_page_exists_and_enabled(self, page):
        # Fixture has <a class="next-page"> (not disabled) → True
        assert has_next_page(page) is True

    def test_no_next_page_element(self):
        p = FakePage("<html><body><div class='pagination'></div></body></html>")
        assert has_next_page(p) is False

    def test_disabled_next_page(self):
        p = FakePage(
            "<html><body>"
            "<a class='next-page disabled'>Next</a>"
            "</body></html>"
        )
        assert has_next_page(p) is False

    def test_aria_disabled_next_page(self):
        p = FakePage(
            "<html><body>"
            "<a class='next-page' aria-disabled='true'>Next</a>"
            "</body></html>"
        )
        assert has_next_page(p) is False


# ── Integration tests: get_total_row_count ────────────────────────────────────

class TestGetTotalRowCount:
    def test_extracts_count_from_fixture(self, page):
        # Fixture has <span class="total-count">Total: 3 records</span>
        count = get_total_row_count(page)
        assert count == 3

    def test_returns_none_when_missing(self):
        p = FakePage("<html><body><p>nothing</p></body></html>")
        assert get_total_row_count(p) is None
