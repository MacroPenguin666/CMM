"""
Fetch Chinese macro, equity, bond, and FX data via AKShare.

Data is stored in data/feeds.db in the `financial_series` table.
Designed to degrade gracefully when upstream sources are unreachable
(AKShare pulls from East Money, Jin10, etc. which throttle non-China IPs).

Usage:
    python financial.py                 # fetch all available data
    python financial.py --show          # show latest stored snapshots
    python financial.py --json          # export as JSON
"""

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("financial")

# ---------------------------------------------------------------------------
# DB schema extension
# ---------------------------------------------------------------------------
FINANCIAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS financial_series (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator   TEXT NOT NULL,
    category    TEXT NOT NULL,
    date        TEXT,
    value       REAL,
    unit        TEXT,
    extra       TEXT,
    fetched_at  TEXT NOT NULL,
    UNIQUE(indicator, date)
);

CREATE TABLE IF NOT EXISTS financial_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator   TEXT NOT NULL,
    category    TEXT NOT NULL,
    latest_value REAL,
    change      REAL,
    unit        TEXT,
    data_date   TEXT,
    fetched_at  TEXT NOT NULL
);
"""


def get_financial_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(FINANCIAL_SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Data fetchers — each returns (rows, snapshot) or raises on failure
# Row: (indicator, category, date, value, unit)
# Snapshot: dict with latest_value, change, data_date
# ---------------------------------------------------------------------------

def _safe_fetch(name, fn):
    """Run a fetch function, return result or None on failure."""
    try:
        result = fn()
        log.info(f"  OK  {name}")
        return result
    except Exception as e:
        log.warning(f"  FAIL {name}: {type(e).__name__}: {str(e)[:120]}")
        return None


def fetch_shibor():
    """SHIBOR interbank rates — works from outside China."""
    df = ak.macro_china_shibor_all()
    df["日期"] = df["日期"].astype(str)
    rows = []
    latest = df.iloc[-1]
    for tenor in ["O/N", "1W", "1M", "3M", "6M", "1Y"]:
        col = f"{tenor}-定价"
        if col in df.columns:
            for _, r in df.tail(60).iterrows():  # last 60 data points
                val = r.get(col)
                if val is not None and str(val).strip():
                    try:
                        rows.append(("SHIBOR_" + tenor.replace("/", ""), "bond", str(r["日期"]), float(val), "%"))
                    except (ValueError, TypeError):
                        pass

    snapshot = {
        "indicator": "SHIBOR_ON",
        "category": "bond",
        "latest_value": float(latest.get("O/N-定价", 0)),
        "change": float(latest.get("O/N-涨跌幅", 0)) if latest.get("O/N-涨跌幅") else None,
        "unit": "%",
        "data_date": str(latest["日期"]),
    }
    return rows, [snapshot]


def fetch_bond_yield():
    """China government bond yield curve."""
    end = datetime.now()
    start = end - timedelta(days=300)
    df = ak.bond_china_yield(
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
    )
    rows = []
    # Filter for the main curve
    main = df[df["曲线名称"] == "中债国债收益率曲线(到期)"] if "曲线名称" in df.columns else df
    if main.empty:
        main = df
    for _, r in main.iterrows():
        date_str = str(r.get("日期", ""))
        for tenor in ["1年", "3年", "5年", "10年", "30年"]:
            if tenor in r and r[tenor] is not None:
                try:
                    rows.append((f"CGB_YIELD_{tenor}", "bond", date_str, float(r[tenor]), "%"))
                except (ValueError, TypeError):
                    pass

    snapshots = []
    if not main.empty:
        latest = main.iloc[-1]
        for tenor, label in [("10年", "CGB_10Y"), ("1年", "CGB_1Y")]:
            if tenor in latest:
                try:
                    snapshots.append({
                        "indicator": label,
                        "category": "bond",
                        "latest_value": float(latest[tenor]),
                        "change": None,
                        "unit": "%",
                        "data_date": str(latest.get("日期", "")),
                    })
                except (ValueError, TypeError):
                    pass
    return rows, snapshots


def fetch_stock_indices():
    """Major stock indices — Shanghai Composite, CSI 300, etc."""
    indices = {
        "000001": "SSE_Composite",
        "399001": "SZSE_Component",
        "000300": "CSI_300",
        "399006": "ChiNext",
    }
    rows = []
    snapshots = []
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

    for code, name in indices.items():
        try:
            df = ak.index_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end)
            if df.empty:
                continue
            for _, r in df.iterrows():
                try:
                    rows.append((name, "equity", str(r["日期"]), float(r["收盘"]), "points"))
                except (ValueError, TypeError):
                    pass
            latest = df.iloc[-1]
            snapshots.append({
                "indicator": name,
                "category": "equity",
                "latest_value": float(latest["收盘"]),
                "change": float(latest["涨跌幅"]) if "涨跌幅" in latest else None,
                "unit": "points",
                "data_date": str(latest["日期"]),
            })
            log.info(f"  OK  {name} ({code})")
        except Exception as e:
            log.warning(f"  FAIL {name} ({code}): {type(e).__name__}: {str(e)[:80]}")

    return rows, snapshots


def fetch_forex():
    """USD/CNH and EUR/CNH exchange rates."""
    pairs = {"USDCNH": "USD_CNH", "EURCNH": "EUR_CNH"}
    rows = []
    snapshots = []
    for symbol, name in pairs.items():
        try:
            df = ak.forex_hist_em(symbol=symbol)
            if df.empty:
                continue
            for _, r in df.tail(90).iterrows():
                try:
                    rows.append((name, "fx", str(r["日期"]), float(r["最新价"]), "CNH"))
                except (ValueError, TypeError):
                    pass
            latest = df.iloc[-1]
            snapshots.append({
                "indicator": name,
                "category": "fx",
                "latest_value": float(latest["最新价"]),
                "change": None,
                "unit": "CNH",
                "data_date": str(latest["日期"]),
            })
            log.info(f"  OK  {name}")
        except Exception as e:
            log.warning(f"  FAIL {name}: {type(e).__name__}: {str(e)[:80]}")

    return rows, snapshots


def fetch_macro_cpi():
    """China CPI monthly."""
    df = ak.macro_china_cpi_monthly()
    rows = []
    for _, r in df.tail(36).iterrows():
        try:
            rows.append(("CPI_MoM", "macro", str(r.get("日期", "")), float(r.get("今值", 0)), "%"))
        except (ValueError, TypeError):
            pass
    snapshot = []
    if not df.empty:
        latest = df.iloc[-1]
        try:
            snapshot.append({
                "indicator": "CPI_MoM",
                "category": "macro",
                "latest_value": float(latest.get("今值", 0)),
                "change": None,
                "unit": "%",
                "data_date": str(latest.get("日期", "")),
            })
        except (ValueError, TypeError):
            pass
    return rows, snapshot


def fetch_macro_pmi():
    """China PMI."""
    df = ak.macro_china_pmi_yearly()
    rows = []
    for _, r in df.tail(36).iterrows():
        try:
            rows.append(("PMI", "macro", str(r.get("日期", "")), float(r.get("今值", 0)), "index"))
        except (ValueError, TypeError):
            pass
    snapshot = []
    if not df.empty:
        latest = df.iloc[-1]
        try:
            snapshot.append({
                "indicator": "PMI",
                "category": "macro",
                "latest_value": float(latest.get("今值", 0)),
                "change": None,
                "unit": "index",
                "data_date": str(latest.get("日期", "")),
            })
        except (ValueError, TypeError):
            pass
    return rows, snapshot


def fetch_trade():
    """China exports and imports YoY."""
    indicators = [
        ("macro_china_exports_yoy", "Exports_YoY"),
        ("macro_china_imports_yoy", "Imports_YoY"),
        ("macro_china_trade_balance", "Trade_Balance"),
    ]
    rows = []
    snapshots = []
    for func_name, label in indicators:
        try:
            fn = getattr(ak, func_name)
            df = fn()
            unit = "%" if "YoY" in label else "USD_100M"
            for _, r in df.tail(24).iterrows():
                try:
                    rows.append((label, "trade", str(r.get("日期", "")), float(r.get("今值", 0)), unit))
                except (ValueError, TypeError):
                    pass
            if not df.empty:
                latest = df.iloc[-1]
                snapshots.append({
                    "indicator": label,
                    "category": "trade",
                    "latest_value": float(latest.get("今值", 0)),
                    "change": None,
                    "unit": unit,
                    "data_date": str(latest.get("日期", "")),
                })
            log.info(f"  OK  {label}")
        except Exception as e:
            log.warning(f"  FAIL {label}: {type(e).__name__}: {str(e)[:80]}")
    return rows, snapshots


# ---------------------------------------------------------------------------
# Store results
# ---------------------------------------------------------------------------

def store_financial_data(conn: sqlite3.Connection, rows: list, snapshots: list):
    now = datetime.utcnow().isoformat()
    inserted = 0
    for indicator, category, date, value, unit in rows:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO financial_series "
                "(indicator, category, date, value, unit, fetched_at) VALUES (?,?,?,?,?,?)",
                (indicator, category, date, value, unit, now),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    for snap in snapshots:
        conn.execute(
            "INSERT INTO financial_snapshots "
            "(indicator, category, latest_value, change, unit, data_date, fetched_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (snap["indicator"], snap["category"], snap["latest_value"],
             snap.get("change"), snap["unit"], snap["data_date"], now),
        )
    conn.commit()
    return inserted


def get_latest_snapshots(conn: sqlite3.Connection) -> list[dict]:
    """Return the most recent snapshot for each indicator."""
    cur = conn.execute("""
        SELECT indicator, category, latest_value, change, unit, data_date, fetched_at
        FROM financial_snapshots
        WHERE id IN (
            SELECT MAX(id) FROM financial_snapshots GROUP BY indicator
        )
        ORDER BY category, indicator
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_series(conn: sqlite3.Connection, indicator: str, limit: int = 90) -> list[dict]:
    """Return time series for a specific indicator."""
    cur = conn.execute(
        "SELECT date, value FROM financial_series WHERE indicator = ? ORDER BY date DESC LIMIT ?",
        (indicator, limit),
    )
    return [{"date": row[0], "value": row[1]} for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def fetch_all_financial():
    """Run all fetchers, return (all_rows, all_snapshots)."""
    fetchers = [
        ("SHIBOR", fetch_shibor),
        ("Gov Bond Yield", fetch_bond_yield),
        ("Stock Indices", fetch_stock_indices),
        ("Forex", fetch_forex),
        ("CPI", fetch_macro_cpi),
        ("PMI", fetch_macro_pmi),
        ("Trade", fetch_trade),
    ]

    all_rows = []
    all_snapshots = []
    ok = 0
    fail = 0

    for name, fn in fetchers:
        result = _safe_fetch(name, fn)
        if result:
            rows, snaps = result
            all_rows.extend(rows)
            all_snapshots.extend(snaps)
            ok += 1
        else:
            fail += 1

    return all_rows, all_snapshots, ok, fail


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Fetch Chinese financial data")
    parser.add_argument("--show", action="store_true", help="Show latest snapshots from DB")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Output as JSON")
    args = parser.parse_args()

    conn = get_financial_db()

    if args.show:
        snapshots = get_latest_snapshots(conn)
        if args.as_json:
            print(json.dumps(snapshots, ensure_ascii=False, indent=2))
        else:
            print(f"{'Indicator':<20} {'Value':>12} {'Change':>10} {'Unit':<10} {'Date':<12}")
            print("-" * 70)
            for s in snapshots:
                chg = f"{s['change']:+.2f}" if s["change"] is not None else "n/a"
                print(f"{s['indicator']:<20} {s['latest_value']:>12.4f} {chg:>10} {s['unit']:<10} {s['data_date']:<12}")
        conn.close()
        return

    log.info("Fetching financial data via AKShare...")
    all_rows, all_snapshots, ok, fail = fetch_all_financial()

    inserted = store_financial_data(conn, all_rows, all_snapshots)
    log.info(f"Done: {ok} OK, {fail} failed, {inserted} data points, {len(all_snapshots)} snapshots")

    total = conn.execute("SELECT COUNT(*) FROM financial_series").fetchone()[0]
    log.info(f"Total in DB: {total} data points")
    conn.close()


if __name__ == "__main__":
    main()
