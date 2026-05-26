"""
Yahoo Finance market data fetcher.
Uses the yfinance library (pip install yfinance).

Table: yfinance_daily in data/yfinance.db

Tickers fetched (China macro focus):
  Equity indices — Shanghai, Shenzhen, Hang Seng, Nikkei, S&P 500, DAX
  China ETFs     — FXI, MCHI, KWEB, ASHR (A-shares)
  FX             — USD/CNY, EUR/CNY, JPY/CNY, GBP/CNY, USD/HKD, DXY
  Commodities    — Crude oil (WTI), Brent, Gold, Copper, Iron ore proxy, Soybeans
  Rates          — US 10Y, US 3M T-bill, VIX
  HK / ADR       — Alibaba, Tencent (HK), BYD (HK), CNOOC (HK)

Fetch strategy:
  First run  : HISTORY_YEARS of daily data
  Subsequent : last 30 days (fills any gaps, handles weekends gracefully)
"""

import logging
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from policy_monitor.storage import DB_DIR

log = logging.getLogger("yfinance")

YFINANCE_DB = DB_DIR / "yfinance.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS yfinance_daily (
    ticker      TEXT    NOT NULL,
    date        TEXT    NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    adj_close   REAL,
    volume      REAL,
    fetched_at  TEXT    NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_yf_ticker ON yfinance_daily(ticker);
CREATE INDEX IF NOT EXISTS idx_yf_date   ON yfinance_daily(date);

CREATE TABLE IF NOT EXISTS yfinance_meta (
    ticker       TEXT PRIMARY KEY,
    short_name   TEXT,
    category     TEXT,
    currency     TEXT,
    exchange     TEXT,
    last_updated TEXT
);
"""

# (ticker, category, description)
TICKERS: list[tuple[str, str, str]] = [
    # China equity indices
    ("000001.SS", "equity_index", "Shanghai Composite Index"),
    ("399001.SZ", "equity_index", "Shenzhen Component Index"),
    ("^HSI",      "equity_index", "Hang Seng Index"),
    ("^HSCE",     "equity_index", "Hang Seng China Enterprises (H-shares)"),
    # Global indices (comparison)
    ("^N225",     "equity_index", "Nikkei 225"),
    ("^GSPC",     "equity_index", "S&P 500"),
    ("^GDAXI",    "equity_index", "DAX 40"),
    # China ETFs (USD-denominated)
    ("FXI",       "etf_china",    "iShares China Large-Cap ETF"),
    ("MCHI",      "etf_china",    "iShares MSCI China ETF"),
    ("KWEB",      "etf_china",    "KraneShares China Internet ETF"),
    ("ASHR",      "etf_china",    "Xtrackers CSI 300 China A-Shares ETF"),
    ("GXC",       "etf_china",    "SPDR S&P China ETF"),
    # Foreign exchange
    ("USDCNY=X",  "fx",           "USD/CNY spot rate"),
    ("EURCNY=X",  "fx",           "EUR/CNY spot rate"),
    ("JPYCNY=X",  "fx",           "JPY/CNY spot rate"),
    ("GBPCNY=X",  "fx",           "GBP/CNY spot rate"),
    ("USDCNH=X",  "fx",           "USD/CNH offshore renminbi"),
    ("USDHKD=X",  "fx",           "USD/HKD spot rate"),
    ("DX-Y.NYB",  "fx",           "US Dollar Index (DXY)"),
    # Commodities (China is major consumer of all)
    ("CL=F",      "commodity",    "WTI Crude Oil front-month futures"),
    ("BZ=F",      "commodity",    "Brent Crude Oil futures"),
    ("GC=F",      "commodity",    "Gold futures"),
    ("HG=F",      "commodity",    "Copper futures (LME proxy)"),
    ("ZS=F",      "commodity",    "Soybean futures"),
    ("ZC=F",      "commodity",    "Corn futures"),
    # Interest rates / credit
    ("^TNX",      "rates",        "US 10-Year Treasury yield"),
    ("^IRX",      "rates",        "US 13-Week T-Bill yield"),
    ("^VIX",      "rates",        "CBOE Volatility Index (VIX)"),
    # Major HK-listed Chinese stocks (USD-equivalent exposure)
    ("9988.HK",   "equity_cn",    "Alibaba Group (HK)"),
    ("0700.HK",   "equity_cn",    "Tencent Holdings (HK)"),
    ("1211.HK",   "equity_cn",    "BYD Co. (HK)"),
    ("0883.HK",   "equity_cn",    "CNOOC Ltd (HK)"),
    ("0939.HK",   "equity_cn",    "China Construction Bank (HK)"),
]

HISTORY_YEARS = 5


def get_yfinance_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(YFINANCE_DB))
    conn.executescript(_SCHEMA)
    return conn


def _latest_date(conn: sqlite3.Connection, ticker: str) -> str | None:
    try:
        cur = conn.execute(
            "SELECT MAX(date) FROM yfinance_daily WHERE ticker = ?", (ticker,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except sqlite3.OperationalError:
        return None


def fetch_all(conn: sqlite3.Connection, force_full: bool = False,
              tickers: list[str] | None = None) -> dict[str, int]:
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance not installed — run: pip install yfinance")
        return {}

    now = datetime.now(timezone.utc).isoformat()
    today = date.today()
    history_start = (today - timedelta(days=HISTORY_YEARS * 365)).isoformat()
    incremental_start = (today - timedelta(days=35)).isoformat()

    target_tickers = tickers or [t[0] for t in TICKERS]
    results: dict[str, int] = {}

    # Upsert metadata
    meta_rows = [(ticker, desc, cat, "", "", now)
                 for ticker, cat, desc in TICKERS if ticker in target_tickers]
    conn.executemany(
        "INSERT OR REPLACE INTO yfinance_meta "
        "(ticker, short_name, category, currency, exchange, last_updated) "
        "VALUES (?,?,?,?,?,?)",
        meta_rows,
    )

    for ticker, cat, desc in TICKERS:
        if ticker not in target_tickers:
            continue

        latest = _latest_date(conn, ticker)
        if force_full or not latest:
            start = history_start
            log.info(f"  {ticker:<15} FULL from {start}")
        else:
            start = incremental_start
            log.debug(f"  {ticker:<15} incremental from {start} (latest: {latest})")

        try:
            yf_ticker = yf.Ticker(ticker)
            hist = yf_ticker.history(start=start, end=today.isoformat(), interval="1d", auto_adjust=True)
            if hist.empty:
                log.debug(f"  {ticker}: no data")
                results[ticker] = 0
                continue

            records = []
            for idx, row in hist.iterrows():
                date_str = str(idx.date())
                records.append((
                    ticker,
                    date_str,
                    float(row.get("Open",  0) or 0),
                    float(row.get("High",  0) or 0),
                    float(row.get("Low",   0) or 0),
                    float(row.get("Close", 0) or 0),
                    float(row.get("Close", 0) or 0),  # adj_close = close when auto_adjust=True
                    float(row.get("Volume", 0) or 0),
                    now,
                ))

            conn.executemany(
                "INSERT OR REPLACE INTO yfinance_daily "
                "(ticker, date, open, high, low, close, adj_close, volume, fetched_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                records,
            )
            conn.commit()
            results[ticker] = len(records)
            log.info(f"  {ticker:<15} {len(records):>5} rows  ({start} → {today})")
            time.sleep(0.3)

        except Exception as exc:
            log.warning(f"  {ticker}: error — {exc}")
            results[ticker] = 0

    # Update metadata with currency/exchange from yfinance info
    conn.commit()
    return results
