"""
Fetch live ship positions around China from AIS data providers.

Supports two backends (configured in data/config.json):
  1. AISHub REST API (recommended) — free, requires registration + sharing AIS data
     Register at aishub.net, get a username/API key.
  2. AISStream WebSocket — free, requires API key from aisstream.io

Data is stored in data/feeds.db in the `ship_positions` table.

Usage:
    python ships.py              # fetch ship positions, store to DB
    python ships.py --show       # show stored positions
    python ships.py --duration 30  # AISStream collection duration (seconds)
"""

import argparse
import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("ships")

SHIPS_SCHEMA = """
CREATE TABLE IF NOT EXISTS ship_positions (
    mmsi            TEXT PRIMARY KEY,
    ship_name       TEXT,
    latitude        REAL,
    longitude       REAL,
    cog             REAL,
    sog             REAL,
    heading         REAL,
    nav_status      INTEGER,
    destination     TEXT,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ship_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_ts     TEXT NOT NULL,
    mmsi            TEXT NOT NULL,
    ship_name       TEXT,
    latitude        REAL,
    longitude       REAL,
    cog             REAL,
    sog             REAL,
    heading         REAL,
    nav_status      INTEGER,
    destination     TEXT
);
CREATE INDEX IF NOT EXISTS idx_sh_ts ON ship_history(snapshot_ts);
CREATE INDEX IF NOT EXISTS idx_sh_mmsi ON ship_history(mmsi);
"""

# China maritime bounding box
CHINA_BBOX = dict(latmin=18, latmax=54, lonmin=73, lonmax=135)

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
AISHUB_URL = "https://data.aishub.net/ws.php"

CONFIG_PATH = Path(__file__).parent.parent / "data" / "config.json"


def _load_config() -> dict:
    """Load ship tracking config from env vars or data/config.json."""
    cfg = {}
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text())
        except (json.JSONDecodeError, KeyError):
            pass

    return {
        "aisstream_api_key": os.environ.get("AISSTREAM_API_KEY") or cfg.get("aisstream_api_key") or "",
        "aishub_username": os.environ.get("AISHUB_USERNAME") or cfg.get("aishub_username") or "",
    }


# Keep backward compat for dashboard imports
def _load_api_key() -> str | None:
    cfg = _load_config()
    return cfg["aisstream_api_key"] or cfg["aishub_username"] or None


def get_ships_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SHIPS_SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Backend 1: AISHub REST API (simple HTTP GET)
# ---------------------------------------------------------------------------

def fetch_aishub(username: str) -> list[dict]:
    """Fetch vessel positions from AISHub REST API."""
    params = {
        "username": username,
        "format": "1",       # 1 = AIS format
        "output": "json",
        "compress": "0",     # no compression
        "latmin": CHINA_BBOX["latmin"],
        "latmax": CHINA_BBOX["latmax"],
        "lonmin": CHINA_BBOX["lonmin"],
        "lonmax": CHINA_BBOX["lonmax"],
    }
    resp = requests.get(AISHUB_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # AISHub returns a list with metadata in [0] and vessel array in [1]
    if isinstance(data, list) and len(data) >= 2:
        vessels = data[1]
    elif isinstance(data, list):
        vessels = data
    else:
        vessels = []

    positions = []
    for v in vessels:
        lat = v.get("LATITUDE")
        lon = v.get("LONGITUDE")
        if lat is None or lon is None:
            continue
        positions.append({
            "mmsi": str(v.get("MMSI", "")),
            "ship_name": (v.get("NAME") or "").strip(),
            "latitude": float(lat),
            "longitude": float(lon),
            "cog": v.get("COG"),
            "sog": v.get("SOG"),
            "heading": v.get("HEADING"),
            "nav_status": v.get("NAVSTAT"),
            "destination": (v.get("DEST") or "").strip() or None,
        })
    return positions


# ---------------------------------------------------------------------------
# Backend 2: AISStream WebSocket
# ---------------------------------------------------------------------------

CHINA_MARITIME_BBOX_WS = [[18, 73], [54, 135]]  # [[south_lat, west_lon], [north_lat, east_lon]]


async def _collect_ais(api_key: str, duration_seconds: int = 55) -> list[dict]:
    """Connect to AISStream WebSocket, collect position reports for duration_seconds."""
    import websockets

    positions = {}

    subscribe_msg = json.dumps({
        "APIKey": api_key,
        "BoundingBoxes": [CHINA_MARITIME_BBOX_WS],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    })

    try:
        async with websockets.connect(AISSTREAM_URL) as ws:
            await ws.send(subscribe_msg)
            end_time = asyncio.get_event_loop().time() + duration_seconds

            while asyncio.get_event_loop().time() < end_time:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(),
                        timeout=max(0.1, end_time - asyncio.get_event_loop().time()),
                    )
                except asyncio.TimeoutError:
                    break

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("MessageType", "")
                meta = msg.get("MetaData", {})
                mmsi = str(meta.get("MMSI", ""))
                if not mmsi:
                    continue

                ship_name = (meta.get("ShipName") or "").strip()

                if msg_type == "PositionReport":
                    report = msg.get("Message", {}).get("PositionReport", {})
                    positions[mmsi] = {
                        "mmsi": mmsi,
                        "ship_name": ship_name,
                        "latitude": report.get("Latitude"),
                        "longitude": report.get("Longitude"),
                        "cog": report.get("Cog"),
                        "sog": report.get("Sog"),
                        "heading": report.get("TrueHeading"),
                        "nav_status": report.get("NavigationalStatus"),
                        "destination": None,
                    }
                elif msg_type == "ShipStaticData":
                    static = msg.get("Message", {}).get("ShipStaticData", {})
                    if mmsi in positions:
                        positions[mmsi]["destination"] = (
                            static.get("Destination") or ""
                        ).strip() or None
                    else:
                        positions[mmsi] = {
                            "mmsi": mmsi,
                            "ship_name": ship_name,
                            "latitude": meta.get("latitude"),
                            "longitude": meta.get("longitude"),
                            "cog": None,
                            "sog": None,
                            "heading": None,
                            "nav_status": None,
                            "destination": (
                                static.get("Destination") or ""
                            ).strip() or None,
                        }

    except Exception as e:
        log.warning(f"AISStream connection error: {e}")

    return list(positions.values())


def run_ais_stream(api_key: str, duration_seconds: int = 55) -> list[dict]:
    """Synchronous wrapper around the async AIS collector."""
    return asyncio.run(_collect_ais(api_key, duration_seconds))


# ---------------------------------------------------------------------------
# Unified fetch: tries AISHub first, falls back to AISStream
# ---------------------------------------------------------------------------

def fetch_ship_positions(duration_seconds: int = 10) -> list[dict]:
    """Fetch ship positions from the best available backend."""
    cfg = _load_config()

    # Try AISHub REST first (fast, reliable)
    if cfg["aishub_username"]:
        try:
            positions = fetch_aishub(cfg["aishub_username"])
            if positions:
                log.info(f"AISHub: {len(positions)} vessels")
                return positions
        except Exception as e:
            log.warning(f"AISHub failed: {e}")

    # Fall back to AISStream WebSocket
    if cfg["aisstream_api_key"]:
        try:
            positions = run_ais_stream(cfg["aisstream_api_key"], duration_seconds)
            if positions:
                log.info(f"AISStream: {len(positions)} vessels")
            else:
                log.warning("AISStream: connected but received 0 messages "
                            "(API key may be invalid or not yet activated)")
            return positions
        except Exception as e:
            log.warning(f"AISStream failed: {e}")

    log.debug("No ship tracking API configured")
    return []


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def store_ship_positions(conn: sqlite3.Connection, positions: list[dict]) -> int:
    """Upsert live positions and append to history."""
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for p in positions:
        if p.get("latitude") is None or p.get("longitude") is None:
            continue

        conn.execute(
            """INSERT OR REPLACE INTO ship_positions
               (mmsi, ship_name, latitude, longitude, cog, sog,
                heading, nav_status, destination, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (p["mmsi"], p.get("ship_name"), p["latitude"], p["longitude"],
             p.get("cog"), p.get("sog"), p.get("heading"),
             p.get("nav_status"), p.get("destination"), now),
        )

        conn.execute(
            """INSERT INTO ship_history
               (snapshot_ts, mmsi, ship_name, latitude, longitude, cog, sog,
                heading, nav_status, destination)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (now, p["mmsi"], p.get("ship_name"), p["latitude"], p["longitude"],
             p.get("cog"), p.get("sog"), p.get("heading"),
             p.get("nav_status"), p.get("destination")),
        )
        inserted += 1

    conn.commit()
    return inserted


def cleanup_stale(conn: sqlite3.Connection, max_age_minutes: int = 30):
    """Remove positions older than max_age_minutes."""
    conn.execute(
        "DELETE FROM ship_positions WHERE updated_at < datetime('now', ?)",
        (f"-{max_age_minutes} minutes",),
    )
    conn.commit()


def get_current_ships(conn: sqlite3.Connection) -> list[dict]:
    """Return all current ship positions as list of dicts."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ship_positions").fetchall()
    return [dict(r) for r in rows]


def main():
    parser = argparse.ArgumentParser(description="Fetch ship positions around China")
    parser.add_argument("--show", action="store_true", help="Show stored positions")
    parser.add_argument("--duration", type=int, default=55,
                        help="AIS stream collection duration in seconds (default: 55)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    conn = get_ships_db()

    if args.show:
        positions = get_current_ships(conn)
        if not positions:
            print("No ship positions stored.")
        else:
            print(f"{len(positions)} ships:")
            for p in positions[:20]:
                spd = f"{p['sog']:.1f}kts" if p["sog"] is not None else "?"
                print(f"  {p['ship_name'] or p['mmsi']:20s}  "
                      f"MMSI={p['mmsi']}  "
                      f"({p['latitude']:.4f}, {p['longitude']:.4f})  "
                      f"spd={spd}")
            if len(positions) > 20:
                print(f"  ... and {len(positions) - 20} more")
    else:
        log.info(f"Fetching ship positions...")
        try:
            positions = fetch_ship_positions(duration_seconds=args.duration)
            n = store_ship_positions(conn, positions)
            cleanup_stale(conn)
            log.info(f"Stored {n} ship positions.")
        except Exception as e:
            log.error(f"Ship fetch error: {e}")

    conn.close()


if __name__ == "__main__":
    main()
