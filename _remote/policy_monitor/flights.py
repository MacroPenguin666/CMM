"""
Fetch live flight positions over China from the OpenSky Network API.

Data is stored in data/feeds.db in the `flight_positions` table.
Works anonymously (100 calls/day ≈ 1 poll/15min) or with credentials
(4,000 calls/day ≈ 1 poll/min).  Register at opensky-network.org.

Usage:
    python flights.py              # fetch current positions, store to DB
    python flights.py --show       # show stored positions
"""

import argparse
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("flights")

FLIGHTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS flight_positions (
    icao24          TEXT PRIMARY KEY,
    callsign        TEXT,
    origin_country  TEXT,
    longitude       REAL,
    latitude        REAL,
    geo_altitude    REAL,
    baro_altitude   REAL,
    velocity        REAL,
    heading         REAL,
    vertical_rate   REAL,
    on_ground       INTEGER,
    squawk          TEXT,
    last_contact    INTEGER,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS flight_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_ts     TEXT NOT NULL,
    icao24          TEXT NOT NULL,
    callsign        TEXT,
    origin_country  TEXT,
    longitude       REAL,
    latitude        REAL,
    geo_altitude    REAL,
    baro_altitude   REAL,
    velocity        REAL,
    heading         REAL,
    vertical_rate   REAL,
    on_ground       INTEGER,
    squawk          TEXT,
    last_contact    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_fh_ts ON flight_history(snapshot_ts);
CREATE INDEX IF NOT EXISTS idx_fh_icao ON flight_history(icao24);
"""

# China bounding box (covers mainland + Taiwan + nearby airspace)
CHINA_BBOX = dict(lamin=18, lamax=54, lomin=73, lomax=135)

OPENSKY_URL = "https://opensky-network.org/api/states/all"

CONFIG_PATH = Path(__file__).parent.parent / "data" / "config.json"


def _load_credentials():
    """Load OpenSky credentials from env vars or data/config.json."""
    username = os.environ.get("OPENSKY_USERNAME")
    password = os.environ.get("OPENSKY_PASSWORD")
    if username and password:
        return username, password
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text())
            u = cfg.get("opensky_username", "")
            p = cfg.get("opensky_password", "")
            if u and p:
                return u, p
        except (json.JSONDecodeError, KeyError):
            pass
    return None, None


def get_flights_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(FLIGHTS_SCHEMA)
    return conn


def fetch_flight_positions(username=None, password=None) -> list[dict]:
    """Fetch current flight positions over China from OpenSky Network."""
    if username is None:
        username, password = _load_credentials()

    auth = (username, password) if username else None
    params = CHINA_BBOX.copy()

    resp = requests.get(OPENSKY_URL, params=params, auth=auth, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    states = data.get("states") or []
    positions = []
    for s in states:
        if len(s) < 17:
            continue
        if s[6] is None or s[5] is None:  # lat/lon required
            continue
        positions.append({
            "icao24": s[0],
            "callsign": (s[1] or "").strip(),
            "origin_country": s[2],
            "longitude": s[5],
            "latitude": s[6],
            "geo_altitude": s[7],
            "baro_altitude": s[13],
            "velocity": s[9],
            "heading": s[10],
            "vertical_rate": s[11],
            "on_ground": 1 if s[8] else 0,
            "squawk": s[14],
            "last_contact": s[4],
        })
    return positions


def store_flight_positions(conn: sqlite3.Connection, positions: list[dict]) -> int:
    """Replace live positions with a fresh snapshot and append to history."""
    now = datetime.now(timezone.utc).isoformat()

    # Live table — full replace
    conn.execute("DELETE FROM flight_positions")
    for p in positions:
        conn.execute(
            """INSERT INTO flight_positions
               (icao24, callsign, origin_country, longitude, latitude,
                geo_altitude, baro_altitude, velocity, heading, vertical_rate,
                on_ground, squawk, last_contact, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (p["icao24"], p["callsign"], p["origin_country"],
             p["longitude"], p["latitude"], p["geo_altitude"],
             p["baro_altitude"], p["velocity"], p["heading"],
             p["vertical_rate"], p["on_ground"], p["squawk"],
             p["last_contact"], now),
        )

    # History table — append all
    for p in positions:
        conn.execute(
            """INSERT INTO flight_history
               (snapshot_ts, icao24, callsign, origin_country, longitude, latitude,
                geo_altitude, baro_altitude, velocity, heading, vertical_rate,
                on_ground, squawk, last_contact)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (now, p["icao24"], p["callsign"], p["origin_country"],
             p["longitude"], p["latitude"], p["geo_altitude"],
             p["baro_altitude"], p["velocity"], p["heading"],
             p["vertical_rate"], p["on_ground"], p["squawk"],
             p["last_contact"]),
        )

    conn.commit()
    return len(positions)


def get_current_flights(conn: sqlite3.Connection) -> list[dict]:
    """Return all current flight positions as list of dicts."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM flight_positions").fetchall()
    return [dict(r) for r in rows]


def main():
    parser = argparse.ArgumentParser(description="Fetch flight positions over China")
    parser.add_argument("--show", action="store_true", help="Show stored positions")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    conn = get_flights_db()

    if args.show:
        positions = get_current_flights(conn)
        if not positions:
            print("No flight positions stored.")
        else:
            print(f"{len(positions)} flights:")
            for p in positions[:20]:
                alt = f"{p['geo_altitude']:.0f}m" if p["geo_altitude"] else "?"
                spd = f"{p['velocity']:.0f}m/s" if p["velocity"] else "?"
                print(f"  {p['callsign'] or p['icao24']:8s}  "
                      f"{p['origin_country']:15s}  "
                      f"({p['latitude']:.2f}, {p['longitude']:.2f})  "
                      f"alt={alt}  spd={spd}")
            if len(positions) > 20:
                print(f"  ... and {len(positions) - 20} more")
    else:
        log.info("Fetching flight positions from OpenSky Network...")
        try:
            positions = fetch_flight_positions()
            n = store_flight_positions(conn, positions)
            log.info(f"Stored {n} flight positions.")
        except Exception as e:
            log.error(f"Flight fetch error: {e}")

    conn.close()


if __name__ == "__main__":
    main()
