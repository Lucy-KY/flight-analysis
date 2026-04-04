"""
OpenSky Network API Daily Fetcher
===================================
Fetches the previous day's flight departure/arrival data for major US airports
using the FREE OpenSky Network REST API.

API Docs: https://openskynetwork.github.io/opensky-api/rest.html

Free tier capabilities:
  - /states/all          : real-time flight states (anonymous or registered)
  - /flights/departure   : departures from airport (registered, last 30 days)
  - /flights/arrival     : arrivals at airport (registered, last 30 days)
  Rate limit: 100 requests/10s (anonymous), 1000/10s (registered)

Usage:
    python opensky_fetcher.py --date 2025-04-02 --output ./raw_data/opensky
    python opensky_fetcher.py --yesterday --output ./raw_data/opensky
"""

import argparse
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

OPENSKY_BASE = "https://opensky-network.org/api"

# Top 50 busiest US airports (ICAO codes)
US_AIRPORTS_ICAO = [
    "KATL", "KDFW", "KDEN", "KORD", "KLAX",
    "KJFK", "KLAS", "KMCO", "KMIA", "KCLT",
    "KSEA", "KPHX", "KEWR", "KSFO", "KIAH",
    "KBOS", "KFLL", "KMSP", "KLGA", "KBWI",
    "KDTW", "KPHL", "KSLC", "KDCA", "KIAD",
    "KMDW", "KSAN", "KTPA", "PDXP", "KPDX",
    "KHOU", "KBNA", "KMEM", "KAUS", "KSTL",
    "KONT", "KSNA", "KOAK", "KCLE", "KRDU",
    "KMSY", "KSMF", "KSJC", "KPIT", "KRSW",
    "KCVG", "KIND", "KABQ", "KELP", "KBUF",
]


class OpenSkyFetcher:
    def __init__(self, username: str = None, password: str = None):
        self.session = requests.Session()
        if username and password:
            self.session.auth = (username, password)
            logger.info("OpenSky: authenticated as %s", username)
        else:
            logger.info("OpenSky: anonymous mode (limited rate)")
        self.request_count = 0
        self.last_request_time = 0.0

    def _rate_limit(self, min_interval: float = 0.5):
        """Simple rate limiter to avoid 429 responses."""
        elapsed = time.time() - self.last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.time()
        self.request_count += 1

    def get_states(self, bounding_box: dict = None) -> dict:
        """
        Get current flight states (real-time snapshot).
        bounding_box: {'lamin': 24.0, 'lamax': 50.0, 'lomin': -125.0, 'lomax': -66.0}
        US bounding box used by default.
        """
        self._rate_limit()
        params = bounding_box or {
            "lamin": 24.0, "lamax": 50.0,
            "lomin": -125.0, "lomax": -66.0,
        }
        resp = self.session.get(f"{OPENSKY_BASE}/states/all", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_departures(self, airport_icao: str, begin_ts: int, end_ts: int,
                       retry: int = 3) -> list:
        """
        Get departures from an airport in a time window.
        Requires registered account; works for data within last 30 days.
        Returns list of flight dicts.
        """
        for attempt in range(1, retry + 1):
            try:
                self._rate_limit(1.0)
                params = {
                    "airport": airport_icao,
                    "begin": begin_ts,
                    "end": end_ts,
                }
                resp = self.session.get(
                    f"{OPENSKY_BASE}/flights/departure",
                    params=params,
                    timeout=30,
                )
                if resp.status_code == 404:
                    return []  # No data for this airport/window
                if resp.status_code == 429:
                    wait = 10 * attempt
                    logger.warning("Rate limited, waiting %ds", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json() or []
            except requests.exceptions.RequestException as e:
                logger.warning("Attempt %d failed for %s: %s", attempt, airport_icao, e)
                if attempt < retry:
                    time.sleep(5 * attempt)
        return []

    def get_arrivals(self, airport_icao: str, begin_ts: int, end_ts: int,
                     retry: int = 3) -> list:
        """Get arrivals at an airport in a time window."""
        for attempt in range(1, retry + 1):
            try:
                self._rate_limit(1.0)
                params = {
                    "airport": airport_icao,
                    "begin": begin_ts,
                    "end": end_ts,
                }
                resp = self.session.get(
                    f"{OPENSKY_BASE}/flights/arrival",
                    params=params,
                    timeout=30,
                )
                if resp.status_code == 404:
                    return []
                if resp.status_code == 429:
                    wait = 10 * attempt
                    logger.warning("Rate limited, waiting %ds", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json() or []
            except requests.exceptions.RequestException as e:
                logger.warning("Attempt %d failed for %s: %s", attempt, airport_icao, e)
                if attempt < retry:
                    time.sleep(5 * attempt)
        return []


def fetch_day(fetcher: OpenSkyFetcher, target_date: date, output_dir: Path,
              airports: list = None) -> dict:
    """
    Fetch all departures from US airports for a given date.
    Saves raw JSON + summary.
    """
    airports = airports or US_AIRPORTS_ICAO
    date_str = target_date.strftime("%Y-%m-%d")
    out_dir = output_dir / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    # Unix timestamps for the full day (UTC)
    dt_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    dt_end   = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)
    begin_ts = int(dt_start.timestamp())
    end_ts   = int(dt_end.timestamp())

    all_departures = []
    all_arrivals   = []
    summary = {"date": date_str, "airports_fetched": 0, "total_departures": 0, "total_arrivals": 0}

    for i, airport in enumerate(airports, 1):
        logger.info("[%d/%d] %s", i, len(airports), airport)

        deps = fetcher.get_departures(airport, begin_ts, end_ts)
        for d in deps:
            d["fetch_date"]      = date_str
            d["fetch_airport"]   = airport
            d["direction"]       = "departure"
        all_departures.extend(deps)

        arrs = fetcher.get_arrivals(airport, begin_ts, end_ts)
        for a in arrs:
            a["fetch_date"]    = date_str
            a["fetch_airport"] = airport
            a["direction"]     = "arrival"
        all_arrivals.extend(arrs)

        summary["airports_fetched"] += 1
        logger.info("  deps=%d arrs=%d", len(deps), len(arrs))

    summary["total_departures"] = len(all_departures)
    summary["total_arrivals"]   = len(all_arrivals)

    # Save raw JSON
    all_flights = all_departures + all_arrivals
    flights_path = out_dir / "flights.json"
    with open(flights_path, "w") as f:
        json.dump(all_flights, f)
    logger.info("Saved %d flight records → %s", len(all_flights), flights_path)

    # Save summary
    summary_path = out_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    return summary


def fetch_states_snapshot(fetcher: OpenSkyFetcher, output_dir: Path) -> dict:
    """Fetch a real-time snapshot of all US flight states."""
    ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = output_dir / "states"
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching real-time US flight states...")
    data = fetcher.get_states()
    states = data.get("states", [])

    # Column mapping from OpenSky API docs
    columns = [
        "icao24", "callsign", "origin_country", "time_position",
        "last_contact", "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source",
    ]

    records = []
    for state in states:
        rec = dict(zip(columns, state))
        rec["fetch_ts"] = ts_str
        records.append(rec)

    out_path = out_dir / f"states_{ts_str}.json"
    with open(out_path, "w") as f:
        json.dump(records, f)

    summary = {"fetch_ts": ts_str, "total_states": len(records)}
    logger.info("Saved %d state records → %s", len(records), out_path)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Fetch daily flight data from OpenSky Network")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date",      help="Fetch date YYYY-MM-DD")
    group.add_argument("--yesterday", action="store_true", help="Fetch yesterday's data")
    parser.add_argument("--output",      default="./raw_data/opensky", help="Output directory")
    parser.add_argument("--states",      action="store_true",
                        help="Also fetch real-time state snapshot (free tier)")
    parser.add_argument("--states-only", action="store_true",
                        help="Only fetch real-time states; skip departure/arrival "
                             "(use when OpenSky Membership not available)")
    args = parser.parse_args()

    if args.yesterday or not args.date:
        target = date.today() - timedelta(days=1)
    else:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()

    username = os.getenv("OPENSKY_USERNAME")
    password = os.getenv("OPENSKY_PASSWORD")

    fetcher    = OpenSkyFetcher(username=username, password=password)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.states_only:
        # Free tier path: only real-time state vectors, skip departure/arrival
        logger.info("--states-only mode: fetching /states/all (free tier)")
        summary = fetch_states_snapshot(fetcher, output_dir)
        logger.info("\nStates summary: %s", json.dumps(summary, indent=2))
        return

    if args.states:
        fetch_states_snapshot(fetcher, output_dir)

    # departure/arrival fetch (requires OpenSky Membership)
    summary = fetch_day(fetcher, target, output_dir)
    logger.info("\nSummary: %s", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
