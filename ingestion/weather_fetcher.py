import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARS = (
    "temperature_2m,precipitation,rain,snowfall,snow_depth,"
    "wind_speed_10m,wind_gusts_10m,wind_direction_10m,"
    "visibility,cloud_cover,weather_code,pressure_msl"
)

# Seconds to sleep between every successful request (workers=1 → ~1 req/7s)
REQUEST_INTERVAL = 7.0
# Seconds to wait after a 429 before retrying
RATE_LIMIT_WAIT = 120.0
# Max number of 429 retries per task
MAX_RETRIES = 4

DEFAULT_AIRPORTS = [
    "ATL", "DFW", "DEN", "ORD", "LAX",
    "JFK", "LAS", "MCO", "MIA", "CLT",
    "SEA", "PHX", "EWR", "SFO", "IAH",
    "BOS", "FLL", "MSP", "LGA", "BWI",
    "DTW", "PHL", "SLC", "DCA", "IAD",
    "MDW", "SAN", "TPA", "PDX", "HOU",
    "BNA", "AUS", "STL", "OAK", "MCI",
    "SMF", "SJC", "PIT", "RDU", "MSY",
    "SAT", "CVG", "IND", "CMH", "MKE",
    "SNA", "OGG", "HNL", "ANC",
]


def fetch_airport_year(
    iata: str,
    year: int,
    lat: float,
    lon: float,
    tz: str,
    output_dir: Path,
) -> dict:
    """
    Fetch one airport × one year of hourly weather from Open-Meteo.

    Returns {"status": "ok"|"skip"|"error", "file": str, "records": int}
    """
    airport_dir = output_dir / iata
    airport_dir.mkdir(parents=True, exist_ok=True)
    out_file = airport_dir / f"{year}.json"

    if out_file.exists():
        logger.debug("Skip (exists): %s", out_file)
        return {"status": "skip", "file": str(out_file), "records": 0}

    params = {
        "latitude":        lat,
        "longitude":       lon,
        "start_date":      f"{year}-01-01",
        "end_date":        f"{year}-12-31",
        "hourly":          HOURLY_VARS,
        "timezone":        tz,
        "wind_speed_unit": "kmh",
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(OPEN_METEO_URL, params=params, timeout=60)

            if resp.status_code == 429:
                # Honour Retry-After if present, else use fixed wait
                raw_ra = resp.headers.get("Retry-After", "")
                try:
                    wait = int(raw_ra) + 5
                except (ValueError, TypeError):
                    wait = RATE_LIMIT_WAIT
                logger.warning("429 for %s %d — waiting %ds (attempt %d/%d)",
                               iata, year, wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
                continue   # one retry

            resp.raise_for_status()
            data = resp.json()
            data["iata_code"] = iata

            n_records = len(data.get("hourly", {}).get("time", []))
            with open(out_file, "w") as f:
                json.dump(data, f)

            logger.info("Fetched %s %d → %d hourly records", iata, year, n_records)
            time.sleep(REQUEST_INTERVAL)   # pace after success
            return {"status": "ok", "file": str(out_file), "records": n_records}

        except requests.exceptions.HTTPError as e:
            msg = f"HTTP {e.response.status_code}: {e}"
            logger.warning("Error %s %d: %s", iata, year, msg)
            return {"status": "error", "file": str(out_file), "records": 0, "message": msg}
        except Exception as e:
            logger.warning("Error %s %d: %s", iata, year, e)
            return {"status": "error", "file": str(out_file), "records": 0, "message": str(e)}

    msg = f"429 after {MAX_RETRIES} attempts"
    logger.error("Failed %s %d: %s", iata, year, msg)
    return {"status": "error", "file": str(out_file), "records": 0, "message": msg}


def fetch_all_airports_bulk(
    output_dir: Path,
    start_year: int = 2000,
    end_year: int = 2025,
    airports: list = None,
    workers: int = 1,
) -> dict:
    """
    Fetch weather for all airports across a year range.

    workers=1  (default): fully sequential, ~1 req/7s, zero 429s expected.
    workers=2: two threads share the same 7s sleep cadence; may hit 429
               occasionally but the fixed-wait retry handles it cleanly.

    Recommended: workers=1 for reliability, workers=2 for ~2x speed if
    your IP is not already rate-limited.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config.airport_coords import AIRPORT_COORDS

    airports = airports or DEFAULT_AIRPORTS
    years = list(range(start_year, end_year + 1))

    tasks = []
    for iata in airports:
        coords = AIRPORT_COORDS.get(iata.upper())
        if coords is None:
            logger.warning("No coordinates for %s — skipping", iata)
            continue
        lat, lon, tz = coords
        for year in years:
            tasks.append((iata, year, lat, lon, tz))

    total = len(tasks)
    est_min = total * REQUEST_INTERVAL / 60
    logger.info(
        "Submitting %d tasks (%d airports × %d years, workers=%d)",
        total, len(airports), len(years), workers,
    )
    logger.info("Estimated time (no skips): ~%.0f min at %.0fs/req", est_min, REQUEST_INTERVAL)

    summary = {"total": total, "ok": 0, "skip": 0, "error": 0, "records": 0}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_airport_year, iata, year, lat, lon, tz, output_dir): (iata, year)
            for iata, year, lat, lon, tz in tasks
        }
        for future in as_completed(futures):
            iata, year = futures[future]
            try:
                result = future.result()
                summary[result["status"]] += 1
                summary["records"] += result.get("records", 0)
            except Exception as e:
                logger.error("Unexpected error %s %d: %s", iata, year, e)
                summary["error"] += 1

    logger.info(
        "Done: ok=%d skip=%d error=%d records=%d",
        summary["ok"], summary["skip"], summary["error"], summary["records"],
    )
    return summary


def main():
    parser = argparse.ArgumentParser(description="Fetch historical weather from Open-Meteo")
    parser.add_argument("--airport",    help="Single IATA code (e.g. ATL)")
    parser.add_argument("--year",       type=int, help="Single year (e.g. 2023)")
    parser.add_argument("--output-dir", default="./raw_data/weather")
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year",   type=int, default=2025)
    parser.add_argument("--workers",    type=int, default=1,
                        help="1=sequential (safest); 2=~2x faster but may hit 429")
    parser.add_argument("--test",       action="store_true",
                        help="Fetch ATL 2023 only")
    args = parser.parse_args()

    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config.airport_coords import AIRPORT_COORDS

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.test:
        logger.info("-- TEST MODE: fetching ATL 2023 --")
        lat, lon, tz = AIRPORT_COORDS["ATL"]
        result = fetch_airport_year("ATL", 2023, lat, lon, tz, output_dir)
        logger.info("Result: %s", json.dumps(result, indent=2))
        return

    if args.airport and args.year:
        iata = args.airport.upper()
        coords = AIRPORT_COORDS.get(iata)
        if coords is None:
            logger.error("Unknown airport: %s", iata)
            sys.exit(1)
        lat, lon, tz = coords
        result = fetch_airport_year(iata, args.year, lat, lon, tz, output_dir)
        logger.info("Result: %s", json.dumps(result, indent=2))
        return

    airports = [args.airport.upper()] if args.airport else None
    summary = fetch_all_airports_bulk(
        output_dir=output_dir,
        start_year=args.start_year,
        end_year=args.end_year,
        airports=airports,
        workers=args.workers,
    )
    logger.info("Summary: %s", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
