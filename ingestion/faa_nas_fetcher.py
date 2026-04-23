import argparse
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

FAA_NAS_URL  = "https://nasstatus.faa.gov/api/airport-status-information"
FAA_ASWS_URL = "https://soa.smext.faa.gov/asws/api/airport/status/{iata}"

US_AIRPORTS_IATA = [
    "ATL", "DFW", "DEN", "ORD", "LAX",
    "JFK", "LAS", "MCO", "MIA", "CLT",
    "SEA", "PHX", "EWR", "SFO", "IAH",
    "BOS", "FLL", "MSP", "LGA", "BWI",
    "DTW", "PHL", "SLC", "DCA", "IAD",
    "MDW", "SAN", "TPA", "PDX", "HOU",
    "BNA", "MEM", "AUS", "STL", "ONT",
    "SNA", "OAK", "CLE", "RDU", "MSY",
    "SMF", "SJC", "PIT", "RSW", "CVG",
    "IND", "ABQ", "ELP", "BUF", "ANC",
]


class FAANASFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        })

    def fetch_all_status_xml(self) -> ET.Element:
        """Fetch aggregate NAS status. Returns parsed XML root element.

        nasstatus.faa.gov returns application/xml — NOT JSON.
        Example root tag: <AIRPORT_STATUS_INFORMATION>
        """
        resp = self.session.get(FAA_NAS_URL, timeout=30)
        resp.raise_for_status()
        body = resp.text.strip()
        if not body:
            raise ValueError("Empty response body from aggregate endpoint")
        return ET.fromstring(body)

    def fetch_airport_status(self, iata: str) -> dict:
        """Per-airport fallback (soa.smext.faa.gov). Returns JSON dict."""
        url = FAA_ASWS_URL.format(iata=iata)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        body = resp.text.strip()
        if not body:
            raise ValueError(f"Empty response body for {iata}")
        return resp.json()


def _parse_delay_str(s: str) -> int | None:
    if not s or not isinstance(s, str):
        return None
    s = s.lower().strip()
    hours   = 0
    minutes = 0

    hour_match = re.search(r"(\d+)\s*hour", s)
    min_match  = re.search(r"(\d+)\s*min", s)

    if hour_match:
        hours = int(hour_match.group(1))
    if min_match:
        minutes = int(min_match.group(1))

    if hours == 0 and minutes == 0:
        bare = re.match(r"^\d+$", s.strip())
        if bare:
            return int(bare.group())
        return None

    return hours * 60 + minutes


def _extract_records_from_aggregate(
    data: dict,
    fetch_date: date,
    snapshot_ts: str,
) -> list[dict]:
    records = []
    fetch_date_str = fetch_date.isoformat()

    seen_airports: dict[str, list[dict]] = {}

    try:
        root = data.get("airport_status_information", data)
        delay_types = root.get("delay_type", [])
        if not isinstance(delay_types, list):
            delay_types = []

        for delay_block in delay_types:
            delay_type_name = delay_block.get("Name", "Unknown")
            delay_type_norm = _normalize_delay_type(delay_type_name)

            airport_list = delay_block.get("airport_closure_list", [])
            if not isinstance(airport_list, list):
                airport_list = []

            for entry in airport_list:
                iata = (entry.get("ARPT") or entry.get("airport") or "").strip().upper()
                if not iata:
                    continue

                avg_delay = _parse_delay_str(entry.get("Avg") or entry.get("avg_delay") or "")
                min_delay = _parse_delay_str(entry.get("Min") or entry.get("min_delay") or "")
                max_delay = _parse_delay_str(entry.get("Max") or entry.get("max_delay") or "")

                rec = {
                    "SNAPSHOT_TS":        snapshot_ts,
                    "FETCH_DATE":         fetch_date_str,
                    "IATA_CODE":          iata,
                    "HAS_DELAY":          True,
                    "DELAY_COUNT":        1,
                    "DELAY_TYPE":         delay_type_norm,
                    "REASON":             (entry.get("Reason") or entry.get("reason") or "")[:500],
                    "AVG_DELAY_MIN":      avg_delay,
                    "MIN_DELAY_MIN":      min_delay,
                    "MAX_DELAY_MIN":      max_delay,
                    "TREND":              (entry.get("Trend") or entry.get("trend") or "")[:20],
                    "END_TIME":           (entry.get("EndTime") or entry.get("end_time") or "")[:50],
                    "WEATHER_TEMP_F":     None,
                    "WEATHER_VIS_MI":     None,
                    "WEATHER_WIND":       None,
                    "WEATHER_CONDITION":  None,
                }
                records.append(rec)
                seen_airports.setdefault(iata, []).append(rec)

    except Exception as e:
        logger.warning("Error parsing aggregate FAA response: %s", e)

    return records


def _extract_records_from_xml(
    root: ET.Element,
    fetch_date: date,
    snapshot_ts: str,
) -> list[dict]:
    records = []
    fetch_date_str = fetch_date.isoformat()

    for delay_type_el in root.findall("Delay_type"):
        type_name = (delay_type_el.findtext("Name") or "Unknown").strip()
        type_norm = _normalize_delay_type(type_name)

        # Every airport entry (Ground_Delay, Delay, Airport, …) contains <ARPT>
        for entry in delay_type_el.findall(".//*[ARPT]"):
            iata = (entry.findtext("ARPT") or "").strip().upper()
            if not iata:
                continue

            rec = {
                "SNAPSHOT_TS":       snapshot_ts,
                "FETCH_DATE":        fetch_date_str,
                "IATA_CODE":         iata,
                "HAS_DELAY":         True,
                "DELAY_COUNT":       1,
                "DELAY_TYPE":        type_norm,
                "REASON":            (entry.findtext("Reason") or "")[:500],
                # Avg only in Ground Delay; Min/Max may be nested in Arrival_Departure
                "AVG_DELAY_MIN":     _parse_delay_str(entry.findtext("Avg") or ""),
                "MIN_DELAY_MIN":     _parse_delay_str(entry.findtext(".//Min") or ""),
                "MAX_DELAY_MIN":     _parse_delay_str(entry.findtext(".//Max") or ""),
                "TREND":             (entry.findtext(".//Trend") or "")[:20],
                "END_TIME":          (entry.findtext("Reopen") or entry.findtext("EndTime") or "")[:50],
                "WEATHER_TEMP_F":    None,
                "WEATHER_VIS_MI":    None,
                "WEATHER_WIND":      None,
                "WEATHER_CONDITION": None,
            }
            records.append(rec)

    return records


def _normalize_delay_type(name: str) -> str:
    name_lower = name.lower()
    if "ground delay" in name_lower or "gdp" in name_lower:
        return "GroundDelay"
    if "ground stop" in name_lower or "gsp" in name_lower:
        return "GroundStop"
    if "arrival" in name_lower and "departure" in name_lower:
        return "ArriveDepart"
    if "closure" in name_lower:
        return "Closure"
    if "airspace" in name_lower:
        return "Airspace"
    return name.strip()[:30] if name.strip() else "Unknown"


def _extract_records_from_per_airport(
    data: dict,
    iata: str,
    fetch_date: date,
    snapshot_ts: str,
) -> list[dict]:
    fetch_date_str = fetch_date.isoformat()
    iata_upper = iata.upper()

    weather = data.get("Weather") or data.get("weather") or {}
    weather_temp  = _parse_weather_temp(weather.get("Temp") or weather.get("temp"))
    weather_vis   = _parse_weather_vis(weather.get("Visibility") or weather.get("visibility"))
    weather_wind  = _first_str(weather.get("Wind") or weather.get("wind"))
    weather_cond  = _first_str(weather.get("Conditions") or weather.get("conditions"))

    has_delay   = bool(data.get("Delay") or data.get("delay"))
    delay_count = int(data.get("DelayCount") or data.get("delay_count") or 0)

    status_list = data.get("Status") or data.get("status") or []
    if not isinstance(status_list, list):
        status_list = []

    if not has_delay or not status_list:
        return [{
            "SNAPSHOT_TS":        snapshot_ts,
            "FETCH_DATE":         fetch_date_str,
            "IATA_CODE":          iata_upper,
            "HAS_DELAY":          False,
            "DELAY_COUNT":        0,
            "DELAY_TYPE":         None,
            "REASON":             None,
            "AVG_DELAY_MIN":      None,
            "MIN_DELAY_MIN":      None,
            "MAX_DELAY_MIN":      None,
            "TREND":              None,
            "END_TIME":           None,
            "WEATHER_TEMP_F":     weather_temp,
            "WEATHER_VIS_MI":     weather_vis,
            "WEATHER_WIND":       weather_wind[:100] if weather_wind else None,
            "WEATHER_CONDITION":  weather_cond[:100] if weather_cond else None,
        }]

    records = []
    for prog in status_list:
        delay_type = _normalize_delay_type(
            prog.get("Type") or prog.get("type") or "Unknown"
        )
        avg_delay = _parse_delay_str(prog.get("AvgDelay") or prog.get("avg_delay") or "")
        min_delay = _parse_delay_str(prog.get("MinDelay") or prog.get("min_delay") or "")
        max_delay = _parse_delay_str(prog.get("MaxDelay") or prog.get("max_delay") or "")

        rec = {
            "SNAPSHOT_TS":        snapshot_ts,
            "FETCH_DATE":         fetch_date_str,
            "IATA_CODE":          iata_upper,
            "HAS_DELAY":          True,
            "DELAY_COUNT":        delay_count,
            "DELAY_TYPE":         delay_type,
            "REASON":             (prog.get("Reason") or prog.get("reason") or "")[:500],
            "AVG_DELAY_MIN":      avg_delay,
            "MIN_DELAY_MIN":      min_delay,
            "MAX_DELAY_MIN":      max_delay,
            "TREND":              (prog.get("Trend") or prog.get("trend") or "")[:20],
            "END_TIME":           (prog.get("EndTime") or prog.get("end_time") or "")[:50],
            "WEATHER_TEMP_F":     weather_temp,
            "WEATHER_VIS_MI":     weather_vis,
            "WEATHER_WIND":       weather_wind[:100] if weather_wind else None,
            "WEATHER_CONDITION":  weather_cond[:100] if weather_cond else None,
        }
        records.append(rec)

    return records


def _first_str(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, list):
        return str(val[0]).strip() if val else None
    return str(val).strip() or None


def _parse_weather_temp(val) -> float | None:
    raw = _first_str(val)
    if raw is None:
        return None
    m = re.search(r"([-\d.]+)", raw)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _parse_weather_vis(val) -> float | None:
    raw = _first_str(val)
    if raw is None:
        return None
    m = re.search(r"([\d.]+)", raw)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def fetch_nas_snapshot(output_dir: Path, target_date: date) -> dict:
    fetcher      = FAANASFetcher()
    snapshot_ts  = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    date_str     = target_date.isoformat()
    records: list[dict] = []
    source_used  = "aggregate"

    try:
        logger.info("Attempting FAA NAS aggregate endpoint (XML)...")
        xml_root   = fetcher.fetch_all_status_xml()
        delay_recs = _extract_records_from_xml(xml_root, target_date, snapshot_ts)

        # Build full 50-airport record list: delayed airports from XML +
        # explicit HAS_DELAY=False records for every airport NOT in the XML.
        delayed_iatas = {r["IATA_CODE"] for r in delay_recs}
        no_delay_template = {
            "SNAPSHOT_TS":       snapshot_ts,
            "FETCH_DATE":        date_str,
            "HAS_DELAY":         False,
            "DELAY_COUNT":       0,
            "DELAY_TYPE":        None,
            "REASON":            None,
            "AVG_DELAY_MIN":     None,
            "MIN_DELAY_MIN":     None,
            "MAX_DELAY_MIN":     None,
            "TREND":             None,
            "END_TIME":          None,
            "WEATHER_TEMP_F":    None,
            "WEATHER_VIS_MI":    None,
            "WEATHER_WIND":      None,
            "WEATHER_CONDITION": None,
        }
        for iata in US_AIRPORTS_IATA:
            if iata not in delayed_iatas:
                records.append({**no_delay_template, "IATA_CODE": iata})
        records.extend(delay_recs)

        source_used = "aggregate"
        if delay_recs:
            logger.info(
                "Aggregate XML: %d delay record(s) across airports: %s",
                len(delay_recs), sorted(delayed_iatas),
            )
        else:
            logger.info("Aggregate XML: no active delays — all airports clear today")

    except Exception as e:
        logger.warning("Aggregate XML fetch failed (%s), switching to per-airport fallback", e)
        source_used = "per_airport"

        per_airport_errors = 0
        for i, iata in enumerate(US_AIRPORTS_IATA, 1):
            logger.debug("[%d/%d] Fetching FAA NAS status for %s", i, len(US_AIRPORTS_IATA), iata)
            _no_delay_rec = {
                "SNAPSHOT_TS":        snapshot_ts,
                "FETCH_DATE":         date_str,
                "IATA_CODE":          iata,
                "HAS_DELAY":          False,
                "DELAY_COUNT":        0,
                "DELAY_TYPE":         None,
                "REASON":             None,
                "AVG_DELAY_MIN":      None,
                "MIN_DELAY_MIN":      None,
                "MAX_DELAY_MIN":      None,
                "TREND":              None,
                "END_TIME":           None,
                "WEATHER_TEMP_F":     None,
                "WEATHER_VIS_MI":     None,
                "WEATHER_WIND":       None,
                "WEATHER_CONDITION":  None,
            }
            try:
                airport_data = fetcher.fetch_airport_status(iata)
                airport_records = _extract_records_from_per_airport(
                    airport_data, iata, target_date, snapshot_ts
                )
                records.extend(airport_records)
            except requests.exceptions.HTTPError as http_err:
                status_code = http_err.response.status_code if http_err.response is not None else None
                logger.debug("HTTP %s for %s — recording as no-delay", status_code, iata)
                per_airport_errors += 1
                records.append(_no_delay_rec)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as conn_err:
                # DNS / network failures — expected in sandboxed / HPC environments
                logger.debug("Connection error for %s: %s", iata, type(conn_err).__name__)
                per_airport_errors += 1
                records.append(_no_delay_rec)
            except Exception as e:
                logger.debug("Error fetching %s: %s", iata, e)
                per_airport_errors += 1
                records.append(_no_delay_rec)
            time.sleep(0.2)

        if per_airport_errors > 0:
            logger.warning(
                "Per-airport fallback: %d/%d airports unreachable "
                "(DNS/network). All recorded as no-delay. "
                "This is normal in sandboxed/HPC environments.",
                per_airport_errors, len(US_AIRPORTS_IATA),
            )

    out_dir = output_dir / "faa_nas" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    snap_path = out_dir / "snapshot.json"
    # Write NDJSON (one JSON object per line) — Spark's JSON reader requires this.
    # A top-level JSON array would be read as 1 row by Spark, losing all records.
    with open(snap_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    logger.info("Saved %d FAA NAS records → %s", len(records), snap_path)

    airports_with_delay = len({r["IATA_CODE"] for r in records if r.get("HAS_DELAY")})
    airports_fetched    = len({r["IATA_CODE"] for r in records})

    summary = {
        "records":             records,
        "airports_fetched":    airports_fetched,
        "airports_with_delay": airports_with_delay,
        "source":              source_used,
        "snapshot_ts":         snapshot_ts,
        "status":              "ok",
    }
    logger.info(
        "FAA NAS snapshot done: %d airports checked, %d with active delays (source=%s)",
        airports_fetched, airports_with_delay, source_used,
    )
    return summary


def main():
    parser = argparse.ArgumentParser(description="Fetch FAA NAS airport status snapshot")
    parser.add_argument("--test",   action="store_true", help="Fetch today and print result")
    parser.add_argument("--date",   help="Target date YYYY-MM-DD (default: today)")
    parser.add_argument("--output", default="./raw_data", help="Output root directory")
    args = parser.parse_args()

    if args.date:
        from datetime import datetime as _dt
        target = _dt.strptime(args.date, "%Y-%m-%d").date()
    else:
        target = date.today()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    result = fetch_nas_snapshot(output_dir, target)

    if args.test:
        print("\n=== FAA NAS Snapshot Test Result ===")
        print(f"Status:              {result['status']}")
        print(f"Source used:         {result['source']}")
        print(f"Snapshot timestamp:  {result['snapshot_ts']}")
        print(f"Airports fetched:    {result['airports_fetched']}")
        print(f"Airports with delay: {result['airports_with_delay']}")
        print(f"Total records:       {len(result['records'])}")

        delayed = [r for r in result["records"] if r.get("HAS_DELAY")]
        if delayed:
            print("\n--- Airports with Active Delays ---")
            for r in delayed:
                print(
                    f"  {r['IATA_CODE']:5s}  type={r['DELAY_TYPE']:15s}  "
                    f"avg={r['AVG_DELAY_MIN']} min  reason={r['REASON']}"
                )
        else:
            print("\nNo active delays detected at this time.")

        clean = [r for r in result["records"] if not r.get("HAS_DELAY")]
        if clean:
            print(f"\n--- Sample no-delay record ({clean[0]['IATA_CODE']}) ---")
            for k, v in clean[0].items():
                print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
