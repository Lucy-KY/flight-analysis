"""
weather_join.py — Join RAW.AIRPORT_WEATHER_HOURLY into STAGING.FLIGHTS
=======================================================================
Populates the 13 weather columns (ORIGIN_* and DEST_*) on STAGING.FLIGHTS
by matching each flight's departure/arrival time to the closest hourly
weather observation for that airport.

Join keys
---------
  Origin : FLIGHTS.ORIGIN      = WEATHER.IATA_CODE
           FLIGHTS.FLIGHT_DATE = WEATHER.OBS_DATE
           FLOOR(FLIGHTS.SCHEDULED_DEP / 100) = WEATHER.OBS_HOUR

  Dest   : FLIGHTS.DEST        = WEATHER.IATA_CODE
           arr_date             = WEATHER.OBS_DATE  (next day if overnight)
           FLOOR(FLIGHTS.SCHEDULED_ARR / 100) = WEATHER.OBS_HOUR

Only flights with ORIGIN_TEMP_C IS NULL are updated (idempotent).

Usage
-----
    cd code/
    python pipeline/weather_join.py            # full join
    python pipeline/weather_join.py --dry-run  # show row counts, no changes
"""

import argparse
import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

from config.snowflake_conn import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


ORIGIN_UPDATE = """
UPDATE STAGING.FLIGHTS f
SET
    ORIGIN_TEMP_C          = w.TEMP_C,
    ORIGIN_PRECIP_MM       = w.PRECIP_MM,
    ORIGIN_SNOWFALL_CM     = w.SNOWFALL_CM,
    ORIGIN_WIND_SPEED_KMH  = w.WIND_SPEED_KMH,
    ORIGIN_WIND_GUST_KMH   = w.WIND_GUST_KMH,
    ORIGIN_VISIBILITY_M    = w.VISIBILITY_M,
    ORIGIN_CLOUD_COVER_PCT = w.CLOUD_COVER_PCT,
    ORIGIN_WEATHER_CODE    = w.WEATHER_CODE
FROM RAW.AIRPORT_WEATHER_HOURLY w
WHERE f.ORIGIN      = w.IATA_CODE
  AND f.FLIGHT_DATE = w.OBS_DATE
  AND FLOOR(f.SCHEDULED_DEP / 100) = w.OBS_HOUR
  AND f.ORIGIN_TEMP_C IS NULL
"""

# For DEST, the arrival date may be FLIGHT_DATE + 1 (overnight flights).
# Overnight = SCHEDULED_ARR (HHMM) < SCHEDULED_DEP (HHMM).
DEST_UPDATE = """
UPDATE STAGING.FLIGHTS f
SET
    DEST_TEMP_C           = w.TEMP_C,
    DEST_PRECIP_MM        = w.PRECIP_MM,
    DEST_WIND_SPEED_KMH   = w.WIND_SPEED_KMH,
    DEST_VISIBILITY_M     = w.VISIBILITY_M,
    DEST_WEATHER_CODE     = w.WEATHER_CODE
FROM RAW.AIRPORT_WEATHER_HOURLY w
WHERE f.DEST = w.IATA_CODE
  AND CASE
        WHEN f.SCHEDULED_ARR IS NOT NULL
         AND f.SCHEDULED_DEP IS NOT NULL
         AND f.SCHEDULED_ARR < f.SCHEDULED_DEP   -- overnight: arrives next day
        THEN DATEADD('day', 1, f.FLIGHT_DATE)
        ELSE f.FLIGHT_DATE
      END = w.OBS_DATE
  AND FLOOR(COALESCE(f.SCHEDULED_ARR, 0) / 100) = w.OBS_HOUR
  AND f.DEST_TEMP_C IS NULL
"""

COUNT_QUERY = """
SELECT
    COUNT(*)                                                   AS total_flights,
    SUM(CASE WHEN ORIGIN_TEMP_C IS NOT NULL THEN 1 ELSE 0 END) AS origin_filled,
    SUM(CASE WHEN DEST_TEMP_C   IS NOT NULL THEN 1 ELSE 0 END) AS dest_filled,
    SUM(CASE WHEN ORIGIN_TEMP_C IS NULL     THEN 1 ELSE 0 END) AS origin_null
FROM STAGING.FLIGHTS
WHERE YEAR >= 2025
"""


def main():
    parser = argparse.ArgumentParser(description="Join weather data into STAGING.FLIGHTS")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show row counts only, do not modify data")
    args = parser.parse_args()

    conn = get_conn(schema="STAGING")
    cur  = conn.cursor()

    try:
        cur.execute("USE DATABASE FLIGHT_DB")
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")

        # ── Pre-join stats ────────────────────────────────────────────────────
        logger.info("Checking current fill status (2025 flights) …")
        cur.execute(COUNT_QUERY)
        r = cur.fetchone()
        logger.info(
            "2025 flights: total=%s  origin_filled=%s  dest_filled=%s  origin_null=%s",
            f"{r[0]:,}", f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}",
        )

        cur.execute("SELECT COUNT(*) FROM RAW.AIRPORT_WEATHER_HOURLY")
        wx_rows = cur.fetchone()[0]
        logger.info("Weather rows available: %s", f"{wx_rows:,}")

        if args.dry_run:
            logger.info("[DRY RUN] Skipping UPDATE statements.")
            return

        # ── Origin weather UPDATE ─────────────────────────────────────────────
        logger.info("Running ORIGIN weather UPDATE (may take several minutes) …")
        cur.execute(ORIGIN_UPDATE)
        logger.info("Origin weather: %s rows updated", f"{cur.rowcount:,}")

        # ── Dest weather UPDATE ───────────────────────────────────────────────
        logger.info("Running DEST weather UPDATE …")
        cur.execute(DEST_UPDATE)
        logger.info("Dest weather: %s rows updated", f"{cur.rowcount:,}")

        conn.commit()

        # ── Post-join stats ───────────────────────────────────────────────────
        logger.info("Post-join fill status (2025 flights) …")
        cur.execute(COUNT_QUERY)
        r = cur.fetchone()
        logger.info(
            "2025 flights: total=%s  origin_filled=%s  dest_filled=%s  origin_null=%s",
            f"{r[0]:,}", f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}",
        )

        logger.info("Weather join complete. Run 'python analysis/run_analytics.py' to refresh analytics.")

    except Exception as e:
        logger.error("Weather join failed: %s", e, exc_info=True)
        conn.rollback()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
