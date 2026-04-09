"""
Snowflake Analytics Runner
============================
Executes the analytics refresh queries against Snowflake.
Called automatically by the daily pipeline after data load.

Usage:
    python run_analytics.py                   # refresh all analytics tables
    python run_analytics.py --date 2025-04-02 # incremental update for date
    python run_analytics.py --query delay_trends  # run specific query set
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.snowflake_conn import get_conn as _get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_conn():
    return _get_conn(schema="ANALYTICS")


# ── Query definitions ─────────────────────────────────────────────────────────

def q_delay_trends_monthly(cur, target_date: date = None):
    """Refresh monthly delay trends."""
    where_clause = ""
    if target_date:
        where_clause = f"WHERE YEAR = {target_date.year} AND MONTH = {target_date.month}"

    cur.execute(f"""
        INSERT OVERWRITE INTO ANALYTICS.DELAY_TRENDS_MONTHLY
        SELECT
            YEAR, MONTH,
            COUNT(*) AS TOTAL_FLIGHTS,
            SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS TOTAL_DELAYED,
            SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS TOTAL_CANCELLED,
            ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2) AS DELAY_RATE,
            ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2) AS AVG_DEP_DELAY_MIN,
            ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2) AS AVG_ARR_DELAY_MIN,
            ROUND(100.0 * SUM(CARRIER_DELAY) / NULLIF(SUM(DEP_DELAY_MIN), 0), 2) AS CARRIER_DELAY_SHARE,
            ROUND(100.0 * SUM(WEATHER_DELAY) / NULLIF(SUM(DEP_DELAY_MIN), 0), 2) AS WEATHER_DELAY_SHARE,
            ROUND(100.0 * SUM(NAS_DELAY)     / NULLIF(SUM(DEP_DELAY_MIN), 0), 2) AS NAS_DELAY_SHARE,
            CURRENT_TIMESTAMP() AS _UPDATED_TS
        FROM STAGING.FLIGHTS
        WHERE NOT IS_CANCELLED {where_clause.replace('WHERE', 'AND') if where_clause else ''}
        GROUP BY YEAR, MONTH
    """)
    logger.info("delay_trends_monthly: %d rows affected", cur.rowcount)


def q_delay_trends_daily(cur, target_date: date = None):
    """Refresh daily delay trends."""
    where_clause = f"AND FLIGHT_DATE = '{target_date}'" if target_date else ""
    cur.execute(f"""
        INSERT OVERWRITE INTO ANALYTICS.DELAY_TRENDS_DAILY
        SELECT
            FLIGHT_DATE,
            COUNT(*) AS TOTAL_FLIGHTS,
            SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS TOTAL_DELAYED,
            SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS TOTAL_CANCELLED,
            ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2) AS DELAY_RATE,
            ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2) AS AVG_DEP_DELAY_MIN,
            ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2) AS AVG_ARR_DELAY_MIN,
            ROUND(100.0 * SUM(CARRIER_DELAY) / NULLIF(SUM(DEP_DELAY_MIN), 0), 2) AS CARRIER_DELAY_SHARE,
            ROUND(100.0 * SUM(WEATHER_DELAY) / NULLIF(SUM(DEP_DELAY_MIN), 0), 2) AS WEATHER_DELAY_SHARE,
            ROUND(100.0 * SUM(NAS_DELAY)     / NULLIF(SUM(DEP_DELAY_MIN), 0), 2) AS NAS_DELAY_SHARE,
            CURRENT_TIMESTAMP() AS _UPDATED_TS
        FROM STAGING.FLIGHTS
        WHERE NOT IS_CANCELLED {where_clause}
        GROUP BY FLIGHT_DATE
    """)
    logger.info("delay_trends_daily: %d rows affected", cur.rowcount)


def q_airline_performance(cur, target_date: date = None):
    """Refresh airline performance."""
    where_clause = f"AND YEAR = {target_date.year} AND MONTH = {target_date.month}" if target_date else ""
    cur.execute(f"""
        INSERT OVERWRITE INTO ANALYTICS.AIRLINE_PERFORMANCE
        SELECT
            YEAR, MONTH, AIRLINE_CODE,
            COUNT(*) AS TOTAL_FLIGHTS,
            SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS DELAYED_FLIGHTS,
            SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS CANCELLED_FLIGHTS,
            SUM(CASE WHEN NOT IS_DEP_DELAYED AND NOT IS_CANCELLED THEN 1 ELSE 0 END) AS ON_TIME_FLIGHTS,
            ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2) AS DELAY_RATE,
            ROUND(100.0 * SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) / COUNT(*), 2) AS CANCEL_RATE,
            ROUND(100.0 * SUM(CASE WHEN NOT IS_DEP_DELAYED AND NOT IS_CANCELLED THEN 1 ELSE 0 END)
                  / COUNT(*), 2) AS ON_TIME_RATE,
            ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 AND NOT IS_CANCELLED THEN DEP_DELAY_MIN END), 2) AS AVG_DEP_DELAY_MIN,
            ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 AND NOT IS_CANCELLED THEN ARR_DELAY_MIN END), 2) AS AVG_ARR_DELAY_MIN,
            CURRENT_TIMESTAMP() AS _UPDATED_TS
        FROM STAGING.FLIGHTS
        WHERE 1=1 {where_clause}
        GROUP BY YEAR, MONTH, AIRLINE_CODE
    """)
    logger.info("airline_performance: %d rows affected", cur.rowcount)


def q_airport_stats(cur, target_date: date = None):
    """Refresh airport statistics."""
    where_clause = f"WHERE YEAR = {target_date.year} AND MONTH = {target_date.month}" if target_date else ""
    cur.execute(f"""
        INSERT OVERWRITE INTO ANALYTICS.AIRPORT_STATS
        WITH departures AS (
            SELECT YEAR, MONTH, ORIGIN AS AIRPORT, ORIGIN_CITY AS CITY, ORIGIN_STATE AS STATE,
                COUNT(*) AS DEP_FLIGHTS,
                SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS DEP_DELAYED,
                SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS DEP_CANCELLED,
                ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2) AS AVG_DEP_DELAY_MIN
            FROM STAGING.FLIGHTS {where_clause}
            GROUP BY YEAR, MONTH, ORIGIN, ORIGIN_CITY, ORIGIN_STATE
        ),
        arrivals AS (
            SELECT YEAR, MONTH, DEST AS AIRPORT,
                COUNT(*) AS ARR_FLIGHTS,
                SUM(CASE WHEN IS_ARR_DELAYED THEN 1 ELSE 0 END) AS ARR_DELAYED,
                ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2) AS AVG_ARR_DELAY_MIN
            FROM STAGING.FLIGHTS
            WHERE NOT IS_CANCELLED {where_clause.replace('WHERE', 'AND') if where_clause else ''}
            GROUP BY YEAR, MONTH, DEST
        )
        SELECT d.YEAR, d.MONTH, d.AIRPORT, d.CITY, d.STATE,
            d.DEP_FLIGHTS, a.ARR_FLIGHTS,
            d.DEP_DELAYED, a.ARR_DELAYED, d.DEP_CANCELLED,
            d.AVG_DEP_DELAY_MIN, a.AVG_ARR_DELAY_MIN,
            ROUND(100.0 * d.DEP_DELAYED / NULLIF(d.DEP_FLIGHTS, 0), 2) AS DEP_DELAY_RATE,
            ROUND(100.0 * a.ARR_DELAYED / NULLIF(a.ARR_FLIGHTS, 0), 2) AS ARR_DELAY_RATE,
            CURRENT_TIMESTAMP() AS _UPDATED_TS
        FROM departures d
        LEFT JOIN arrivals a ON d.YEAR = a.YEAR AND d.MONTH = a.MONTH AND d.AIRPORT = a.AIRPORT
    """)
    logger.info("airport_stats: %d rows affected", cur.rowcount)


def q_delay_cause_breakdown(cur, target_date: date = None):
    """Refresh delay cause breakdown."""
    where_clause = f"AND YEAR = {target_date.year} AND MONTH = {target_date.month}" if target_date else ""
    cur.execute(f"""
        INSERT OVERWRITE INTO ANALYTICS.DELAY_CAUSE_BREAKDOWN
        SELECT
            YEAR, MONTH,
            SUM(CASE WHEN CARRIER_DELAY       > 0 THEN 1 ELSE 0 END) AS CARRIER_DELAY_FLIGHTS,
            SUM(CASE WHEN WEATHER_DELAY        > 0 THEN 1 ELSE 0 END) AS WEATHER_DELAY_FLIGHTS,
            SUM(CASE WHEN NAS_DELAY            > 0 THEN 1 ELSE 0 END) AS NAS_DELAY_FLIGHTS,
            SUM(CASE WHEN SECURITY_DELAY       > 0 THEN 1 ELSE 0 END) AS SECURITY_DELAY_FLIGHTS,
            SUM(CASE WHEN LATE_AIRCRAFT_DELAY  > 0 THEN 1 ELSE 0 END) AS LATE_AIRCRAFT_DELAY_FLIGHTS,
            ROUND(SUM(COALESCE(CARRIER_DELAY,      0)), 2) AS CARRIER_DELAY_TOTAL_MIN,
            ROUND(SUM(COALESCE(WEATHER_DELAY,      0)), 2) AS WEATHER_DELAY_TOTAL_MIN,
            ROUND(SUM(COALESCE(NAS_DELAY,          0)), 2) AS NAS_DELAY_TOTAL_MIN,
            ROUND(SUM(COALESCE(SECURITY_DELAY,     0)), 2) AS SECURITY_DELAY_TOTAL_MIN,
            ROUND(SUM(COALESCE(LATE_AIRCRAFT_DELAY, 0)), 2) AS LATE_AIRCRAFT_DELAY_TOTAL_MIN,
            CURRENT_TIMESTAMP() AS _UPDATED_TS
        FROM STAGING.FLIGHTS
        WHERE IS_DEP_DELAYED AND NOT IS_CANCELLED {where_clause}
        GROUP BY YEAR, MONTH
    """)
    logger.info("delay_cause_breakdown: %d rows affected", cur.rowcount)


def q_route_performance(cur, target_date: date = None):
    """Refresh route performance."""
    where_clause = f"AND YEAR = {target_date.year} AND MONTH = {target_date.month}" if target_date else ""
    cur.execute(f"""
        INSERT OVERWRITE INTO ANALYTICS.ROUTE_PERFORMANCE
        SELECT
            YEAR, MONTH, ORIGIN, DEST, CONCAT(ORIGIN, '-', DEST) AS ROUTE,
            COUNT(*) AS TOTAL_FLIGHTS,
            SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS DELAYED_FLIGHTS,
            ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2) AS DELAY_RATE,
            ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2) AS AVG_DEP_DELAY_MIN,
            ROUND(AVG(DISTANCE), 2) AS AVG_DISTANCE,
            CURRENT_TIMESTAMP() AS _UPDATED_TS
        FROM STAGING.FLIGHTS
        WHERE NOT IS_CANCELLED {where_clause}
        GROUP BY YEAR, MONTH, ORIGIN, DEST
        HAVING COUNT(*) >= 10
    """)
    logger.info("route_performance: %d rows affected", cur.rowcount)


def q_airport_nas_status(cur, target_date: date = None):
    """Refresh ANALYTICS.AIRPORT_NAS_STATUS from RAW.FAA_NAS_STATUS_RAW."""
    # Idempotent DDL — safe to run on every refresh in case setup.sql was not executed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ANALYTICS.AIRPORT_NAS_STATUS (
            FETCH_DATE         DATE,
            IATA_CODE          VARCHAR(5),
            DELAY_PROGRAMS     INTEGER,
            MAX_AVG_DELAY_MIN  INTEGER,
            HAS_GROUND_DELAY   BOOLEAN,
            HAS_GROUND_STOP    BOOLEAN,
            PRIMARY KEY (FETCH_DATE, IATA_CODE)
        )
    """)
    cur.execute("""
        INSERT OVERWRITE INTO ANALYTICS.AIRPORT_NAS_STATUS
        SELECT
            FETCH_DATE,
            IATA_CODE,
            COUNT(DISTINCT DELAY_TYPE)             AS DELAY_PROGRAMS,
            MAX(AVG_DELAY_MIN)                     AS MAX_AVG_DELAY_MIN,
            BOOLOR_AGG(DELAY_TYPE = 'GroundDelay') AS HAS_GROUND_DELAY,
            BOOLOR_AGG(DELAY_TYPE = 'GroundStop')  AS HAS_GROUND_STOP
        FROM RAW.FAA_NAS_STATUS_RAW
        WHERE HAS_DELAY = TRUE
          AND FETCH_DATE IS NOT NULL
          AND IATA_CODE  IS NOT NULL
        GROUP BY FETCH_DATE, IATA_CODE
    """)
    logger.info("airport_nas_status: %d rows affected", cur.rowcount)


def q_weather_delay_correlation(cur, target_date: date = None):
    """Refresh weather-delay correlation analytics table.

    Skipped when STAGING.FLIGHTS does not yet have the ORIGIN_TEMP_C column
    (i.e. setup.sql ALTER TABLE statements have not been run) or when no rows
    with non-null ORIGIN_TEMP_C exist (weather join not applied yet).
    """
    # Guard step 1: check the column exists (ALTER TABLE may not have been run)
    cur.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'STAGING'
          AND TABLE_NAME   = 'FLIGHTS'
          AND COLUMN_NAME  = 'ORIGIN_TEMP_C'
    """)
    col_row = cur.fetchone()
    if not col_row or col_row[0] == 0:
        logger.warning(
            "q_weather_delay_correlation: ORIGIN_TEMP_C column not found in "
            "STAGING.FLIGHTS — run the ALTER TABLE statements in setup.sql first"
        )
        return

    # Guard step 2: check that joined weather rows actually exist
    cur.execute("""
        SELECT COUNT(*)
        FROM STAGING.FLIGHTS
        WHERE ORIGIN_TEMP_C IS NOT NULL
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row or row[0] == 0:
        logger.warning(
            "q_weather_delay_correlation: STAGING.FLIGHTS has no rows with "
            "non-null ORIGIN_TEMP_C — skipping (run weather join first)"
        )
        return

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ANALYTICS.WEATHER_DELAY_CORRELATION (
            YEAR                INTEGER,
            MONTH               INTEGER,
            IATA_CODE           VARCHAR(5),
            AVG_TEMP_C          FLOAT,
            AVG_WIND_SPEED_KMH  FLOAT,
            AVG_VISIBILITY_M    FLOAT,
            SNOW_FLIGHT_COUNT   INTEGER,
            AVG_DEP_DELAY_MIN   FLOAT,
            DELAY_RATE          FLOAT,
            PRIMARY KEY (YEAR, MONTH, IATA_CODE)
        ) CLUSTER BY (YEAR, IATA_CODE)
    """)
    cur.execute("""
        INSERT OVERWRITE INTO ANALYTICS.WEATHER_DELAY_CORRELATION
        SELECT
            f.YEAR,
            f.MONTH,
            f.ORIGIN                                              AS IATA_CODE,
            AVG(f.ORIGIN_TEMP_C)                                 AS AVG_TEMP_C,
            AVG(f.ORIGIN_WIND_SPEED_KMH)                         AS AVG_WIND_SPEED_KMH,
            AVG(f.ORIGIN_VISIBILITY_M)                           AS AVG_VISIBILITY_M,
            SUM(CASE WHEN f.ORIGIN_SNOWFALL_CM > 0 THEN 1 ELSE 0 END) AS SNOW_FLIGHT_COUNT,
            AVG(f.DEP_DELAY_MIN)                                 AS AVG_DEP_DELAY_MIN,
            AVG(CASE WHEN f.IS_CANCELLED = FALSE
                     THEN f.IS_DEP_DELAYED::INTEGER END)          AS DELAY_RATE
        FROM STAGING.FLIGHTS f
        WHERE f.IS_CANCELLED = FALSE
          AND f.ORIGIN_TEMP_C IS NOT NULL
        GROUP BY f.YEAR, f.MONTH, f.ORIGIN
    """)
    logger.info("weather_delay_correlation: %d rows affected", cur.rowcount)


QUERY_MAP = {
    "delay_trends":   [q_delay_trends_monthly, q_delay_trends_daily],
    "airline":        [q_airline_performance],
    "airports":       [q_airport_stats],
    "delay_causes":   [q_delay_cause_breakdown],
    "routes":         [q_route_performance],
    "nas_status":     [q_airport_nas_status],
    "weather":        [q_weather_delay_correlation],
    "all":            [
        q_delay_trends_monthly, q_delay_trends_daily,
        q_airline_performance, q_airport_stats,
        q_delay_cause_breakdown, q_route_performance,
        q_airport_nas_status,
        q_weather_delay_correlation,
    ],
}


def main():
    parser = argparse.ArgumentParser(description="Run Snowflake analytics refresh")
    parser.add_argument("--date",  help="Target date YYYY-MM-DD for incremental refresh")
    parser.add_argument("--query", choices=list(QUERY_MAP.keys()), default="all")
    args = parser.parse_args()

    env_file = BASE_DIR / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    conn = get_conn()
    cur  = conn.cursor()

    try:
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")
        cur.execute("USE DATABASE FLIGHT_DB")

        queries = QUERY_MAP[args.query]
        logger.info("Running %d analytics query set(s)", len(queries))

        for q_fn in queries:
            logger.info("Running: %s", q_fn.__name__)
            try:
                q_fn(cur, target_date=target_date)
            except Exception as e:
                logger.error("Query %s failed: %s", q_fn.__name__, e)

        conn.commit()
        logger.info("Analytics refresh complete.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
