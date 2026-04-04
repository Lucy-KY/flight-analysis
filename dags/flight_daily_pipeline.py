"""
Flight Daily Pipeline DAG
==========================
Runs every day at 06:00 to:
  1. Fetch previous day's flight data from OpenSky Network API
  2. Clean with PySpark
  3. Load cleaned data into Snowflake (RAW + STAGING)
  4. Refresh all ANALYTICS tables

Deploy: copy this file to /home/compute/kaiyuanx/airflow25/dags/
Requires: snowflake_default connection configured in Airflow (same as Assignment3)
"""

import json
import os
import tempfile
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ── Config ────────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_default"
OPENSKY_BASE      = "https://opensky-network.org/api"
OPENSKY_USER      = os.getenv("OPENSKY_USERNAME", "")
OPENSKY_PASS      = os.getenv("OPENSKY_PASSWORD", "")

# Working directory on the school server (same style as Assignment4)
WORK_DIR = "/home/compute/kaiyuanx/airflow25"

# Top 30 busiest US airports (ICAO codes) — balance coverage vs API quota
US_AIRPORTS = [
    "KATL", "KDFW", "KDEN", "KORD", "KLAX",
    "KJFK", "KLAS", "KMCO", "KMIA", "KCLT",
    "KSEA", "KPHX", "KEWR", "KSFO", "KIAH",
    "KBOS", "KFLL", "KMSP", "KLGA", "KBWI",
    "KDTW", "KPHL", "KSLC", "KDCA", "KIAD",
    "KMDW", "KSAN", "KTPA", "KPDX", "KBNA",
]

# ── Default args (same style as Assignment4) ──────────────────────────────────
default_args = {
    "owner":             "kaiyuanx",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 4, 1),
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="flight_daily_pipeline",
    default_args=default_args,
    description="Daily US flight data: OpenSky fetch → Spark clean → Snowflake load → Analytics",
    schedule="0 6 * * *",   # every day at 06:00
    catchup=False,
    tags=["flight", "opensky", "snowflake", "spark"],
) as dag:

    # ── Task 1: Fetch OpenSky flight data ─────────────────────────────────────
    @task(task_id="fetch_opensky")
    def fetch_opensky(**context):
        """
        Call OpenSky /flights/departure for each US airport.
        Save raw JSON to a temp file; return the path for downstream tasks.
        """
        import requests
        import time

        # Target date = yesterday (data_interval_start gives the logical run date)
        target = context["data_interval_start"].date() - timedelta(days=1)
        date_str = target.strftime("%Y-%m-%d")

        dt_start = datetime(target.year, target.month, target.day, 0, 0, 0)
        dt_end   = datetime(target.year, target.month, target.day, 23, 59, 59)
        begin_ts = int(dt_start.timestamp())
        end_ts   = int(dt_end.timestamp())

        session = requests.Session()
        if OPENSKY_USER:
            session.auth = (OPENSKY_USER, OPENSKY_PASS)

        all_flights = []
        for airport in US_AIRPORTS:
            for endpoint in ("departure", "arrival"):
                try:
                    time.sleep(1.0)   # stay within 400 req/hour free limit
                    resp = session.get(
                        f"{OPENSKY_BASE}/flights/{endpoint}",
                        params={"airport": airport, "begin": begin_ts, "end": end_ts},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        flights = resp.json() or []
                        for f in flights:
                            f["fetch_date"]    = date_str
                            f["fetch_airport"] = airport
                            f["direction"]     = endpoint
                        all_flights.extend(flights)
                    elif resp.status_code == 404:
                        pass   # no data for this airport/window
                    else:
                        print(f"  {airport}/{endpoint}: HTTP {resp.status_code}")
                except Exception as e:
                    print(f"  {airport}/{endpoint} error: {e}")

        # Save to temp JSON file
        out_dir = os.path.join(WORK_DIR, "data", "opensky", date_str)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "flights.json")
        with open(out_path, "w") as f:
            json.dump(all_flights, f)

        print(f"Fetched {len(all_flights)} flights for {date_str} → {out_path}")
        return out_path

    # ── Task 2: Spark cleaning ────────────────────────────────────────────────
    @task(task_id="spark_clean")
    def spark_clean(json_path: str, **context):
        """
        Run PySpark to clean the OpenSky JSON and write Parquet.
        Returns path to output parquet directory.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            LongType, StringType, IntegerType, DoubleType,
            BooleanType, StructType, StructField,
        )

        target = context["data_interval_start"].date() - timedelta(days=1)
        date_str = target.strftime("%Y-%m-%d")

        spark = (
            SparkSession.builder
            .appName("OpenSky_Clean_Daily")
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        schema = StructType([
            StructField("icao24",                           StringType(),  True),
            StructField("firstSeen",                        LongType(),    True),
            StructField("estDepartureAirport",              StringType(),  True),
            StructField("lastSeen",                         LongType(),    True),
            StructField("estArrivalAirport",                StringType(),  True),
            StructField("callsign",                         StringType(),  True),
            StructField("estDepartureAirportHorizDistance", IntegerType(), True),
            StructField("estDepartureAirportVertDistance",  IntegerType(), True),
            StructField("estArrivalAirportHorizDistance",   IntegerType(), True),
            StructField("estArrivalAirportVertDistance",    IntegerType(), True),
            StructField("departureAirportCandidatesCount",  IntegerType(), True),
            StructField("arrivalAirportCandidatesCount",    IntegerType(), True),
            StructField("fetch_date",                       StringType(),  True),
            StructField("fetch_airport",                    StringType(),  True),
            StructField("direction",                        StringType(),  True),
        ])

        df = spark.read.schema(schema).json(json_path)

        df = (
            df
            .dropDuplicates(["icao24", "firstSeen", "estDepartureAirport", "estArrivalAirport"])
            .withColumn("FETCH_DATE",             F.to_date(F.col("fetch_date"), "yyyy-MM-dd"))
            .withColumn("ICAO24",                 F.upper(F.trim(F.col("icao24"))))
            .withColumn("CALLSIGN",               F.trim(F.col("callsign")))
            .withColumn("FIRST_SEEN",             F.col("firstSeen"))
            .withColumn("EST_DEPARTURE_AIRPORT",  F.upper(F.trim(F.col("estDepartureAirport"))))
            .withColumn("LAST_SEEN",              F.col("lastSeen"))
            .withColumn("EST_ARRIVAL_AIRPORT",    F.upper(F.trim(F.col("estArrivalAirport"))))
            .withColumn("DEP_AIRPORT_HORIZ_DIST", F.col("estDepartureAirportHorizDistance"))
            .withColumn("DEP_AIRPORT_VERT_DIST",  F.col("estDepartureAirportVertDistance"))
            .withColumn("ARR_AIRPORT_HORIZ_DIST", F.col("estArrivalAirportHorizDistance"))
            .withColumn("ARR_AIRPORT_VERT_DIST",  F.col("estArrivalAirportVertDistance"))
            .withColumn("DEP_CANDIDATES_COUNT",   F.col("departureAirportCandidatesCount"))
            .withColumn("ARR_CANDIDATES_COUNT",   F.col("arrivalAirportCandidatesCount"))
            .withColumn("_LOAD_TS",               F.current_timestamp())
            .filter(
                F.col("EST_DEPARTURE_AIRPORT").isNotNull() |
                F.col("EST_ARRIVAL_AIRPORT").isNotNull()
            )
            .select(
                "FETCH_DATE", "ICAO24", "CALLSIGN",
                "FIRST_SEEN", "EST_DEPARTURE_AIRPORT",
                "LAST_SEEN",  "EST_ARRIVAL_AIRPORT",
                "DEP_AIRPORT_HORIZ_DIST", "DEP_AIRPORT_VERT_DIST",
                "ARR_AIRPORT_HORIZ_DIST", "ARR_AIRPORT_VERT_DIST",
                "DEP_CANDIDATES_COUNT",   "ARR_CANDIDATES_COUNT",
                "_LOAD_TS",
            )
        )

        row_count = df.count()
        out_dir   = os.path.join(WORK_DIR, "data", "opensky_clean", date_str)
        df.write.mode("overwrite").parquet(out_dir)
        spark.stop()

        print(f"Cleaned {row_count} rows → {out_dir}")
        return out_dir

    # ── Task 3: Load to Snowflake (same style as Assignment3) ────────────────
    @task(task_id="load_snowflake")
    def load_snowflake(parquet_dir: str, **context):
        """
        PUT parquet files to Snowflake internal stage, then COPY INTO RAW table.
        Uses SnowflakeHook (same as Assignment3).
        """
        import glob

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
        if not parquet_files:
            print(f"No parquet files found in {parquet_dir}, skipping load.")
            return

        # PUT files to internal stage
        for pf in parquet_files:
            hook.run(
                f"PUT 'file://{pf}' @FLIGHT_DB.RAW.OPENSKY_STAGE "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            print(f"  PUT: {os.path.basename(pf)}")

        # COPY INTO raw table
        hook.run("""
            COPY INTO FLIGHT_DB.RAW.OPENSKY_FLIGHTS_RAW
            FROM @FLIGHT_DB.RAW.OPENSKY_STAGE
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
            ON_ERROR = CONTINUE
        """)
        print("COPY INTO OPENSKY_FLIGHTS_RAW done.")

    # ── Task 4: Refresh analytics tables ─────────────────────────────────────
    @task(task_id="run_analytics")
    def run_analytics(**context):
        """
        Refresh all ANALYTICS schema tables for the current month.
        Uses SnowflakeHook (same as Assignment3).
        """
        hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        target = context["data_interval_start"].date() - timedelta(days=1)
        year, month = target.year, target.month

        analytics_sqls = {
            "delay_trends_monthly": f"""
                INSERT OVERWRITE INTO FLIGHT_DB.ANALYTICS.DELAY_TRENDS_MONTHLY
                SELECT
                    YEAR, MONTH,
                    COUNT(*) AS TOTAL_FLIGHTS,
                    SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS TOTAL_DELAYED,
                    SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS TOTAL_CANCELLED,
                    ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END)
                          / COUNT(*), 2)                            AS DELAY_RATE,
                    ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2)
                                                                    AS AVG_DEP_DELAY_MIN,
                    ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2)
                                                                    AS AVG_ARR_DELAY_MIN,
                    ROUND(100.0 * SUM(CARRIER_DELAY)
                          / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)      AS CARRIER_DELAY_SHARE,
                    ROUND(100.0 * SUM(WEATHER_DELAY)
                          / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)      AS WEATHER_DELAY_SHARE,
                    ROUND(100.0 * SUM(NAS_DELAY)
                          / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)      AS NAS_DELAY_SHARE,
                    CURRENT_TIMESTAMP()                             AS _UPDATED_TS
                FROM FLIGHT_DB.STAGING.FLIGHTS
                WHERE NOT IS_CANCELLED
                GROUP BY YEAR, MONTH
            """,

            "delay_trends_daily": f"""
                INSERT OVERWRITE INTO FLIGHT_DB.ANALYTICS.DELAY_TRENDS_DAILY
                SELECT
                    FLIGHT_DATE,
                    COUNT(*) AS TOTAL_FLIGHTS,
                    SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS TOTAL_DELAYED,
                    SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS TOTAL_CANCELLED,
                    ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END)
                          / COUNT(*), 2)                            AS DELAY_RATE,
                    ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2)
                                                                    AS AVG_DEP_DELAY_MIN,
                    ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2)
                                                                    AS AVG_ARR_DELAY_MIN,
                    ROUND(100.0 * SUM(CARRIER_DELAY)
                          / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)      AS CARRIER_DELAY_SHARE,
                    ROUND(100.0 * SUM(WEATHER_DELAY)
                          / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)      AS WEATHER_DELAY_SHARE,
                    ROUND(100.0 * SUM(NAS_DELAY)
                          / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)      AS NAS_DELAY_SHARE,
                    CURRENT_TIMESTAMP()                             AS _UPDATED_TS
                FROM FLIGHT_DB.STAGING.FLIGHTS
                WHERE NOT IS_CANCELLED
                GROUP BY FLIGHT_DATE
            """,

            "airline_performance": f"""
                INSERT OVERWRITE INTO FLIGHT_DB.ANALYTICS.AIRLINE_PERFORMANCE
                SELECT
                    YEAR, MONTH, AIRLINE_CODE,
                    COUNT(*) AS TOTAL_FLIGHTS,
                    SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS DELAYED_FLIGHTS,
                    SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS CANCELLED_FLIGHTS,
                    SUM(CASE WHEN NOT IS_DEP_DELAYED AND NOT IS_CANCELLED
                             THEN 1 ELSE 0 END)                     AS ON_TIME_FLIGHTS,
                    ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END)
                          / COUNT(*), 2)                            AS DELAY_RATE,
                    ROUND(100.0 * SUM(CASE WHEN IS_CANCELLED THEN 1 ELSE 0 END)
                          / COUNT(*), 2)                            AS CANCEL_RATE,
                    ROUND(100.0 * SUM(CASE WHEN NOT IS_DEP_DELAYED AND NOT IS_CANCELLED
                                          THEN 1 ELSE 0 END)
                          / COUNT(*), 2)                            AS ON_TIME_RATE,
                    ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 AND NOT IS_CANCELLED
                                   THEN DEP_DELAY_MIN END), 2)     AS AVG_DEP_DELAY_MIN,
                    ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 AND NOT IS_CANCELLED
                                   THEN ARR_DELAY_MIN END), 2)     AS AVG_ARR_DELAY_MIN,
                    CURRENT_TIMESTAMP()                             AS _UPDATED_TS
                FROM FLIGHT_DB.STAGING.FLIGHTS
                WHERE YEAR = {year} AND MONTH = {month}
                GROUP BY YEAR, MONTH, AIRLINE_CODE
            """,

            "airport_stats": f"""
                INSERT OVERWRITE INTO FLIGHT_DB.ANALYTICS.AIRPORT_STATS
                WITH dep AS (
                    SELECT YEAR, MONTH, ORIGIN AS AIRPORT,
                        ORIGIN_CITY AS CITY, ORIGIN_STATE AS STATE,
                        COUNT(*) AS DEP_FLIGHTS,
                        SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS DEP_DELAYED,
                        SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS DEP_CANCELLED,
                        ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0
                                       THEN DEP_DELAY_MIN END), 2)  AS AVG_DEP_DELAY_MIN
                    FROM FLIGHT_DB.STAGING.FLIGHTS
                    WHERE YEAR = {year} AND MONTH = {month}
                    GROUP BY YEAR, MONTH, ORIGIN, ORIGIN_CITY, ORIGIN_STATE
                ),
                arr AS (
                    SELECT YEAR, MONTH, DEST AS AIRPORT,
                        COUNT(*) AS ARR_FLIGHTS,
                        SUM(CASE WHEN IS_ARR_DELAYED THEN 1 ELSE 0 END) AS ARR_DELAYED,
                        ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0
                                       THEN ARR_DELAY_MIN END), 2)  AS AVG_ARR_DELAY_MIN
                    FROM FLIGHT_DB.STAGING.FLIGHTS
                    WHERE YEAR = {year} AND MONTH = {month} AND NOT IS_CANCELLED
                    GROUP BY YEAR, MONTH, DEST
                )
                SELECT d.YEAR, d.MONTH, d.AIRPORT, d.CITY, d.STATE,
                    d.DEP_FLIGHTS, a.ARR_FLIGHTS,
                    d.DEP_DELAYED, a.ARR_DELAYED, d.DEP_CANCELLED,
                    d.AVG_DEP_DELAY_MIN, a.AVG_ARR_DELAY_MIN,
                    ROUND(100.0 * d.DEP_DELAYED / NULLIF(d.DEP_FLIGHTS, 0), 2) AS DEP_DELAY_RATE,
                    ROUND(100.0 * a.ARR_DELAYED / NULLIF(a.ARR_FLIGHTS, 0), 2) AS ARR_DELAY_RATE,
                    CURRENT_TIMESTAMP() AS _UPDATED_TS
                FROM dep d LEFT JOIN arr a
                ON d.YEAR = a.YEAR AND d.MONTH = a.MONTH AND d.AIRPORT = a.AIRPORT
            """,

            "delay_cause_breakdown": f"""
                INSERT OVERWRITE INTO FLIGHT_DB.ANALYTICS.DELAY_CAUSE_BREAKDOWN
                SELECT YEAR, MONTH,
                    SUM(CASE WHEN CARRIER_DELAY      > 0 THEN 1 ELSE 0 END) AS CARRIER_DELAY_FLIGHTS,
                    SUM(CASE WHEN WEATHER_DELAY       > 0 THEN 1 ELSE 0 END) AS WEATHER_DELAY_FLIGHTS,
                    SUM(CASE WHEN NAS_DELAY           > 0 THEN 1 ELSE 0 END) AS NAS_DELAY_FLIGHTS,
                    SUM(CASE WHEN SECURITY_DELAY      > 0 THEN 1 ELSE 0 END) AS SECURITY_DELAY_FLIGHTS,
                    SUM(CASE WHEN LATE_AIRCRAFT_DELAY > 0 THEN 1 ELSE 0 END) AS LATE_AIRCRAFT_DELAY_FLIGHTS,
                    ROUND(SUM(COALESCE(CARRIER_DELAY,       0)), 2) AS CARRIER_DELAY_TOTAL_MIN,
                    ROUND(SUM(COALESCE(WEATHER_DELAY,       0)), 2) AS WEATHER_DELAY_TOTAL_MIN,
                    ROUND(SUM(COALESCE(NAS_DELAY,           0)), 2) AS NAS_DELAY_TOTAL_MIN,
                    ROUND(SUM(COALESCE(SECURITY_DELAY,      0)), 2) AS SECURITY_DELAY_TOTAL_MIN,
                    ROUND(SUM(COALESCE(LATE_AIRCRAFT_DELAY, 0)), 2) AS LATE_AIRCRAFT_DELAY_TOTAL_MIN,
                    CURRENT_TIMESTAMP()                              AS _UPDATED_TS
                FROM FLIGHT_DB.STAGING.FLIGHTS
                WHERE IS_DEP_DELAYED AND NOT IS_CANCELLED
                  AND YEAR = {year} AND MONTH = {month}
                GROUP BY YEAR, MONTH
            """,
        }

        for name, sql in analytics_sqls.items():
            print(f"Running analytics: {name}")
            hook.run(sql)
            print(f"  {name} done.")

        print(f"All analytics refreshed for {year}-{month:02d}.")

    # ── Task dependencies ─────────────────────────────────────────────────────
    json_path    = fetch_opensky()
    parquet_path = spark_clean(json_path)
    load_snowflake(parquet_path) >> run_analytics()
