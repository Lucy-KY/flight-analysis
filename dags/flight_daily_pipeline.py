"""
Flight Daily Pipeline DAG
==========================
Runs every day at 06:00 to:
  1. Fetch FAA NAS status snapshot for the previous day
  2. Clean with PySpark
  3. Load cleaned data into Snowflake (RAW.FAA_NAS_STATUS_RAW)
  4. Refresh all ANALYTICS tables (sourced from BTS STAGING.FLIGHTS)

Deploy: copy this file to /home/compute/kaiyuanx/airflow25/dags/
Requires: snowflake_default connection configured in Airflow (same as Assignment3)
"""

import os
import subprocess
import sys as _sys
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ── Config ────────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_default"

# Working directory on the school server (same style as Assignment4)
WORK_DIR = "/home/compute/kaiyuanx/airflow25"

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "kaiyuanx",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 4, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="flight_daily_pipeline",
    default_args=default_args,
    description="Daily FAA NAS fetch → Spark clean → Snowflake load → Analytics refresh",
    schedule="0 6 * * *",
    catchup=False,
    tags=["flight", "faa-nas", "snowflake", "spark"],
) as dag:

    # ── Task 1: Fetch FAA NAS status ──────────────────────────────────────────
    @task(task_id="fetch_faa_nas")
    def fetch_faa_nas_task(**context):
        """
        Call FAA NAS aggregate endpoint (with per-airport fallback).
        Saves raw JSON snapshot to WORK_DIR/raw_data/faa_nas/{date}/snapshot.json.
        Returns the snapshot path for downstream tasks.
        """
        target   = context["data_interval_start"].date() - timedelta(days=1)
        date_str = target.strftime("%Y-%m-%d")
        out_dir  = os.path.join(WORK_DIR, "raw_data")

        result = subprocess.run(
            [
                _sys.executable,
                os.path.join(WORK_DIR, "ingestion", "faa_nas_fetcher.py"),
                "--date",   date_str,
                "--output", out_dir,
            ],
            cwd=WORK_DIR,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout)
        if result.returncode != 0:
            print(f"STDERR: {result.stderr}")
            raise RuntimeError(f"FAA NAS fetch failed (exit {result.returncode}): {result.stderr}")

        snap_path = os.path.join(out_dir, "faa_nas", date_str, "snapshot.json")
        print(f"FAA NAS snapshot saved → {snap_path}")
        return snap_path

    # ── Task 2: Spark-clean FAA NAS snapshot ──────────────────────────────────
    @task(task_id="clean_faa_nas")
    def clean_faa_nas_task(snap_path: str, **context):
        """
        Run PySpark to clean the FAA NAS JSON snapshot and write Parquet.
        Returns path to output parquet directory.
        """
        target   = context["data_interval_start"].date() - timedelta(days=1)
        date_str = target.strftime("%Y-%m-%d")

        raw_dir     = os.path.join(WORK_DIR, "raw_data")
        cleaned_dir = os.path.join(WORK_DIR, "cleaned_data")

        result = subprocess.run(
            [
                _sys.executable,
                os.path.join(WORK_DIR, "processing", "spark_clean_faa_nas.py"),
                "--input-dir",  raw_dir,
                "--output-dir", cleaned_dir,
                "--date",       date_str,
            ],
            cwd=WORK_DIR,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout)
        if result.returncode != 0:
            print(f"STDERR: {result.stderr}")
            raise RuntimeError(f"FAA NAS Spark clean failed (exit {result.returncode}): {result.stderr}")

        out_dir = os.path.join(cleaned_dir, "faa_nas")
        print(f"FAA NAS cleaned data → {out_dir}")
        return out_dir

    # ── Task 3: Load FAA NAS Parquet → Snowflake ─────────────────────────────
    @task(task_id="load_faa_nas")
    def load_faa_nas_task(parquet_dir: str, **context):
        """
        PUT cleaned FAA NAS Parquet files to Snowflake internal stage,
        then COPY INTO RAW.FAA_NAS_STATUS_RAW.
        """
        import glob

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        parquet_files = glob.glob(os.path.join(parquet_dir, "**", "*.parquet"), recursive=True)
        if not parquet_files:
            print(f"No parquet files found in {parquet_dir}, skipping FAA NAS load.")
            return

        for pf in parquet_files:
            hook.run(
                f"PUT 'file://{pf}' @FLIGHT_DB.RAW.FAA_NAS_STAGE "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            print(f"  PUT: {os.path.basename(pf)}")

        hook.run("""
            COPY INTO FLIGHT_DB.RAW.FAA_NAS_STATUS_RAW
            FROM @FLIGHT_DB.RAW.FAA_NAS_STAGE
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
            ON_ERROR = CONTINUE
        """)
        print("COPY INTO FAA_NAS_STATUS_RAW done.")

    # ── Task 4: Refresh analytics tables ─────────────────────────────────────
    @task(task_id="run_analytics")
    def run_analytics(**context):
        """
        Refresh all ANALYTICS schema tables.
        All queries source from STAGING.FLIGHTS (BTS data).
        """
        hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        target = context["data_interval_start"].date() - timedelta(days=1)
        year, month = target.year, target.month

        analytics_sqls = {
            "delay_trends_monthly": """
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

            "delay_trends_daily": """
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
    snap_path     = fetch_faa_nas_task()
    nas_clean_dir = clean_faa_nas_task(snap_path)
    nas_load_task = load_faa_nas_task(nas_clean_dir)
    analytics_task = run_analytics()
    nas_load_task >> analytics_task
