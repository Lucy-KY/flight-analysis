"""
Historical BTS Pipeline (One-Time Bulk Load)
=============================================
Orchestrates: Download → Spark Clean → Upload to Snowflake

Steps:
  1. Download all BTS ZIP files (2000-2025) if not already present
  2. Clean each file with PySpark → write staging Parquet
  3. PUT parquet files to Snowflake internal stage
  4. COPY INTO Snowflake tables

Usage:
    # Full historical load (all years):
    python historical_pipeline.py --start 2000-01 --end 2025-12

    # Resume / incremental (already-downloaded files are skipped):
    python historical_pipeline.py --start 2020-01 --end 2024-12

    # Dry run (download + clean only, no Snowflake upload):
    python historical_pipeline.py --start 2024-01 --end 2024-03 --dry-run
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.snowflake_conn import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("historical_pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)

BASE_DIR     = Path(__file__).parent.parent
RAW_DIR      = BASE_DIR / "raw_data"
CLEANED_DIR  = BASE_DIR / "cleaned_data" / "bts"
SCRIPTS_DIR  = BASE_DIR




def step_download(start: str, end: str, workers: int = 2):
    """Download BTS ZIP files using existing download script."""
    logger.info("=" * 60)
    logger.info("STEP 1: Downloading BTS data %s → %s", start, end)
    logger.info("=" * 60)

    download_script = SCRIPTS_DIR / "download_bts_data.py"
    cmd = [
        sys.executable, str(download_script),
        "--start", start,
        "--end",   end,
        "--output", str(RAW_DIR),
        "--workers", str(workers),
        "--delay", "1.5",
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.warning("Download step exited with code %d (some files may have failed)", result.returncode)
    else:
        logger.info("Download step completed successfully")


def step_clean(start: str, end: str):
    """Run Spark cleaning for date range."""
    logger.info("=" * 60)
    logger.info("STEP 2: Spark cleaning %s → %s", start, end)
    logger.info("=" * 60)

    clean_script = SCRIPTS_DIR / "processing" / "spark_clean_bts.py"
    cmd = [
        sys.executable, str(clean_script),
        "--input",  str(RAW_DIR),
        "--output", str(CLEANED_DIR),
    ]

    # Filter by year range if specified
    start_year = int(start.split("-")[0])
    end_year   = int(end.split("-")[0])
    if start_year == end_year:
        cmd += ["--year", str(start_year)]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("Spark cleaning failed with code %d", result.returncode)
        return False
    logger.info("Spark cleaning completed")
    return True


def step_load_snowflake(dry_run: bool = False):
    """Upload cleaned Parquet to Snowflake via PUT + COPY INTO."""
    logger.info("=" * 60)
    logger.info("STEP 3: Loading to Snowflake")
    logger.info("=" * 60)

    if dry_run:
        logger.info("[DRY RUN] Skipping Snowflake load")
        return

    # Load from the RAW parquet (column names match BTS_ONTIME_RAW exactly).
    # The STAGING parquet uses renamed columns (IS_CANCELLED, DEP_DELAY_MIN, …)
    # and is NOT used here — STAGING.FLIGHTS is populated later via SQL INSERT.
    raw_parquet_dir = CLEANED_DIR / "raw"
    if not raw_parquet_dir.exists():
        logger.error("Raw parquet dir not found: %s", raw_parquet_dir)
        return

    parquet_dirs = sorted(raw_parquet_dir.iterdir())
    if not parquet_dirs:
        logger.error("No parquet directories found in %s", raw_parquet_dir)
        return

    conn = get_conn(schema="RAW")
    cur  = conn.cursor()

    try:
        cur.execute("USE DATABASE FLIGHT_DB")
        cur.execute("USE SCHEMA RAW")
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")

        total_loaded = 0

        for parquet_dir in parquet_dirs:
            if not parquet_dir.is_dir():
                continue

            part_files = list(parquet_dir.glob("*.parquet"))
            if not part_files:
                continue

            logger.info("Uploading %s (%d files)", parquet_dir.name, len(part_files))

            # PUT each parquet file to internal stage
            for pf in part_files:
                put_sql = f"PUT 'file://{pf}' @RAW.BTS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                cur.execute(put_sql)

            # COPY INTO raw table from stage
            # Note: cannot use explicit column list together with MATCH_BY_COLUMN_NAME
            # Parquet column names already match table columns (renamed in Spark)
            copy_sql = """
                COPY INTO RAW.BTS_ONTIME_RAW
                FROM @RAW.BTS_STAGE
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
                ON_ERROR = CONTINUE
            """
            cur.execute(copy_sql)
            rows = cur.fetchone()
            logger.info("  Loaded: %s", rows)
            total_loaded += 1

        # Populate STAGING.FLIGHTS from RAW using INSERT OVERWRITE (full refresh)
        # Avoid correlated subqueries on 150M rows - use direct INSERT instead
        logger.info("Populating STAGING.FLIGHTS from RAW...")
        cur.execute("TRUNCATE TABLE STAGING.FLIGHTS")
        staging_sql = """
            INSERT INTO STAGING.FLIGHTS
            SELECT
                CONCAT(
                    COALESCE(IATA_CODE_REPORTING_AIRLINE,'XX'), '_',
                    COALESCE(FLIGHT_NUMBER,'0'),               '_',
                    TO_VARCHAR(FLIGHT_DATE, 'YYYYMMDD'),        '_',
                    COALESCE(ORIGIN,'XXX'),                     '_',
                    COALESCE(DEST,'XXX')
                )                                               AS FLIGHT_ID,
                FLIGHT_DATE, YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK, QUARTER,
                IATA_CODE_REPORTING_AIRLINE                     AS AIRLINE_CODE,
                FLIGHT_NUMBER, TAIL_NUMBER,
                ORIGIN, ORIGIN_CITY_NAME                        AS ORIGIN_CITY, ORIGIN_STATE,
                DEST,  DEST_CITY_NAME                           AS DEST_CITY,  DEST_STATE,
                CRS_DEP_TIME      AS SCHEDULED_DEP,
                DEP_TIME          AS ACTUAL_DEP,
                DEP_DELAY_MINUTES AS DEP_DELAY_MIN,
                (DEP_DEL15 = 1)   AS IS_DEP_DELAYED,
                CRS_ARR_TIME      AS SCHEDULED_ARR,
                ARR_TIME          AS ACTUAL_ARR,
                ARR_DELAY_MINUTES AS ARR_DELAY_MIN,
                (ARR_DEL15 = 1)   AS IS_ARR_DELAYED,
                (CANCELLED = 1)   AS IS_CANCELLED,
                CANCELLATION_CODE AS CANCEL_CODE,
                (DIVERTED = 1)    AS IS_DIVERTED,
                CRS_ELAPSED_TIME    AS SCHEDULED_ELAPSED,
                ACTUAL_ELAPSED_TIME AS ACTUAL_ELAPSED,
                AIR_TIME, DISTANCE,
                CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY,
                SECURITY_DELAY, LATE_AIRCRAFT_DELAY,
                'BTS'               AS DATA_SOURCE,
                CURRENT_TIMESTAMP() AS _LOAD_TS
            FROM RAW.BTS_ONTIME_RAW
        """
        cur.execute(staging_sql)
        logger.info("STAGING.FLIGHTS populated")

        conn.commit()
        logger.info("Snowflake load done. Total batches: %d", total_loaded)

    except Exception as e:
        logger.error("Snowflake load failed: %s", e, exc_info=True)
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Run historical BTS data pipeline")
    parser.add_argument("--start",    default="2000-01", help="Start year-month YYYY-MM")
    parser.add_argument("--end",      default="2024-12", help="End year-month YYYY-MM")
    parser.add_argument("--skip-download", action="store_true", help="Skip download step")
    parser.add_argument("--skip-clean",    action="store_true", help="Skip Spark cleaning step")
    parser.add_argument("--dry-run",       action="store_true", help="Download + clean only, no Snowflake")
    parser.add_argument("--workers",  type=int, default=2,  help="Download threads")
    args = parser.parse_args()

    # Load .env if present
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    logger.info("Historical pipeline: %s → %s", args.start, args.end)

    if not args.skip_download:
        step_download(args.start, args.end, workers=args.workers)

    if not args.skip_clean:
        ok = step_clean(args.start, args.end)
        if not ok:
            logger.error("Cleaning failed, aborting pipeline")
            sys.exit(1)

    step_load_snowflake(dry_run=args.dry_run)

    logger.info("Historical pipeline complete.")


if __name__ == "__main__":
    main()
