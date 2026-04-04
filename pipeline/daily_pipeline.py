"""
Daily Incremental Pipeline
============================
Runs every day (triggered by scheduler.py or cron) to:
  1. Fetch yesterday's flight data from OpenSky Network API
  2. Clean with PySpark
  3. Load to Snowflake (RAW + STAGING)
  4. Trigger analytics refresh in Snowflake

Usage:
    python daily_pipeline.py                # process yesterday
    python daily_pipeline.py --date 2025-04-02
    python daily_pipeline.py --dry-run      # skip Snowflake load
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.snowflake_conn import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("daily_pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)

BASE_DIR       = Path(__file__).parent.parent
RAW_OPENSKY    = BASE_DIR / "raw_data"    / "opensky"
CLEANED_OPENSKY = BASE_DIR / "cleaned_data" / "opensky"




def step_fetch(target_date: date, states_only: bool = False):
    """Fetch OpenSky data for target_date.

    states_only=True  →  uses /states/all (free tier, no Membership needed)
    states_only=False →  uses /flights/departure + /flights/arrival
                          (requires OpenSky Membership; returns 403 otherwise)
    """
    logger.info("STEP 1: Fetching OpenSky data for %s (states_only=%s)",
                target_date, states_only)
    fetch_script = BASE_DIR / "ingestion" / "opensky_fetcher.py"
    cmd = [
        sys.executable, str(fetch_script),
        "--output", str(RAW_OPENSKY),
    ]
    if states_only:
        cmd.append("--states-only")
    else:
        cmd += ["--date", target_date.strftime("%Y-%m-%d")]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("OpenSky fetch failed with code %d", result.returncode)
        return False
    logger.info("Fetch completed")
    return True


def step_clean(target_date: date):
    """Run Spark cleaning on OpenSky JSON for target_date."""
    logger.info("STEP 2: Spark cleaning for %s", target_date)
    date_str = target_date.strftime("%Y-%m-%d")
    input_dir = RAW_OPENSKY / date_str

    if not input_dir.exists():
        logger.warning("No raw data found at %s", input_dir)
        return False

    clean_script = BASE_DIR / "processing" / "spark_clean_opensky.py"
    cmd = [
        sys.executable, str(clean_script),
        "--input",  str(input_dir),
        "--output", str(CLEANED_OPENSKY),
        "--type",   "flights",
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("Spark cleaning failed with code %d", result.returncode)
        return False
    logger.info("Cleaning completed")
    return True


def step_load_opensky(target_date: date, dry_run: bool = False):
    """Load cleaned OpenSky Parquet to Snowflake."""
    logger.info("STEP 3: Loading OpenSky data to Snowflake for %s", target_date)
    if dry_run:
        logger.info("[DRY RUN] Skipping Snowflake load")
        return

    flights_dir = CLEANED_OPENSKY / "flights"
    if not flights_dir.exists():
        logger.warning("No cleaned flights parquet at %s", flights_dir)
        return

    parquet_files = list(flights_dir.rglob("*.parquet"))
    if not parquet_files:
        logger.warning("No parquet files found")
        return

    conn = get_conn(schema="RAW")
    cur  = conn.cursor()

    try:
        cur.execute("USE DATABASE FLIGHT_DB")
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")

        # PUT files to Snowflake stage
        for pf in parquet_files:
            put_sql = f"PUT 'file://{pf}' @RAW.OPENSKY_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cur.execute(put_sql)

        # COPY INTO raw table
        # Note: cannot use explicit column list together with MATCH_BY_COLUMN_NAME
        copy_sql = """
            COPY INTO RAW.OPENSKY_FLIGHTS_RAW
            FROM @RAW.OPENSKY_STAGE
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
            ON_ERROR = CONTINUE
        """
        cur.execute(copy_sql)
        logger.info("OpenSky raw load: %s", cur.fetchone())

        conn.commit()
        logger.info("Snowflake load complete")

    except Exception as e:
        logger.error("Snowflake load failed: %s", e, exc_info=True)
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def step_run_analytics(target_date: date, dry_run: bool = False):
    """Trigger analytics refresh in Snowflake."""
    logger.info("STEP 4: Running analytics for %s", target_date)
    if dry_run:
        logger.info("[DRY RUN] Skipping analytics")
        return

    analytics_script = BASE_DIR / "analysis" / "run_analytics.py"
    cmd = [
        sys.executable, str(analytics_script),
        "--date", target_date.strftime("%Y-%m-%d"),
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("Analytics step failed with code %d", result.returncode)
    else:
        logger.info("Analytics completed")


def main():
    parser = argparse.ArgumentParser(description="Daily incremental flight data pipeline")
    parser.add_argument("--date",    help="Target date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--dry-run", action="store_true", help="Skip Snowflake operations")
    parser.add_argument("--skip-fetch",     action="store_true")
    parser.add_argument("--skip-clean",     action="store_true")
    parser.add_argument("--skip-load",      action="store_true")
    parser.add_argument("--skip-analytics", action="store_true")
    parser.add_argument("--states-only",    action="store_true",
                        help="Use OpenSky /states/all (free) instead of "
                             "departure/arrival (requires Membership)")
    args = parser.parse_args()

    # Load .env
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    target = (
        datetime.strptime(args.date, "%Y-%m-%d").date()
        if args.date
        else date.today() - timedelta(days=1)
    )
    logger.info("Daily pipeline for: %s", target)

    fetch_ok = True
    if not args.skip_fetch:
        fetch_ok = step_fetch(target, states_only=args.states_only)
        if not fetch_ok:
            logger.warning(
                "Fetch failed (OpenSky 403 = Membership required for "
                "departure/arrival API). Skipping clean + load; "
                "analytics will still run on existing BTS data."
            )

    clean_ok = True
    if fetch_ok and not args.skip_clean:
        clean_ok = step_clean(target)
        if not clean_ok:
            logger.warning("Clean failed, skipping load step.")

    if fetch_ok and clean_ok and not args.skip_load:
        step_load_opensky(target, dry_run=args.dry_run)

    # Analytics always runs — refreshes BTS-based ANALYTICS tables regardless
    # of whether today's OpenSky data was available.
    if not args.skip_analytics:
        step_run_analytics(target, dry_run=args.dry_run)

    logger.info("Daily pipeline complete for %s", target)


if __name__ == "__main__":
    main()
