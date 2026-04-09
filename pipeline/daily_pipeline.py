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

BASE_DIR        = Path(__file__).parent.parent
RAW_OPENSKY     = BASE_DIR / "raw_data"    / "opensky"
CLEANED_OPENSKY = BASE_DIR / "cleaned_data" / "opensky"
RAW_DATA        = BASE_DIR / "raw_data"
CLEANED_DATA    = BASE_DIR / "cleaned_data"




def step_fetch_faa_nas(target_date: date) -> bool:
    """Fetch FAA NAS snapshot. Always runs regardless of --states-only."""
    logger.info("STEP FAA-1: Fetching FAA NAS status for %s", target_date)
    fetch_script = BASE_DIR / "ingestion" / "faa_nas_fetcher.py"
    cmd = [
        sys.executable, str(fetch_script),
        "--date",   target_date.strftime("%Y-%m-%d"),
        "--output", str(RAW_DATA),
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("FAA NAS fetch failed with code %d", result.returncode)
        return False
    logger.info("FAA NAS fetch completed")
    return True


def step_clean_faa_nas(target_date: date) -> bool:
    """Run Spark cleaning on FAA NAS JSON snapshot for target_date."""
    logger.info("STEP FAA-2: Spark cleaning FAA NAS data for %s", target_date)
    date_str = target_date.strftime("%Y-%m-%d")
    snap_path = RAW_DATA / "faa_nas" / date_str / "snapshot.json"

    if not snap_path.exists():
        logger.warning("No FAA NAS snapshot found at %s", snap_path)
        return False

    clean_script = BASE_DIR / "processing" / "spark_clean_faa_nas.py"
    cmd = [
        sys.executable, str(clean_script),
        "--input-dir",  str(RAW_DATA),
        "--output-dir", str(CLEANED_DATA),
        "--date",       date_str,
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("FAA NAS Spark cleaning failed with code %d", result.returncode)
        return False
    logger.info("FAA NAS cleaning completed")
    return True


def step_load_faa_nas(target_date: date, dry_run: bool = False) -> bool:
    """PUT cleaned FAA NAS parquet → FAA_NAS_STAGE → COPY INTO RAW.FAA_NAS_STATUS_RAW."""
    logger.info("STEP FAA-3: Loading FAA NAS data to Snowflake for %s", target_date)
    if dry_run:
        logger.info("[DRY RUN] Skipping Snowflake FAA NAS load")
        return True

    nas_parquet_dir = CLEANED_DATA / "faa_nas"
    if not nas_parquet_dir.exists():
        logger.warning("No cleaned FAA NAS parquet at %s", nas_parquet_dir)
        return False

    parquet_files = list(nas_parquet_dir.rglob("*.parquet"))
    if not parquet_files:
        logger.warning("No FAA NAS parquet files found")
        return False

    conn = get_conn(schema="RAW")
    cur  = conn.cursor()

    try:
        cur.execute("USE DATABASE FLIGHT_DB")
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")

        # Ensure stage and table exist (idempotent)
        cur.execute("CREATE STAGE IF NOT EXISTS RAW.FAA_NAS_STAGE FILE_FORMAT = (TYPE = PARQUET)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RAW.FAA_NAS_STATUS_RAW (
                SNAPSHOT_TS        TIMESTAMP_NTZ,
                FETCH_DATE         DATE,
                IATA_CODE          VARCHAR(5),
                HAS_DELAY          BOOLEAN,
                DELAY_COUNT        INTEGER,
                DELAY_TYPE         VARCHAR(30),
                REASON             VARCHAR(500),
                AVG_DELAY_MIN      INTEGER,
                MIN_DELAY_MIN      INTEGER,
                MAX_DELAY_MIN      INTEGER,
                TREND              VARCHAR(20),
                END_TIME           VARCHAR(50),
                WEATHER_TEMP_F     FLOAT,
                WEATHER_VIS_MI     FLOAT,
                WEATHER_WIND       VARCHAR(100),
                WEATHER_CONDITION  VARCHAR(100),
                _LOAD_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            ) CLUSTER BY (FETCH_DATE)
        """)
        logger.info("FAA NAS stage and table ready.")

        # Idempotent: delete today's rows before re-loading (prevents duplicates
        # if the pipeline is re-run for the same date, e.g. after a failure).
        # Also purge any historically corrupt rows with NULL FETCH_DATE.
        cur.execute(
            "DELETE FROM RAW.FAA_NAS_STATUS_RAW WHERE FETCH_DATE = %s OR FETCH_DATE IS NULL",
            (target_date.isoformat(),),
        )
        deleted = cur.rowcount
        if deleted:
            logger.info("Removed %d stale/corrupt row(s) for %s before reload", deleted, target_date)

        # PUT files to Snowflake stage
        for pf in parquet_files:
            put_sql = f"PUT 'file://{pf}' @RAW.FAA_NAS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cur.execute(put_sql)

        # COPY INTO raw table
        copy_sql = """
            COPY INTO RAW.FAA_NAS_STATUS_RAW
            FROM @RAW.FAA_NAS_STAGE
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
            ON_ERROR = CONTINUE
        """
        cur.execute(copy_sql)
        logger.info("FAA NAS raw load: %s", cur.fetchone())

        conn.commit()
        logger.info("FAA NAS Snowflake load complete")
        return True

    except Exception as e:
        logger.error("FAA NAS Snowflake load failed: %s", e, exc_info=True)
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


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
    # Do NOT pass --date: incremental mode restricts queries to a single
    # YEAR+MONTH, which returns 0 rows for current dates not yet in BTS data.
    # Always run a full refresh to correctly aggregate all historical BTS data.
    cmd = [sys.executable, str(analytics_script)]
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
                             "departure/arrival (requires Membership); "
                             "kept for backward compatibility")
    parser.add_argument("--opensky",        action="store_true", default=False,
                        help="Enable OpenSky fetch/clean/load steps "
                             "(requires Membership for departure/arrival API)")
    parser.add_argument("--faa-nas",        action="store_true", default=True,
                        dest="faa_nas",
                        help="Enable FAA NAS status fetch/clean/load (default: on)")
    parser.add_argument("--no-faa-nas",     action="store_false",
                        dest="faa_nas",
                        help="Disable FAA NAS status steps")
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

    # ── FAA NAS steps (always-on by default) ─────────────────────────────────
    if args.faa_nas:
        nas_fetch_ok = True
        if not args.skip_fetch:
            nas_fetch_ok = step_fetch_faa_nas(target)
            if not nas_fetch_ok:
                logger.warning("FAA NAS fetch failed; skipping clean + load.")

        nas_clean_ok = True
        if nas_fetch_ok and not args.skip_clean:
            nas_clean_ok = step_clean_faa_nas(target)
            if not nas_clean_ok:
                logger.warning("FAA NAS clean failed; skipping load step.")

        if nas_fetch_ok and nas_clean_ok and not args.skip_load:
            step_load_faa_nas(target, dry_run=args.dry_run)
    else:
        logger.info("FAA NAS steps disabled (--no-faa-nas)")

    # ── OpenSky steps (off by default; requires Membership) ──────────────────
    if args.opensky or args.states_only:
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
    else:
        logger.info("OpenSky steps disabled (use --opensky or --states-only to enable)")

    # Analytics always runs — refreshes BTS-based ANALYTICS tables regardless
    # of whether today's OpenSky data was available.
    if not args.skip_analytics:
        step_run_analytics(target, dry_run=args.dry_run)

    logger.info("Daily pipeline complete for %s", target)


if __name__ == "__main__":
    main()
