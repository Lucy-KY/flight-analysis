"""
Daily Incremental Pipeline
============================
Runs every day (triggered by launchd or Airflow) to:
  1. Fetch FAA NAS status snapshot for target date
  2. Clean with PySpark
  3. Load to Snowflake RAW.FAA_NAS_STATUS_RAW
  4. Refresh all ANALYTICS tables (BTS-sourced)
  5. Generate Claude-powered Chinese anomaly report (requires ANTHROPIC_API_KEY)

Usage:
    python daily_pipeline.py                # process yesterday (all steps)
    python daily_pipeline.py --date 2025-04-02
    python daily_pipeline.py --dry-run      # skip Snowflake load
    python daily_pipeline.py --no-faa-nas   # analytics-only run
    python daily_pipeline.py --skip-report  # skip Claude report generation
"""

import argparse
import logging
import os
import socket
import subprocess
import sys
import time
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


def wait_for_network(host: str = "8.8.8.8", port: int = 53,
                     timeout: int = 300, interval: int = 10) -> bool:
    """
    Block until TCP connection to host:port succeeds or timeout is reached.
    Used to wait for DNS/network to become available after Mac wakes from sleep.
    """
    deadline = time.time() + timeout
    attempt  = 0
    while time.time() < deadline:
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
            if attempt > 0:
                logger.info("Network ready after %d attempt(s).", attempt + 1)
            return True
        except OSError:
            attempt += 1
            logger.info("Network not ready (attempt %d), retrying in %ds…", attempt, interval)
            time.sleep(interval)
    logger.error("Network did not become available within %ds. Aborting.", timeout)
    return False


BASE_DIR     = Path(__file__).parent.parent
RAW_DATA     = BASE_DIR / "raw_data"
CLEANED_DATA = BASE_DIR / "cleaned_data"


def step_fetch_faa_nas(target_date: date) -> bool:
    """Fetch FAA NAS snapshot for target_date."""
    logger.info("STEP 1: Fetching FAA NAS status for %s", target_date)
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
    logger.info("STEP 2: Spark cleaning FAA NAS data for %s", target_date)
    date_str  = target_date.strftime("%Y-%m-%d")
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
    logger.info("STEP 3: Loading FAA NAS data to Snowflake for %s", target_date)
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
        # if the pipeline is re-run for the same date after a failure).
        cur.execute(
            "DELETE FROM RAW.FAA_NAS_STATUS_RAW WHERE FETCH_DATE = %s OR FETCH_DATE IS NULL",
            (target_date.isoformat(),),
        )
        deleted = cur.rowcount
        if deleted:
            logger.info("Removed %d stale row(s) for %s before reload", deleted, target_date)

        for pf in parquet_files:
            cur.execute(f"PUT 'file://{pf}' @RAW.FAA_NAS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        cur.execute("""
            COPY INTO RAW.FAA_NAS_STATUS_RAW
            FROM @RAW.FAA_NAS_STAGE
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
            ON_ERROR = CONTINUE
        """)
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


def step_run_analytics(target_date: date, dry_run: bool = False):
    """Refresh all ANALYTICS tables (BTS-sourced, always runs)."""
    logger.info("STEP 4: Running analytics refresh")
    if dry_run:
        logger.info("[DRY RUN] Skipping analytics")
        return

    analytics_script = BASE_DIR / "analysis" / "run_analytics.py"
    # Do NOT pass --date: incremental mode restricts queries to a single
    # YEAR+MONTH and returns 0 rows for months not yet in BTS data.
    # Full refresh correctly aggregates all historical BTS data.
    cmd = [sys.executable, str(analytics_script)]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("Analytics step failed with code %d", result.returncode)
    else:
        logger.info("Analytics completed")


def step_generate_report(target_date: date, dry_run: bool = False):
    """
    STEP 5: Call Claude API to generate a structured Chinese anomaly report
    from FAA NAS data loaded in Snowflake.
    Non-fatal — a failure here will not abort the pipeline.
    Requires ANTHROPIC_API_KEY in the environment.
    """
    logger.info("STEP 5: Generating FAA NAS anomaly report for %s", target_date)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.warning(
            "ANTHROPIC_API_KEY not set — skipping Claude report generation. "
            "Add ANTHROPIC_API_KEY to code/.env to enable this step."
        )
        return

    try:
        # Import lazily — avoids hard dependency if anthropic package is missing
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "faa_nas_anomaly_report",
            BASE_DIR / "analysis" / "faa_nas_anomaly_report.py",
        )
        report_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(report_mod)

        summary = report_mod.run_report(target_date, dry_run=dry_run)
        logger.info("FAA NAS report generated (%d chars)", len(summary))
    except ImportError as exc:
        logger.warning(
            "Could not import 'anthropic' package — skipping report generation. "
            "Run: pip install anthropic>=0.49.0  (%s)", exc
        )
    except Exception as exc:
        logger.warning("FAA NAS report generation failed (non-fatal): %s", exc, exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="Daily incremental flight data pipeline")
    parser.add_argument("--date",           help="Target date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--dry-run",        action="store_true", help="Skip Snowflake operations")
    parser.add_argument("--skip-fetch",     action="store_true")
    parser.add_argument("--skip-clean",     action="store_true")
    parser.add_argument("--skip-load",      action="store_true")
    parser.add_argument("--skip-analytics", action="store_true")
    parser.add_argument("--skip-report",   action="store_true",
                        help="Skip Claude NAS report generation")
    parser.add_argument("--faa-nas",        action="store_true", default=True,
                        dest="faa_nas",
                        help="Enable FAA NAS status fetch/clean/load (default: on)")
    parser.add_argument("--no-faa-nas",     action="store_false",
                        dest="faa_nas",
                        help="Disable FAA NAS status steps (analytics still runs)")
    args = parser.parse_args()

    env_file = BASE_DIR / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    # Wait for network — necessary when launchd fires shortly after Mac wakes.
    if not wait_for_network():
        logger.error("Aborting daily pipeline: no network.")
        sys.exit(1)
    logger.info("Network check passed.")

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

    # Analytics always runs regardless of FAA NAS outcome —
    # it refreshes BTS-sourced ANALYTICS tables.
    if not args.skip_analytics:
        step_run_analytics(target, dry_run=args.dry_run)

    # Claude-powered NAS anomaly report (non-fatal, needs ANTHROPIC_API_KEY)
    if not args.skip_report:
        step_generate_report(target, dry_run=args.dry_run)

    logger.info("Daily pipeline complete for %s", target)


if __name__ == "__main__":
    main()
