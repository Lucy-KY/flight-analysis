import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.snowflake_conn import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("weather_pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)

BASE_DIR    = Path(__file__).parent.parent
RAW_DIR     = BASE_DIR / "raw_data"
CLEANED_DIR = BASE_DIR / "cleaned_data"


def step_download(
    start_year: int,
    end_year: int,
    airports: list = None,
    workers: int = 1,
) -> bool:
    logger.info("=" * 60)
    logger.info("STEP 1: Downloading weather data %d → %d", start_year, end_year)
    logger.info("=" * 60)

    from ingestion.weather_fetcher import fetch_all_airports_bulk

    output_dir = RAW_DIR / "weather"
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = fetch_all_airports_bulk(
        output_dir=output_dir,
        start_year=start_year,
        end_year=end_year,
        airports=airports,
        workers=workers,
    )
    logger.info("Download summary: %s", summary)

    if summary["error"] > 0:
        logger.warning("%d fetch errors occurred; pipeline will continue", summary["error"])
    return True


def step_clean(start_year: int, end_year: int) -> bool:
    logger.info("=" * 60)
    logger.info("STEP 2: Spark cleaning weather data %d → %d", start_year, end_year)
    logger.info("=" * 60)

    import subprocess

    clean_script = BASE_DIR / "processing" / "spark_clean_weather.py"
    cmd = [
        sys.executable, str(clean_script),
        "--input",  str(RAW_DIR),
        "--output", str(CLEANED_DIR),
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("Spark cleaning failed with code %d", result.returncode)
        return False
    logger.info("Spark cleaning completed")
    return True


def step_load_snowflake(dry_run: bool = False) -> bool:
    logger.info("=" * 60)
    logger.info("STEP 3: Loading weather data to Snowflake")
    logger.info("=" * 60)

    if dry_run:
        logger.info("[DRY RUN] Skipping Snowflake load")
        return True

    weather_parquet_dir = CLEANED_DIR / "weather"
    if not weather_parquet_dir.exists():
        logger.error("Weather parquet dir not found: %s", weather_parquet_dir)
        return False

    parquet_files = list(weather_parquet_dir.rglob("*.parquet"))
    if not parquet_files:
        logger.error("No parquet files found under %s", weather_parquet_dir)
        return False

    logger.info("Found %d parquet files to upload", len(parquet_files))

    conn = get_conn(schema="RAW")
    cur  = conn.cursor()

    try:
        cur.execute("USE DATABASE FLIGHT_DB")
        cur.execute("USE SCHEMA RAW")
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")

        # Ensure stage and table exist (idempotent — safe to run every time)
        cur.execute("CREATE STAGE IF NOT EXISTS RAW.WEATHER_STAGE FILE_FORMAT = (TYPE = PARQUET)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RAW.AIRPORT_WEATHER_HOURLY (
                IATA_CODE         VARCHAR(5)    NOT NULL,
                OBS_TIMESTAMP     TIMESTAMP_NTZ NOT NULL,
                OBS_DATE          DATE,
                YEAR              INTEGER,
                MONTH             INTEGER,
                OBS_HOUR          INTEGER,
                TEMP_C            FLOAT,
                PRECIP_MM         FLOAT,
                RAIN_MM           FLOAT,
                SNOWFALL_CM       FLOAT,
                SNOW_DEPTH_CM     FLOAT,
                WIND_SPEED_KMH    FLOAT,
                WIND_GUST_KMH     FLOAT,
                WIND_DIR_DEG      FLOAT,
                VISIBILITY_M      FLOAT,
                CLOUD_COVER_PCT   FLOAT,
                WEATHER_CODE      INTEGER,
                PRESSURE_HPA      FLOAT,
                _LOAD_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            ) CLUSTER BY (YEAR, MONTH, IATA_CODE)
        """)
        logger.info("Stage and table ready.")

        # Truncate before full reload to avoid duplicates from previous runs
        cur.execute("TRUNCATE TABLE RAW.AIRPORT_WEATHER_HOURLY")
        logger.info("Truncated RAW.AIRPORT_WEATHER_HOURLY before reload.")

        for pf in parquet_files:
            put_sql = (
                f"PUT 'file://{pf}' @RAW.WEATHER_STAGE "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            cur.execute(put_sql)
            logger.debug("PUT: %s", pf.name)

        logger.info("All %d files staged. Running COPY INTO...", len(parquet_files))

        # FORCE=TRUE bypasses Snowflake's load-history deduplication so all
        # files are always reloaded (critical after table truncation or when
        # re-running the pipeline after a previous partial load).
        copy_sql = """
            COPY INTO RAW.AIRPORT_WEATHER_HOURLY
            FROM @RAW.WEATHER_STAGE
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            PURGE = TRUE
            ON_ERROR = CONTINUE
        """
        cur.execute(copy_sql)
        # COPY INTO returns one row per file; fetch all to count total loaded
        # Result columns: file, status, rows_parsed, rows_loaded,
        #                 error_limit, errors_seen, first_error, ...
        all_results = cur.fetchall()
        loaded = sum(r[3] for r in all_results if r[1] == "LOADED")
        errors = sum(r[5] for r in all_results if len(r) > 5 and r[5])
        logger.info("COPY INTO: %d files, %s rows loaded, %s errors",
                    len(all_results), f"{loaded:,}", errors)

        conn.commit()
        logger.info("Snowflake load complete.")
        return True

    except Exception as e:
        logger.error("Snowflake load failed: %s", e, exc_info=True)
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="One-time bulk weather data pipeline")
    parser.add_argument("--start-year",      type=int, default=2000,  help="Start year (inclusive)")
    parser.add_argument("--end-year",        type=int, default=2025,  help="End year (inclusive)")
    parser.add_argument("--airports",        default=None,
                        help="Comma-separated IATA codes (default: all 50 airports)")
    parser.add_argument("--workers",         type=int, default=1,     help="1=sequential (safest); 2=~2x faster")
    parser.add_argument("--skip-download",   action="store_true",     help="Skip download step")
    parser.add_argument("--skip-clean",      action="store_true",     help="Skip Spark cleaning step")
    parser.add_argument("--dry-run",         action="store_true",     help="Download + clean only, no Snowflake")
    args = parser.parse_args()

    env_file = BASE_DIR / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    airports = (
        [a.strip().upper() for a in args.airports.split(",")]
        if args.airports else None
    )

    logger.info(
        "Weather pipeline: years %d–%d, airports=%s",
        args.start_year, args.end_year,
        airports or "all",
    )

    if not args.skip_download:
        ok = step_download(args.start_year, args.end_year, airports, args.workers)
        if not ok:
            logger.error("Download step failed")
            sys.exit(1)

    if not args.skip_clean:
        ok = step_clean(args.start_year, args.end_year)
        if not ok:
            logger.error("Cleaning step failed, aborting pipeline")
            sys.exit(1)

    ok = step_load_snowflake(dry_run=args.dry_run)
    if not ok:
        logger.error("Snowflake load failed")
        sys.exit(1)

    logger.info("Weather pipeline complete.")


if __name__ == "__main__":
    main()
