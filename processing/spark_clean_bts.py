"""
BTS On-Time Performance Data Cleaner (PySpark)
================================================
Reads raw BTS ZIP files, extracts CSVs, applies cleaning/normalization,
and writes clean Parquet files ready for Snowflake bulk load.

Usage:
    python spark_clean_bts.py --input ./raw_data --output ./cleaned_data/bts
    python spark_clean_bts.py --input ./raw_data --output ./cleaned_data/bts --year 2024
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ── Column mapping: original BTS CSV name → clean internal name ───────────────
COLUMN_RENAMES = {
    "Year":                          "YEAR",
    "Quarter":                       "QUARTER",
    "Month":                         "MONTH",
    "DayofMonth":                    "DAY_OF_MONTH",
    "DayOfWeek":                     "DAY_OF_WEEK",
    "FlightDate":                    "FLIGHT_DATE",
    "Reporting_Airline":             "REPORTING_AIRLINE",
    "IATA_CODE_Reporting_Airline":   "IATA_CODE_REPORTING_AIRLINE",
    "Tail_Number":                   "TAIL_NUMBER",
    "Flight_Number_Reporting_Airline": "FLIGHT_NUMBER",
    "OriginAirportID":               "ORIGIN_AIRPORT_ID",
    "Origin":                        "ORIGIN",
    "OriginCityName":                "ORIGIN_CITY_NAME",
    "OriginState":                   "ORIGIN_STATE",
    "OriginStateName":               "ORIGIN_STATE_NAME",
    "DestAirportID":                 "DEST_AIRPORT_ID",
    "Dest":                          "DEST",
    "DestCityName":                  "DEST_CITY_NAME",
    "DestState":                     "DEST_STATE",
    "DestStateName":                 "DEST_STATE_NAME",
    "CRSDepTime":                    "CRS_DEP_TIME",
    "DepTime":                       "DEP_TIME",
    "DepDelay":                      "DEP_DELAY",
    "DepDelayMinutes":               "DEP_DELAY_MINUTES",
    "DepDel15":                      "DEP_DEL15",
    "TaxiOut":                       "TAXI_OUT",
    "WheelsOff":                     "WHEELS_OFF",
    "WheelsOn":                      "WHEELS_ON",
    "TaxiIn":                        "TAXI_IN",
    "CRSArrTime":                    "CRS_ARR_TIME",
    "ArrTime":                       "ARR_TIME",
    "ArrDelay":                      "ARR_DELAY",
    "ArrDelayMinutes":               "ARR_DELAY_MINUTES",
    "ArrDel15":                      "ARR_DEL15",
    "Cancelled":                     "CANCELLED",
    "CancellationCode":              "CANCELLATION_CODE",
    "Diverted":                      "DIVERTED",
    "CRSElapsedTime":                "CRS_ELAPSED_TIME",
    "ActualElapsedTime":             "ACTUAL_ELAPSED_TIME",
    "AirTime":                       "AIR_TIME",
    "Flights":                       "FLIGHTS",
    "Distance":                      "DISTANCE",
    "DistanceGroup":                 "DISTANCE_GROUP",
    "CarrierDelay":                  "CARRIER_DELAY",
    "WeatherDelay":                  "WEATHER_DELAY",
    "NASDelay":                      "NAS_DELAY",
    "SecurityDelay":                 "SECURITY_DELAY",
    "LateAircraftDelay":             "LATE_AIRCRAFT_DELAY",
}

# Columns to keep in RAW table (drop diversion sub-columns to save space)
KEEP_COLUMNS = list(COLUMN_RENAMES.values())

# Columns expected to be numeric (cast to DOUBLE/INT)
DOUBLE_COLS = {
    "DEP_TIME", "DEP_DELAY", "DEP_DELAY_MINUTES",
    "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN",
    "ARR_TIME", "ARR_DELAY", "ARR_DELAY_MINUTES",
    "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME",
    "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY",
    "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
}
INT_COLS = {
    "YEAR", "QUARTER", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK",
    "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
    "CRS_DEP_TIME", "CRS_ARR_TIME",
    "DEP_DEL15", "ARR_DEL15",
    "CANCELLED", "DIVERTED", "FLIGHTS", "DISTANCE_GROUP",
}


def create_spark_session(app_name: str = "BTS_Cleaner") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def find_zip_files(input_dir: Path, year: int = None, month: int = None) -> list:
    """Locate raw ZIP files, optionally filtered by year/month."""
    pattern = "ontime_*.zip"
    zips = sorted(input_dir.glob(pattern))
    if year:
        zips = [z for z in zips if f"_{year}_" in z.name]
    if month:
        zips = [z for z in zips if z.name.endswith(f"_{month:02d}.zip")]
    return zips


def read_bts_zip(spark: SparkSession, zip_path: Path):
    """Read a BTS ZIP file's CSV directly via Spark (requires zip codec or pre-extract)."""
    # Spark can read CSV inside ZIP if the file is a standard deflate ZIP.
    # For safety, we extract to a temp dir first.
    import zipfile
    import tempfile

    tmp_dir = tempfile.mkdtemp(prefix="bts_extract_")
    logger.info(f"  Extracting {zip_path.name} → {tmp_dir}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            logger.warning(f"  No CSV found in {zip_path.name}")
            return None
        csv_name = csv_names[0]
        zf.extract(csv_name, tmp_dir)

    csv_path = os.path.join(tmp_dir, csv_name)
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(csv_path)
    return df, tmp_dir


def rename_and_cast(df):
    """Rename columns, keep relevant ones, cast types."""
    # Rename available columns (CSV may have trailing empty column)
    for orig, new in COLUMN_RENAMES.items():
        if orig in df.columns:
            df = df.withColumnRenamed(orig, new)

    # Keep only target columns that exist
    existing = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df.select(existing)

    # Cast types
    # BTS CSV stores some int columns as "0.00" float strings, so cast via DOUBLE first
    for col in df.columns:
        if col in INT_COLS:
            df = df.withColumn(col, F.col(col).cast(DoubleType()).cast(IntegerType()))
        elif col in DOUBLE_COLS:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # Cast FLIGHT_DATE to DateType
    if "FLIGHT_DATE" in df.columns:
        df = df.withColumn("FLIGHT_DATE", F.to_date(F.col("FLIGHT_DATE"), "yyyy-MM-dd"))

    return df


def clean_bts(df):
    """Apply data quality rules."""
    # Drop rows missing core identifiers
    df = df.filter(
        F.col("IATA_CODE_REPORTING_AIRLINE").isNotNull() &
        F.col("FLIGHT_DATE").isNotNull() &
        F.col("ORIGIN").isNotNull() &
        F.col("DEST").isNotNull()
    )

    # Trim string columns
    str_cols = ["REPORTING_AIRLINE", "IATA_CODE_REPORTING_AIRLINE",
                "TAIL_NUMBER", "FLIGHT_NUMBER",
                "ORIGIN", "DEST", "CANCELLATION_CODE"]
    for c in str_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))

    # Replace empty strings with NULL in string columns
    for c in str_cols:
        if c in df.columns:
            df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

    # Clamp unrealistic delay values (e.g., BTS uses 0 for negative on cancelled)
    for delay_col in ["DEP_DELAY", "ARR_DELAY", "DEP_DELAY_MINUTES", "ARR_DELAY_MINUTES"]:
        if delay_col in df.columns:
            df = df.withColumn(
                delay_col,
                F.when(F.col("CANCELLED") == 1, None).otherwise(F.col(delay_col))
            )

    # Add metadata
    df = df.withColumn("_SOURCE", F.lit("BTS"))
    df = df.withColumn("_LOAD_TS", F.current_timestamp())

    return df


def build_staging_flights(df):
    """Transform raw BTS into STAGING.FLIGHTS schema."""
    # Composite primary key
    df = df.withColumn(
        "FLIGHT_ID",
        F.concat_ws(
            "_",
            F.coalesce(F.col("IATA_CODE_REPORTING_AIRLINE"), F.lit("XX")),
            F.coalesce(F.col("FLIGHT_NUMBER"), F.lit("0")),
            F.date_format(F.col("FLIGHT_DATE"), "yyyyMMdd"),
            F.coalesce(F.col("ORIGIN"), F.lit("XXX")),
            F.coalesce(F.col("DEST"), F.lit("XXX")),
        )
    )

    df = df.select(
        F.col("FLIGHT_ID"),
        F.col("FLIGHT_DATE"),
        F.col("YEAR"),
        F.col("MONTH"),
        F.col("DAY_OF_MONTH"),
        F.col("DAY_OF_WEEK"),
        F.col("QUARTER"),
        F.col("IATA_CODE_REPORTING_AIRLINE").alias("AIRLINE_CODE"),
        F.col("FLIGHT_NUMBER"),
        F.col("TAIL_NUMBER"),
        F.col("ORIGIN"),
        F.col("ORIGIN_CITY_NAME").alias("ORIGIN_CITY"),
        F.col("ORIGIN_STATE"),
        F.col("DEST"),
        F.col("DEST_CITY_NAME").alias("DEST_CITY"),
        F.col("DEST_STATE"),
        F.col("CRS_DEP_TIME").alias("SCHEDULED_DEP"),
        F.col("DEP_TIME").alias("ACTUAL_DEP"),
        F.col("DEP_DELAY_MINUTES").alias("DEP_DELAY_MIN"),
        (F.col("DEP_DEL15") == 1).cast(BooleanType()).alias("IS_DEP_DELAYED"),
        F.col("CRS_ARR_TIME").alias("SCHEDULED_ARR"),
        F.col("ARR_TIME").alias("ACTUAL_ARR"),
        F.col("ARR_DELAY_MINUTES").alias("ARR_DELAY_MIN"),
        (F.col("ARR_DEL15") == 1).cast(BooleanType()).alias("IS_ARR_DELAYED"),
        (F.col("CANCELLED") == 1).cast(BooleanType()).alias("IS_CANCELLED"),
        F.col("CANCELLATION_CODE").alias("CANCEL_CODE"),
        (F.col("DIVERTED") == 1).cast(BooleanType()).alias("IS_DIVERTED"),
        F.col("CRS_ELAPSED_TIME").alias("SCHEDULED_ELAPSED"),
        F.col("ACTUAL_ELAPSED_TIME").alias("ACTUAL_ELAPSED"),
        F.col("AIR_TIME"),
        F.col("DISTANCE"),
        F.col("CARRIER_DELAY"),
        F.col("WEATHER_DELAY"),
        F.col("NAS_DELAY"),
        F.col("SECURITY_DELAY"),
        F.col("LATE_AIRCRAFT_DELAY"),
        F.lit("BTS").alias("DATA_SOURCE"),
        F.current_timestamp().alias("_LOAD_TS"),
    )
    return df


def process_zip(spark: SparkSession, zip_path: Path, output_dir: Path, write_staging: bool = True):
    """Process one ZIP → write raw + staging parquet."""
    import shutil

    stem = zip_path.stem  # e.g. ontime_2024_01
    logger.info(f"\nProcessing {zip_path.name}")

    result = read_bts_zip(spark, zip_path)
    if result is None:
        return
    df, tmp_dir = result

    try:
        df = rename_and_cast(df)
        df = clean_bts(df)

        row_count = df.count()
        logger.info(f"  {row_count:,} rows after cleaning")

        # Write RAW parquet
        raw_out = output_dir / "raw" / stem
        df.write.mode("overwrite").parquet(str(raw_out))
        logger.info(f"  RAW parquet → {raw_out}")

        # Write STAGING parquet
        if write_staging:
            staging_df = build_staging_flights(df)
            staging_out = output_dir / "staging" / stem
            staging_df.write.mode("overwrite").parquet(str(staging_out))
            logger.info(f"  STAGING parquet → {staging_out}")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="Clean BTS flight data using PySpark")
    parser.add_argument("--input",   default="./raw_data",        help="Input dir with .zip files")
    parser.add_argument("--output",  default="./cleaned_data/bts", help="Output dir for parquet files")
    parser.add_argument("--year",    type=int, default=None,       help="Filter by year (optional)")
    parser.add_argument("--month",   type=int, default=None,       help="Filter by month (optional)")
    parser.add_argument("--no-staging", action="store_true",       help="Skip staging transform")
    args = parser.parse_args()

    input_dir  = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "raw").mkdir(exist_ok=True)
    (output_dir / "staging").mkdir(exist_ok=True)

    zip_files = find_zip_files(input_dir, year=args.year, month=args.month)
    if not zip_files:
        logger.error(f"No ZIP files found in {input_dir} (year={args.year}, month={args.month})")
        sys.exit(1)

    logger.info(f"Found {len(zip_files)} ZIP files to process")
    spark = create_spark_session()

    for i, zf in enumerate(zip_files, 1):
        logger.info(f"[{i}/{len(zip_files)}]")
        try:
            process_zip(spark, zf, output_dir, write_staging=not args.no_staging)
        except Exception as e:
            logger.error(f"  FAILED {zf.name}: {e}", exc_info=True)

    spark.stop()
    logger.info("\nDone.")


if __name__ == "__main__":
    main()
