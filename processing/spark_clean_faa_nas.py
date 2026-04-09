"""
FAA NAS Status Data Cleaner (PySpark)
=======================================
Reads raw FAA NAS JSON snapshot files produced by faa_nas_fetcher.py,
cleans and normalizes the data, then writes Parquet for Snowflake load.

Usage:
    python spark_clean_faa_nas.py --input ./raw_data --output ./cleaned_data
    python spark_clean_faa_nas.py --input ./raw_data --output ./cleaned_data --date 2026-04-07
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, DateType, FloatType, IntegerType,
    StringType, StructField, StructType, TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Schema matching Snowflake RAW.FAA_NAS_STATUS_RAW column names exactly
FAA_NAS_SCHEMA = StructType([
    StructField("SNAPSHOT_TS",       StringType(),  True),   # ISO string → cast to TimestampType
    StructField("FETCH_DATE",        StringType(),  True),   # ISO date string → cast to DateType
    StructField("IATA_CODE",         StringType(),  True),
    StructField("HAS_DELAY",         BooleanType(), True),
    StructField("DELAY_COUNT",       IntegerType(), True),
    StructField("DELAY_TYPE",        StringType(),  True),
    StructField("REASON",            StringType(),  True),
    StructField("AVG_DELAY_MIN",     IntegerType(), True),
    StructField("MIN_DELAY_MIN",     IntegerType(), True),
    StructField("MAX_DELAY_MIN",     IntegerType(), True),
    StructField("TREND",             StringType(),  True),
    StructField("END_TIME",          StringType(),  True),
    StructField("WEATHER_TEMP_F",    FloatType(),   True),
    StructField("WEATHER_VIS_MI",    FloatType(),   True),
    StructField("WEATHER_WIND",      StringType(),  True),
    StructField("WEATHER_CONDITION", StringType(),  True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FAA_NAS_Cleaner")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def find_json_files(input_dir: Path, date_filter: str = None) -> list:
    """Find all snapshot.json files under input_dir/faa_nas/, optionally filtered by date."""
    nas_dir = input_dir / "faa_nas"
    if not nas_dir.exists():
        return []
    if date_filter:
        pattern = f"faa_nas/{date_filter}/snapshot.json"
        return sorted(input_dir.glob(pattern))
    return sorted(nas_dir.rglob("snapshot.json"))


def clean_faa_nas_df(spark: SparkSession, json_files: list):
    """Load and clean FAA NAS snapshot records."""
    if not json_files:
        return None

    paths = [str(p) for p in json_files]
    df = spark.read.schema(FAA_NAS_SCHEMA).json(paths)

    # Cast SNAPSHOT_TS from ISO string to TimestampType
    df = df.withColumn(
        "SNAPSHOT_TS",
        F.to_timestamp(F.col("SNAPSHOT_TS"), "yyyy-MM-dd'T'HH:mm:ss"),
    )

    # Cast FETCH_DATE from ISO string to DateType
    df = df.withColumn(
        "FETCH_DATE",
        F.to_date(F.col("FETCH_DATE"), "yyyy-MM-dd"),
    )

    # Normalize IATA_CODE to uppercase, trim whitespace
    df = df.withColumn("IATA_CODE", F.upper(F.trim(F.col("IATA_CODE"))))

    # Drop exact duplicates where FETCH_DATE, IATA_CODE, and DELAY_TYPE are all non-null
    df = df.filter(
        F.col("FETCH_DATE").isNotNull() &
        F.col("IATA_CODE").isNotNull() &
        F.col("DELAY_TYPE").isNotNull()
    ).dropDuplicates(["FETCH_DATE", "IATA_CODE", "DELAY_TYPE"]).union(
        # Re-union the no-delay rows (DELAY_TYPE is null) after separate dedup on airport+date
        df.filter(F.col("DELAY_TYPE").isNull())
          .dropDuplicates(["FETCH_DATE", "IATA_CODE"])
    )

    # Add load timestamp
    df = df.withColumn("_LOAD_TS", F.current_timestamp())

    result_cols = [
        "SNAPSHOT_TS", "FETCH_DATE", "IATA_CODE",
        "HAS_DELAY", "DELAY_COUNT", "DELAY_TYPE",
        "REASON", "AVG_DELAY_MIN", "MIN_DELAY_MIN", "MAX_DELAY_MIN",
        "TREND", "END_TIME",
        "WEATHER_TEMP_F", "WEATHER_VIS_MI", "WEATHER_WIND", "WEATHER_CONDITION",
        "_LOAD_TS",
    ]
    return df.select(result_cols)


def main():
    parser = argparse.ArgumentParser(description="Clean FAA NAS snapshot data using PySpark")
    parser.add_argument("--input-dir",  default="./raw_data",      help="Input root dir with JSON files")
    parser.add_argument("--output-dir", default="./cleaned_data",  help="Output root dir for parquet")
    parser.add_argument("--date",       default=None,              help="Filter to a single date YYYY-MM-DD")
    args = parser.parse_args()

    input_dir  = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    json_files = find_json_files(input_dir, date_filter=args.date)
    logger.info("Found %d FAA NAS JSON snapshot file(s)", len(json_files))

    if not json_files:
        logger.warning("No FAA NAS snapshot files found under %s", input_dir / "faa_nas")
        spark.stop()
        return

    df = clean_faa_nas_df(spark, json_files)
    if df is not None:
        out = output_dir / "faa_nas"
        # Do NOT partitionBy here: Spark strips partition columns from the
        # Parquet file body, so Snowflake COPY INTO MATCH_BY_COLUMN_NAME
        # cannot find FETCH_DATE and loads it as NULL (violates PK).
        # FAA NAS snapshots are tiny (~50 rows/day) — no partitioning needed.
        df.coalesce(1).write.mode("overwrite").parquet(str(out))
        logger.info("FAA NAS cleaned data written → %s", out)
    else:
        logger.warning("No records to write after cleaning")

    spark.stop()
    logger.info("Done.")


if __name__ == "__main__":
    main()
