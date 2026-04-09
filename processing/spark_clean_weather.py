"""
Weather Data Cleaner (PySpark)
==============================
Reads raw Open-Meteo JSON files, explodes hourly arrays into one row per hour,
applies column renames and type casts, deduplicates, and writes clean Parquet
files ready for Snowflake bulk load into RAW.AIRPORT_WEATHER_HOURLY.

Usage:
    python spark_clean_weather.py --input ./raw_data --output ./cleaned_data
    python spark_clean_weather.py --input ./raw_data --output ./cleaned_data --year 2023
"""

import argparse
import glob as glob_module
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType, IntegerType, StringType, StructField, StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ── Output schema — column names MUST match Snowflake DDL exactly ─────────────
WEATHER_SCHEMA = StructType([
    StructField("IATA_CODE",         StringType(),  False),
    StructField("OBS_TIMESTAMP",     StringType(),  False),
    StructField("OBS_DATE",          StringType(),  False),
    StructField("YEAR",              IntegerType(), False),
    StructField("MONTH",             IntegerType(), False),
    StructField("OBS_HOUR",          IntegerType(), True),
    StructField("TEMP_C",            FloatType(),   True),
    StructField("PRECIP_MM",         FloatType(),   True),
    StructField("RAIN_MM",           FloatType(),   True),
    StructField("SNOWFALL_CM",       FloatType(),   True),
    StructField("SNOW_DEPTH_CM",     FloatType(),   True),
    StructField("WIND_SPEED_KMH",    FloatType(),   True),
    StructField("WIND_GUST_KMH",     FloatType(),   True),
    StructField("WIND_DIR_DEG",      FloatType(),   True),
    StructField("VISIBILITY_M",      FloatType(),   True),
    StructField("CLOUD_COVER_PCT",   FloatType(),   True),
    StructField("WEATHER_CODE",      IntegerType(), True),
    StructField("PRESSURE_HPA",      FloatType(),   True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("WeatherCleaner")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def clean_weather(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
    year_filter: int = None,
) -> int:
    """
    Read all Open-Meteo JSON files, flatten hourly arrays, and write Parquet.

    Returns the number of output rows written.
    """
    # Spark 4.x does not expand glob strings on local filesystems —
    # use Python glob to resolve real paths and pass a list to Spark.
    pattern = str(input_dir / "weather" / "*" / "*.json")
    json_files = glob_module.glob(pattern)
    logger.info("Found %d JSON files matching: %s", len(json_files), pattern)

    if not json_files:
        logger.warning("No JSON files found — check that download completed.")
        return 0

    raw = spark.read.option("multiLine", True).json(json_files)

    logger.info("Raw schema: %s", raw.schema.simpleString())

    # ── Zip all hourly arrays into an array of structs ──────────────────────────
    # Open-Meteo returns parallel arrays; arrays_zip binds them by index.
    hourly_cols = [
        F.col("hourly.time").alias("time"),
        F.col("hourly.temperature_2m").alias("temperature_2m"),
        F.col("hourly.precipitation").alias("precipitation"),
        F.col("hourly.rain").alias("rain"),
        F.col("hourly.snowfall").alias("snowfall"),
        F.col("hourly.snow_depth").alias("snow_depth"),
        F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
        F.col("hourly.wind_gusts_10m").alias("wind_gusts_10m"),
        F.col("hourly.wind_direction_10m").alias("wind_direction_10m"),
        F.col("hourly.visibility").alias("visibility"),
        F.col("hourly.cloud_cover").alias("cloud_cover"),
        F.col("hourly.weather_code").alias("weather_code"),
        F.col("hourly.pressure_msl").alias("pressure_msl"),
    ]

    zipped = raw.select(
        F.col("iata_code").alias("iata_raw"),
        F.arrays_zip(
            "hourly.time",
            "hourly.temperature_2m",
            "hourly.precipitation",
            "hourly.rain",
            "hourly.snowfall",
            "hourly.snow_depth",
            "hourly.wind_speed_10m",
            "hourly.wind_gusts_10m",
            "hourly.wind_direction_10m",
            "hourly.visibility",
            "hourly.cloud_cover",
            "hourly.weather_code",
            "hourly.pressure_msl",
        ).alias("hourly_zip"),
    )

    # ── Explode zipped array into one row per hour ──────────────────────────────
    exploded = zipped.select(
        F.col("iata_raw"),
        F.explode("hourly_zip").alias("h"),
    )

    # ── Extract fields from the exploded struct ─────────────────────────────────
    # arrays_zip field names follow the last segment of the column path
    df = exploded.select(
        F.upper(F.col("iata_raw")).alias("IATA_CODE"),
        F.col("h.time").alias("OBS_TIMESTAMP"),
        F.substring(F.col("h.time"), 1, 10).alias("OBS_DATE"),
        F.substring(F.col("h.time"), 1, 4).cast(IntegerType()).alias("YEAR"),
        F.substring(F.col("h.time"), 6, 2).cast(IntegerType()).alias("MONTH"),
        F.substring(F.col("h.time"), 12, 2).cast(IntegerType()).alias("OBS_HOUR"),
        F.col("h.temperature_2m").cast(FloatType()).alias("TEMP_C"),
        F.col("h.precipitation").cast(FloatType()).alias("PRECIP_MM"),
        F.col("h.rain").cast(FloatType()).alias("RAIN_MM"),
        F.col("h.snowfall").cast(FloatType()).alias("SNOWFALL_CM"),
        F.col("h.snow_depth").cast(FloatType()).alias("SNOW_DEPTH_CM"),
        F.col("h.wind_speed_10m").cast(FloatType()).alias("WIND_SPEED_KMH"),
        F.col("h.wind_gusts_10m").cast(FloatType()).alias("WIND_GUST_KMH"),
        F.col("h.wind_direction_10m").cast(FloatType()).alias("WIND_DIR_DEG"),
        F.col("h.visibility").cast(FloatType()).alias("VISIBILITY_M"),
        F.col("h.cloud_cover").cast(FloatType()).alias("CLOUD_COVER_PCT"),
        F.col("h.weather_code").cast(IntegerType()).alias("WEATHER_CODE"),
        F.col("h.pressure_msl").cast(FloatType()).alias("PRESSURE_HPA"),
    )

    # Drop rows with no timestamp (malformed records)
    df = df.filter(F.col("OBS_TIMESTAMP").isNotNull() & F.col("IATA_CODE").isNotNull())

    # Optional year filter
    if year_filter is not None:
        logger.info("Filtering to year %d", year_filter)
        df = df.filter(F.col("YEAR") == year_filter)

    # ── Deduplicate on (IATA_CODE, OBS_TIMESTAMP) ──────────────────────────────
    df = df.dropDuplicates(["IATA_CODE", "OBS_TIMESTAMP"])

    # ── Write Parquet ──────────────────────────────────────────────────────────
    # Do NOT use partitionBy("YEAR"): Spark strips the partition column from
    # the Parquet file body, so Snowflake COPY INTO MATCH_BY_COLUMN_NAME loads
    # YEAR as NULL. Write flat files instead; YEAR column stays in the data.
    out_path = str(output_dir / "weather")
    logger.info("Writing Parquet to: %s", out_path)

    n = df.count()
    df.repartition(26).write.mode("overwrite").parquet(out_path)
    logger.info("Wrote %d rows to %s", n, out_path)
    return n


def main():
    parser = argparse.ArgumentParser(description="Clean Open-Meteo weather JSON with PySpark")
    parser.add_argument("--input",  default="./raw_data",    help="Base input directory (contains weather/)")
    parser.add_argument("--output", default="./cleaned_data", help="Base output directory")
    parser.add_argument("--year",   type=int, default=None,   help="Process only this year")
    args = parser.parse_args()

    input_dir  = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    try:
        n = clean_weather(spark, input_dir, output_dir, year_filter=args.year)
        logger.info("Cleaning complete. Total rows: %d", n)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
