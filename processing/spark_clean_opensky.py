"""
OpenSky Flight Data Cleaner (PySpark)
=======================================
Reads raw OpenSky JSON files produced by opensky_fetcher.py,
cleans and normalizes the data, then writes Parquet for Snowflake load.

Usage:
    python spark_clean_opensky.py --input ./raw_data/opensky --output ./cleaned_data/opensky
    python spark_clean_opensky.py --input ./raw_data/opensky/2025-04-02 --output ./cleaned_data/opensky
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Schema for /flights/departure and /flights/arrival records
FLIGHT_SCHEMA = StructType([
    StructField("icao24",                              StringType(),  True),
    StructField("firstSeen",                           LongType(),    True),
    StructField("estDepartureAirport",                 StringType(),  True),
    StructField("lastSeen",                            LongType(),    True),
    StructField("estArrivalAirport",                   StringType(),  True),
    StructField("callsign",                            StringType(),  True),
    StructField("estDepartureAirportHorizDistance",    IntegerType(), True),
    StructField("estDepartureAirportVertDistance",     IntegerType(), True),
    StructField("estArrivalAirportHorizDistance",      IntegerType(), True),
    StructField("estArrivalAirportVertDistance",       IntegerType(), True),
    StructField("departureAirportCandidatesCount",     IntegerType(), True),
    StructField("arrivalAirportCandidatesCount",       IntegerType(), True),
    StructField("fetch_date",                          StringType(),  True),
    StructField("fetch_airport",                       StringType(),  True),
    StructField("direction",                           StringType(),  True),
])

# Schema for /states/all snapshot records
STATES_SCHEMA = StructType([
    StructField("icao24",          StringType(),  True),
    StructField("callsign",        StringType(),  True),
    StructField("origin_country",  StringType(),  True),
    StructField("time_position",   LongType(),    True),
    StructField("last_contact",    LongType(),    True),
    StructField("longitude",       DoubleType(),  True),
    StructField("latitude",        DoubleType(),  True),
    StructField("baro_altitude",   DoubleType(),  True),
    StructField("on_ground",       BooleanType(), True),
    StructField("velocity",        DoubleType(),  True),
    StructField("true_track",      DoubleType(),  True),
    StructField("vertical_rate",   DoubleType(),  True),
    StructField("squawk",          StringType(),  True),
    StructField("spi",             BooleanType(), True),
    StructField("position_source", IntegerType(), True),
    StructField("fetch_ts",        StringType(),  True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OpenSky_Cleaner")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def find_json_files(input_dir: Path, pattern: str = "flights.json") -> list:
    return sorted(input_dir.rglob(pattern))


def clean_flights_df(spark: SparkSession, json_files: list):
    """Load and clean OpenSky flight records."""
    if not json_files:
        return None

    paths = [str(p) for p in json_files]
    df = spark.read.schema(FLIGHT_SCHEMA).json(paths)

    # Drop duplicates (same flight may appear in both departure and arrival fetches)
    df = df.dropDuplicates(["icao24", "firstSeen", "estDepartureAirport", "estArrivalAirport"])

    # Convert timestamps to human-readable dates
    df = (
        df
        .withColumn("FETCH_DATE",           F.to_date(F.col("fetch_date"), "yyyy-MM-dd"))
        .withColumn("ICAO24",               F.upper(F.trim(F.col("icao24"))))
        .withColumn("CALLSIGN",             F.trim(F.col("callsign")))
        .withColumn("FIRST_SEEN",           F.col("firstSeen").cast(LongType()))
        .withColumn("EST_DEPARTURE_AIRPORT", F.upper(F.trim(F.col("estDepartureAirport"))))
        .withColumn("LAST_SEEN",            F.col("lastSeen").cast(LongType()))
        .withColumn("EST_ARRIVAL_AIRPORT",  F.upper(F.trim(F.col("estArrivalAirport"))))
        .withColumn("DEP_AIRPORT_HORIZ_DIST",  F.col("estDepartureAirportHorizDistance"))
        .withColumn("DEP_AIRPORT_VERT_DIST",   F.col("estDepartureAirportVertDistance"))
        .withColumn("ARR_AIRPORT_HORIZ_DIST",  F.col("estArrivalAirportHorizDistance"))
        .withColumn("ARR_AIRPORT_VERT_DIST",   F.col("estArrivalAirportVertDistance"))
        .withColumn("DEP_CANDIDATES_COUNT",    F.col("departureAirportCandidatesCount"))
        .withColumn("ARR_CANDIDATES_COUNT",    F.col("arrivalAirportCandidatesCount"))
        .withColumn("_LOAD_TS",             F.current_timestamp())
    )

    # Keep rows with at least one valid airport
    df = df.filter(
        F.col("EST_DEPARTURE_AIRPORT").isNotNull() |
        F.col("EST_ARRIVAL_AIRPORT").isNotNull()
    )

    # Drop empty callsigns (likely noise)
    df = df.withColumn(
        "CALLSIGN",
        F.when(F.col("CALLSIGN") == "", None).otherwise(F.col("CALLSIGN"))
    )

    result_cols = [
        "FETCH_DATE", "ICAO24", "CALLSIGN",
        "FIRST_SEEN", "EST_DEPARTURE_AIRPORT",
        "LAST_SEEN", "EST_ARRIVAL_AIRPORT",
        "DEP_AIRPORT_HORIZ_DIST", "DEP_AIRPORT_VERT_DIST",
        "ARR_AIRPORT_HORIZ_DIST", "ARR_AIRPORT_VERT_DIST",
        "DEP_CANDIDATES_COUNT", "ARR_CANDIDATES_COUNT",
        "_LOAD_TS",
    ]
    return df.select(result_cols)


def clean_states_df(spark: SparkSession, json_files: list):
    """Load and clean OpenSky state snapshot records."""
    if not json_files:
        return None

    paths = [str(p) for p in json_files]
    df = spark.read.schema(STATES_SCHEMA).json(paths)

    df = (
        df
        .withColumn("FETCH_DATE",      F.to_date(F.substring(F.col("fetch_ts"), 1, 8), "yyyyMMdd"))
        .withColumn("ICAO24",          F.upper(F.trim(F.col("icao24"))))
        .withColumn("CALLSIGN",        F.trim(F.col("callsign")))
        .withColumn("ORIGIN_COUNTRY",  F.col("origin_country"))
        .withColumn("TIME_POSITION",   F.col("time_position"))
        .withColumn("LAST_CONTACT",    F.col("last_contact"))
        .withColumn("LONGITUDE",       F.col("longitude"))
        .withColumn("LATITUDE",        F.col("latitude"))
        .withColumn("BARO_ALTITUDE",   F.col("baro_altitude"))
        .withColumn("ON_GROUND",       F.col("on_ground"))
        .withColumn("VELOCITY",        F.col("velocity"))
        .withColumn("TRUE_TRACK",      F.col("true_track"))
        .withColumn("VERTICAL_RATE",   F.col("vertical_rate"))
        .withColumn("SQUAWK",          F.col("squawk"))
        .withColumn("SPI",             F.col("spi"))
        .withColumn("POSITION_SOURCE", F.col("position_source"))
        .withColumn("_LOAD_TS",        F.current_timestamp())
    )

    # Keep only airborne US flights (filter noise)
    df = df.filter(
        F.col("ICAO24").isNotNull() &
        (F.col("ON_GROUND") == False) &
        F.col("LATITUDE").between(15.0, 75.0) &
        F.col("LONGITUDE").between(-180.0, -50.0)
    )

    result_cols = [
        "FETCH_DATE", "ICAO24", "CALLSIGN", "ORIGIN_COUNTRY",
        "TIME_POSITION", "LAST_CONTACT", "LONGITUDE", "LATITUDE",
        "BARO_ALTITUDE", "ON_GROUND", "VELOCITY", "TRUE_TRACK",
        "VERTICAL_RATE", "SQUAWK", "SPI", "POSITION_SOURCE",
        "_LOAD_TS",
    ]
    return df.select(result_cols)


def main():
    parser = argparse.ArgumentParser(description="Clean OpenSky data using PySpark")
    parser.add_argument("--input",  default="./raw_data/opensky",       help="Input dir with JSON files")
    parser.add_argument("--output", default="./cleaned_data/opensky",   help="Output dir for parquet")
    parser.add_argument("--type",   choices=["flights", "states", "all"], default="all")
    args = parser.parse_args()

    input_dir  = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    if args.type in ("flights", "all"):
        flight_files = find_json_files(input_dir, "flights.json")
        logger.info("Found %d flight JSON files", len(flight_files))
        if flight_files:
            df = clean_flights_df(spark, flight_files)
            if df:
                count = df.count()
                out = output_dir / "flights"
                df.write.mode("overwrite").partitionBy("FETCH_DATE").parquet(str(out))
                logger.info("Flights: %d rows → %s", count, out)

    if args.type in ("states", "all"):
        state_files = find_json_files(input_dir, "states_*.json")
        logger.info("Found %d state JSON files", len(state_files))
        if state_files:
            df = clean_states_df(spark, state_files)
            if df:
                count = df.count()
                out = output_dir / "states"
                df.write.mode("overwrite").partitionBy("FETCH_DATE").parquet(str(out))
                logger.info("States: %d rows → %s", count, out)

    spark.stop()
    logger.info("Done.")


if __name__ == "__main__":
    main()
