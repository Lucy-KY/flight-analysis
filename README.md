# U.S. Flight Delay, Weather, and NAS Analysis

End-to-end data engineering and analytics project for U.S. flight operations. The repository combines historical BTS flight data, daily OpenSky flight data, FAA NAS airport-status snapshots, and airport weather data, then loads everything into Snowflake for analytics, dashboarding, and delay prediction.

## What This Project Does

- Downloads historical BTS On-Time Performance data.
- Fetches daily OpenSky flight data.
- Fetches FAA NAS airport delay-status snapshots.
- Fetches historical airport weather from Open-Meteo.
- Cleans raw data with PySpark and writes Parquet.
- Loads cleaned data into Snowflake using a layered model:
  `RAW -> STAGING -> ANALYTICS`
- Exposes results through:
  - scheduled pipelines
  - an Airflow DAG
  - a Streamlit dashboard
  - an XGBoost-based delay predictor

## Data Sources

- BTS PREZIP server: historical U.S. flight performance data
- OpenSky Network API: flight departures, arrivals, and flight-state snapshots
- FAA NAS Status API: airport delay programs and operational constraints
- Open-Meteo Archive API: hourly airport weather

## Repository Structure

```text
code/
├── README.md
├── requirements.txt
├── download_bts_data.py
├── com.flight.daily-pipeline.plist
├── test.java
│
├── analysis/
│   ├── __init__.py
│   └── run_analytics.py
│
├── config/
│   ├── airport_coords.py
│   └── snowflake_conn.py
│
├── dags/
│   └── flight_daily_pipeline.py
│
├── ingestion/
│   ├── __init__.py
│   ├── faa_nas_fetcher.py
│   ├── opensky_fetcher.py
│   └── weather_fetcher.py
│
├── models/
│   ├── __init__.py
│   ├── predict.py
│   └── train_delay_model.py
│
├── pipeline/
│   ├── __init__.py
│   ├── daily_pipeline.py
│   ├── historical_pipeline.py
│   ├── scheduler.py
│   └── weather_pipeline.py
│
├── processing/
│   ├── __init__.py
│   ├── spark_clean_bts.py
│   ├── spark_clean_faa_nas.py
│   ├── spark_clean_opensky.py
│   └── spark_clean_weather.py
│
├── snowflake/
│   ├── analytics_queries.sql
│   └── setup.sql
│
└── visualization/
    ├── app.py
    ├── utils/
    │   ├── __init__.py
    │   └── db.py
    └── pages/
        ├── 1_Delay_Trends.py
        ├── 2_Airline_Performance.py
        ├── 3_Airport_Stats.py
        ├── 4_Delay_Causes.py
        ├── 5_Route_Performance.py
        ├── 6_Live_NAS_Status.py
        └── 7_Delay_Predictor.py
```

## Main Components

### Ingestion

- `download_bts_data.py`
  Downloads BTS ZIP files in bulk from the PREZIP endpoint.
- `ingestion/opensky_fetcher.py`
  Fetches OpenSky departure, arrival, and state-snapshot JSON.
- `ingestion/faa_nas_fetcher.py`
  Fetches FAA NAS airport status and delay-program snapshots.
- `ingestion/weather_fetcher.py`
  Fetches hourly weather history for major U.S. airports.

### Processing

- `processing/spark_clean_bts.py`
  Cleans BTS ZIP/CSV data and writes Parquet for raw and staging layers.
- `processing/spark_clean_opensky.py`
  Cleans OpenSky JSON into Parquet flight and state datasets.
- `processing/spark_clean_faa_nas.py`
  Cleans FAA NAS JSON snapshots into Parquet.
- `processing/spark_clean_weather.py`
  Expands hourly weather arrays into one row per airport-hour.

### Pipelines and Scheduling

- `pipeline/historical_pipeline.py`
  One-time historical BTS load orchestrator.
- `pipeline/daily_pipeline.py`
  Daily operational pipeline for OpenSky, FAA NAS, and analytics refresh.
- `pipeline/weather_pipeline.py`
  Weather-data bulk pipeline.
- `pipeline/scheduler.py`
  APScheduler-based local scheduler for daily runs.
- `dags/flight_daily_pipeline.py`
  Airflow DAG for daily orchestration.
- `com.flight.daily-pipeline.plist`
  macOS `launchd` job definition for background scheduling.

### Snowflake and Analytics

- `snowflake/setup.sql`
  Creates warehouse, database, schemas, tables, and internal stages.
- `snowflake/analytics_queries.sql`
  Contains analytics refresh SQL and ad hoc analysis queries.
- `analysis/run_analytics.py`
  Executes analytics refresh queries against Snowflake.

### Modeling and Prediction

- `models/train_delay_model.py`
  Trains XGBoost classification and regression models from `STAGING.FLIGHTS`.
- `models/predict.py`
  Loads trained models and predicts delay probability and expected delay.

### Dashboard

- `visualization/app.py`
  Streamlit dashboard entry point.
- `visualization/pages/`
  Dashboard pages for trends, airlines, airports, delay causes, routes, live NAS status, and prediction.
- `visualization/utils/db.py`
  Cached Snowflake query helper for Streamlit pages.

## Data Flow

### Historical Flight Data

1. `download_bts_data.py` downloads raw BTS ZIP files.
2. `processing/spark_clean_bts.py` cleans and normalizes the data.
3. `pipeline/historical_pipeline.py` uploads Parquet to Snowflake.
4. Data lands in `RAW.BTS_ONTIME_RAW` and is used to populate `STAGING.FLIGHTS`.

### Daily Flight and NAS Data

1. `ingestion/opensky_fetcher.py` fetches daily flight data.
2. `ingestion/faa_nas_fetcher.py` fetches FAA NAS airport-status snapshots.
3. Spark cleaning scripts write cleaned Parquet.
4. `pipeline/daily_pipeline.py` loads the results into Snowflake.
5. `analysis/run_analytics.py` refreshes analytics tables.

### Weather Data

1. `ingestion/weather_fetcher.py` fetches airport weather history.
2. `processing/spark_clean_weather.py` transforms it into hourly rows.
3. `pipeline/weather_pipeline.py` loads it into `RAW.AIRPORT_WEATHER_HOURLY`.

## Snowflake Layout

### `RAW`

Raw or lightly standardized ingested data:

- `RAW.BTS_ONTIME_RAW`
- `RAW.OPENSKY_STATES_RAW`
- `RAW.OPENSKY_FLIGHTS_RAW`
- `RAW.FAA_NAS_STATUS_RAW`
- `RAW.AIRPORT_WEATHER_HOURLY`

### `STAGING`

Cleaned analytical base tables:

- `STAGING.FLIGHTS`

### `ANALYTICS`

Aggregated or dashboard-facing tables:

- `ANALYTICS.DELAY_TRENDS_DAILY`
- `ANALYTICS.DELAY_TRENDS_MONTHLY`
- `ANALYTICS.AIRLINE_PERFORMANCE`
- `ANALYTICS.AIRPORT_STATS`
- `ANALYTICS.DELAY_CAUSE_BREAKDOWN`
- `ANALYTICS.ROUTE_PERFORMANCE`
- `ANALYTICS.AIRPORT_NAS_STATUS`
- `ANALYTICS.WEATHER_DELAY_CORRELATION`

## Setup

### Prerequisites

- Python 3.10+
- Java 11+ for PySpark
- Snowflake account and RSA key-pair authentication configured

### Install

```bash
cd code
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Environment Variables

The code expects Snowflake credentials and other runtime settings via environment variables or a `.env` file. At minimum, configure:

```bash
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PRIVATE_KEY_PATH=...
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=...
SNOWFLAKE_WAREHOUSE=FLIGHT_WH
SNOWFLAKE_DATABASE=FLIGHT_DB
SNOWFLAKE_ROLE=TRAINING_ROLE
OPENSKY_USERNAME=...
OPENSKY_PASSWORD=...
```

## Typical Commands

### Initialize Snowflake

Run `snowflake/setup.sql` in Snowflake first.

### Historical BTS Load

```bash
cd code
python pipeline/historical_pipeline.py --start 2024-01 --end 2024-03 --workers 1
```

### Daily Pipeline

```bash
cd code
python pipeline/daily_pipeline.py
python pipeline/daily_pipeline.py --date 2026-04-08
python pipeline/daily_pipeline.py --dry-run
```

### Weather Pipeline

```bash
cd code
python pipeline/weather_pipeline.py --start-year 2024 --end-year 2025
```

### Run Analytics Only

```bash
cd code
python analysis/run_analytics.py
python analysis/run_analytics.py --date 2026-04-08
```

### Start Local Scheduler

```bash
cd code
python pipeline/scheduler.py
python pipeline/scheduler.py --run-now
```

### Launch Dashboard

```bash
cd code
streamlit run visualization/app.py
```

### Train Delay Models

```bash
cd code
python models/train_delay_model.py --sample-frac 0.1
```

## Dashboard Pages

- `1_Delay_Trends.py`: long-term and seasonal delay trends
- `2_Airline_Performance.py`: airline comparison
- `3_Airport_Stats.py`: airport-level traffic and delay metrics
- `4_Delay_Causes.py`: carrier, weather, NAS, security, and late-aircraft causes
- `5_Route_Performance.py`: route-level delay patterns
- `6_Live_NAS_Status.py`: latest FAA NAS airport delay programs
- `7_Delay_Predictor.py`: model-backed single-flight delay prediction

## Notes

- The repository contains both local scheduler orchestration and an Airflow DAG.
- `raw_data/` and `cleaned_data/` are local working directories generated by the pipelines.
- `test.java` is not part of the main production pipeline.
