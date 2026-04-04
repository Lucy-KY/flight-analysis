# U.S. Flight Delay & Traffic Pattern Analysis
**CSE 5114 Final Project** | Yiyang Sun · Kaiyuan Xu

---

## Project Overview

An end-to-end data engineering pipeline that:
- Ingests **25 years** (2000–2025) of BTS airline on-time performance data (~150M records, >128 GB)
- Performs **daily incremental updates** from the OpenSky Network free API
- Cleans and transforms data using **PySpark** (local mode)
- Stores everything in **Snowflake** with a layered data model (RAW → STAGING → ANALYTICS)
- Runs **scheduled analytics** queries automatically each morning

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                          │
│                                                                              │
│  ┌──────────────────────────┐    ┌──────────────────────────────────────┐   │
│  │  BTS PREZIP Server        │    │  OpenSky Network REST API (FREE)      │   │
│  │  transtats.bts.gov        │    │  opensky-network.org/api              │   │
│  │  Historical: 2000-2025    │    │  Daily: departures/arrivals           │   │
│  │  ~310 ZIP files           │    │  Top 50 US airports                   │   │
│  │  ~135 GB uncompressed     │    │  Real-time + 30-day history           │   │
│  └────────────┬─────────────┘    └───────────────────┬──────────────────┘   │
└───────────────┼──────────────────────────────────────┼──────────────────────┘
                │ (one-time)                            │ (daily @ 06:00)
                ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                       │
│                                                                              │
│  ┌─────────────────────────────┐    ┌──────────────────────────────────┐    │
│  │  download_bts_data.py        │    │  ingestion/opensky_fetcher.py     │    │
│  │  • Multi-threaded HTTP DL   │    │  • Authenticated REST API         │    │
│  │  • Resume on failure        │    │  • Rate-limited (1 req/s)         │    │
│  │  • Output: raw_data/*.zip   │    │  • Output: raw_data/opensky/      │    │
│  └────────────┬────────────────┘    └──────────────────┬───────────────┘    │
└───────────────┼─────────────────────────────────────────┼────────────────────┘
                │                                          │
                ▼                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     PROCESSING LAYER (PySpark local[*])                      │
│                                                                              │
│  ┌────────────────────────────────┐  ┌───────────────────────────────────┐  │
│  │  processing/spark_clean_bts.py  │  │  processing/spark_clean_opensky.py│  │
│  │  • Unzip + read CSV             │  │  • Read JSON flight records        │  │
│  │  • Rename 110 → clean columns   │  │  • Deduplicate (dep+arr overlap)   │  │
│  │  • Cast types, nullify empties  │  │  • Filter US airspace              │  │
│  │  • Derive FLIGHT_ID PK          │  │  • Convert Unix ts → dates         │  │
│  │  • Write: cleaned_data/bts/     │  │  • Write: cleaned_data/opensky/    │  │
│  │    ├── raw/  (all columns)      │  │    ├── flights/ (partitioned)      │  │
│  │    └── staging/ (normalized)    │  │    └── states/  (snapshots)        │  │
│  └────────────┬───────────────────┘  └─────────────────┬──────────────────┘  │
└───────────────┼───────────────────────────────────────────┼──────────────────┘
                │  PUT parquet → @BTS_STAGE                 │  PUT → @OPENSKY_STAGE
                ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE (FLIGHT_DB)                                │
│                                                                              │
│  ┌─────────────── RAW schema ──────────────────────────────────────────┐    │
│  │  BTS_ONTIME_RAW          (clustered by YEAR, MONTH)  ~150M rows     │    │
│  │  OPENSKY_STATES_RAW      (clustered by FETCH_DATE)   daily          │    │
│  │  OPENSKY_FLIGHTS_RAW     (clustered by FETCH_DATE)   daily          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                 │ INSERT INTO                                │
│  ┌─────────────── STAGING schema ──────────────────────────────────────┐    │
│  │  FLIGHTS  (primary analytical table)                                 │    │
│  │  • FLIGHT_ID (PK), cleaned & normalized columns                      │    │
│  │  • Clustered by (YEAR, MONTH, AIRLINE_CODE)                          │    │
│  │  • Sources: BTS + OpenSky                                            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                 │ Analytics queries                          │
│  ┌─────────────── ANALYTICS schema ────────────────────────────────────┐    │
│  │  DELAY_TRENDS_MONTHLY      – monthly delay KPIs (25yr trend)        │    │
│  │  DELAY_TRENDS_DAILY        – daily delay monitoring                  │    │
│  │  AIRLINE_PERFORMANCE       – per-airline on-time rates               │    │
│  │  AIRPORT_STATS             – per-airport dep/arr statistics          │    │
│  │  DELAY_CAUSE_BREAKDOWN     – carrier/weather/NAS/security/lateAC    │    │
│  │  ROUTE_PERFORMANCE         – per-route delay metrics                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                 ▲
                                 │
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION (pipeline/)                                │
│                                                                              │
│  scheduler.py  ──► daily_pipeline.py ──► [fetch → clean → load → analytics] │
│  (APScheduler, daily @ 06:00 Central)                                        │
│                                                                              │
│  historical_pipeline.py  (one-time, full BTS bulk load)                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
code/
├── download_bts_data.py          # BTS PREZIP bulk downloader
├── requirements.txt              # Python dependencies
├── .env.example                  # Credentials template (copy to .env)
│
├── ingestion/
│   └── opensky_fetcher.py        # OpenSky daily API fetcher
│
├── processing/
│   ├── spark_clean_bts.py        # PySpark: clean BTS ZIP → Parquet
│   └── spark_clean_opensky.py    # PySpark: clean OpenSky JSON → Parquet
│
├── snowflake/
│   ├── setup.sql                 # DDL: warehouse, db, schemas, tables, stages
│   └── analytics_queries.sql     # Analytics SQL (ad-hoc + scheduled)
│
├── pipeline/
│   ├── historical_pipeline.py    # One-time BTS full load orchestrator
│   ├── daily_pipeline.py         # Daily incremental update orchestrator
│   └── scheduler.py              # APScheduler-based daily trigger
│
└── analysis/
    └── run_analytics.py          # Analytics refresh runner
```

---

## Quick Start

### 1. Prerequisites

```bash
# Python 3.10+, Java 11+ (required for PySpark)
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

cd code
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 2. Initialize Snowflake

Run `snowflake/setup.sql` in your Snowflake worksheet (as TRAINING_ROLE):
```sql
-- Creates: FLIGHT_WH, FLIGHT_DB, schemas RAW/STAGING/ANALYTICS, all tables & stages
```

### 3. One-Time Historical Load (BTS 2000–2025)

```bash
# Full 25-year load (~135 GB CSV, takes several hours)
python pipeline/historical_pipeline.py --start 2000-01 --end 2025-12 --workers 2

# Test with 3 months first:
python pipeline/historical_pipeline.py --start 2024-01 --end 2024-03 --workers 1
```

This runs automatically: Download → Spark Clean → Snowflake Load → Analytics Refresh

### 4. Start Daily Scheduler

```bash
# Runs daily at 06:00 Central time
python pipeline/scheduler.py

# Custom time:
python pipeline/scheduler.py --hour 7 --minute 30

# Run once immediately (for testing):
python pipeline/scheduler.py --run-now
```

### 5. Run Analytics Manually

```bash
# Refresh all analytics tables
python analysis/run_analytics.py

# Incremental refresh for a specific date
python analysis/run_analytics.py --date 2025-04-02

# Only airline stats
python analysis/run_analytics.py --query airline
```

---

## API Decision: Why OpenSky Network?

| API | Free Tier | Daily Automation | US Coverage | Delay Data |
|-----|-----------|-----------------|-------------|-----------|
| **OpenSky Network** | ✅ Fully free | ✅ Yes | ✅ Full | ❌ ADS-B only |
| FlightAware AeroAPI | ❌ $0.005/query | ✅ Yes | ✅ Full | ✅ Yes |
| AviationStack | ⚠️ 500 req/month | ❌ Too limited | ✅ Full | ✅ Yes |
| Aviation Edge | ❌ Paid only | ✅ Yes | ✅ Full | ✅ Yes |
| ADS-B Exchange | ❌ Commercial requires license | ✅ Yes | ✅ Full | ❌ ADS-B only |

**Choice: OpenSky Network** (free, research-friendly, sufficient for daily incremental volume tracking).
Delay data comes from the historical BTS dataset which remains the primary analytical source.

---

## Snowflake Schema Design

### Layered Architecture

| Schema | Purpose | Tables |
|--------|---------|--------|
| `RAW` | Immutable raw ingested data | `BTS_ONTIME_RAW`, `OPENSKY_STATES_RAW`, `OPENSKY_FLIGHTS_RAW` |
| `STAGING` | Cleaned, deduplicated, PK-assigned | `FLIGHTS` |
| `ANALYTICS` | Aggregated results for visualization | 6 tables (see below) |

### Analytics Tables

| Table | Grain | Key Analysis |
|-------|-------|-------------|
| `DELAY_TRENDS_MONTHLY` | year+month | 25-year delay rate trend |
| `DELAY_TRENDS_DAILY` | date | Real-time monitoring |
| `AIRLINE_PERFORMANCE` | year+month+airline | Best/worst airlines |
| `AIRPORT_STATS` | year+month+airport | Hub congestion analysis |
| `DELAY_CAUSE_BREAKDOWN` | year+month | Carrier vs weather vs NAS |
| `ROUTE_PERFORMANCE` | year+month+route | Most delay-prone routes |

### Cost Controls
- Warehouse: **X-Small**, auto-suspends after **60 seconds** idle
- Estimated monthly cost: ~$2–5 for daily analytics workload

---

## Analysis Goals

1. **Long-term delay trends** (2000–2025): Are flights getting better or worse?
2. **Seasonal patterns**: Summer/holiday delay spikes
3. **Airline comparison**: On-time performance ranking
4. **Airport hotspots**: Which hubs cause the most propagated delays?
5. **Delay cause analysis**: COVID-19 impact on weather vs carrier delays
6. **Route-level insights**: Worst delay routes by origin-destination pair
7. **Day-of-week patterns**: When is the best day to fly?
