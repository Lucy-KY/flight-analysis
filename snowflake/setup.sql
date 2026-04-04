-- =============================================================================
-- Snowflake Setup: U.S. Flight Delay & Traffic Pattern Analysis
-- Run this script ONCE as TRAINING_ROLE to initialize all infrastructure
-- =============================================================================

-- ── 1. Warehouse ──────────────────────────────────────────────────────────────
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS FLIGHT_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60          -- suspend after 60s idle (cost control)
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for flight delay analytics project';

-- ── 2. Database & Schemas ─────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS FLIGHT_DB
    COMMENT = 'U.S. Flight Delay and Traffic Pattern Analysis';

USE DATABASE FLIGHT_DB;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw ingested data, no transformation';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned and normalized flight records';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Aggregated analytical results';

-- ── 3. RAW Tables ─────────────────────────────────────────────────────────────
USE SCHEMA RAW;

-- BTS On-Time Performance (historical, 2000-2025)
CREATE TABLE IF NOT EXISTS BTS_ONTIME_RAW (
    YEAR                         INT,
    QUARTER                      INT,
    MONTH                        INT,
    DAY_OF_MONTH                 INT,
    DAY_OF_WEEK                  INT,
    FLIGHT_DATE                  DATE,
    REPORTING_AIRLINE            VARCHAR(10),
    IATA_CODE_REPORTING_AIRLINE  VARCHAR(5),
    TAIL_NUMBER                  VARCHAR(10),
    FLIGHT_NUMBER                VARCHAR(10),
    ORIGIN_AIRPORT_ID            INT,
    ORIGIN                       VARCHAR(5),
    ORIGIN_CITY_NAME             VARCHAR(100),
    ORIGIN_STATE                 VARCHAR(5),
    ORIGIN_STATE_NAME            VARCHAR(50),
    DEST_AIRPORT_ID              INT,
    DEST                         VARCHAR(5),
    DEST_CITY_NAME               VARCHAR(100),
    DEST_STATE                   VARCHAR(5),
    DEST_STATE_NAME              VARCHAR(50),
    CRS_DEP_TIME                 INT,
    DEP_TIME                     FLOAT,
    DEP_DELAY                    FLOAT,
    DEP_DELAY_MINUTES            FLOAT,
    DEP_DEL15                    INT,
    TAXI_OUT                     FLOAT,
    WHEELS_OFF                   FLOAT,
    WHEELS_ON                    FLOAT,
    TAXI_IN                      FLOAT,
    CRS_ARR_TIME                 INT,
    ARR_TIME                     FLOAT,
    ARR_DELAY                    FLOAT,
    ARR_DELAY_MINUTES            FLOAT,
    ARR_DEL15                    INT,
    CANCELLED                    INT,
    CANCELLATION_CODE            VARCHAR(5),
    DIVERTED                     INT,
    CRS_ELAPSED_TIME             FLOAT,
    ACTUAL_ELAPSED_TIME          FLOAT,
    AIR_TIME                     FLOAT,
    FLIGHTS                      INT,
    DISTANCE                     FLOAT,
    DISTANCE_GROUP               INT,
    CARRIER_DELAY                FLOAT,
    WEATHER_DELAY                FLOAT,
    NAS_DELAY                    FLOAT,
    SECURITY_DELAY               FLOAT,
    LATE_AIRCRAFT_DELAY          FLOAT,
    _LOAD_TS                     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE                      VARCHAR(10) DEFAULT 'BTS'
)
CLUSTER BY (YEAR, MONTH)
COMMENT = 'Raw BTS On-Time Performance records';

-- OpenSky Network (daily live flight states)
CREATE TABLE IF NOT EXISTS OPENSKY_STATES_RAW (
    FETCH_DATE            DATE,
    ICAO24                VARCHAR(10),
    CALLSIGN              VARCHAR(20),
    ORIGIN_COUNTRY        VARCHAR(50),
    TIME_POSITION         BIGINT,
    LAST_CONTACT          BIGINT,
    LONGITUDE             FLOAT,
    LATITUDE              FLOAT,
    BARO_ALTITUDE         FLOAT,
    ON_GROUND             BOOLEAN,
    VELOCITY              FLOAT,
    TRUE_TRACK            FLOAT,
    VERTICAL_RATE         FLOAT,
    SQUAWK                VARCHAR(10),
    SPI                   BOOLEAN,
    POSITION_SOURCE       INT,
    _LOAD_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (FETCH_DATE)
COMMENT = 'Raw OpenSky Network flight state snapshots';

-- OpenSky Flight Records (departures/arrivals per airport)
CREATE TABLE IF NOT EXISTS OPENSKY_FLIGHTS_RAW (
    FETCH_DATE              DATE,
    ICAO24                  VARCHAR(10),
    FIRST_SEEN              BIGINT,
    EST_DEPARTURE_AIRPORT   VARCHAR(10),
    LAST_SEEN               BIGINT,
    EST_ARRIVAL_AIRPORT     VARCHAR(10),
    CALLSIGN                VARCHAR(20),
    EST_DEPARTURE_AIRPORT_HORIZ_DIST   INT,
    EST_DEPARTURE_AIRPORT_VERT_DIST    INT,
    EST_ARRIVAL_AIRPORT_HORIZ_DIST     INT,
    EST_ARRIVAL_AIRPORT_VERT_DIST      INT,
    DEPARTURE_AIRPORT_CANDIDATES_COUNT INT,
    ARRIVAL_AIRPORT_CANDIDATES_COUNT   INT,
    _LOAD_TS                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (FETCH_DATE)
COMMENT = 'Raw OpenSky flight records (departures/arrivals)';

-- ── 4. STAGING Tables ─────────────────────────────────────────────────────────
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS FLIGHTS (
    FLIGHT_ID            VARCHAR(50),      -- {IATA_CODE}_{FLIGHT_NUM}_{FLIGHT_DATE}
    FLIGHT_DATE          DATE,
    YEAR                 INT,
    MONTH                INT,
    DAY_OF_MONTH         INT,
    DAY_OF_WEEK          INT,
    QUARTER              INT,
    AIRLINE_CODE         VARCHAR(5),
    FLIGHT_NUMBER        VARCHAR(10),
    TAIL_NUMBER          VARCHAR(10),
    ORIGIN               VARCHAR(5),
    ORIGIN_CITY          VARCHAR(100),
    ORIGIN_STATE         VARCHAR(5),
    DEST                 VARCHAR(5),
    DEST_CITY            VARCHAR(100),
    DEST_STATE           VARCHAR(5),
    SCHEDULED_DEP        INT,             -- HHMM format
    ACTUAL_DEP           FLOAT,
    DEP_DELAY_MIN        FLOAT,           -- departure delay in minutes
    IS_DEP_DELAYED       BOOLEAN,         -- delay >= 15 min
    SCHEDULED_ARR        INT,
    ACTUAL_ARR           FLOAT,
    ARR_DELAY_MIN        FLOAT,
    IS_ARR_DELAYED       BOOLEAN,
    IS_CANCELLED         BOOLEAN,
    CANCEL_CODE          VARCHAR(5),      -- A=Carrier, B=Weather, C=NAS, D=Security
    IS_DIVERTED          BOOLEAN,
    SCHEDULED_ELAPSED    FLOAT,
    ACTUAL_ELAPSED       FLOAT,
    AIR_TIME             FLOAT,
    DISTANCE             FLOAT,
    CARRIER_DELAY        FLOAT,
    WEATHER_DELAY        FLOAT,
    NAS_DELAY            FLOAT,
    SECURITY_DELAY       FLOAT,
    LATE_AIRCRAFT_DELAY  FLOAT,
    DATA_SOURCE          VARCHAR(10),     -- 'BTS' or 'OPENSKY'
    _LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (YEAR, MONTH, AIRLINE_CODE)
COMMENT = 'Cleaned, deduplicated flight records (primary analytical table)';

-- ── 5. ANALYTICS Tables ───────────────────────────────────────────────────────
USE SCHEMA ANALYTICS;

-- Daily delay trend summary
CREATE TABLE IF NOT EXISTS DELAY_TRENDS_DAILY (
    FLIGHT_DATE          DATE,
    TOTAL_FLIGHTS        INT,
    TOTAL_DELAYED        INT,
    TOTAL_CANCELLED      INT,
    DELAY_RATE           FLOAT,
    AVG_DEP_DELAY_MIN    FLOAT,
    AVG_ARR_DELAY_MIN    FLOAT,
    CARRIER_DELAY_SHARE  FLOAT,
    WEATHER_DELAY_SHARE  FLOAT,
    NAS_DELAY_SHARE      FLOAT,
    _UPDATED_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Daily aggregated delay statistics';

-- Monthly delay trend summary
CREATE TABLE IF NOT EXISTS DELAY_TRENDS_MONTHLY (
    YEAR                 INT,
    MONTH                INT,
    TOTAL_FLIGHTS        INT,
    TOTAL_DELAYED        INT,
    TOTAL_CANCELLED      INT,
    DELAY_RATE           FLOAT,
    AVG_DEP_DELAY_MIN    FLOAT,
    AVG_ARR_DELAY_MIN    FLOAT,
    CARRIER_DELAY_SHARE  FLOAT,
    WEATHER_DELAY_SHARE  FLOAT,
    NAS_DELAY_SHARE      FLOAT,
    _UPDATED_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Monthly aggregated delay statistics';

-- Airline performance
CREATE TABLE IF NOT EXISTS AIRLINE_PERFORMANCE (
    YEAR                 INT,
    MONTH                INT,
    AIRLINE_CODE         VARCHAR(5),
    TOTAL_FLIGHTS        INT,
    DELAYED_FLIGHTS      INT,
    CANCELLED_FLIGHTS    INT,
    ON_TIME_FLIGHTS      INT,
    DELAY_RATE           FLOAT,
    CANCEL_RATE          FLOAT,
    ON_TIME_RATE         FLOAT,
    AVG_DEP_DELAY_MIN    FLOAT,
    AVG_ARR_DELAY_MIN    FLOAT,
    _UPDATED_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Per-airline monthly performance metrics';

-- Airport statistics
CREATE TABLE IF NOT EXISTS AIRPORT_STATS (
    YEAR                  INT,
    MONTH                 INT,
    AIRPORT               VARCHAR(5),
    CITY                  VARCHAR(100),
    STATE                 VARCHAR(5),
    DEP_FLIGHTS           INT,
    ARR_FLIGHTS           INT,
    DEP_DELAYED           INT,
    ARR_DELAYED           INT,
    DEP_CANCELLED         INT,
    AVG_DEP_DELAY_MIN     FLOAT,
    AVG_ARR_DELAY_MIN     FLOAT,
    DEP_DELAY_RATE        FLOAT,
    ARR_DELAY_RATE        FLOAT,
    _UPDATED_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Per-airport monthly departure/arrival statistics';

-- Delay cause breakdown
CREATE TABLE IF NOT EXISTS DELAY_CAUSE_BREAKDOWN (
    YEAR                      INT,
    MONTH                     INT,
    CARRIER_DELAY_FLIGHTS     INT,
    WEATHER_DELAY_FLIGHTS     INT,
    NAS_DELAY_FLIGHTS         INT,
    SECURITY_DELAY_FLIGHTS    INT,
    LATE_AIRCRAFT_DELAY_FLIGHTS INT,
    CARRIER_DELAY_TOTAL_MIN   FLOAT,
    WEATHER_DELAY_TOTAL_MIN   FLOAT,
    NAS_DELAY_TOTAL_MIN       FLOAT,
    SECURITY_DELAY_TOTAL_MIN  FLOAT,
    LATE_AIRCRAFT_DELAY_TOTAL_MIN FLOAT,
    _UPDATED_TS               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Monthly breakdown of delay causes';

-- Route performance
CREATE TABLE IF NOT EXISTS ROUTE_PERFORMANCE (
    YEAR                  INT,
    MONTH                 INT,
    ORIGIN                VARCHAR(5),
    DEST                  VARCHAR(5),
    ROUTE                 VARCHAR(15),
    TOTAL_FLIGHTS         INT,
    DELAYED_FLIGHTS       INT,
    DELAY_RATE            FLOAT,
    AVG_DEP_DELAY_MIN     FLOAT,
    AVG_DISTANCE          FLOAT,
    _UPDATED_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Per-route monthly performance metrics';

-- ── 6. Internal Stage for bulk loading ───────────────────────────────────────
USE DATABASE FLIGHT_DB;
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS BTS_STAGE
    FILE_FORMAT = (TYPE = 'PARQUET')
    COMMENT = 'Internal stage for bulk BTS parquet uploads';

CREATE STAGE IF NOT EXISTS OPENSKY_STAGE
    FILE_FORMAT = (TYPE = 'PARQUET')
    COMMENT = 'Internal stage for daily OpenSky parquet uploads';

-- ── 7. Verify setup ───────────────────────────────────────────────────────────
SHOW WAREHOUSES LIKE 'FLIGHT_WH';
SHOW DATABASES LIKE 'FLIGHT_DB';
SHOW SCHEMAS IN DATABASE FLIGHT_DB;
SHOW TABLES IN SCHEMA FLIGHT_DB.RAW;
