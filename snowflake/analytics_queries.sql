-- =============================================================================
-- Analytics Queries: U.S. Flight Delay & Traffic Pattern Analysis
-- These queries populate the ANALYTICS schema tables and can also be run
-- interactively for ad-hoc exploration.
-- =============================================================================

USE WAREHOUSE FLIGHT_WH;
USE DATABASE FLIGHT_DB;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Overall Delay Trends (Monthly) – 25-year view
-- Goal: How have US flight delays changed over time?
-- ─────────────────────────────────────────────────────────────────────────────
INSERT OVERWRITE INTO ANALYTICS.DELAY_TRENDS_MONTHLY
SELECT
    YEAR,
    MONTH,
    COUNT(*)                                            AS TOTAL_FLIGHTS,
    SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END)    AS TOTAL_DELAYED,
    SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END)    AS TOTAL_CANCELLED,
    ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS DELAY_RATE,
    ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2)
                                                        AS AVG_DEP_DELAY_MIN,
    ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2)
                                                        AS AVG_ARR_DELAY_MIN,
    ROUND(100.0 * SUM(CARRIER_DELAY)      / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)
                                                        AS CARRIER_DELAY_SHARE,
    ROUND(100.0 * SUM(WEATHER_DELAY)      / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)
                                                        AS WEATHER_DELAY_SHARE,
    ROUND(100.0 * SUM(NAS_DELAY)          / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)
                                                        AS NAS_DELAY_SHARE,
    CURRENT_TIMESTAMP()                                 AS _UPDATED_TS
FROM STAGING.FLIGHTS
WHERE NOT IS_CANCELLED
GROUP BY YEAR, MONTH
ORDER BY YEAR, MONTH;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Daily Delay Trends (for recent data / real-time monitoring)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT OVERWRITE INTO ANALYTICS.DELAY_TRENDS_DAILY
SELECT
    FLIGHT_DATE,
    COUNT(*)                                            AS TOTAL_FLIGHTS,
    SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END)    AS TOTAL_DELAYED,
    SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END)    AS TOTAL_CANCELLED,
    ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS DELAY_RATE,
    ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2)
                                                        AS AVG_DEP_DELAY_MIN,
    ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2)
                                                        AS AVG_ARR_DELAY_MIN,
    ROUND(100.0 * SUM(CARRIER_DELAY) / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)
                                                        AS CARRIER_DELAY_SHARE,
    ROUND(100.0 * SUM(WEATHER_DELAY) / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)
                                                        AS WEATHER_DELAY_SHARE,
    ROUND(100.0 * SUM(NAS_DELAY)     / NULLIF(SUM(DEP_DELAY_MIN), 0), 2)
                                                        AS NAS_DELAY_SHARE,
    CURRENT_TIMESTAMP()                                 AS _UPDATED_TS
FROM STAGING.FLIGHTS
WHERE NOT IS_CANCELLED
GROUP BY FLIGHT_DATE
ORDER BY FLIGHT_DATE;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Airline Performance (Monthly)
-- Goal: Which airlines have the best/worst on-time performance?
-- ─────────────────────────────────────────────────────────────────────────────
INSERT OVERWRITE INTO ANALYTICS.AIRLINE_PERFORMANCE
SELECT
    YEAR,
    MONTH,
    AIRLINE_CODE,
    COUNT(*)                                                    AS TOTAL_FLIGHTS,
    SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END)            AS DELAYED_FLIGHTS,
    SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END)            AS CANCELLED_FLIGHTS,
    SUM(CASE WHEN NOT IS_DEP_DELAYED AND NOT IS_CANCELLED THEN 1 ELSE 0 END)
                                                                AS ON_TIME_FLIGHTS,
    ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                AS DELAY_RATE,
    ROUND(100.0 * SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                AS CANCEL_RATE,
    ROUND(100.0 * SUM(CASE WHEN NOT IS_DEP_DELAYED AND NOT IS_CANCELLED THEN 1 ELSE 0 END)
          / COUNT(*), 2)                                        AS ON_TIME_RATE,
    ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 AND NOT IS_CANCELLED THEN DEP_DELAY_MIN END), 2)
                                                                AS AVG_DEP_DELAY_MIN,
    ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 AND NOT IS_CANCELLED THEN ARR_DELAY_MIN END), 2)
                                                                AS AVG_ARR_DELAY_MIN,
    CURRENT_TIMESTAMP()                                         AS _UPDATED_TS
FROM STAGING.FLIGHTS
GROUP BY YEAR, MONTH, AIRLINE_CODE
ORDER BY YEAR, MONTH, DELAY_RATE DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Airport Statistics (Monthly)
-- Goal: Which airports have the most delays and why?
-- ─────────────────────────────────────────────────────────────────────────────
INSERT OVERWRITE INTO ANALYTICS.AIRPORT_STATS
WITH departures AS (
    SELECT
        YEAR, MONTH, ORIGIN AS AIRPORT, ORIGIN_CITY AS CITY, ORIGIN_STATE AS STATE,
        COUNT(*) AS DEP_FLIGHTS,
        SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) AS DEP_DELAYED,
        SUM(CASE WHEN IS_CANCELLED   THEN 1 ELSE 0 END) AS DEP_CANCELLED,
        ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2) AS AVG_DEP_DELAY_MIN
    FROM STAGING.FLIGHTS
    GROUP BY YEAR, MONTH, ORIGIN, ORIGIN_CITY, ORIGIN_STATE
),
arrivals AS (
    SELECT
        YEAR, MONTH, DEST AS AIRPORT,
        COUNT(*) AS ARR_FLIGHTS,
        SUM(CASE WHEN IS_ARR_DELAYED THEN 1 ELSE 0 END) AS ARR_DELAYED,
        ROUND(AVG(CASE WHEN ARR_DELAY_MIN > 0 THEN ARR_DELAY_MIN END), 2) AS AVG_ARR_DELAY_MIN
    FROM STAGING.FLIGHTS
    WHERE NOT IS_CANCELLED
    GROUP BY YEAR, MONTH, DEST
)
SELECT
    d.YEAR, d.MONTH, d.AIRPORT, d.CITY, d.STATE,
    d.DEP_FLIGHTS, a.ARR_FLIGHTS,
    d.DEP_DELAYED, a.ARR_DELAYED,
    d.DEP_CANCELLED,
    d.AVG_DEP_DELAY_MIN, a.AVG_ARR_DELAY_MIN,
    ROUND(100.0 * d.DEP_DELAYED / NULLIF(d.DEP_FLIGHTS, 0), 2) AS DEP_DELAY_RATE,
    ROUND(100.0 * a.ARR_DELAYED / NULLIF(a.ARR_FLIGHTS, 0), 2) AS ARR_DELAY_RATE,
    CURRENT_TIMESTAMP() AS _UPDATED_TS
FROM departures d
LEFT JOIN arrivals a ON d.YEAR = a.YEAR AND d.MONTH = a.MONTH AND d.AIRPORT = a.AIRPORT
ORDER BY d.YEAR, d.MONTH, d.DEP_DELAY_RATE DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Delay Cause Breakdown (Monthly)
-- Goal: What causes delays and how do proportions shift over time?
-- ─────────────────────────────────────────────────────────────────────────────
INSERT OVERWRITE INTO ANALYTICS.DELAY_CAUSE_BREAKDOWN
SELECT
    YEAR,
    MONTH,
    SUM(CASE WHEN CARRIER_DELAY     > 0 THEN 1 ELSE 0 END) AS CARRIER_DELAY_FLIGHTS,
    SUM(CASE WHEN WEATHER_DELAY     > 0 THEN 1 ELSE 0 END) AS WEATHER_DELAY_FLIGHTS,
    SUM(CASE WHEN NAS_DELAY         > 0 THEN 1 ELSE 0 END) AS NAS_DELAY_FLIGHTS,
    SUM(CASE WHEN SECURITY_DELAY    > 0 THEN 1 ELSE 0 END) AS SECURITY_DELAY_FLIGHTS,
    SUM(CASE WHEN LATE_AIRCRAFT_DELAY > 0 THEN 1 ELSE 0 END) AS LATE_AIRCRAFT_DELAY_FLIGHTS,
    ROUND(SUM(COALESCE(CARRIER_DELAY,     0)), 2) AS CARRIER_DELAY_TOTAL_MIN,
    ROUND(SUM(COALESCE(WEATHER_DELAY,     0)), 2) AS WEATHER_DELAY_TOTAL_MIN,
    ROUND(SUM(COALESCE(NAS_DELAY,         0)), 2) AS NAS_DELAY_TOTAL_MIN,
    ROUND(SUM(COALESCE(SECURITY_DELAY,    0)), 2) AS SECURITY_DELAY_TOTAL_MIN,
    ROUND(SUM(COALESCE(LATE_AIRCRAFT_DELAY, 0)), 2) AS LATE_AIRCRAFT_DELAY_TOTAL_MIN,
    CURRENT_TIMESTAMP()                              AS _UPDATED_TS
FROM STAGING.FLIGHTS
WHERE IS_DEP_DELAYED AND NOT IS_CANCELLED
GROUP BY YEAR, MONTH
ORDER BY YEAR, MONTH;

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Route Performance (Top routes by volume, monthly)
-- Goal: Which routes are most delay-prone?
-- ─────────────────────────────────────────────────────────────────────────────
INSERT OVERWRITE INTO ANALYTICS.ROUTE_PERFORMANCE
SELECT
    YEAR,
    MONTH,
    ORIGIN,
    DEST,
    CONCAT(ORIGIN, '-', DEST)                                AS ROUTE,
    COUNT(*)                                                  AS TOTAL_FLIGHTS,
    SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END)          AS DELAYED_FLIGHTS,
    ROUND(100.0 * SUM(CASE WHEN IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                              AS DELAY_RATE,
    ROUND(AVG(CASE WHEN DEP_DELAY_MIN > 0 THEN DEP_DELAY_MIN END), 2)
                                                              AS AVG_DEP_DELAY_MIN,
    ROUND(AVG(DISTANCE), 2)                                   AS AVG_DISTANCE,
    CURRENT_TIMESTAMP()                                       AS _UPDATED_TS
FROM STAGING.FLIGHTS
WHERE NOT IS_CANCELLED
GROUP BY YEAR, MONTH, ORIGIN, DEST
HAVING COUNT(*) >= 10
ORDER BY YEAR, MONTH, TOTAL_FLIGHTS DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- AD-HOC ANALYSIS QUERIES (for exploration, not scheduled)
-- ─────────────────────────────────────────────────────────────────────────────

-- Q1: Year-over-year delay rate trend
SELECT
    YEAR,
    ROUND(AVG(DELAY_RATE), 2) AS AVG_ANNUAL_DELAY_RATE,
    SUM(TOTAL_FLIGHTS)         AS TOTAL_ANNUAL_FLIGHTS,
    SUM(TOTAL_CANCELLED)       AS TOTAL_CANCELLATIONS
FROM ANALYTICS.DELAY_TRENDS_MONTHLY
GROUP BY YEAR
ORDER BY YEAR;

-- Q2: Top 10 most delayed airlines (all-time average)
SELECT
    AIRLINE_CODE,
    ROUND(AVG(DELAY_RATE), 2)        AS AVG_DELAY_RATE,
    ROUND(AVG(AVG_DEP_DELAY_MIN), 2) AS AVG_DELAY_MINUTES,
    SUM(TOTAL_FLIGHTS)                AS TOTAL_FLIGHTS
FROM ANALYTICS.AIRLINE_PERFORMANCE
GROUP BY AIRLINE_CODE
ORDER BY AVG_DELAY_RATE DESC
LIMIT 10;

-- Q3: Seasonal delay patterns (delay rate by month across all years)
SELECT
    MONTH,
    ROUND(AVG(DELAY_RATE), 2)        AS AVG_DELAY_RATE,
    ROUND(AVG(AVG_DEP_DELAY_MIN), 2) AS AVG_DELAY_MIN,
    ROUND(AVG(WEATHER_DELAY_SHARE), 2) AS AVG_WEATHER_SHARE
FROM ANALYTICS.DELAY_TRENDS_MONTHLY
GROUP BY MONTH
ORDER BY MONTH;

-- Q4: Top 20 worst delay airports (by average departure delay rate)
SELECT
    AIRPORT, CITY, STATE,
    ROUND(AVG(DEP_DELAY_RATE), 2)    AS AVG_DEP_DELAY_RATE,
    ROUND(AVG(AVG_DEP_DELAY_MIN), 2) AS AVG_DELAY_MINUTES,
    SUM(DEP_FLIGHTS)                  AS TOTAL_DEP_FLIGHTS
FROM ANALYTICS.AIRPORT_STATS
GROUP BY AIRPORT, CITY, STATE
HAVING SUM(DEP_FLIGHTS) > 1000
ORDER BY AVG_DEP_DELAY_RATE DESC
LIMIT 20;

-- Q5: Delay cause composition for each year
SELECT
    YEAR,
    ROUND(100.0 * SUM(CARRIER_DELAY_TOTAL_MIN) /
          NULLIF(SUM(CARRIER_DELAY_TOTAL_MIN + WEATHER_DELAY_TOTAL_MIN +
                     NAS_DELAY_TOTAL_MIN + SECURITY_DELAY_TOTAL_MIN +
                     LATE_AIRCRAFT_DELAY_TOTAL_MIN), 0), 2) AS CARRIER_PCT,
    ROUND(100.0 * SUM(WEATHER_DELAY_TOTAL_MIN) /
          NULLIF(SUM(CARRIER_DELAY_TOTAL_MIN + WEATHER_DELAY_TOTAL_MIN +
                     NAS_DELAY_TOTAL_MIN + SECURITY_DELAY_TOTAL_MIN +
                     LATE_AIRCRAFT_DELAY_TOTAL_MIN), 0), 2) AS WEATHER_PCT,
    ROUND(100.0 * SUM(NAS_DELAY_TOTAL_MIN) /
          NULLIF(SUM(CARRIER_DELAY_TOTAL_MIN + WEATHER_DELAY_TOTAL_MIN +
                     NAS_DELAY_TOTAL_MIN + SECURITY_DELAY_TOTAL_MIN +
                     LATE_AIRCRAFT_DELAY_TOTAL_MIN), 0), 2) AS NAS_PCT,
    ROUND(100.0 * SUM(LATE_AIRCRAFT_DELAY_TOTAL_MIN) /
          NULLIF(SUM(CARRIER_DELAY_TOTAL_MIN + WEATHER_DELAY_TOTAL_MIN +
                     NAS_DELAY_TOTAL_MIN + SECURITY_DELAY_TOTAL_MIN +
                     LATE_AIRCRAFT_DELAY_TOTAL_MIN), 0), 2) AS LATE_AIRCRAFT_PCT
FROM ANALYTICS.DELAY_CAUSE_BREAKDOWN
GROUP BY YEAR
ORDER BY YEAR;

-- Q6: Impact of COVID-19 on flight traffic and delays (2019-2022 comparison)
SELECT
    YEAR, MONTH,
    TOTAL_FLIGHTS,
    TOTAL_CANCELLED,
    ROUND(100.0 * TOTAL_CANCELLED / NULLIF(TOTAL_FLIGHTS, 0), 2) AS CANCEL_RATE,
    DELAY_RATE,
    AVG_DEP_DELAY_MIN
FROM ANALYTICS.DELAY_TRENDS_MONTHLY
WHERE YEAR BETWEEN 2019 AND 2022
ORDER BY YEAR, MONTH;

-- Q7: Busiest routes and their delay performance
SELECT
    ROUTE,
    SUM(TOTAL_FLIGHTS)                AS TOTAL_FLIGHTS,
    ROUND(AVG(DELAY_RATE), 2)         AS AVG_DELAY_RATE,
    ROUND(AVG(AVG_DEP_DELAY_MIN), 2)  AS AVG_DELAY_MIN,
    ROUND(AVG(AVG_DISTANCE), 0)       AS DISTANCE_MILES
FROM ANALYTICS.ROUTE_PERFORMANCE
GROUP BY ROUTE
HAVING SUM(TOTAL_FLIGHTS) > 5000
ORDER BY TOTAL_FLIGHTS DESC
LIMIT 25;

-- Q8: Day-of-week delay patterns
SELECT
    f.DAY_OF_WEEK,
    CASE f.DAY_OF_WEEK
        WHEN 1 THEN 'Monday'    WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'    WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS DAY_NAME,
    COUNT(*) AS TOTAL_FLIGHTS,
    ROUND(100.0 * SUM(CASE WHEN f.IS_DEP_DELAYED THEN 1 ELSE 0 END) / COUNT(*), 2) AS DELAY_RATE,
    ROUND(AVG(CASE WHEN f.DEP_DELAY_MIN > 0 THEN f.DEP_DELAY_MIN END), 2) AS AVG_DELAY_MIN
FROM STAGING.FLIGHTS f
WHERE NOT f.IS_CANCELLED
GROUP BY f.DAY_OF_WEEK
ORDER BY f.DAY_OF_WEEK;
