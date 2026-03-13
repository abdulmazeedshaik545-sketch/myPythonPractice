<!-- Part of: BigQuery Medallion Architecture Guide | File: 04_Silver_Layer_Cleansed_Conformed.md -->

## SECTION 5: SILVER LAYER — CLEANSED & CONFORMED DATA

### Overview
The Silver layer **cleans, validates, and conforms** the raw Bronze data.

**Transformations Applied:**
- Remove duplicate trips (using row hash)
- Cast strings to correct types
- Handle nulls with business-rule defaults
- Derive calculated fields (duration in minutes, speed, tip rate)
- Attach data quality score (0–100) per row
- Enrich with weather and community area lookups
- Standardize payment types and company names

---

### Step 5.1 — Create Silver Tables

```sql
-- ═══════════════════════════════════════════════════
-- SILVER TABLE 1: Cleansed Taxi Trips
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.silver.taxi_trips`
(
  -- Keys
  trip_id                 STRING NOT NULL,   -- renamed from unique_key
  taxi_id                 STRING NOT NULL,

  -- Temporal
  trip_start_timestamp    TIMESTAMP NOT NULL,
  trip_end_timestamp      TIMESTAMP,
  trip_date               DATE NOT NULL,     -- derived: DATE(trip_start_timestamp)
  trip_year               INT64,
  trip_month              INT64,
  trip_day_of_week        INT64,             -- 1=Sunday ... 7=Saturday
  trip_hour               INT64,
  is_weekend              BOOL,
  time_of_day_bucket      STRING,            -- 'overnight','morning_rush','midday','evening_rush','evening'

  -- Duration & Distance
  trip_seconds            INT64,
  trip_minutes            FLOAT64,           -- derived
  trip_miles              FLOAT64,
  avg_speed_mph           FLOAT64,           -- derived: miles / hours

  -- Geography
  pickup_community_area   INT64,
  dropoff_community_area  INT64,
  pickup_community_name   STRING,            -- enriched from reference
  dropoff_community_name  STRING,
  is_same_community_area  BOOL,
  pickup_latitude         FLOAT64,
  pickup_longitude        FLOAT64,
  dropoff_latitude        FLOAT64,
  dropoff_longitude       FLOAT64,

  -- Financials
  fare                    FLOAT64,
  tips                    FLOAT64,
  tolls                   FLOAT64,
  extras                  FLOAT64,
  trip_total              FLOAT64,
  tip_rate_pct            FLOAT64,           -- derived: tips / fare * 100
  is_tipped               BOOL,

  -- Categorical
  payment_type            STRING,
  payment_type_std        STRING,            -- standardized: 'card','cash','other'
  company                 STRING,
  company_std             STRING,            -- standardized company name

  -- Weather (joined from NOAA)
  weather_temp_f          FLOAT64,
  weather_precip_in       FLOAT64,
  weather_visibility_mi   FLOAT64,
  weather_wind_speed_kt   FLOAT64,
  weather_condition       STRING,            -- 'clear','rain','snow','fog'

  -- Data Quality
  dq_score                INT64,             -- 0-100 quality score
  dq_flags                STRING,            -- comma-separated issue flags

  -- Medallion Metadata
  _bronze_ingestion_date  DATE,
  _silver_load_timestamp  TIMESTAMP,
  _silver_batch_id        STRING,
  _source_row_hash        STRING
)
PARTITION BY trip_date
CLUSTER BY taxi_id, pickup_community_area, payment_type_std
OPTIONS (
  description = 'Silver: Cleaned, enriched, deduplicated taxi trips. Partitioned by trip date.',
  require_partition_filter = FALSE
);


-- ═══════════════════════════════════════════════════
-- SILVER TABLE 2: Cleansed Weather
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.silver.weather_daily`
(
  weather_date        DATE NOT NULL,
  station_id          STRING,
  avg_temp_f          FLOAT64,
  max_temp_f          FLOAT64,
  min_temp_f          FLOAT64,
  precipitation_in    FLOAT64,
  avg_wind_speed_kt   FLOAT64,
  visibility_mi       FLOAT64,
  has_fog             BOOL,
  has_rain            BOOL,
  has_snow            BOOL,
  has_thunder         BOOL,
  weather_condition   STRING,     -- derived: most severe condition of the day
  temp_category       STRING,     -- 'freezing','cold','cool','mild','warm','hot'

  _silver_load_timestamp  TIMESTAMP,
  _silver_batch_id        STRING
)
PARTITION BY weather_date
CLUSTER BY station_id
OPTIONS (description = 'Silver: Daily weather summary for Chicago stations.');


-- ═══════════════════════════════════════════════════
-- SILVER TABLE 3: Community Area Reference (Dimension)
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.silver.dim_community_areas`
(
  community_area_id     INT64,
  community_area_name   STRING,
  area_category         STRING,    -- 'downtown','north_side','south_side','west_side','airport'
  population_estimate   INT64,
  median_income         INT64,
  is_airport            BOOL,      -- O'Hare (76) or Midway (56)
  centroid_lat          FLOAT64,
  centroid_lng          FLOAT64,

  _silver_load_timestamp  TIMESTAMP
)
OPTIONS (description = 'Silver: Chicago community area dimension table.');
```

---

### Step 5.2 — Silver Transformation SQL (Bronze → Silver)

```sql
-- ═══════════════════════════════════════════════════
-- SILVER TRANSFORM: Bronze Taxi Trips → Silver Taxi Trips
-- Run as a Scheduled Query: daily at 3:00 AM
-- ═══════════════════════════════════════════════════

INSERT INTO `chicago-taxi-analytics.silver.taxi_trips`

WITH

-- Step A: Deduplicate Bronze (keep first occurrence of each unique_key)
deduped AS (
  SELECT *
  FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
  WHERE _ingestion_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    -- Deduplication: only process rows not already in Silver
    AND unique_key NOT IN (
      SELECT trip_id
      FROM `chicago-taxi-analytics.silver.taxi_trips`
      WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY unique_key
    ORDER BY _load_timestamp DESC
  ) = 1
),

-- Step B: Clean and derive fields
cleaned AS (
  SELECT
    -- Keys
    unique_key                                        AS trip_id,
    COALESCE(taxi_id, 'UNKNOWN')                      AS taxi_id,

    -- Temporal fields
    trip_start_timestamp,
    trip_end_timestamp,
    DATE(trip_start_timestamp)                        AS trip_date,
    EXTRACT(YEAR FROM trip_start_timestamp)           AS trip_year,
    EXTRACT(MONTH FROM trip_start_timestamp)          AS trip_month,
    EXTRACT(DAYOFWEEK FROM trip_start_timestamp)      AS trip_day_of_week,
    EXTRACT(HOUR FROM trip_start_timestamp)           AS trip_hour,
    EXTRACT(DAYOFWEEK FROM trip_start_timestamp)
      IN (1, 7)                                       AS is_weekend,
    CASE
      WHEN EXTRACT(HOUR FROM trip_start_timestamp) BETWEEN 0  AND 5  THEN 'overnight'
      WHEN EXTRACT(HOUR FROM trip_start_timestamp) BETWEEN 6  AND 9  THEN 'morning_rush'
      WHEN EXTRACT(HOUR FROM trip_start_timestamp) BETWEEN 10 AND 15 THEN 'midday'
      WHEN EXTRACT(HOUR FROM trip_start_timestamp) BETWEEN 16 AND 19 THEN 'evening_rush'
      ELSE 'evening'
    END                                               AS time_of_day_bucket,

    -- Duration and distance
    CASE
      WHEN trip_seconds < 0 THEN NULL
      WHEN trip_seconds > 86400 THEN NULL   -- trips > 24h are invalid
      ELSE trip_seconds
    END                                               AS trip_seconds,
    CASE
      WHEN trip_seconds > 0 THEN ROUND(trip_seconds / 60.0, 2)
      ELSE NULL
    END                                               AS trip_minutes,
    CASE
      WHEN trip_miles < 0 THEN NULL
      WHEN trip_miles > 200 THEN NULL
      ELSE trip_miles
    END                                               AS trip_miles,
    -- Average speed: miles / (seconds / 3600)
    CASE
      WHEN trip_seconds > 0 AND trip_miles > 0
      THEN ROUND(trip_miles / (trip_seconds / 3600.0), 1)
      ELSE NULL
    END                                               AS avg_speed_mph,

    -- Geography
    COALESCE(pickup_community_area, 0)                AS pickup_community_area,
    COALESCE(dropoff_community_area, 0)               AS dropoff_community_area,
    (pickup_community_area = dropoff_community_area)  AS is_same_community_area,
    pickup_centroid_latitude                          AS pickup_latitude,
    pickup_centroid_longitude                         AS pickup_longitude,
    dropoff_centroid_latitude                         AS dropoff_latitude,
    dropoff_centroid_longitude                        AS dropoff_longitude,

    -- Financials — clamp negatives to NULL
    CASE WHEN fare      < 0 THEN NULL ELSE fare      END AS fare,
    CASE WHEN tips      < 0 THEN NULL ELSE tips      END AS tips,
    CASE WHEN tolls     < 0 THEN NULL ELSE tolls     END AS tolls,
    CASE WHEN extras    < 0 THEN NULL ELSE extras    END AS extras,
    CASE WHEN trip_total < 0 THEN NULL ELSE trip_total END AS trip_total,
    -- Tip rate
    CASE
      WHEN fare > 0 AND tips >= 0
      THEN ROUND(tips / fare * 100, 1)
      ELSE 0
    END                                               AS tip_rate_pct,
    COALESCE(tips, 0) > 0                             AS is_tipped,

    -- Categorical: raw
    payment_type,
    -- Standardize payment type
    CASE
      WHEN LOWER(COALESCE(payment_type,'')) LIKE '%credit%'
        OR LOWER(COALESCE(payment_type,'')) LIKE '%card%'   THEN 'card'
      WHEN LOWER(COALESCE(payment_type,'')) LIKE '%cash%'   THEN 'cash'
      WHEN LOWER(COALESCE(payment_type,'')) LIKE '%mobile%'
        OR LOWER(COALESCE(payment_type,'')) LIKE '%prcard%' THEN 'mobile'
      ELSE 'other'
    END                                               AS payment_type_std,
    company,
    TRIM(UPPER(COALESCE(company, 'UNKNOWN')))         AS company_std,

    -- Metadata passthrough
    _ingestion_date                                   AS _bronze_ingestion_date,
    _source_row_hash
  FROM deduped
  WHERE unique_key IS NOT NULL
    AND trip_start_timestamp IS NOT NULL
    AND trip_start_timestamp >= TIMESTAMP '2013-01-01'
    AND trip_start_timestamp < CURRENT_TIMESTAMP()
),

-- Step C: Enrich with weather data
with_weather AS (
  SELECT
    c.*,
    w.avg_temp_f          AS weather_temp_f,
    w.precipitation_in    AS weather_precip_in,
    w.visibility_mi       AS weather_visibility_mi,
    w.avg_wind_speed_kt   AS weather_wind_speed_kt,
    w.weather_condition
  FROM cleaned c
  LEFT JOIN `chicago-taxi-analytics.silver.weather_daily` w
    ON  c.trip_date    = w.weather_date
    AND w.station_id   = '725300'   -- O'Hare (primary Chicago station)
),

-- Step D: Enrich with community area names
with_community AS (
  SELECT
    ww.*,
    pu.community_area_name  AS pickup_community_name,
    dr.community_area_name  AS dropoff_community_name
  FROM with_weather ww
  LEFT JOIN `chicago-taxi-analytics.silver.dim_community_areas` pu
    ON ww.pickup_community_area  = pu.community_area_id
  LEFT JOIN `chicago-taxi-analytics.silver.dim_community_areas` dr
    ON ww.dropoff_community_area = dr.community_area_id
),

-- Step E: Calculate Data Quality Score
with_dq AS (
  SELECT
    wc.*,
    -- DQ Score: start at 100, deduct for issues
    100
    - IF(taxi_id = 'UNKNOWN', 10, 0)
    - IF(trip_seconds IS NULL, 15, 0)
    - IF(trip_miles IS NULL, 15, 0)
    - IF(fare IS NULL, 20, 0)
    - IF(trip_total IS NULL, 20, 0)
    - IF(pickup_community_area = 0, 5, 0)
    - IF(avg_speed_mph > 80, 5, 0)       -- suspiciously fast
    - IF(trip_minutes > 0 AND trip_minutes < 1, 5, 0)  -- suspiciously short
                                                        AS dq_score,
    ARRAY_TO_STRING(ARRAY(
      SELECT flag FROM UNNEST([
        IF(taxi_id = 'UNKNOWN', 'UNKNOWN_TAXI', NULL),
        IF(trip_seconds IS NULL, 'NULL_DURATION', NULL),
        IF(trip_miles IS NULL, 'NULL_DISTANCE', NULL),
        IF(fare IS NULL, 'NULL_FARE', NULL),
        IF(avg_speed_mph > 80, 'HIGH_SPEED', NULL),
        IF(trip_minutes < 1, 'SHORT_TRIP', NULL)
      ]) AS flag
      WHERE flag IS NOT NULL
    ), ',')                                             AS dq_flags
  FROM with_community wc
)

-- Final SELECT with all Silver columns
SELECT
  trip_id, taxi_id,
  trip_start_timestamp, trip_end_timestamp,
  trip_date, trip_year, trip_month, trip_day_of_week, trip_hour,
  is_weekend, time_of_day_bucket,
  trip_seconds, trip_minutes, trip_miles, avg_speed_mph,
  pickup_community_area, dropoff_community_area,
  pickup_community_name, dropoff_community_name,
  is_same_community_area,
  pickup_latitude, pickup_longitude,
  dropoff_latitude, dropoff_longitude,
  fare, tips, tolls, extras, trip_total,
  tip_rate_pct, is_tipped,
  payment_type, payment_type_std,
  company, company_std,
  weather_temp_f, weather_precip_in, weather_visibility_mi,
  weather_wind_speed_kt, weather_condition,
  dq_score, dq_flags,
  _bronze_ingestion_date,
  CURRENT_TIMESTAMP()     AS _silver_load_timestamp,
  GENERATE_UUID()         AS _silver_batch_id,
  _source_row_hash
FROM with_dq
WHERE dq_score >= 30;  -- reject severely corrupt rows
```

---

### Step 5.3 — Weather Silver Transform

```sql
-- Bronze Weather → Silver Weather (run once for historical, then daily)
INSERT INTO `chicago-taxi-analytics.silver.weather_daily`

SELECT
  DATE(CONCAT(year, '-', mo, '-', da))    AS weather_date,
  stn                                     AS station_id,
  ROUND(CAST(temp AS FLOAT64), 1)         AS avg_temp_f,
  ROUND(CAST(max AS FLOAT64), 1)          AS max_temp_f,
  ROUND(CAST(min AS FLOAT64), 1)          AS min_temp_f,
  -- NOAA uses 99.99 as missing value indicator
  CASE WHEN CAST(prcp AS FLOAT64) >= 99.99 THEN 0
       ELSE ROUND(CAST(prcp AS FLOAT64), 2) END  AS precipitation_in,
  ROUND(CAST(wdsp AS FLOAT64), 1)         AS avg_wind_speed_kt,
  ROUND(CAST(visib AS FLOAT64), 1)        AS visibility_mi,
  fog = '1'                               AS has_fog,
  rain_drizzle = '1'                      AS has_rain,
  snow_ice = '1'                          AS has_snow,
  thunder = '1'                           AS has_thunder,
  -- Derive dominant weather condition
  CASE
    WHEN snow_ice = '1'      THEN 'snow'
    WHEN thunder = '1'       THEN 'thunderstorm'
    WHEN rain_drizzle = '1'  THEN 'rain'
    WHEN fog = '1'           THEN 'fog'
    ELSE                          'clear'
  END                                     AS weather_condition,
  -- Temperature category
  CASE
    WHEN CAST(avg_temp_f AS FLOAT64) < 32  THEN 'freezing'
    WHEN CAST(avg_temp_f AS FLOAT64) < 45  THEN 'cold'
    WHEN CAST(avg_temp_f AS FLOAT64) < 60  THEN 'cool'
    WHEN CAST(avg_temp_f AS FLOAT64) < 72  THEN 'mild'
    WHEN CAST(avg_temp_f AS FLOAT64) < 85  THEN 'warm'
    ELSE                                        'hot'
  END                                     AS temp_category,

  CURRENT_TIMESTAMP()                     AS _silver_load_timestamp,
  GENERATE_UUID()                         AS _silver_batch_id
FROM `chicago-taxi-analytics.bronze.weather_raw`
WHERE stn = '725300'                      -- O'Hare primary station
  AND year IS NOT NULL AND mo IS NOT NULL AND da IS NOT NULL
  AND SAFE.CAST(temp AS FLOAT64) IS NOT NULL
  AND SAFE.CAST(temp AS FLOAT64) < 999   -- NOAA missing value sentinel

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CONCAT(year, '-', mo, '-', da)), stn
  ORDER BY _load_timestamp DESC
) = 1;   -- deduplicate: keep latest load for each station-date
```

---

### Step 5.4 — Community Area Dimension Load

```sql
-- Populate community area dimension (static reference data)
-- In production this would come from Chicago Data Portal API
INSERT INTO `chicago-taxi-analytics.silver.dim_community_areas`
VALUES
  -- Downtown / Loop
  (8,  'NEAR NORTH SIDE',    'downtown',   105125, 95000, FALSE, 41.9012, -87.6337, CURRENT_TIMESTAMP()),
  (32, 'LOOP',               'downtown',   42298,  98000, FALSE, 41.8827, -87.6278, CURRENT_TIMESTAMP()),
  (33, 'NEAR SOUTH SIDE',    'downtown',   28795,  68000, FALSE, 41.8680, -87.6219, CURRENT_TIMESTAMP()),
  -- Airports
  (76, "O'HARE",             'airport',    12756,  45000, TRUE,  41.9742, -87.9073, CURRENT_TIMESTAMP()),
  (56, 'GARFIELD RIDGE',     'airport',    34975,  55000, TRUE,  41.7879, -87.7618, CURRENT_TIMESTAMP()),
  -- North Side
  (3,  'UPTOWN',             'north_side', 56323,  52000, FALSE, 41.9660, -87.6524, CURRENT_TIMESTAMP()),
  (4,  'LINCOLN SQUARE',     'north_side', 37435,  68000, FALSE, 41.9680, -87.6832, CURRENT_TIMESTAMP()),
  (6,  'LAKE VIEW',          'north_side', 98978,  83000, FALSE, 41.9440, -87.6468, CURRENT_TIMESTAMP()),
  (7,  'LINCOLN PARK',       'north_side', 66896,  109000,FALSE, 41.9219, -87.6449, CURRENT_TIMESTAMP()),
  -- South Side
  (35, 'DOUGLAS',            'south_side', 18238,  28000, FALSE, 41.8434, -87.6185, CURRENT_TIMESTAMP()),
  (38, 'GRAND BOULEVARD',    'south_side', 21929,  24000, FALSE, 41.8145, -87.6148, CURRENT_TIMESTAMP()),
  (43, 'SOUTH SHORE',        'south_side', 49767,  27000, FALSE, 41.7626, -87.5741, CURRENT_TIMESTAMP()),
  -- West Side
  (25, 'AUSTIN',             'west_side',  98514,  30000, FALSE, 41.8954, -87.7686, CURRENT_TIMESTAMP()),
  (27, 'EAST GARFIELD PARK', 'west_side',  20567,  22000, FALSE, 41.8819, -87.7212, CURRENT_TIMESTAMP()),
  (28, 'NEAR WEST SIDE',     'west_side',  54881,  55000, FALSE, 41.8714, -87.6725, CURRENT_TIMESTAMP());
```

---

### Step 5.5 — Silver Data Quality Dashboard Query

```sql
-- Data quality summary for Silver layer monitoring
SELECT
  trip_date,
  COUNT(*)                                        AS total_trips,
  AVG(dq_score)                                   AS avg_dq_score,
  COUNTIF(dq_score = 100)                         AS perfect_score_trips,
  COUNTIF(dq_score >= 70)                         AS acceptable_quality_trips,
  COUNTIF(dq_score < 70)                          AS low_quality_trips,
  ROUND(COUNTIF(dq_score >= 70) / COUNT(*) * 100, 1) AS quality_pass_rate_pct,
  -- Most common DQ issues
  COUNTIF(dq_flags LIKE '%NULL_FARE%')            AS null_fare_count,
  COUNTIF(dq_flags LIKE '%NULL_DURATION%')        AS null_duration_count,
  COUNTIF(dq_flags LIKE '%HIGH_SPEED%')           AS high_speed_count,
  COUNTIF(dq_flags LIKE '%UNKNOWN_TAXI%')         AS unknown_taxi_count
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY 1 DESC;
```


---

