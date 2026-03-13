<!-- Part of: BigQuery Medallion Architecture Guide | File: 05_Gold_Layer_Star_Schema_and_Materialized_Views.md -->

## SECTION 6: GOLD LAYER — BUSINESS ANALYTICS SERVING LAYER

### Overview
The Gold layer is the **business-facing serving layer**: pre-aggregated, enriched, optimized for BI tools and ML. Built as a star schema (fact + dimension tables) with Materialized Views for the most-queried metrics.

---

### Step 6.1 — Create Gold Star Schema

```sql
-- ═══════════════════════════════════════════════════
-- FACT TABLE: Daily Trip Summary (main fact)
-- Aggregated from Silver for fast dashboard queries
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.gold.fact_daily_trips`
(
  -- Surrogate key
  fact_id                   STRING DEFAULT (GENERATE_UUID()),

  -- Dimension keys (FK to dims)
  trip_date                 DATE,
  taxi_id                   STRING,
  pickup_community_area_id  INT64,
  dropoff_community_area_id INT64,
  payment_type_std          STRING,
  company_std               STRING,
  time_of_day_bucket        STRING,
  weather_condition         STRING,

  -- Measures
  trip_count                INT64,
  total_revenue             FLOAT64,
  total_fare                FLOAT64,
  total_tips                FLOAT64,
  total_miles               FLOAT64,
  total_minutes             FLOAT64,
  avg_fare                  FLOAT64,
  avg_tip_rate_pct          FLOAT64,
  avg_speed_mph             FLOAT64,
  avg_trip_miles            FLOAT64,
  avg_trip_minutes          FLOAT64,
  pct_tipped                FLOAT64,
  pct_card_payment          FLOAT64,

  -- Weather context
  weather_temp_f            FLOAT64,
  weather_precip_in         FLOAT64,

  -- Metadata
  _gold_load_timestamp      TIMESTAMP
)
PARTITION BY trip_date
CLUSTER BY taxi_id, pickup_community_area_id
OPTIONS (description = 'Gold: Daily aggregated trip metrics. Primary fact table for BI.');


-- ═══════════════════════════════════════════════════
-- DIMENSION: Date
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.gold.dim_date`
AS
SELECT
  spine_date                                           AS date_key,
  EXTRACT(YEAR FROM spine_date)                        AS year,
  EXTRACT(MONTH FROM spine_date)                       AS month,
  FORMAT_DATE('%B', spine_date)                        AS month_name,
  EXTRACT(QUARTER FROM spine_date)                     AS quarter,
  EXTRACT(DAYOFWEEK FROM spine_date)                   AS day_of_week,
  FORMAT_DATE('%A', spine_date)                        AS day_name,
  EXTRACT(WEEK FROM spine_date)                        AS week_of_year,
  EXTRACT(DAYOFYEAR FROM spine_date)                   AS day_of_year,
  spine_date = LAST_DAY(spine_date, MONTH)             AS is_month_end,
  EXTRACT(DAYOFWEEK FROM spine_date) IN (1,7)          AS is_weekend,
  -- US Federal Holidays (approximate)
  FORMAT_DATE('%m-%d', spine_date) IN (
    '01-01','07-04','12-25','11-11','01-15','02-19',
    '05-27','09-02','10-14','11-28'
  )                                                    AS is_holiday,
  DATE_TRUNC(spine_date, MONTH)                        AS month_start_date,
  DATE_TRUNC(spine_date, YEAR)                         AS year_start_date
FROM UNNEST(
  GENERATE_DATE_ARRAY(DATE '2013-01-01', DATE '2030-12-31', INTERVAL 1 DAY)
) AS spine_date;


-- ═══════════════════════════════════════════════════
-- DIMENSION: Taxi (slowly changing)
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.gold.dim_taxi`
AS
SELECT DISTINCT
  taxi_id,
  company_std,
  COUNT(*) OVER (PARTITION BY company_std)   AS company_fleet_size,
  MIN(trip_date) OVER (PARTITION BY taxi_id) AS first_seen_date,
  MAX(trip_date) OVER (PARTITION BY taxi_id) AS last_seen_date,
  DATE_DIFF(
    MAX(trip_date) OVER (PARTITION BY taxi_id),
    MIN(trip_date) OVER (PARTITION BY taxi_id),
    DAY
  )                                          AS active_days
FROM `chicago-taxi-analytics.silver.taxi_trips`
QUALIFY ROW_NUMBER() OVER (PARTITION BY taxi_id ORDER BY trip_date DESC) = 1;
```

---

### Step 6.2 — Populate Gold Fact Table

```sql
-- Gold ETL: Aggregate Silver → Gold Fact
-- Scheduled daily at 4:30 AM (after Silver completes at 3 AM)

INSERT INTO `chicago-taxi-analytics.gold.fact_daily_trips`

SELECT
  GENERATE_UUID()                               AS fact_id,
  trip_date,
  taxi_id,
  pickup_community_area                         AS pickup_community_area_id,
  dropoff_community_area                        AS dropoff_community_area_id,
  payment_type_std,
  company_std,
  time_of_day_bucket,
  weather_condition,

  -- Aggregate measures
  COUNT(*)                                      AS trip_count,
  ROUND(SUM(trip_total),      2)                AS total_revenue,
  ROUND(SUM(fare),            2)                AS total_fare,
  ROUND(SUM(tips),            2)                AS total_tips,
  ROUND(SUM(trip_miles),      2)                AS total_miles,
  ROUND(SUM(trip_minutes),    2)                AS total_minutes,
  ROUND(AVG(fare),            2)                AS avg_fare,
  ROUND(AVG(tip_rate_pct),    2)                AS avg_tip_rate_pct,
  ROUND(AVG(avg_speed_mph),   2)                AS avg_speed_mph,
  ROUND(AVG(trip_miles),      2)                AS avg_trip_miles,
  ROUND(AVG(trip_minutes),    2)                AS avg_trip_minutes,
  ROUND(COUNTIF(is_tipped) / COUNT(*) * 100, 1) AS pct_tipped,
  ROUND(COUNTIF(payment_type_std = 'card')
        / COUNT(*) * 100, 1)                    AS pct_card_payment,

  -- Weather (take median for the day)
  ROUND(AVG(weather_temp_f),     1)             AS weather_temp_f,
  ROUND(AVG(weather_precip_in),  2)             AS weather_precip_in,

  CURRENT_TIMESTAMP()                           AS _gold_load_timestamp
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND dq_score >= 50
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;
```

---

### Step 6.3 — Create Materialized Views for Real-Time Dashboards

```sql
-- ═══════════════════════════════════════════════════
-- MATERIALIZED VIEW 1: Today's live KPIs
-- Auto-refreshes every 30 minutes
-- ═══════════════════════════════════════════════════
CREATE MATERIALIZED VIEW IF NOT EXISTS
  `chicago-taxi-analytics.gold.mv_daily_kpis`
OPTIONS (
  enable_refresh = TRUE,
  refresh_interval_minutes = 30
)
AS
SELECT
  trip_date,
  COUNT(*)                                        AS total_trips,
  COUNT(DISTINCT taxi_id)                         AS active_taxis,
  ROUND(SUM(trip_total), 2)                       AS total_revenue,
  ROUND(AVG(trip_total), 2)                       AS avg_revenue_per_trip,
  ROUND(AVG(tip_rate_pct), 2)                     AS avg_tip_pct,
  ROUND(SUM(trip_miles), 1)                       AS total_miles,
  COUNTIF(payment_type_std = 'card')              AS card_payments,
  COUNTIF(payment_type_std = 'cash')              AS cash_payments
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  AND dq_score >= 50
GROUP BY 1;


-- ═══════════════════════════════════════════════════
-- MATERIALIZED VIEW 2: Community area performance
-- ═══════════════════════════════════════════════════
CREATE MATERIALIZED VIEW IF NOT EXISTS
  `chicago-taxi-analytics.gold.mv_community_metrics`
OPTIONS (
  enable_refresh = TRUE,
  refresh_interval_minutes = 60
)
AS
SELECT
  pickup_community_area,
  pickup_community_name,
  DATE_TRUNC(trip_date, MONTH)                    AS trip_month,
  COUNT(*)                                        AS pickups,
  ROUND(SUM(trip_total), 2)                       AS total_revenue,
  ROUND(AVG(tip_rate_pct), 2)                     AS avg_tip_pct,
  COUNT(DISTINCT taxi_id)                         AS unique_taxis
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND pickup_community_area > 0
GROUP BY 1, 2, 3;
```

---

