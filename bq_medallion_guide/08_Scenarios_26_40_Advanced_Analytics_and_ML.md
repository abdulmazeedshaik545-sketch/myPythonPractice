<!-- Part of: BigQuery Medallion Architecture Guide | File: 08_Scenarios_26_40_Advanced_Analytics_and_ML.md -->

### Scenario 26 — Real-Time Streaming Aggregates (Last 5 Minutes)

**Layer:** Streaming Bronze | **Concept:** Streaming window aggregation, TIMESTAMP_TRUNC, live monitoring

```sql
-- Real-time 5-minute tumbling window aggregations from streaming table
SELECT
  TIMESTAMP_TRUNC(event_timestamp, MINUTE)   AS minute_window,
  event_type,
  COUNT(DISTINCT trip_id)                    AS active_trips,
  COUNT(DISTINCT taxi_id)                    AS active_taxis,
  SUM(meter_amount)                          AS window_revenue,
  AVG(speed_mph)                             AS avg_speed,
  -- Trips by community (top areas right now)
  APPROX_TOP_COUNT(CAST(community_area AS STRING), 5) AS top_areas
FROM `chicago-taxi-analytics.bronze.trip_events_stream`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
GROUP BY 1, 2
ORDER BY minute_window DESC, active_trips DESC;
```

---

### Scenario 27 — Streaming + Historical Hybrid Query

**Layer:** Streaming + Silver | **Concept:** UNION ALL of streaming and historical, time-stitching

```sql
-- Stitch last 3 hours of streaming with historical Silver for a seamless time series
WITH streaming_recent AS (
  SELECT
    TIMESTAMP_TRUNC(event_timestamp, HOUR)  AS hour_bucket,
    COUNT(DISTINCT trip_id)                 AS trips,
    SUM(meter_amount)                       AS revenue,
    'streaming'                             AS data_source
  FROM `chicago-taxi-analytics.bronze.trip_events_stream`
  WHERE event_type = 'trip_ended'
    AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
  GROUP BY 1
),

historical_hours AS (
  SELECT
    TIMESTAMP_TRUNC(trip_start_timestamp, HOUR) AS hour_bucket,
    COUNT(*)                                    AS trips,
    SUM(trip_total)                             AS revenue,
    'historical'                                AS data_source
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
    AND trip_start_timestamp <  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
    AND dq_score >= 50
  GROUP BY 1
)

SELECT
  hour_bucket,
  data_source,
  trips,
  ROUND(revenue, 2) AS revenue,
  -- Rolling 3-hour average across unified time series
  AVG(trips) OVER (
    ORDER BY hour_bucket
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  )                 AS trips_3h_ma
FROM (
  SELECT * FROM historical_hours
  UNION ALL
  SELECT * FROM streaming_recent
)
ORDER BY hour_bucket DESC;
```

---

### Scenario 28 — Community Area Revenue Heatmap by Hour × Day

**Layer:** Silver | **Concept:** Multi-dimensional aggregation, heatmap data for Looker Studio

```sql
SELECT
  pickup_community_name                             AS community,
  trip_day_of_week,
  trip_hour,
  COUNT(*)                                          AS trip_count,
  ROUND(SUM(trip_total), 2)                         AS total_revenue,
  ROUND(AVG(trip_total), 2)                         AS avg_fare,
  -- Relative demand index (vs. community average)
  ROUND(
    COUNT(*) / AVG(COUNT(*)) OVER (PARTITION BY pickup_community_name),
  2)                                                AS demand_index
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  AND pickup_community_name IS NOT NULL
  AND dq_score >= 70
GROUP BY 1, 2, 3
ORDER BY pickup_community_name, trip_day_of_week, trip_hour;
```

---

### Scenario 29 — Fare Component Breakdown (Waterfall)

**Layer:** Silver | **Concept:** Component analysis, UNPIVOT, waterfall chart data

```sql
-- Break down trip total into components: base fare, tip, tolls, extras
WITH component_summary AS (
  SELECT
    time_of_day_bucket,
    payment_type_std,
    COUNT(*)                              AS trips,
    ROUND(AVG(fare), 2)                   AS avg_base_fare,
    ROUND(AVG(tips), 2)                   AS avg_tips,
    ROUND(AVG(tolls), 2)                  AS avg_tolls,
    ROUND(AVG(extras), 2)                 AS avg_extras,
    ROUND(AVG(trip_total), 2)             AS avg_total,
    -- Component shares
    ROUND(AVG(fare) / AVG(trip_total) * 100, 1)   AS fare_pct,
    ROUND(AVG(tips) / AVG(trip_total) * 100, 1)   AS tip_pct,
    ROUND(AVG(tolls) / AVG(trip_total) * 100, 1)  AS toll_pct,
    ROUND(AVG(extras) / AVG(trip_total) * 100, 1) AS extras_pct
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND trip_total > 0
    AND dq_score >= 70
  GROUP BY 1, 2
)

-- UNPIVOT components for charting
SELECT
  time_of_day_bucket,
  payment_type_std,
  component_name,
  component_value
FROM component_summary
UNPIVOT (
  component_value FOR component_name IN (
    avg_base_fare, avg_tips, avg_tolls, avg_extras
  )
)
ORDER BY time_of_day_bucket, payment_type_std, component_name;
```

---

### Scenario 30 — Medallion Data Lineage Audit

**Layer:** Monitoring | **Concept:** INFORMATION_SCHEMA, lineage tracking, pipeline health

```sql
-- Audit data freshness across all medallion layers
WITH bronze_freshness AS (
  SELECT
    'bronze' AS layer,
    'taxi_trips_raw' AS table_name,
    MAX(_ingestion_date) AS latest_date,
    COUNT(*) AS row_count
  FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
  WHERE _ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

silver_freshness AS (
  SELECT
    'silver' AS layer,
    'taxi_trips' AS table_name,
    MAX(trip_date) AS latest_date,
    COUNT(*) AS row_count
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

gold_freshness AS (
  SELECT
    'gold' AS layer,
    'fact_daily_trips' AS table_name,
    MAX(trip_date) AS latest_date,
    COUNT(*) AS row_count
  FROM `chicago-taxi-analytics.gold.fact_daily_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)

SELECT
  layer,
  table_name,
  latest_date,
  DATE_DIFF(CURRENT_DATE(), latest_date, DAY) AS days_behind,
  row_count,
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), latest_date, DAY) = 0  THEN '🟢 Current'
    WHEN DATE_DIFF(CURRENT_DATE(), latest_date, DAY) <= 1 THEN '🟡 1 Day Late'
    WHEN DATE_DIFF(CURRENT_DATE(), latest_date, DAY) <= 3 THEN '🟠 Delayed'
    ELSE '🔴 Critical Lag'
  END AS freshness_status
FROM (
  SELECT * FROM bronze_freshness
  UNION ALL SELECT * FROM silver_freshness
  UNION ALL SELECT * FROM gold_freshness
)
ORDER BY layer;
```

---

### Scenario 31 — MERGE-Based Incremental Gold Update

**Layer:** Gold | **Concept:** MERGE upsert, incremental processing, idempotent pipeline

```sql
-- Idempotent MERGE: re-run safely without creating duplicates
MERGE `chicago-taxi-analytics.gold.fact_daily_trips` AS target
USING (
  -- Source: today's aggregated Silver data
  SELECT
    trip_date, taxi_id,
    pickup_community_area AS pickup_community_area_id,
    dropoff_community_area AS dropoff_community_area_id,
    payment_type_std, company_std, time_of_day_bucket, weather_condition,
    COUNT(*)                      AS trip_count,
    ROUND(SUM(trip_total), 2)     AS total_revenue,
    ROUND(SUM(fare), 2)           AS total_fare,
    ROUND(SUM(tips), 2)           AS total_tips,
    ROUND(SUM(trip_miles), 2)     AS total_miles,
    ROUND(SUM(trip_minutes), 2)   AS total_minutes,
    ROUND(AVG(fare), 2)           AS avg_fare,
    ROUND(AVG(tip_rate_pct), 2)   AS avg_tip_rate_pct,
    ROUND(AVG(avg_speed_mph), 2)  AS avg_speed_mph,
    ROUND(AVG(trip_miles), 2)     AS avg_trip_miles,
    ROUND(AVG(trip_minutes), 2)   AS avg_trip_minutes,
    ROUND(COUNTIF(is_tipped)/COUNT(*)*100, 1)        AS pct_tipped,
    ROUND(COUNTIF(payment_type_std='card')/COUNT(*)*100, 1) AS pct_card_payment,
    ROUND(AVG(weather_temp_f), 1) AS weather_temp_f,
    ROUND(AVG(weather_precip_in), 2) AS weather_precip_in
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND dq_score >= 50
  GROUP BY 1,2,3,4,5,6,7,8
) AS source
ON (
  target.trip_date = source.trip_date
  AND target.taxi_id = source.taxi_id
  AND target.pickup_community_area_id = source.pickup_community_area_id
  AND target.payment_type_std = source.payment_type_std
  AND target.time_of_day_bucket = source.time_of_day_bucket
)
WHEN MATCHED THEN
  UPDATE SET
    target.trip_count      = source.trip_count,
    target.total_revenue   = source.total_revenue,
    target._gold_load_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET THEN
  INSERT ROW;
```

---

### Scenario 32 — JSON Event Parsing from Streaming

**Layer:** Bronze Streaming | **Concept:** JSON_VALUE, JSON_QUERY, semi-structured streaming data

```sql
-- Parse and analyze JSON event payloads from streaming table
SELECT
  event_id,
  event_type,
  taxi_id,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', event_timestamp) AS event_time,
  -- Extract nested JSON fields from event_payload
  JSON_VALUE(event_payload, '$.community_area')     AS community_area,
  JSON_VALUE(event_payload, '$.payment_type')       AS payment_type,
  JSON_VALUE(event_payload, '$.passenger_count')    AS passengers,
  CAST(JSON_VALUE(event_payload, '$.meter_amount') AS FLOAT64) AS meter_amount,
  -- Extract nested arrays
  JSON_QUERY(event_payload, '$.route_waypoints')    AS route_json,
  -- Compute from JSON
  TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    event_timestamp,
    SECOND
  )                                                 AS event_age_sec
FROM `chicago-taxi-analytics.bronze.trip_events_stream`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  AND event_type IN ('trip_started', 'trip_ended')
ORDER BY event_timestamp DESC
LIMIT 200;
```

---

### Scenario 33 — Lag/Lead: Inter-Trip Wait Time Analysis

**Layer:** Silver | **Concept:** LAG/LEAD cross-day, gap analysis, idle time quantification

```sql
WITH trip_gaps AS (
  SELECT
    taxi_id,
    company_std,
    trip_date,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_total,
    pickup_community_name,
    time_of_day_bucket,
    -- Time since last trip ended
    TIMESTAMP_DIFF(
      trip_start_timestamp,
      LAG(trip_end_timestamp) OVER (
        PARTITION BY taxi_id ORDER BY trip_start_timestamp
      ),
      MINUTE
    ) AS wait_minutes_after_prev_trip,
    -- Next trip revenue (forward looking)
    LEAD(trip_total) OVER (
      PARTITION BY taxi_id ORDER BY trip_start_timestamp
    ) AS next_trip_revenue
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND dq_score >= 70
)

SELECT
  company_std,
  time_of_day_bucket,
  COUNT(*)                                            AS trips,
  ROUND(AVG(wait_minutes_after_prev_trip), 1)         AS avg_wait_min,
  ROUND(PERCENTILE_CONT(wait_minutes_after_prev_trip, 0.50)
        OVER (PARTITION BY company_std, time_of_day_bucket), 1) AS median_wait_min,
  ROUND(PERCENTILE_CONT(wait_minutes_after_prev_trip, 0.90)
        OVER (PARTITION BY company_std, time_of_day_bucket), 1) AS p90_wait_min,
  -- Next-trip value by wait: do longer waits lead to higher-value trips?
  ROUND(AVG(IF(wait_minutes_after_prev_trip > 20, next_trip_revenue, NULL)), 2)
                                                      AS avg_next_rev_after_long_wait,
  ROUND(AVG(IF(wait_minutes_after_prev_trip <= 5, next_trip_revenue, NULL)), 2)
                                                      AS avg_next_rev_after_quick_turnaround
FROM trip_gaps
WHERE wait_minutes_after_prev_trip BETWEEN 0 AND 120
GROUP BY 1, 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY company_std, time_of_day_bucket ORDER BY COUNT(*) DESC) = 1
ORDER BY avg_wait_min ASC;
```

---

### Scenario 34 — GENERATE_DATE_ARRAY: Active Taxis Per Day Spine

**Layer:** Silver | **Concept:** Date spine, cross-join, gap detection, churn analysis

```sql
-- Find taxis that were active then went dormant (potential churn)
WITH date_spine AS (
  SELECT spine_date
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY),
      CURRENT_DATE(),
      INTERVAL 1 DAY
    )
  ) AS spine_date
),

taxi_daily_activity AS (
  SELECT DISTINCT
    taxi_id,
    company_std,
    trip_date
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND dq_score >= 50
),

taxi_list AS (
  SELECT DISTINCT taxi_id, company_std FROM taxi_daily_activity
),

-- Cross join every taxi with every date
full_spine AS (
  SELECT tl.taxi_id, tl.company_std, ds.spine_date
  FROM taxi_list tl
  CROSS JOIN date_spine ds
),

with_activity AS (
  SELECT
    fs.taxi_id,
    fs.company_std,
    fs.spine_date,
    IF(tda.trip_date IS NOT NULL, 1, 0) AS was_active
  FROM full_spine fs
  LEFT JOIN taxi_daily_activity tda
    ON  fs.taxi_id    = tda.taxi_id
    AND fs.spine_date = tda.trip_date
),

streaks AS (
  SELECT
    taxi_id,
    company_std,
    spine_date,
    was_active,
    -- Consecutive inactive days
    SUM(IF(was_active = 0, 1, 0)) OVER (
      PARTITION BY taxi_id
      ORDER BY spine_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS inactive_days_last_14
  FROM with_activity
  WHERE spine_date = CURRENT_DATE()
)

SELECT
  taxi_id,
  company_std,
  inactive_days_last_14,
  14 - inactive_days_last_14 AS active_days_last_14,
  CASE
    WHEN inactive_days_last_14 >= 14 THEN 'Churned (14+ days inactive)'
    WHEN inactive_days_last_14 >= 7  THEN 'At Risk (7+ days inactive)'
    WHEN inactive_days_last_14 >= 3  THEN 'Sporadic'
    ELSE 'Active'
  END AS activity_status
FROM streaks
ORDER BY inactive_days_last_14 DESC;
```

---

### Scenario 35 — STRING_AGG: Trip Route Summary Per Taxi

**Layer:** Silver | **Concept:** STRING_AGG ordered, route compression, path encoding

```sql
-- Build a daily route summary for each taxi using STRING_AGG
SELECT
  taxi_id,
  trip_date,
  COUNT(*) AS trips,
  ROUND(SUM(trip_total), 2) AS daily_revenue,
  -- Ordered route of community areas visited
  STRING_AGG(
    COALESCE(pickup_community_name, '?'),
    ' → '
    ORDER BY trip_start_timestamp
    LIMIT 10
  ) AS daily_route,
  -- Unique areas visited
  STRING_AGG(
    DISTINCT COALESCE(pickup_community_name, '?')
    ORDER BY COALESCE(pickup_community_name, '?')
  ) AS areas_served,
  COUNT(DISTINCT pickup_community_area) AS unique_pickup_areas,
  -- First and last area
  FIRST_VALUE(pickup_community_name) OVER (
    PARTITION BY taxi_id, trip_date
    ORDER BY trip_start_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS day_started_in,
  LAST_VALUE(dropoff_community_name) OVER (
    PARTITION BY taxi_id, trip_date
    ORDER BY trip_start_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS day_ended_in
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date = CURRENT_DATE() - 1
  AND dq_score >= 70
GROUP BY 1, 2
ORDER BY daily_revenue DESC
LIMIT 50;
```

---

### Scenario 36 — INFORMATION_SCHEMA: Query Cost Analysis

**Layer:** Monitoring | **Concept:** INFORMATION_SCHEMA.JOBS, query cost optimization, top spenders

```sql
-- Identify expensive queries on this project (last 7 days)
SELECT
  user_email,
  DATE(creation_time)                             AS query_date,
  statement_type,
  ROUND(total_bytes_processed / POW(1024,4) * 5, 4) AS est_cost_usd,
  ROUND(total_bytes_processed / POW(1024,3), 1)  AS gb_processed,
  ROUND(total_slot_ms / 1000.0, 1)               AS slot_seconds,
  -- Detect queries not using partition filters (expensive pattern)
  CASE
    WHEN REGEXP_CONTAINS(query, r'WHERE.*trip_date|WHERE.*_ingestion_date')
    THEN '✅ Uses partition filter'
    WHEN REGEXP_CONTAINS(query, r'FROM.*silver|FROM.*bronze|FROM.*gold')
    THEN '⚠️ No partition filter detected'
    ELSE '—'
  END AS partition_filter_check,
  SUBSTR(query, 1, 150) AS query_preview
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND project_id = 'chicago-taxi-analytics'
  AND job_type   = 'QUERY'
  AND state      = 'DONE'
  AND total_bytes_processed > 1e9   -- queries > 1GB
ORDER BY est_cost_usd DESC
LIMIT 50;
```

---

### Scenario 37 — Wildcard Table: Annual Comparison

**Layer:** Bronze | **Concept:** Wildcard table suffix, _TABLE_SUFFIX filtering, year-over-year

```sql
-- If Bronze is sharded by year (bronze.taxi_trips_raw_2022, _2023, _2024)
-- Use wildcard to query across all years efficiently

SELECT
  _TABLE_SUFFIX                              AS year_shard,
  EXTRACT(MONTH FROM trip_start_timestamp)   AS month_num,
  FORMAT_DATE('%B', DATE(trip_start_timestamp)) AS month_name,
  COUNT(*)                                   AS trips,
  ROUND(SUM(trip_total), 2)                  AS total_revenue,
  ROUND(AVG(trip_total), 2)                  AS avg_fare
FROM `chicago-taxi-analytics.bronze.taxi_trips_raw_20*`
WHERE _TABLE_SUFFIX IN ('22', '23', '24')   -- 2022, 2023, 2024
  AND trip_start_timestamp IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY year_shard, month_num;
```

---

### Scenario 38 — PERCENTILE_CONT: Exact SLA Metrics

**Layer:** Silver | **Concept:** PERCENTILE_CONT exact, percentile distribution, SLA compliance

```sql
-- Exact percentile analysis of trip duration and fare (for SLA reporting)
SELECT DISTINCT
  time_of_day_bucket,
  payment_type_std,
  -- Exact percentiles (use when precision required, e.g., regulatory SLA)
  ROUND(PERCENTILE_CONT(trip_minutes, 0.25) OVER w, 1) AS p25_duration_min,
  ROUND(PERCENTILE_CONT(trip_minutes, 0.50) OVER w, 1) AS median_duration_min,
  ROUND(PERCENTILE_CONT(trip_minutes, 0.75) OVER w, 1) AS p75_duration_min,
  ROUND(PERCENTILE_CONT(trip_minutes, 0.90) OVER w, 1) AS p90_duration_min,
  ROUND(PERCENTILE_CONT(fare, 0.50) OVER w, 2)         AS median_fare,
  ROUND(PERCENTILE_CONT(fare, 0.90) OVER w, 2)         AS p90_fare,
  ROUND(PERCENTILE_CONT(fare, 0.99) OVER w, 2)         AS p99_fare,
  COUNT(*) OVER w                                      AS sample_size
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND trip_minutes > 0
  AND fare > 0
  AND dq_score >= 70
WINDOW w AS (PARTITION BY time_of_day_bucket, payment_type_std)
ORDER BY time_of_day_bucket, payment_type_std;
```

---

### Scenario 39 — Real-Time Alert: Unusual Activity Detection

**Layer:** Streaming + Silver | **Concept:** Z-score on streaming, threshold alerting, real-time ops

```sql
-- Flag taxis with unusual activity RIGHT NOW vs. their historical baseline
WITH historical_baseline AS (
  SELECT
    taxi_id,
    EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day,
    AVG(COUNT(*))  OVER (PARTITION BY taxi_id, EXTRACT(HOUR FROM trip_start_timestamp))
                                            AS historical_avg_trips_per_hour,
    STDDEV(COUNT(*)) OVER (PARTITION BY taxi_id, EXTRACT(HOUR FROM trip_start_timestamp))
                                            AS historical_std_trips_per_hour
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND dq_score >= 70
  GROUP BY taxi_id, EXTRACT(HOUR FROM trip_start_timestamp)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY taxi_id, EXTRACT(HOUR FROM trip_start_timestamp)
    ORDER BY COUNT(*) DESC
  ) = 1
),

current_activity AS (
  SELECT
    taxi_id,
    EXTRACT(HOUR FROM event_timestamp) AS current_hour,
    COUNT(DISTINCT trip_id) AS trips_this_hour
  FROM `chicago-taxi-analytics.bronze.trip_events_stream`
  WHERE event_type = 'trip_ended'
    AND event_timestamp >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)
  GROUP BY 1, 2
)

SELECT
  ca.taxi_id,
  ca.trips_this_hour,
  ROUND(hb.historical_avg_trips_per_hour, 1) AS expected_trips,
  ROUND(
    SAFE_DIVIDE(
      ca.trips_this_hour - hb.historical_avg_trips_per_hour,
      NULLIF(hb.historical_std_trips_per_hour, 0)
    ), 2
  )                                          AS z_score,
  CASE
    WHEN SAFE_DIVIDE(
      ca.trips_this_hour - hb.historical_avg_trips_per_hour,
      hb.historical_std_trips_per_hour) > 3  THEN '🔴 Unusually High Activity'
    WHEN SAFE_DIVIDE(
      ca.trips_this_hour - hb.historical_avg_trips_per_hour,
      hb.historical_std_trips_per_hour) < -3 THEN '🔵 Unusually Low Activity'
    ELSE '✅ Normal'
  END                                        AS activity_flag
FROM current_activity ca
LEFT JOIN historical_baseline hb
  ON  ca.taxi_id       = hb.taxi_id
  AND ca.current_hour  = hb.hour_of_day
WHERE ABS(SAFE_DIVIDE(
  ca.trips_this_hour - hb.historical_avg_trips_per_hour,
  hb.historical_std_trips_per_hour)) > 2
ORDER BY z_score DESC;
```

---

### Scenario 40 — Revenue Forecasting with ARIMA Residuals

**Layer:** ML | **Concept:** ML.FORECAST output analysis, forecast vs actual comparison

```sql
-- Compare ARIMA forecast vs. actual (model validation)
WITH actuals AS (
  SELECT
    trip_date,
    pickup_community_area,
    COUNT(*) AS actual_trips
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND dq_score >= 50
  GROUP BY 1, 2
),

forecasts AS (
  SELECT
    DATE(forecast_timestamp) AS forecast_date,
    pickup_community_area,
    ROUND(forecast_value, 0) AS forecasted_trips,
    ROUND(prediction_interval_lower_bound, 0) AS lower_ci,
    ROUND(prediction_interval_upper_bound, 0) AS upper_ci
  FROM ML.FORECAST(
    MODEL `chicago-taxi-analytics.ml.daily_demand_forecast`,
    STRUCT(30 AS horizon, 0.90 AS confidence_level)
  )
)

SELECT
  a.trip_date,
  a.pickup_community_area,
  a.actual_trips,
  f.forecasted_trips,
  f.lower_ci,
  f.upper_ci,
  a.actual_trips - f.forecasted_trips               AS forecast_error,
  ROUND(
    SAFE_DIVIDE(
      ABS(a.actual_trips - f.forecasted_trips),
      a.actual_trips
    ) * 100, 1
  )                                                 AS mape_pct,
  -- Was actual within 90% confidence interval?
  (a.actual_trips BETWEEN f.lower_ci AND f.upper_ci) AS within_ci
FROM actuals a
JOIN forecasts f
  ON  a.trip_date               = f.forecast_date
  AND a.pickup_community_area   = f.pickup_community_area
ORDER BY a.trip_date, a.pickup_community_area;
```

---

### ━━ OPERATIONAL + GOVERNANCE SCENARIOS (41–50) ━━

---

