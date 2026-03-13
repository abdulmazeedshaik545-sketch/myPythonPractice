<!-- Part of: BigQuery Medallion Architecture Guide | File: 07_Scenarios_11_25_Business_Intelligence.md -->

---

### Scenario 11 — Monthly Revenue Cohort Analysis

**Layer:** Silver | **Concept:** First-trip cohort, GENERATE_DATE_ARRAY, retention waterfall

```sql
-- Which monthly cohort of taxis generates the most sustained revenue?
WITH taxi_first_trip AS (
  SELECT
    taxi_id,
    DATE_TRUNC(MIN(trip_date), MONTH) AS cohort_month
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  GROUP BY 1
),

monthly_activity AS (
  SELECT
    t.taxi_id,
    fc.cohort_month,
    DATE_TRUNC(t.trip_date, MONTH)  AS active_month,
    DATE_DIFF(
      DATE_TRUNC(t.trip_date, MONTH),
      fc.cohort_month, MONTH
    )                               AS months_since_first,
    SUM(t.trip_total)               AS monthly_revenue
  FROM `chicago-taxi-analytics.silver.taxi_trips` t
  JOIN taxi_first_trip fc USING (taxi_id)
  GROUP BY 1, 2, 3, 4
)

SELECT
  cohort_month,
  months_since_first,
  COUNT(DISTINCT taxi_id)                          AS active_taxis,
  ROUND(SUM(monthly_revenue), 2)                  AS cohort_revenue,
  -- Retention: what % of original cohort is still active?
  ROUND(
    COUNT(DISTINCT taxi_id) /
    MAX(COUNT(DISTINCT taxi_id)) OVER (
      PARTITION BY cohort_month
    ) * 100, 1
  )                                               AS retention_pct
FROM monthly_activity
WHERE months_since_first BETWEEN 0 AND 12
GROUP BY 1, 2
ORDER BY cohort_month, months_since_first;
```

---

### Scenario 12 — Tip Prediction Feature Engineering

**Layer:** Silver | **Concept:** Feature table creation, lag features, binning, ML-ready output

```sql
-- Build ML feature table for tip prediction
SELECT
  trip_id,
  -- Features
  trip_hour,
  trip_day_of_week,
  is_weekend,
  time_of_day_bucket,
  COALESCE(trip_miles, 0)             AS trip_miles,
  COALESCE(trip_minutes, 0)           AS trip_minutes,
  COALESCE(fare, 0)                   AS fare,
  payment_type_std,
  pickup_community_area,
  dropoff_community_area,
  is_same_community_area,
  COALESCE(weather_temp_f, 55)        AS weather_temp_f,
  weather_condition,
  COALESCE(weather_precip_in, 0)      AS weather_precip_in,
  -- Rolling taxi performance features
  AVG(tip_rate_pct) OVER (
    PARTITION BY taxi_id
    ORDER BY trip_start_timestamp
    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
  )                                   AS taxi_recent_avg_tip,
  COUNT(*) OVER (
    PARTITION BY taxi_id
    ORDER BY trip_start_timestamp
    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
  )                                   AS taxi_recent_trip_count,
  -- Target variable
  tip_rate_pct                        AS label_tip_rate,
  IF(is_tipped, 1, 0)                 AS label_is_tipped
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND dq_score >= 70
  AND fare > 0
  AND trip_miles > 0
  AND payment_type_std = 'card'   -- cash tips are unreported; only card tips reliable
ORDER BY trip_start_timestamp;
```

---

### Scenario 13 — Geospatial Hotspot: ST_GEOGPOINT Cluster Density

**Layer:** Silver | **Concept:** ST_ geography functions, spatial binning, hotspot detection

```sql
-- Find pickup hotspots using geographic grid binning
SELECT
  -- Round to 0.01 degree grid (~1 km cells)
  ROUND(pickup_latitude  / 0.01) * 0.01   AS grid_lat,
  ROUND(pickup_longitude / 0.01) * 0.01   AS grid_lng,
  -- Grid center point
  ST_GEOGPOINT(
    ROUND(pickup_longitude / 0.01) * 0.01,
    ROUND(pickup_latitude  / 0.01) * 0.01
  )                                        AS grid_center,
  COUNT(*)                                 AS pickup_count,
  ROUND(SUM(trip_total), 2)               AS total_revenue,
  ROUND(AVG(trip_total), 2)               AS avg_fare,
  ROUND(AVG(tip_rate_pct), 1)             AS avg_tip_pct,
  COUNT(DISTINCT taxi_id)                 AS unique_taxis,
  -- Peak hour for this grid cell
  APPROX_TOP_COUNT(CAST(trip_hour AS STRING), 1)[OFFSET(0)].value AS peak_hour
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND pickup_latitude  BETWEEN 41.65 AND 42.05
  AND pickup_longitude BETWEEN -87.85 AND -87.52
  AND dq_score >= 70
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 50
ORDER BY pickup_count DESC
LIMIT 100;
```

---

### Scenario 14 — Fare Leakage Detection (Anomalous Trips)

**Layer:** Silver | **Concept:** Statistical outlier detection, IQR method, business rule validation

```sql
WITH fare_stats AS (
  SELECT
    pickup_community_area,
    dropoff_community_area,
    APPROX_QUANTILES(fare, 4)[OFFSET(1)]  AS fare_q1,
    APPROX_QUANTILES(fare, 4)[OFFSET(2)]  AS fare_median,
    APPROX_QUANTILES(fare, 4)[OFFSET(3)]  AS fare_q3,
    AVG(fare)                             AS fare_mean,
    STDDEV(fare)                          AS fare_std
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND fare > 0
  GROUP BY 1, 2
),

flagged_trips AS (
  SELECT
    t.trip_id,
    t.taxi_id,
    t.trip_date,
    t.fare,
    t.trip_miles,
    t.trip_minutes,
    t.payment_type_std,
    fs.fare_median,
    fs.fare_q3,
    -- IQR fence: Q3 + 1.5 × (Q3 - Q1)
    fs.fare_q3 + 1.5 * (fs.fare_q3 - fs.fare_q1) AS upper_fence,
    -- Z-score within corridor
    ROUND(SAFE_DIVIDE(t.fare - fs.fare_mean, fs.fare_std), 2) AS fare_z_score
  FROM `chicago-taxi-analytics.silver.taxi_trips` t
  JOIN fare_stats fs
    ON  t.pickup_community_area  = fs.pickup_community_area
    AND t.dropoff_community_area = fs.dropoff_community_area
  WHERE t.trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND t.fare > 0
)

SELECT
  trip_id,
  taxi_id,
  trip_date,
  ROUND(fare, 2)          AS fare,
  ROUND(fare_median, 2)   AS corridor_median_fare,
  ROUND(upper_fence, 2)   AS outlier_fence,
  fare_z_score,
  trip_miles,
  ROUND(fare / NULLIF(trip_miles,0), 2) AS fare_per_mile,
  payment_type_std,
  CASE
    WHEN fare > upper_fence AND fare_z_score > 3   THEN '🔴 Severe Overcharge'
    WHEN fare > upper_fence                         THEN '🟡 Possible Overcharge'
    WHEN fare < 3.25                               THEN '🔵 Possible Undercharge'
    ELSE '✅ Normal'
  END                     AS fare_flag
FROM flagged_trips
WHERE fare > upper_fence OR fare < 3.25
ORDER BY fare_z_score DESC
LIMIT 500;
```

---

### Scenario 15 — Year-over-Year Seasonality Analysis

**Layer:** Gold | **Concept:** Multiple YoY LAGs, seasonal index, indexed comparison

```sql
WITH monthly_revenue AS (
  SELECT
    DATE_TRUNC(trip_date, MONTH)          AS trip_month,
    SUM(trip_total)                       AS monthly_revenue,
    COUNT(*)                              AS trip_count,
    COUNT(DISTINCT taxi_id)               AS active_taxis
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE dq_score >= 50
  GROUP BY 1
)

SELECT
  trip_month,
  ROUND(monthly_revenue, 2)              AS revenue,
  trip_count,
  -- YoY comparison (12 months prior)
  LAG(monthly_revenue, 12) OVER (ORDER BY trip_month) AS revenue_1y_ago,
  LAG(monthly_revenue, 24) OVER (ORDER BY trip_month) AS revenue_2y_ago,
  ROUND(
    SAFE_DIVIDE(monthly_revenue - LAG(monthly_revenue,12) OVER (ORDER BY trip_month),
    LAG(monthly_revenue,12) OVER (ORDER BY trip_month)) * 100,
  1)                                     AS yoy_growth_pct,
  -- Seasonal index: month / full-year average (>1 = above-average month)
  ROUND(
    monthly_revenue /
    AVG(monthly_revenue) OVER (
      PARTITION BY EXTRACT(YEAR FROM trip_month)
    ),
  3)                                     AS seasonal_index,
  -- Is this month typically a peak?
  AVG(monthly_revenue) OVER (
    PARTITION BY EXTRACT(MONTH FROM trip_month)
  )                                      AS historical_avg_for_this_month
FROM monthly_revenue
ORDER BY trip_month DESC;
```

---

### Scenario 16 — BQML: Demand Forecasting (ARIMA+)

**Layer:** Gold / ML | **Concept:** BQML ARIMA_PLUS, time series forecasting

```sql
-- Train ARIMA+ model on daily trip counts
CREATE OR REPLACE MODEL `chicago-taxi-analytics.ml.daily_demand_forecast`
OPTIONS (
  model_type     = 'ARIMA_PLUS',
  time_series_timestamp_col = 'trip_date',
  time_series_data_col      = 'trip_count',
  time_series_id_col        = 'pickup_community_area',
  holiday_region = 'US',           -- account for US holidays
  clean_spikes_and_dips = TRUE,    -- handle COVID dip etc.
  data_frequency = 'AUTO_FREQUENCY'
)
AS
SELECT
  trip_date,
  pickup_community_area,
  COUNT(*) AS trip_count
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE '2018-01-01'
  AND pickup_community_area IN (8, 32, 76, 56, 6, 7)  -- top 6 areas
  AND dq_score >= 50
GROUP BY 1, 2;


-- Forecast next 30 days
SELECT
  forecast_timestamp,
  pickup_community_area,
  ROUND(forecast_value, 0)      AS predicted_trips,
  ROUND(prediction_interval_lower_bound, 0) AS lower_bound,
  ROUND(prediction_interval_upper_bound, 0) AS upper_bound
FROM ML.FORECAST(
  MODEL `chicago-taxi-analytics.ml.daily_demand_forecast`,
  STRUCT(30 AS horizon, 0.90 AS confidence_level)
)
ORDER BY pickup_community_area, forecast_timestamp;
```

---

### Scenario 17 — BQML: Tip Prediction Model

**Layer:** ML | **Concept:** BQML logistic regression, feature importance, ML.EXPLAIN_PREDICT

```sql
-- Train tip prediction model
CREATE OR REPLACE MODEL `chicago-taxi-analytics.ml.tip_predictor`
OPTIONS (
  model_type       = 'logistic_reg',
  input_label_cols = ['label_is_tipped'],
  max_iterations   = 50,
  l1_reg           = 0.01,
  enable_global_explain = TRUE
)
AS
SELECT
  trip_hour, trip_day_of_week, is_weekend,
  trip_miles, trip_minutes, fare,
  payment_type_std, pickup_community_area,
  weather_condition, weather_temp_f,
  taxi_recent_avg_tip, taxi_recent_trip_count,
  label_is_tipped
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date BETWEEN DATE '2022-01-01' AND DATE '2023-12-31'
  AND dq_score >= 70
  AND fare > 0
  AND payment_type_std = 'card';


-- Score recent trips
SELECT
  t.trip_id,
  t.trip_date,
  t.taxi_id,
  t.fare,
  p.predicted_label_is_tipped      AS will_tip_prediction,
  ROUND(p.predicted_label_is_tipped_probs[OFFSET(1)].prob * 100, 1)
                                   AS tip_probability_pct
FROM ML.PREDICT(
  MODEL `chicago-taxi-analytics.ml.tip_predictor`,
  (SELECT * FROM `chicago-taxi-analytics.silver.taxi_trips`
   WHERE trip_date = CURRENT_DATE() - 1
     AND payment_type_std = 'card')
) AS p
JOIN `chicago-taxi-analytics.silver.taxi_trips` t
  USING (trip_id)
ORDER BY tip_probability_pct DESC
LIMIT 100;
```

---

### Scenario 18 — BQML: Unsupervised Driver Segmentation (K-Means)

**Layer:** ML | **Concept:** BQML K-Means clustering, ML.CENTROIDS, driver personas

```sql
-- Cluster drivers into behavioral segments
CREATE OR REPLACE MODEL `chicago-taxi-analytics.ml.driver_segments`
OPTIONS (
  model_type   = 'k_means',
  num_clusters = 5,
  standardize_features = TRUE
)
AS
SELECT
  taxi_id,
  AVG(trip_miles)                              AS avg_trip_distance,
  AVG(trip_minutes)                            AS avg_trip_duration,
  AVG(tip_rate_pct)                            AS avg_tip_rate,
  COUNTIF(is_weekend) / COUNT(*) * 100         AS pct_weekend_trips,
  COUNTIF(time_of_day_bucket = 'evening_rush') / COUNT(*) * 100
                                               AS pct_peak_trips,
  COUNTIF(pickup_community_area IN (76,56)) / COUNT(*) * 100
                                               AS pct_airport_trips,
  COUNT(*) / 30.0                              AS avg_daily_trips
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND dq_score >= 70
GROUP BY taxi_id
HAVING COUNT(*) >= 30;


-- View cluster centroids (driver personas)
SELECT
  centroid_id,
  feature_name,
  ROUND(numerical_value, 2) AS centroid_value
FROM ML.CENTROIDS(MODEL `chicago-taxi-analytics.ml.driver_segments`)
ORDER BY centroid_id, feature_name;


-- Assign segments and label personas
SELECT
  taxi_id,
  CENTROID_ID AS cluster,
  CASE CENTROID_ID
    WHEN 1 THEN 'Airport Specialist'
    WHEN 2 THEN 'Peak Hour Hunter'
    WHEN 3 THEN 'Long Distance Driver'
    WHEN 4 THEN 'High-Volume City Driver'
    WHEN 5 THEN 'Weekend Warrior'
  END         AS driver_persona
FROM ML.PREDICT(
  MODEL `chicago-taxi-analytics.ml.driver_segments`,
  (SELECT taxi_id,
     AVG(trip_miles) avg_trip_distance, AVG(trip_minutes) avg_trip_duration,
     AVG(tip_rate_pct) avg_tip_rate,
     COUNTIF(is_weekend)/COUNT(*)*100 pct_weekend_trips,
     COUNTIF(time_of_day_bucket='evening_rush')/COUNT(*)*100 pct_peak_trips,
     COUNTIF(pickup_community_area IN (76,56))/COUNT(*)*100 pct_airport_trips,
     COUNT(*)/30.0 avg_daily_trips
   FROM `chicago-taxi-analytics.silver.taxi_trips`
   WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
   GROUP BY taxi_id HAVING COUNT(*) >= 30)
);
```

---

### Scenario 19 — Window Function Deep Dive: Trip Sequence Analytics

**Layer:** Silver | **Concept:** ROW_NUMBER, LEAD, LAG, FIRST_VALUE, deadhead detection

```sql
-- Analyze taxi trip sequences to detect deadhead trips (empty driving)
WITH trip_sequence AS (
  SELECT
    taxi_id,
    trip_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_miles,
    fare,
    pickup_community_area,
    dropoff_community_area,
    pickup_community_name,
    dropoff_community_name,
    -- Sequence number for this taxi
    ROW_NUMBER() OVER (
      PARTITION BY taxi_id ORDER BY trip_start_timestamp
    ) AS trip_seq,
    -- Next trip's pickup vs this trip's dropoff
    LEAD(pickup_community_area) OVER (
      PARTITION BY taxi_id ORDER BY trip_start_timestamp
    ) AS next_pickup_area,
    LEAD(trip_start_timestamp) OVER (
      PARTITION BY taxi_id ORDER BY trip_start_timestamp
    ) AS next_trip_start,
    -- Gap between trips
    TIMESTAMP_DIFF(
      LEAD(trip_start_timestamp) OVER (
        PARTITION BY taxi_id ORDER BY trip_start_timestamp
      ),
      trip_end_timestamp,
      MINUTE
    )                            AS gap_to_next_trip_min,
    -- First trip of the day
    FIRST_VALUE(pickup_community_name) OVER (
      PARTITION BY taxi_id, DATE(trip_start_timestamp)
      ORDER BY trip_start_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                            AS first_pickup_of_day
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date = CURRENT_DATE() - 1
    AND dq_score >= 70
)

SELECT
  taxi_id,
  trip_id,
  trip_seq,
  dropoff_community_name,
  next_pickup_area,
  gap_to_next_trip_min,
  ROUND(fare, 2) AS fare,
  -- Is next pickup in a different area? (requires repositioning)
  (dropoff_community_area != next_pickup_area)    AS repositioning_needed,
  -- Deadhead: long gap + repositioning = wasted time
  CASE
    WHEN gap_to_next_trip_min > 20
     AND dropoff_community_area != next_pickup_area THEN 'Likely Deadhead'
    WHEN gap_to_next_trip_min > 45                  THEN 'Long Wait'
    WHEN gap_to_next_trip_min <= 5                  THEN 'Quick Turnaround'
    ELSE 'Normal Gap'
  END                                             AS inter_trip_status
FROM trip_sequence
WHERE next_pickup_area IS NOT NULL
ORDER BY taxi_id, trip_seq;
```

---

### Scenario 20 — RECURSIVE CTE: Trip Chain Analysis

**Layer:** Silver | **Concept:** WITH RECURSIVE, chain traversal, taxi network hops

```sql
-- Trace a taxi's full day of consecutive trips as a chain
WITH RECURSIVE trip_chain AS (
  -- Anchor: first trip of the day per taxi
  SELECT
    taxi_id,
    trip_id,
    trip_start_timestamp,
    trip_end_timestamp,
    pickup_community_name,
    dropoff_community_name,
    fare,
    1 AS chain_position,
    CAST(trip_id AS STRING) AS chain_path,
    fare AS cumulative_fare
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date = CURRENT_DATE() - 1
    AND dq_score >= 70
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY taxi_id, DATE(trip_start_timestamp)
    ORDER BY trip_start_timestamp
  ) = 1

  UNION ALL

  SELECT
    next_trip.taxi_id,
    next_trip.trip_id,
    next_trip.trip_start_timestamp,
    next_trip.trip_end_timestamp,
    next_trip.pickup_community_name,
    next_trip.dropoff_community_name,
    next_trip.fare,
    chain.chain_position + 1,
    CONCAT(chain.chain_path, ' → ', next_trip.trip_id),
    chain.cumulative_fare + next_trip.fare
  FROM trip_chain chain
  JOIN `chicago-taxi-analytics.silver.taxi_trips` next_trip
    ON  chain.taxi_id = next_trip.taxi_id
    AND TIMESTAMP_DIFF(
          next_trip.trip_start_timestamp,
          chain.trip_end_timestamp, MINUTE
        ) BETWEEN 0 AND 120   -- next trip within 2 hours
    AND next_trip.trip_id != chain.trip_id
  WHERE chain.chain_position < 15  -- safety depth limit
)

SELECT
  taxi_id,
  chain_position,
  pickup_community_name,
  dropoff_community_name,
  ROUND(fare, 2)              AS trip_fare,
  ROUND(cumulative_fare, 2)   AS day_earnings_so_far,
  FORMAT_TIMESTAMP('%H:%M', trip_start_timestamp) AS trip_time
FROM trip_chain
ORDER BY taxi_id, chain_position;
```

---

### Scenario 21 — Materialized View: Real-Time Community KPIs

**Layer:** Gold | **Concept:** Materialized view query, sub-second BI

```sql
-- Query the materialized view (pre-computed, auto-refreshed every 30 min)
SELECT
  cm.pickup_community_area,
  cm.pickup_community_name,
  cm.trip_month,
  cm.pickups,
  cm.total_revenue,
  cm.avg_tip_pct,
  cm.unique_taxis,
  -- Rank communities by revenue this month
  RANK() OVER (
    PARTITION BY cm.trip_month
    ORDER BY cm.total_revenue DESC
  )                                               AS revenue_rank,
  -- MoM revenue change
  LAG(cm.total_revenue) OVER (
    PARTITION BY cm.pickup_community_area
    ORDER BY cm.trip_month
  )                                               AS prior_month_revenue,
  ROUND(
    SAFE_DIVIDE(
      cm.total_revenue - LAG(cm.total_revenue) OVER (
        PARTITION BY cm.pickup_community_area ORDER BY cm.trip_month
      ),
      LAG(cm.total_revenue) OVER (
        PARTITION BY cm.pickup_community_area ORDER BY cm.trip_month
      )
    ) * 100, 1
  )                                               AS mom_revenue_growth_pct
FROM `chicago-taxi-analytics.gold.mv_community_metrics` cm
WHERE cm.trip_month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH)
ORDER BY cm.trip_month DESC, cm.total_revenue DESC;
```

---

### Scenario 22 — ARRAY_AGG: Driver Trip Bag per Day

**Layer:** Silver | **Concept:** ARRAY_AGG with STRUCT, JSON output, driver daily summary

```sql
-- Create a per-driver daily summary with all trips as an array
SELECT
  taxi_id,
  trip_date,
  COUNT(*) AS trips_today,
  ROUND(SUM(trip_total), 2) AS revenue_today,
  -- Array of all trips for this driver today
  ARRAY_AGG(
    STRUCT(
      trip_id              AS id,
      FORMAT_TIMESTAMP('%H:%M', trip_start_timestamp) AS start_time,
      pickup_community_name  AS from_area,
      dropoff_community_name AS to_area,
      ROUND(fare, 2)         AS fare,
      ROUND(tips, 2)         AS tip,
      trip_miles             AS miles
    )
    ORDER BY trip_start_timestamp
    LIMIT 50
  )                          AS trip_log,
  -- Most frequent pickup area today
  APPROX_TOP_COUNT(pickup_community_name, 1)[OFFSET(0)].value AS favorite_pickup
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date = CURRENT_DATE() - 1
  AND dq_score >= 70
GROUP BY 1, 2
ORDER BY revenue_today DESC
LIMIT 100;
```

---

### Scenario 23 — APPROX_QUANTILES: Fare Distribution by Zone

**Layer:** Silver | **Concept:** APPROX_QUANTILES cost-optimized percentiles, distribution summary

```sql
-- Fare distribution across community areas (cost-optimized with APPROX)
SELECT
  pickup_community_name,
  COUNT(*)                                       AS trip_count,
  ROUND(AVG(fare), 2)                            AS avg_fare,
  -- APPROX_QUANTILES: 11 percentile points in one pass
  ROUND(APPROX_QUANTILES(fare, 10)[OFFSET(1)],  2) AS p10_fare,
  ROUND(APPROX_QUANTILES(fare, 10)[OFFSET(2)],  2) AS p20_fare,
  ROUND(APPROX_QUANTILES(fare, 10)[OFFSET(5)],  2) AS median_fare,
  ROUND(APPROX_QUANTILES(fare, 10)[OFFSET(8)],  2) AS p80_fare,
  ROUND(APPROX_QUANTILES(fare, 10)[OFFSET(9)],  2) AS p90_fare,
  ROUND(APPROX_QUANTILES(fare, 10)[OFFSET(10)], 2) AS p100_fare,
  -- Skewness proxy: if mean >> median, right-skewed (outlier high fares)
  ROUND(AVG(fare) - APPROX_QUANTILES(fare, 10)[OFFSET(5)], 2) AS mean_median_gap
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND fare > 0
  AND pickup_community_name IS NOT NULL
  AND dq_score >= 70
GROUP BY 1
HAVING COUNT(*) > 100
ORDER BY median_fare DESC;
```

---

### Scenario 24 — PIVOT + UNPIVOT: KPI Matrix Report

**Layer:** Gold | **Concept:** PIVOT wide report, UNPIVOT for charting

```sql
-- Monthly KPI matrix: rows = metric, columns = last 6 months
WITH monthly_kpis AS (
  SELECT
    FORMAT_DATE('%Y_%m', trip_date) AS month_key,
    ROUND(SUM(trip_total), 0)       AS total_revenue,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(tip_rate_pct), 2)     AS avg_tip_rate,
    ROUND(AVG(trip_miles), 2)       AS avg_trip_miles
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_TRUNC(
    DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH
  )
  GROUP BY 1
)

-- PIVOT: one column per month
SELECT *
FROM (
  SELECT month_key, 'total_revenue'  AS metric, CAST(total_revenue AS FLOAT64) AS value FROM monthly_kpis
  UNION ALL
  SELECT month_key, 'total_trips',    CAST(total_trips AS FLOAT64)    FROM monthly_kpis
  UNION ALL
  SELECT month_key, 'avg_tip_rate',   CAST(avg_tip_rate AS FLOAT64)   FROM monthly_kpis
  UNION ALL
  SELECT month_key, 'avg_trip_miles', CAST(avg_trip_miles AS FLOAT64) FROM monthly_kpis
)
PIVOT (
  MAX(value)
  FOR month_key IN (
    -- Last 6 months (adjust dates as needed)
    '2024_07', '2024_08', '2024_09', '2024_10', '2024_11', '2024_12'
  )
)
ORDER BY metric;
```

---

### Scenario 25 — TABLESAMPLE for Fast Exploration

**Layer:** Silver | **Concept:** TABLESAMPLE, cost reduction, statistical sampling

```sql
-- 1% sample for rapid exploratory data analysis
-- Processes ~0.7GB instead of ~70GB → 99% cost reduction

SELECT
  payment_type_std,
  time_of_day_bucket,
  weather_condition,
  APPROX_COUNT_DISTINCT(taxi_id) * 100    AS est_unique_taxis,
  COUNT(*) * 100                          AS est_trip_count,
  ROUND(AVG(fare), 2)                     AS sample_avg_fare,
  ROUND(AVG(tip_rate_pct), 2)             AS sample_avg_tip_pct,
  ROUND(STDDEV(fare), 2)                  AS sample_fare_std
FROM `chicago-taxi-analytics.silver.taxi_trips`
  TABLESAMPLE SYSTEM (1 PERCENT)
WHERE dq_score >= 70
  AND fare > 0
GROUP BY 1, 2, 3
ORDER BY est_trip_count DESC;
```


---

### ━━ ADVANCED ANALYTICS SCENARIOS (26–40) ━━

---

