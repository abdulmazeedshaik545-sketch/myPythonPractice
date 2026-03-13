<!-- Part of: BigQuery Medallion Architecture Guide | File: 09_Scenarios_41_50_Operations_and_Governance.md -->

### Scenario 41 — Data Quality Scorecard (DQ Dashboard)

**Layer:** Silver + Monitoring | **Concept:** DQ metrics, trend monitoring, alert thresholds

```sql
-- Comprehensive data quality report for Silver layer
WITH daily_dq AS (
  SELECT
    trip_date,
    COUNT(*)                                    AS total_rows,
    AVG(dq_score)                               AS avg_dq_score,
    COUNTIF(dq_score = 100)                     AS perfect_rows,
    COUNTIF(dq_score >= 70 AND dq_score < 100)  AS acceptable_rows,
    COUNTIF(dq_score < 70)                      AS poor_quality_rows,
    COUNTIF(dq_flags LIKE '%NULL_FARE%')         AS null_fare_count,
    COUNTIF(dq_flags LIKE '%NULL_DURATION%')     AS null_duration_count,
    COUNTIF(dq_flags LIKE '%HIGH_SPEED%')        AS high_speed_count,
    COUNTIF(dq_flags LIKE '%UNKNOWN_TAXI%')      AS unknown_taxi_count,
    COUNTIF(dq_flags LIKE '%SHORT_TRIP%')        AS short_trip_count
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1
)

SELECT
  trip_date,
  total_rows,
  ROUND(avg_dq_score, 1)                               AS avg_quality_score,
  ROUND(perfect_rows / total_rows * 100, 1)            AS perfect_pct,
  ROUND((perfect_rows + acceptable_rows) / total_rows * 100, 1) AS pass_pct,
  ROUND(poor_quality_rows / total_rows * 100, 1)       AS fail_pct,
  null_fare_count,
  null_duration_count,
  high_speed_count,
  -- 7-day moving average of quality score
  ROUND(AVG(avg_dq_score) OVER (
    ORDER BY trip_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 1)                                                AS quality_7d_ma,
  -- Quality trend alert
  CASE
    WHEN avg_dq_score < 65 THEN '🔴 Quality Alert'
    WHEN avg_dq_score < 75 THEN '🟡 Quality Warning'
    ELSE                        '🟢 Quality OK'
  END                                                  AS quality_status
FROM daily_dq
ORDER BY trip_date DESC;
```

---

### Scenario 42 — Partitioned Table Scan Efficiency Test

**Layer:** Silver | **Concept:** Partition pruning validation, cost awareness

```sql
-- Compare: with vs. without partition filter
-- Query A: WITH partition filter (efficient — prunes partitions)
SELECT COUNT(*) AS trips_last_month
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
-- Bytes processed: ~1.8 GB (30/1095 = ~2.7% of table)

-- Query B: WITHOUT partition filter (scans entire table — DO NOT DO THIS)
-- SELECT COUNT(*) FROM `chicago-taxi-analytics.silver.taxi_trips`
-- Bytes processed: ~70 GB (100% of table)

-- Validate partition pruning works:
SELECT
  table_name,
  partition_id,
  row_count,
  ROUND(total_logical_bytes / POW(1024,3), 3) AS size_gb,
  last_modified_time
FROM `chicago-taxi-analytics.silver`.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'taxi_trips'
  AND partition_id >= FORMAT_DATE(
    '%Y%m%d',
    DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  )
ORDER BY partition_id DESC
LIMIT 35;
```

---

### Scenario 43 — Row-Level Security: Company-Scoped Access

**Layer:** Gold | **Concept:** Row Access Policies, data governance, column masking

```sql
-- Create row-level security so each company only sees their own taxis
-- (Run as BigQuery Admin)

CREATE ROW ACCESS POLICY taxi_company_filter
ON `chicago-taxi-analytics.gold.fact_daily_trips`
GRANT TO ("group:company-flashcab@yourorg.com")
FILTER USING (company_std = 'FLASH CAB');

CREATE ROW ACCESS POLICY taxi_company_filter_sun
ON `chicago-taxi-analytics.gold.fact_daily_trips`
GRANT TO ("group:company-suntaxi@yourorg.com")
FILTER USING (company_std = 'SUN TAXI');

-- Verify policies:
SELECT
  policy_name,
  grantee,
  filter_predicate
FROM `chicago-taxi-analytics.gold`.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
WHERE table_name = 'fact_daily_trips';
```

---

### Scenario 44 — Silver to Gold Scheduled Query Definition

**Layer:** Pipeline | **Concept:** Scheduled queries, automated orchestration, INFORMATION_SCHEMA jobs

```sql
-- Check scheduled query run history
SELECT
  display_name,
  state,
  next_run_time,
  schedule,
  destination_table,
  error_status
FROM `chicago-taxi-analytics`.INFORMATION_SCHEMA.SCHEDULED_QUERIES
ORDER BY next_run_time;

-- Manual trigger of Bronze→Silver ETL (when debugging)
-- Note: Typically done via Cloud Scheduler or Airflow
-- The scheduled query SQL is the Silver transform from Section 5.2
```

```bash
# Create scheduled query for Silver transform using bq CLI
bq query \
  --use_legacy_sql=false \
  --schedule='every 24 hours' \
  --display_name='Bronze_to_Silver_Daily_Transform' \
  --destination_table='chicago-taxi-analytics:silver.taxi_trips' \
  --append_table=true \
  "$(cat silver_transform.sql)"
```

---

### Scenario 45 — Cross-Layer Reconciliation Check

**Layer:** All layers | **Concept:** Data reconciliation, row count comparison, pipeline validation

```sql
-- Reconcile row counts and revenue across Bronze → Silver → Gold
WITH bronze_counts AS (
  SELECT
    _ingestion_date AS report_date,
    COUNT(*) AS bronze_rows,
    SUM(trip_total) AS bronze_revenue
  FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
  WHERE _ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY 1
),

silver_counts AS (
  SELECT
    trip_date AS report_date,
    COUNT(*) AS silver_rows,
    SUM(trip_total) AS silver_revenue
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY 1
),

gold_counts AS (
  SELECT
    trip_date AS report_date,
    SUM(trip_count) AS gold_rows,
    SUM(total_revenue) AS gold_revenue
  FROM `chicago-taxi-analytics.gold.fact_daily_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY 1
)

SELECT
  COALESCE(b.report_date, s.report_date, g.report_date) AS report_date,
  b.bronze_rows,
  s.silver_rows,
  g.gold_rows,
  -- Row reconciliation
  b.bronze_rows - s.silver_rows                         AS bronze_silver_diff,
  ROUND(s.silver_rows / NULLIF(b.bronze_rows,0) * 100, 1) AS silver_yield_pct,
  -- Revenue reconciliation
  ROUND(b.bronze_revenue, 2)  AS bronze_revenue,
  ROUND(s.silver_revenue, 2)  AS silver_revenue,
  ROUND(g.gold_revenue,   2)  AS gold_revenue,
  ROUND(ABS(s.silver_revenue - g.gold_revenue) /
        NULLIF(s.silver_revenue,0) * 100, 3) AS silver_gold_revenue_variance_pct,
  CASE
    WHEN s.silver_rows / NULLIF(b.bronze_rows,0) < 0.90 THEN '🔴 High Rejection Rate'
    WHEN s.silver_rows / NULLIF(b.bronze_rows,0) < 0.95 THEN '🟡 Elevated Rejection'
    ELSE '🟢 Reconciled'
  END AS reconciliation_status
FROM bronze_counts b
FULL OUTER JOIN silver_counts s USING (report_date)
FULL OUTER JOIN gold_counts   g USING (report_date)
ORDER BY report_date DESC;
```

---

### Scenario 46 — Geospatial: Nearest Taxi to Demand Hotspot

**Layer:** Streaming + Gold | **Concept:** ST_DISTANCE, ST_GEOGPOINT, nearest-neighbor query

```sql
-- Find the 5 nearest active taxis to each high-demand hotspot
WITH demand_hotspots AS (
  -- Top 10 pickup hotspots right now from Gold
  SELECT
    pickup_community_area,
    pickup_community_name,
    ST_GEOGPOINT(
      AVG(dropoff_longitude),  -- approximate centroid
      AVG(dropoff_latitude)
    ) AS hotspot_location,
    COUNT(*) AS current_demand
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date = CURRENT_DATE()
    AND trip_hour = EXTRACT(HOUR FROM CURRENT_TIMESTAMP())
  GROUP BY 1, 2
  ORDER BY current_demand DESC
  LIMIT 10
),

active_taxis AS (
  -- Last known position of each taxi from streaming
  SELECT DISTINCT
    taxi_id,
    ST_GEOGPOINT(longitude, latitude) AS taxi_location,
    event_timestamp
  FROM `chicago-taxi-analytics.bronze.trip_events_stream`
  WHERE event_type = 'trip_ended'
    AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY taxi_id ORDER BY event_timestamp DESC) = 1
)

SELECT
  h.pickup_community_name AS hotspot,
  h.current_demand,
  t.taxi_id,
  ROUND(ST_DISTANCE(h.hotspot_location, t.taxi_location) / 1000, 2) AS distance_km,
  FORMAT_TIMESTAMP('%H:%M:%S', t.event_timestamp) AS last_seen,
  RANK() OVER (
    PARTITION BY h.pickup_community_area
    ORDER BY ST_DISTANCE(h.hotspot_location, t.taxi_location)
  ) AS proximity_rank
FROM demand_hotspots h
CROSS JOIN active_taxis t
QUALIFY RANK() OVER (
  PARTITION BY h.pickup_community_area
  ORDER BY ST_DISTANCE(h.hotspot_location, t.taxi_location)
) <= 5
ORDER BY h.current_demand DESC, proximity_rank;
```

---

### Scenario 47 — BQML: Remote Model Scoring via Vertex AI

**Layer:** ML | **Concept:** BQML remote model, BigQuery × Vertex AI integration

```sql
-- Create connection to Vertex AI (run once as admin)
-- CREATE EXTERNAL CONNECTION `us.vertex-ai-conn`
-- OPTIONS (remote_service_type = 'CLOUD_AI_NATURAL_LANGUAGE_V1')

-- Create remote model pointing to Vertex AI endpoint
CREATE OR REPLACE MODEL `chicago-taxi-analytics.ml.fare_predictor_vertex`
OPTIONS (
  model_type    = 'tensorflow',
  model_path    = 'gs://chicago-taxi-analytics-models/fare_model/*'
  -- Deploys a TF SavedModel from GCS for BigQuery inference
)
-- Then score:
;
SELECT
  trip_id,
  actual_fare AS fare,
  ML.PREDICT(MODEL `chicago-taxi-analytics.ml.fare_predictor_vertex`,
    STRUCT(trip_miles, trip_minutes, pickup_community_area,
           dropoff_community_area, trip_hour)
  ).predicted_fare AS ml_predicted_fare
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date = CURRENT_DATE() - 1
LIMIT 1000;
```

---

### Scenario 48 — Table Clone for Experiment Isolation

**Layer:** Silver | **Concept:** BigQuery TABLE CLONE, zero-cost copy, experiment sandbox

```sql
-- Create a zero-storage-cost clone of Silver for data science experiments
-- Clones share storage with source — only changed rows consume new storage

CREATE TABLE `chicago-taxi-analytics.silver.taxi_trips_experiment_clone`
CLONE `chicago-taxi-analytics.silver.taxi_trips`
OPTIONS (
  description = 'Experiment clone: data science sandbox for trip model testing',
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
);

-- Verify clone
SELECT
  table_name,
  clone_definition.base_table_reference.table_id AS cloned_from,
  ROUND(total_logical_bytes / POW(1024,3), 2) AS clone_size_gb
FROM `chicago-taxi-analytics.silver`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE table_name = 'taxi_trips_experiment_clone';
```

---

### Scenario 49 — Looker Studio Integration Query

**Layer:** Gold | **Concept:** Looker Studio-optimized query, pre-computed metrics, cache-friendly

```sql
-- This query is designed to be saved as a BigQuery Data Source in Looker Studio
-- Optimized: hits Materialized View, adds calculated fields, minimal bytes

SELECT
  -- Date dimensions
  kpi.trip_date,
  d.day_name,
  d.month_name,
  d.year,
  d.quarter,
  d.is_weekend,
  d.is_holiday,

  -- KPI metrics (from Materialized View — sub-second response)
  kpi.total_trips,
  kpi.active_taxis,
  kpi.total_revenue,
  kpi.avg_revenue_per_trip,
  kpi.avg_tip_pct,
  kpi.total_miles,
  kpi.card_payments,
  kpi.cash_payments,

  -- Calculated fields for Looker Studio
  ROUND(kpi.card_payments / NULLIF(kpi.total_trips, 0) * 100, 1) AS card_payment_pct,
  ROUND(kpi.total_revenue / NULLIF(kpi.total_miles, 0), 2)        AS revenue_per_mile,
  ROUND(kpi.total_trips / NULLIF(kpi.active_taxis, 0), 1)         AS trips_per_taxi,

  -- Prior period for Looker Studio comparison
  LAG(kpi.total_revenue, 7) OVER (ORDER BY kpi.trip_date) AS revenue_7d_ago,
  LAG(kpi.total_trips,   7) OVER (ORDER BY kpi.trip_date) AS trips_7d_ago

FROM `chicago-taxi-analytics.gold.mv_daily_kpis` kpi
JOIN `chicago-taxi-analytics.gold.dim_date` d ON kpi.trip_date = d.date_key
WHERE kpi.trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
ORDER BY kpi.trip_date DESC;
```

---

### Scenario 50 — End-to-End Pipeline Health Monitor

**Layer:** Monitoring | **Concept:** UNION ALL, cross-system health check, SLA reporting

```sql
-- Single query that monitors the entire medallion pipeline health
WITH pipeline_checks AS (
  -- Bronze freshness
  SELECT
    'Bronze Ingestion'          AS check_name,
    MAX(_ingestion_date)        AS latest_date,
    COUNT(*)                    AS rows_today,
    DATE_DIFF(CURRENT_DATE(), MAX(_ingestion_date), DAY) AS days_lag
  FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
  WHERE _ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)

  UNION ALL

  -- Silver freshness
  SELECT
    'Silver Transform',
    MAX(trip_date),
    COUNT(*),
    DATE_DIFF(CURRENT_DATE(), MAX(trip_date), DAY)
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND _silver_load_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)

  UNION ALL

  -- Gold freshness
  SELECT
    'Gold Aggregation',
    MAX(trip_date),
    SUM(trip_count),
    DATE_DIFF(CURRENT_DATE(), MAX(trip_date), DAY)
  FROM `chicago-taxi-analytics.gold.fact_daily_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
),

streaming_check AS (
  SELECT
    'Streaming (last 5 min)' AS check_name,
    DATE(MAX(event_timestamp)) AS latest_date,
    COUNT(*) AS rows_today,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(event_timestamp), MINUTE) AS days_lag
  FROM `chicago-taxi-analytics.bronze.trip_events_stream`
  WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
),

storage_check AS (
  SELECT
    CONCAT('Storage: ', table_schema, '.', table_name) AS check_name,
    DATE(last_modified_time) AS latest_date,
    row_count AS rows_today,
    DATE_DIFF(CURRENT_DATE(), DATE(last_modified_time), DAY) AS days_lag
  FROM `chicago-taxi-analytics`.INFORMATION_SCHEMA.TABLE_STORAGE
  WHERE table_schema IN ('bronze','silver','gold')
    AND table_name IN ('taxi_trips_raw','taxi_trips','fact_daily_trips')
)

SELECT
  check_name,
  latest_date,
  FORMAT("%'d", rows_today) AS rows_formatted,
  days_lag,
  CASE
    WHEN days_lag = 0  THEN '🟢 Healthy'
    WHEN days_lag <= 1 THEN '🟡 Warning: 1 day lag'
    WHEN days_lag <= 3 THEN '🟠 Alert: Multi-day lag'
    ELSE                    '🔴 CRITICAL: Pipeline down'
  END AS health_status,
  CURRENT_TIMESTAMP() AS checked_at
FROM (
  SELECT check_name, latest_date, rows_today, CAST(days_lag AS INT64) AS days_lag
  FROM pipeline_checks
  UNION ALL
  SELECT check_name, latest_date, rows_today, CAST(days_lag AS INT64)
  FROM streaming_check
)
ORDER BY
  CASE health_status
    WHEN '🔴 CRITICAL: Pipeline down' THEN 1
    WHEN '🟠 Alert: Multi-day lag'    THEN 2
    WHEN '🟡 Warning: 1 day lag'      THEN 3
    ELSE 4
  END;
```


---

