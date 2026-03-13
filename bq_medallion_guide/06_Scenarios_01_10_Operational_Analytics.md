<!-- Part of: BigQuery Medallion Architecture Guide | File: 06_Scenarios_01_10_Operational_Analytics.md -->

## SECTION 7: 50 REAL-TIME ANALYTICS SCENARIOS

> **How to run these**: All queries target Silver or Gold layer tables.
> For real-time queries, use `streaming.trip_events_stream` + Silver together.

---

### ━━ OPERATIONAL ANALYTICS (Scenarios 1–10) ━━

---

### Scenario 1 — Live Trip Volume Dashboard (Real-Time)

**Layer:** Streaming + Silver | **Concept:** UNION ALL, TIMESTAMP_TRUNC, streaming aggregation

**Business Question:** How many trips are happening RIGHT NOW vs. the same time last week?

```sql
-- Real-time trips in last 60 minutes from streaming table
WITH live_trips AS (
  SELECT
    TIMESTAMP_TRUNC(event_timestamp, MINUTE)    AS minute_bucket,
    COUNT(DISTINCT trip_id)                     AS live_trip_count,
    COUNT(DISTINCT taxi_id)                     AS active_taxis,
    SUM(meter_amount)                           AS live_revenue
  FROM `chicago-taxi-analytics.bronze.trip_events_stream`
  WHERE event_type = 'trip_ended'
    AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
  GROUP BY 1
),

-- Same window last week (from Silver for comparison)
last_week AS (
  SELECT
    TIMESTAMP_TRUNC(
      TIMESTAMP_ADD(trip_start_timestamp, INTERVAL 7 DAY), MINUTE
    )                                           AS minute_bucket,
    COUNT(*)                                    AS lw_trip_count
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_start_timestamp >= TIMESTAMP_SUB(
      TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY),
      INTERVAL 60 MINUTE
    )
    AND trip_start_timestamp <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY 1
)

SELECT
  l.minute_bucket,
  COALESCE(l.live_trip_count, 0)                AS current_trips,
  COALESCE(l.active_taxis, 0)                   AS active_taxis,
  ROUND(COALESCE(l.live_revenue, 0), 2)         AS live_revenue,
  COALESCE(lw.lw_trip_count, 0)                 AS last_week_trips,
  ROUND(
    (COALESCE(l.live_trip_count, 0) - COALESCE(lw.lw_trip_count, 0))
    / NULLIF(COALESCE(lw.lw_trip_count, 0), 0) * 100,
  1)                                            AS wow_change_pct
FROM live_trips l
FULL OUTER JOIN last_week lw USING (minute_bucket)
ORDER BY minute_bucket DESC;
```

---

### Scenario 2 — Hourly Heatmap: Peak Hours by Day of Week

**Layer:** Silver | **Concept:** PIVOT, conditional aggregation, heatmap matrix

**Business Question:** At which hour × day combination is demand highest?

```sql
-- Build a 7×24 demand heatmap matrix
WITH hourly_demand AS (
  SELECT
    trip_day_of_week,
    trip_hour,
    COUNT(*)                AS trip_count,
    ROUND(AVG(trip_total), 2) AS avg_revenue
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND dq_score >= 70
  GROUP BY 1, 2
)

-- PIVOT: hours as columns, days as rows
SELECT *
FROM hourly_demand
PIVOT (
  SUM(trip_count)
  FOR trip_hour IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
)
ORDER BY trip_day_of_week;
```

---

### Scenario 3 — Revenue Per Mile by Company (Ranked)

**Layer:** Silver + Gold | **Concept:** DENSE_RANK, ratio metrics, QUALIFY

**Business Question:** Which taxi companies generate the most revenue per mile driven?

```sql
SELECT
  company_std,
  COUNT(*)                                          AS total_trips,
  ROUND(SUM(trip_total), 2)                         AS total_revenue,
  ROUND(SUM(trip_miles), 1)                         AS total_miles,
  ROUND(SUM(trip_total) / NULLIF(SUM(trip_miles),0), 2) AS revenue_per_mile,
  ROUND(AVG(tip_rate_pct), 2)                       AS avg_tip_rate_pct,
  ROUND(AVG(avg_speed_mph), 1)                      AS avg_speed_mph,
  -- DENSE_RANK: companies with same revenue/mile get same rank
  DENSE_RANK() OVER (ORDER BY SUM(trip_total) / NULLIF(SUM(trip_miles),0) DESC)
                                                    AS revenue_per_mile_rank
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND trip_miles > 0
  AND company_std != 'UNKNOWN'
GROUP BY 1
HAVING COUNT(*) > 1000   -- only companies with meaningful volume
ORDER BY revenue_per_mile DESC;
```

---

### Scenario 4 — Rolling 7-Day Revenue Trend with Anomaly Detection

**Layer:** Silver | **Concept:** Rolling window AVG + STDDEV, Z-score anomaly flagging

**Business Question:** Which days had statistically anomalous revenue (potential outages or events)?

```sql
WITH daily_revenue AS (
  SELECT
    trip_date,
    SUM(trip_total)           AS daily_revenue,
    COUNT(*)                  AS trip_count
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND dq_score >= 50
  GROUP BY 1
),

stats AS (
  SELECT
    *,
    AVG(daily_revenue) OVER (
      ORDER BY trip_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                       AS revenue_7d_ma,
    STDDEV(daily_revenue) OVER (
      ORDER BY trip_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                       AS revenue_7d_std,
    LAG(daily_revenue, 7) OVER (ORDER BY trip_date) AS revenue_wow,
    LAG(daily_revenue, 364) OVER (ORDER BY trip_date) AS revenue_yoy
  FROM daily_revenue
)

SELECT
  trip_date,
  ROUND(daily_revenue, 2)                           AS daily_revenue,
  trip_count,
  ROUND(revenue_7d_ma, 2)                           AS revenue_7d_ma,
  -- Z-score: how many std-devs from rolling mean
  ROUND(
    SAFE_DIVIDE(daily_revenue - revenue_7d_ma, revenue_7d_std),
  2)                                                AS z_score,
  ROUND(SAFE_DIVIDE(daily_revenue - revenue_wow, revenue_wow) * 100, 1) AS wow_pct,
  ROUND(SAFE_DIVIDE(daily_revenue - revenue_yoy, revenue_yoy) * 100, 1) AS yoy_pct,
  CASE
    WHEN SAFE_DIVIDE(daily_revenue - revenue_7d_ma, revenue_7d_std) >  2.5  THEN '📈 Spike'
    WHEN SAFE_DIVIDE(daily_revenue - revenue_7d_ma, revenue_7d_std) < -2.5  THEN '📉 Dip'
    ELSE '✅ Normal'
  END                                               AS anomaly_flag
FROM stats
ORDER BY trip_date DESC;
```

---

### Scenario 5 — Driver Productivity Score (Top/Bottom Performers)

**Layer:** Silver | **Concept:** Composite scoring, NTILE, PERCENTILE_CONT

**Business Question:** Which taxis are underperforming based on trips/hour, revenue/trip, and tip rate?

```sql
WITH taxi_metrics AS (
  SELECT
    taxi_id,
    company_std,
    COUNT(*)                                          AS total_trips,
    SUM(trip_total)                                   AS total_revenue,
    SUM(trip_minutes)                                 AS total_drive_minutes,
    AVG(trip_total)                                   AS avg_revenue_per_trip,
    AVG(tip_rate_pct)                                 AS avg_tip_rate,
    SUM(trip_miles)                                   AS total_miles,
    COUNTIF(is_tipped)                                AS tipped_trips,
    SAFE_DIVIDE(COUNT(*), SUM(trip_minutes) / 60.0)  AS trips_per_hour
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND dq_score >= 70
  GROUP BY 1, 2
  HAVING COUNT(*) >= 50   -- minimum activity threshold
),

scored AS (
  SELECT
    *,
    -- Normalize each metric to 0-100 percentile score
    PERCENT_RANK() OVER (ORDER BY avg_revenue_per_trip) * 100 AS revenue_pct,
    PERCENT_RANK() OVER (ORDER BY trips_per_hour)        * 100 AS efficiency_pct,
    PERCENT_RANK() OVER (ORDER BY avg_tip_rate)          * 100 AS tip_pct,
    NTILE(5) OVER (ORDER BY total_revenue DESC)               AS revenue_quintile
  FROM taxi_metrics
)

SELECT
  taxi_id,
  company_std,
  total_trips,
  ROUND(total_revenue, 2)             AS total_revenue,
  ROUND(avg_revenue_per_trip, 2)      AS avg_rev_per_trip,
  ROUND(trips_per_hour, 2)            AS trips_per_hour,
  ROUND(avg_tip_rate, 1)              AS avg_tip_rate_pct,
  -- Composite performance score (weighted average of percentiles)
  ROUND(
    revenue_pct * 0.40 +
    efficiency_pct * 0.35 +
    tip_pct * 0.25,
  1)                                  AS performance_score,
  revenue_quintile,
  CASE revenue_quintile
    WHEN 1 THEN 'Top Performer'
    WHEN 2 THEN 'Above Average'
    WHEN 3 THEN 'Average'
    WHEN 4 THEN 'Below Average'
    WHEN 5 THEN 'Low Performer'
  END                                 AS performance_tier
FROM scored
ORDER BY performance_score DESC;
```

---

### Scenario 6 — Airport vs Non-Airport Trip Economics

**Layer:** Silver | **Concept:** CASE grouping, comparative analytics, ratio analysis

**Business Question:** How do airport trips (O'Hare/Midway) compare to city trips in revenue and tipping?

```sql
SELECT
  CASE
    WHEN pickup_community_area IN (76, 56)   THEN 'Airport Pickup'
    WHEN dropoff_community_area IN (76, 56)  THEN 'Airport Dropoff'
    ELSE                                          'City Trip'
  END                                             AS trip_category,
  payment_type_std,
  COUNT(*)                                        AS trip_count,
  ROUND(AVG(fare), 2)                             AS avg_fare,
  ROUND(AVG(tips), 2)                             AS avg_tips,
  ROUND(AVG(trip_total), 2)                       AS avg_total,
  ROUND(AVG(tip_rate_pct), 2)                     AS avg_tip_pct,
  ROUND(AVG(trip_miles), 2)                       AS avg_miles,
  ROUND(AVG(trip_minutes), 2)                     AS avg_minutes,
  ROUND(AVG(trip_total / NULLIF(trip_minutes,0)), 3) AS revenue_per_minute,
  COUNTIF(is_tipped) * 100.0 / COUNT(*)           AS tipped_pct
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND dq_score >= 70
  AND trip_total > 0
GROUP BY 1, 2
ORDER BY trip_category, trip_count DESC;
```

---

### Scenario 7 — Weather Impact on Trip Demand

**Layer:** Silver | **Concept:** JOIN with dimension, conditional aggregation, weather bucketing

**Business Question:** How does weather (rain, snow, temperature) affect trip volume and revenue?

```sql
SELECT
  w.weather_condition,
  w.temp_category,
  COUNT(t.trip_id)                                  AS trip_count,
  ROUND(AVG(t.trip_total), 2)                       AS avg_revenue,
  ROUND(AVG(t.trip_miles), 2)                       AS avg_miles,
  ROUND(AVG(t.tip_rate_pct), 2)                     AS avg_tip_pct,
  ROUND(COUNT(t.trip_id) / COUNT(DISTINCT t.trip_date), 0) AS avg_daily_trips,
  -- Surge pricing proxy: higher avg fare in bad weather?
  ROUND(AVG(t.fare) / NULLIF(AVG(t.trip_miles), 0), 2) AS fare_per_mile
FROM `chicago-taxi-analytics.silver.taxi_trips` t
JOIN `chicago-taxi-analytics.silver.weather_daily` w
  ON t.trip_date = w.weather_date
  AND w.station_id = '725300'
WHERE t.trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND t.dq_score >= 70
GROUP BY 1, 2
ORDER BY trip_count DESC;
```

---

### Scenario 8 — Sessionization: Driver Shift Analysis

**Layer:** Silver | **Concept:** Sessionization pattern, SUM OVER, TIMESTAMP_DIFF, shift detection

**Business Question:** How long is a typical driver shift? How many trips per shift?

```sql
WITH trip_gaps AS (
  SELECT
    taxi_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_total,
    trip_miles,
    LAG(trip_end_timestamp) OVER (
      PARTITION BY taxi_id
      ORDER BY trip_start_timestamp
    )                                               AS prev_trip_end,
    TIMESTAMP_DIFF(
      trip_start_timestamp,
      LAG(trip_end_timestamp) OVER (
        PARTITION BY taxi_id ORDER BY trip_start_timestamp
      ),
      MINUTE
    )                                               AS gap_after_prev_trip_min
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND dq_score >= 70
),

shifts AS (
  SELECT
    *,
    SUM(
      IF(gap_after_prev_trip_min IS NULL OR gap_after_prev_trip_min > 90, 1, 0)
    ) OVER (
      PARTITION BY taxi_id
      ORDER BY trip_start_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                               AS shift_id
    -- Gap > 90 minutes = new shift
  FROM trip_gaps
),

shift_summary AS (
  SELECT
    taxi_id,
    shift_id,
    MIN(trip_start_timestamp)                       AS shift_start,
    MAX(trip_end_timestamp)                         AS shift_end,
    TIMESTAMP_DIFF(
      MAX(trip_end_timestamp),
      MIN(trip_start_timestamp), MINUTE
    ) / 60.0                                        AS shift_hours,
    COUNT(*)                                        AS trips_in_shift,
    SUM(trip_total)                                 AS shift_revenue,
    SUM(trip_miles)                                 AS shift_miles
  FROM shifts
  GROUP BY 1, 2
  HAVING COUNT(*) >= 3 AND shift_hours > 0
)

SELECT
  ROUND(AVG(shift_hours), 2)                        AS avg_shift_hours,
  ROUND(PERCENTILE_CONT(shift_hours, 0.50) OVER(), 2) AS median_shift_hours,
  ROUND(AVG(trips_in_shift), 1)                     AS avg_trips_per_shift,
  ROUND(AVG(shift_revenue), 2)                      AS avg_shift_revenue,
  ROUND(AVG(shift_revenue / NULLIF(shift_hours,0)), 2) AS avg_revenue_per_hour,
  ROUND(AVG(shift_miles), 1)                        AS avg_miles_per_shift,
  COUNT(DISTINCT taxi_id)                           AS unique_drivers_sampled,
  COUNT(*)                                          AS total_shifts_analyzed
FROM shift_summary
QUALIFY ROW_NUMBER() OVER (ORDER BY avg_revenue_per_hour) = 1;
```

---

### Scenario 9 — Top Pickup Corridors (Origin–Destination Analysis)

**Layer:** Silver | **Concept:** STRING concatenation for OD pairs, self-join avoided, GROUP BY pairs

**Business Question:** What are the 20 most popular trip corridors (pickup → dropoff community)?

```sql
SELECT
  pickup_community_name                           AS from_area,
  dropoff_community_name                          AS to_area,
  CONCAT(
    COALESCE(pickup_community_name, 'Unknown'), ' → ',
    COALESCE(dropoff_community_name, 'Unknown')
  )                                               AS corridor,
  COUNT(*)                                        AS trip_count,
  ROUND(AVG(trip_total), 2)                       AS avg_fare,
  ROUND(AVG(trip_minutes), 1)                     AS avg_duration_min,
  ROUND(AVG(tip_rate_pct), 1)                     AS avg_tip_pct,
  ROUND(SUM(trip_total), 2)                       AS total_revenue,
  -- % of total trips this corridor represents
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 2) AS pct_of_all_trips
FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  AND pickup_community_area > 0
  AND dropoff_community_area > 0
  AND dq_score >= 70
GROUP BY 1, 2, 3
ORDER BY trip_count DESC
LIMIT 20;
```

---

### Scenario 10 — Payment Type Migration Over Time

**Layer:** Silver | **Concept:** DATE_TRUNC monthly cohorts, SUM OVER total for share %, trend analysis

**Business Question:** Is digital (card/mobile) payment adoption growing over time?

```sql
WITH monthly_payments AS (
  SELECT
    DATE_TRUNC(trip_date, MONTH)                  AS trip_month,
    payment_type_std,
    COUNT(*)                                      AS trip_count,
    SUM(trip_total)                               AS revenue
  FROM `chicago-taxi-analytics.silver.taxi_trips`
  WHERE trip_date >= DATE '2019-01-01'
    AND dq_score >= 50
  GROUP BY 1, 2
),

with_totals AS (
  SELECT
    *,
    SUM(trip_count) OVER (PARTITION BY trip_month) AS month_total_trips
  FROM monthly_payments
)

SELECT
  trip_month,
  payment_type_std,
  trip_count,
  ROUND(trip_count / month_total_trips * 100, 1)  AS payment_share_pct,
  ROUND(revenue, 2)                               AS revenue,
  -- MoM change in share
  ROUND(
    trip_count / month_total_trips -
    LAG(trip_count / month_total_trips) OVER (
      PARTITION BY payment_type_std ORDER BY trip_month
    ),
  3) * 100                                        AS share_mom_change_ppts
FROM with_totals
ORDER BY trip_month DESC, payment_share_pct DESC;
```

---

### ━━ BUSINESS INTELLIGENCE SCENARIOS (11–25) ━━

