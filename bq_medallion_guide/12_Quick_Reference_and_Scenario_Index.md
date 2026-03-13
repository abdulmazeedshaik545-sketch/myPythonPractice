<!-- Part of: BigQuery Medallion Architecture Guide | File: 12_Quick_Reference_and_Scenario_Index.md -->

## SECTION 11: QUICK REFERENCE — COMPLETE PROJECT STRUCTURE

```
chicago-taxi-analytics/
│
├── 📁 bronze/
│   ├── taxi_trips_raw          (partitioned: _ingestion_date, clustered: taxi_id, area)
│   ├── weather_raw             (partitioned: _ingestion_date, clustered: stn)
│   ├── community_areas_raw     (static reference)
│   └── trip_events_stream      (real-time Pub/Sub → BQ, partitioned: _processing_date)
│
├── 📁 silver/
│   ├── taxi_trips              (partitioned: trip_date, clustered: taxi_id, area, payment)
│   ├── weather_daily           (partitioned: weather_date)
│   └── dim_community_areas     (dimension table, small)
│
├── 📁 gold/
│   ├── fact_daily_trips        (partitioned: trip_date, clustered: taxi_id, area)
│   ├── dim_date                (date dimension, no partition needed)
│   ├── dim_taxi                (taxi dimension)
│   ├── mv_daily_kpis           (Materialized View, refreshes every 30 min)
│   └── mv_community_metrics    (Materialized View, refreshes every 60 min)
│
├── 📁 ml/
│   ├── daily_demand_forecast   (BQML ARIMA+ model)
│   ├── tip_predictor           (BQML logistic regression)
│   ├── driver_segments         (BQML K-Means)
│   └── driver_segments_scored  (scored output table)
│
├── 📁 streaming/
│   └── [managed by Pub/Sub subscription → bronze.trip_events_stream]
│
└── 📁 monitoring/
    ├── bronze_load_log         (ETL audit trail)
    └── pipeline_health         (cross-layer health metrics)
```

---

## SECTION 12: ALL 50 SCENARIOS — QUICK INDEX

| # | Scenario Name | Layer | Key Concepts |
|---|---------------|-------|--------------|
| 1 | Live Trip Volume Dashboard | Streaming+Silver | UNION ALL, TIMESTAMP_TRUNC, real-time |
| 2 | Hourly Heatmap | Silver | PIVOT, conditional aggregation |
| 3 | Revenue Per Mile by Company | Silver | DENSE_RANK, QUALIFY, ratios |
| 4 | Rolling Revenue + Anomaly | Silver | Rolling AVG/STDDEV, Z-score |
| 5 | Driver Productivity Score | Silver | Composite score, NTILE, PERCENT_RANK |
| 6 | Airport vs City Economics | Silver | CASE grouping, comparative |
| 7 | Weather Impact on Demand | Silver | JOIN dimension, weather bucketing |
| 8 | Driver Shift Sessionization | Silver | Sessionization, SUM OVER, TIMESTAMP_DIFF |
| 9 | Top Pickup Corridors | Silver | OD pairs, STRING concat, GROUP BY |
| 10 | Payment Type Migration | Silver | Monthly cohorts, SUM OVER for share |
| 11 | Monthly Revenue Cohort | Silver | First-trip cohort, GENERATE_DATE_ARRAY |
| 12 | Tip Prediction Features | Silver | Feature engineering, LAG features |
| 13 | Geospatial Hotspot | Silver | ST_GEOGPOINT, spatial binning |
| 14 | Fare Leakage Detection | Silver | IQR outlier, Z-score, APPROX_QUANTILES |
| 15 | YoY Seasonality | Gold | Multiple LAGs, seasonal index |
| 16 | BQML Demand Forecast | ML | ARIMA_PLUS, ML.FORECAST |
| 17 | BQML Tip Predictor | ML | Logistic regression, ML.PREDICT |
| 18 | BQML Driver Segments | ML | K-Means, ML.CENTROIDS |
| 19 | Trip Sequence Analytics | Silver | ROW_NUMBER, LEAD, FIRST_VALUE |
| 20 | RECURSIVE Trip Chain | Silver | WITH RECURSIVE, chain traversal |
| 21 | Materialized View KPIs | Gold | MV query, RANK, LAG |
| 22 | Driver Trip Bag Array | Silver | ARRAY_AGG with STRUCT, APPROX_TOP_COUNT |
| 23 | Fare Distribution | Silver | APPROX_QUANTILES, percentile distribution |
| 24 | PIVOT KPI Matrix | Gold | PIVOT, UNPIVOT, wide-format report |
| 25 | TABLESAMPLE Exploration | Silver | TABLESAMPLE SYSTEM, cost optimization |
| 26 | Streaming 5-Min Aggregates | Streaming | TIMESTAMP_TRUNC, streaming window |
| 27 | Streaming + Historical Hybrid | Mixed | UNION ALL, time-stitching |
| 28 | Community Heatmap by Hour×Day | Silver | Multi-dimensional aggregation |
| 29 | Fare Component Waterfall | Silver | UNPIVOT, component analysis |
| 30 | Data Lineage Audit | Monitoring | Cross-layer freshness check |
| 31 | MERGE Gold Incremental | Gold | MERGE upsert, idempotent ETL |
| 32 | JSON Streaming Parse | Streaming | JSON_VALUE, JSON_QUERY |
| 33 | Inter-Trip Wait Time | Silver | LAG/LEAD, gap analysis |
| 34 | Taxi Churn Detection | Silver | GENERATE_DATE_ARRAY, activity streaks |
| 35 | Route Summary STRING_AGG | Silver | STRING_AGG ordered, FIRST/LAST VALUE |
| 36 | Query Cost Analysis | Monitoring | INFORMATION_SCHEMA.JOBS |
| 37 | Wildcard Year Comparison | Bronze | Wildcard tables, _TABLE_SUFFIX |
| 38 | Exact SLA Percentiles | Silver | PERCENTILE_CONT named WINDOW |
| 39 | Real-Time Anomaly Alert | Streaming+Silver | Z-score on streaming, threshold alert |
| 40 | Forecast vs Actual | ML | ML.FORECAST, MAPE, CI validation |
| 41 | DQ Scorecard | Silver | DQ metrics, COUNTIF, quality trend |
| 42 | Partition Scan Efficiency | Silver | INFORMATION_SCHEMA.PARTITIONS, cost |
| 43 | Row-Level Security | Gold | Row Access Policies, governance |
| 44 | Scheduled Query Monitor | Pipeline | INFORMATION_SCHEMA.SCHEDULED_QUERIES |
| 45 | Cross-Layer Reconciliation | All | UNION ALL, row count reconciliation |
| 46 | Nearest Taxi to Hotspot | Streaming+Gold | ST_DISTANCE, CROSS JOIN, QUALIFY |
| 47 | Vertex AI Remote Model | ML | BQML remote model, Vertex AI |
| 48 | Table Clone for Experiments | Silver | TABLE CLONE, zero-cost sandbox |
| 49 | Looker Studio Data Source | Gold | MV + dim join, Looker-optimized |
| 50 | End-to-End Pipeline Health | Monitoring | UNION ALL health check, SLA |

---

## BIGQUERY CONCEPTS COVERAGE

| Concept | Scenarios |
|---------|-----------|
| Partition Pruning | 1–50 (all) |
| ROW_NUMBER / RANK / DENSE_RANK | 3, 5, 8, 18, 19, 21, 34, 39 |
| QUALIFY | 3, 8, 19, 20, 33, 38, 46 |
| LAG / LEAD | 4, 8, 10, 15, 19, 33, 38 |
| FIRST_VALUE / LAST_VALUE | 19, 22, 35 |
| Rolling Windows (ROWS/RANGE) | 4, 5, 8, 12, 34, 38 |
| Named WINDOW | 38 |
| GENERATE_DATE_ARRAY + CROSS JOIN | 11, 34 |
| COUNTIF / SUMIF | 6, 7, 12, 28, 41 |
| APPROX_COUNT_DISTINCT | 1, 25 |
| APPROX_QUANTILES | 14, 23 |
| APPROX_TOP_COUNT | 13, 22 |
| TABLESAMPLE | 25 |
| PERCENTILE_CONT | 5, 33, 38 |
| PIVOT | 2, 24 |
| UNPIVOT | 24, 29 |
| ARRAY_AGG + STRUCT | 22 |
| STRING_AGG | 35 |
| JSON_VALUE / JSON_QUERY | 32 |
| ST_ Geography Functions | 13, 46 |
| WITH RECURSIVE | 20 |
| MERGE (Upsert) | 31 |
| Materialized Views | 21, 49 |
| BQML (ARIMA+, logistic_reg, k_means) | 16, 17, 18, 40, 47 |
| Wildcard Tables | 37 |
| INFORMATION_SCHEMA | 36, 42, 44 |
| TABLE CLONE | 48 |
| Row Access Policies | 43 |
| Sessionization Pattern | 8 |
| Streaming (Pub/Sub → BQ) | 26, 27, 32, 39 |
| Cross-Layer Reconciliation | 45 |
| Date Spine (GENERATE_DATE_ARRAY) | 11, 34 |
| DML INSERT / MERGE | 31 |
| Cohort Analysis | 11, 15 |
| Z-Score Anomaly Detection | 4, 14, 39 |
| Correlated Subquery | 14 |

---

*End of document. Total: ~12,000 lines of SQL, Python, and bash scripts.*
*Project: chicago-taxi-analytics | Architecture: Medallion (Bronze/Silver/Gold)*
*Dataset: bigquery-public-data.chicago_taxi_trips (~74 GB source, ~1 TB total across layers)*
