<!-- Part of: BigQuery Medallion Architecture Guide | File: 00_README_and_Architecture_Overview.md -->

# BigQuery Medallion Architecture: End-to-End Analytics Platform
## 1 TB Public Dataset · 50 Real-Time Scenarios · Bronze → Silver → Gold

---

> **What This Guide Covers**
> - Which public dataset to use and why (~1 TB scale)
> - Step-by-step GCP project, IAM, and BigQuery setup
> - Medallion Architecture (Bronze / Silver / Gold) implementation
> - 50 real-time analytics scenarios with production-grade SQL
> - Streaming ingestion, Scheduled Queries, Looker Studio wiring
> - Cost optimization for every layer

---

## ARCHITECTURE OVERVIEW

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES (~1 TB)                            │
│   BigQuery Public Dataset: chicago_taxi_trips                       │
│   + Supplementary: thelook_ecommerce, noaa_gsod weather             │
└─────────────────────┬───────────────────────────────────────────────┘
                      │  bq load / CREATE TABLE AS SELECT
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER  (Raw / Landing Zone)                              │
│  project.bronze.*                                                   │
│  • Exact copy of source, no transformation                          │
│  • Partitioned by ingestion date                                     │
│  • Immutable — never updated, append-only                           │
│  • Stored as PARQUET-equivalent (BigQuery native)                   │
└─────────────────────┬───────────────────────────────────────────────┘
                      │  Scheduled Query / Dataform
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER  (Cleansed / Conformed)                            │
│  project.silver.*                                                   │
│  • Deduplicated, nulls handled, types cast                          │
│  • Business rules applied                                           │
│  • Partitioned + Clustered for performance                          │
│  • Data quality scores attached                                     │
└─────────────────────┬───────────────────────────────────────────────┘
                      │  Scheduled Query / Dataform
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER  (Business / Serving)                                │
│  project.gold.*                                                     │
│  • Aggregated, enriched, business-metric tables                     │
│  • Materialized Views for sub-second BI queries                     │
│  • Star schema (Facts + Dimensions)                                 │
│  • Directly powers dashboards, ML, APIs                             │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
          ┌───────────────────────┐
          │  Looker Studio / APIs  │
          │  BQML Models           │
          │  Real-Time Streaming   │
          └───────────────────────┘
```

---

