<!-- Part of: BigQuery Medallion Architecture Guide | File: 11_Looker_Studio_and_Cost_Management.md -->

## SECTION 9: LOOKER STUDIO DASHBOARD SETUP

### Step 9.1 — Connect BigQuery to Looker Studio

```
1. Go to: https://lookerstudio.google.com
2. Click "Create" → "Data Source"
3. Select "BigQuery" connector
4. Choose:
   Project: chicago-taxi-analytics
   Dataset: gold
   Table: mv_daily_kpis  (for KPI dashboard)
   OR use "Custom Query" to paste Scenario 49 query

5. Set service account: bq-bi-reader@chicago-taxi-analytics.iam.gserviceaccount.com
6. Click "Connect"
```

### Step 9.2 — Recommended Dashboard Pages

```
PAGE 1: Executive KPIs
├── Scorecards: Total Trips, Revenue, Avg Fare, Active Taxis (Today)
├── Time Series: Daily Revenue (last 90 days) with 7-day MA
├── Bar Chart: Revenue by Time of Day Bucket
└── Geo Map: Pickup community area heat map

PAGE 2: Company Performance
├── Table: Company ranked by revenue/mile
├── Bar: Trip volume by company (last 30 days)
└── Scatter: Avg fare vs. avg tip rate by company

PAGE 3: Weather Impact
├── Bar: Trip count by weather condition
├── Scatter: Temperature vs. trip volume (daily)
└── Table: Revenue by temp category × weather condition

PAGE 4: Real-Time Operations (refreshes every 15 min)
├── Gauge: Trips in last hour vs. last week same hour
├── Table: Top 10 active taxis right now
├── Map: Current trip density by community area
└── Time Series: Last 24 hours trip volume

PAGE 5: Data Quality
├── Line: 30-day DQ score trend
├── Stacked Bar: Quality tier distribution by day
└── Table: Top DQ issues by frequency
```

### Step 9.3 — Looker Studio Calculated Fields

```
# Add these as Calculated Fields in Looker Studio:

Revenue per Taxi = total_revenue / active_taxis
Revenue WoW % = (total_revenue - revenue_7d_ago) / revenue_7d_ago * 100
Trips WoW % = (total_trips - trips_7d_ago) / trips_7d_ago * 100
Card % = card_payments / total_trips * 100
Revenue per Mile = total_revenue / total_miles
```

---

## SECTION 10: COST MANAGEMENT

### Step 10.1 — BigQuery Budgets and Alerts

```bash
# Set up budget alert for BigQuery costs
gcloud billing budgets create \
  --billing-account=YOUR_BILLING_ACCOUNT_ID \
  --display-name="Chicago Taxi BigQuery Budget" \
  --budget-amount=100USD \
  --threshold-rule=percent=50,basis=CURRENT_SPEND \
  --threshold-rule=percent=75,basis=CURRENT_SPEND \
  --threshold-rule=percent=90,basis=CURRENT_SPEND \
  --filter-services=services/95FF-2EF5-5EA1  # BigQuery service ID
```

### Step 10.2 — Cost Optimization Checklists

```sql
-- ✅ CHECKLIST: Before running any query on Silver/Bronze tables

-- 1. Does your WHERE clause include trip_date or _ingestion_date?
--    → Required to trigger partition pruning

-- 2. Can you use a Materialized View instead of Silver directly?
--    → gold.mv_daily_kpis and gold.mv_community_metrics are FREE to query

-- 3. Is this an exploratory query? Use TABLESAMPLE first:
--    FROM silver.taxi_trips TABLESAMPLE SYSTEM (1 PERCENT)
--    → 99% cost reduction, ~99% statistical accuracy

-- 4. Can you use APPROX functions?
--    APPROX_COUNT_DISTINCT instead of COUNT(DISTINCT)
--    APPROX_QUANTILES instead of PERCENTILE_CONT
--    → Same accuracy for dashboards, 10–100× cheaper

-- 5. Query cost estimation before running (LIMIT 0 trick):
SELECT * FROM `chicago-taxi-analytics.silver.taxi_trips`
WHERE trip_date = CURRENT_DATE() - 1
LIMIT 0;
-- BigQuery shows bytes to be scanned WITHOUT running the query
```

---

