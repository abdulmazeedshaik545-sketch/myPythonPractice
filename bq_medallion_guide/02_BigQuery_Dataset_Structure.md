<!-- Part of: BigQuery Medallion Architecture Guide | File: 02_BigQuery_Dataset_Structure.md -->

## SECTION 3: BIGQUERY DATASET STRUCTURE

### Step 3.1 — Create Medallion Datasets

```bash
PROJECT_ID="chicago-taxi-analytics"
REGION="US"

# ── Bronze Layer ──
bq mk \
  --dataset \
  --location=$REGION \
  --description="Bronze layer: raw ingested data, immutable" \
  --default_table_expiration=0 \
  ${PROJECT_ID}:bronze

# ── Silver Layer ──
bq mk \
  --dataset \
  --location=$REGION \
  --description="Silver layer: cleansed, deduplicated, typed" \
  --default_table_expiration=0 \
  ${PROJECT_ID}:silver

# ── Gold Layer ──
bq mk \
  --dataset \
  --location=$REGION \
  --description="Gold layer: business metrics, aggregated, serving" \
  --default_table_expiration=0 \
  ${PROJECT_ID}:gold

# ── Streaming Layer ──
bq mk \
  --dataset \
  --location=$REGION \
  --description="Streaming: real-time trip events via Pub/Sub" \
  --default_table_expiration=0 \
  ${PROJECT_ID}:streaming

# ── ML Layer ──
bq mk \
  --dataset \
  --location=$REGION \
  --description="ML: BQML models and feature tables" \
  --default_table_expiration=0 \
  ${PROJECT_ID}:ml

# ── Monitoring Layer ──
bq mk \
  --dataset \
  --location=$REGION \
  --description="Monitoring: data quality, pipeline metrics" \
  --default_table_expiration=0 \
  ${PROJECT_ID}:monitoring

echo "✅ All datasets created:"
bq ls --project_id=$PROJECT_ID
```

### Step 3.2 — Dataset Access Control

```sql
-- Grant dataset-level access in BigQuery Console or via bq command

-- Bronze: only ingestor can write
-- bq update --dataset_description '' ... (use Console for fine-grained ACLs)

-- SQL to view current dataset ACLs
SELECT
  *
FROM `chicago-taxi-analytics`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE schema_name IN ('bronze', 'silver', 'gold', 'streaming', 'ml');
```

---

