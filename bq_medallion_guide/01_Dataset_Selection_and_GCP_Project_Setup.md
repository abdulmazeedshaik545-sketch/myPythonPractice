<!-- Part of: BigQuery Medallion Architecture Guide | File: 01_Dataset_Selection_and_GCP_Project_Setup.md -->

## SECTION 1: DATASET SELECTION

### Primary Dataset: `bigquery-public-data.chicago_taxi_trips`

| Property | Value |
|----------|-------|
| **Size** | ~74 GB (trips table) — scale to ~1 TB with joins + history |
| **Rows** | ~210 million trip records |
| **Coverage** | 2013 – present (real, continuously updated) |
| **Domain** | Urban mobility, payment analytics, geospatial |
| **Key columns** | Fare, tips, tolls, trip miles, duration, pickup/dropoff community areas, payment type, company |

### Supplementary Datasets (to reach ~1 TB total)

| Dataset | Size | Purpose |
|---------|------|---------|
| `bigquery-public-data.thelook_ecommerce` | ~15 GB | E-commerce enrichment (driver demographics proxy) |
| `bigquery-public-data.noaa_gsod` | ~10 GB | Weather join (weather impact on trips) |
| `bigquery-public-data.chicago_crime` | ~1.5 GB | Safety index by community area |
| `bigquery-public-data.geo_us_boundaries` | ~500 MB | Geographic boundaries for spatial analytics |
| **Simulated streaming data** | ~900 GB | Real-time trip events (generated via Dataflow) |

> **Reaching 1 TB**: The taxi trips table is ~74 GB. By creating Silver and Gold layer tables (denormalized, with weather joins, community enrichment, and rolling aggregates), the total storage across all medallion layers reaches ~1–1.2 TB.

---

## SECTION 2: GCP PROJECT SETUP

### Step 2.1 — Create GCP Project

```bash
# Install Google Cloud SDK first: https://cloud.google.com/sdk/docs/install

# Authenticate
gcloud auth login
gcloud auth application-default login

# Create new project (or use existing)
gcloud projects create chicago-taxi-analytics \
  --name="Chicago Taxi Analytics" \
  --set-as-default

# Set project
gcloud config set project chicago-taxi-analytics

# Link billing account (required for BigQuery)
# Find your billing account ID:
gcloud billing accounts list

# Link it:
gcloud billing projects link chicago-taxi-analytics \
  --billing-account=YOUR_BILLING_ACCOUNT_ID
```

### Step 2.2 — Enable Required APIs

```bash
# Enable all required Google Cloud APIs
gcloud services enable \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  bigquerydatatransfer.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  storage.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com

echo "✅ All APIs enabled successfully"
```

### Step 2.3 — Create Service Accounts & IAM Roles

```bash
# ── Service Account 1: Data Ingestion (Bronze layer writer) ──
gcloud iam service-accounts create bq-bronze-ingestor \
  --display-name="BigQuery Bronze Layer Ingestor" \
  --description="Writes raw data to Bronze datasets"

# ── Service Account 2: Data Transformer (Silver + Gold writer) ──
gcloud iam service-accounts create bq-transformer \
  --display-name="BigQuery Silver/Gold Transformer" \
  --description="Transforms data across medallion layers"

# ── Service Account 3: BI Reader (read-only for dashboards) ──
gcloud iam service-accounts create bq-bi-reader \
  --display-name="BigQuery BI Reader" \
  --description="Read-only access for Looker Studio"

# ── Assign IAM Roles ──
PROJECT_ID="chicago-taxi-analytics"

# Bronze ingestor: can write to BigQuery and read from GCS
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bq-bronze-ingestor@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bq-bronze-ingestor@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# Transformer: full BigQuery access (reads bronze, writes silver/gold)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bq-transformer@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bq-transformer@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# BI reader: read-only
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bq-bi-reader@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bq-bi-reader@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Download service account keys (for local development)
gcloud iam service-accounts keys create ./keys/bronze-ingestor-key.json \
  --iam-account=bq-bronze-ingestor@${PROJECT_ID}.iam.gserviceaccount.com

gcloud iam service-accounts keys create ./keys/transformer-key.json \
  --iam-account=bq-transformer@${PROJECT_ID}.iam.gserviceaccount.com

echo "✅ Service accounts and IAM roles configured"
```

### Step 2.4 — Create GCS Buckets (Landing Zone)

```bash
PROJECT_ID="chicago-taxi-analytics"
REGION="us-central1"

# Raw data landing bucket (Bronze source)
gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION \
  gs://chicago-taxi-bronze-landing/

# Archive bucket (keep raw files for compliance)
gsutil mb -p $PROJECT_ID -c NEARLINE -l $REGION \
  gs://chicago-taxi-archive/

# Temp bucket for Dataflow jobs
gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION \
  gs://chicago-taxi-dataflow-temp/

# Set lifecycle policy on archive (move to coldline after 90 days)
cat > /tmp/lifecycle.json << 'EOF'
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
      "condition": {"age": 90}
    }
  ]
}
EOF

gsutil lifecycle set /tmp/lifecycle.json gs://chicago-taxi-archive/

echo "✅ GCS buckets created"
```

---

