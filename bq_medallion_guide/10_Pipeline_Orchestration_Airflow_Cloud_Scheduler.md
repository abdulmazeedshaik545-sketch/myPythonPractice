<!-- Part of: BigQuery Medallion Architecture Guide | File: 10_Pipeline_Orchestration_Airflow_Cloud_Scheduler.md -->

## SECTION 8: PIPELINE ORCHESTRATION

### Step 8.1 — Cloud Scheduler + Scheduled Queries (No-Code Option)

```bash
# Create Cloud Scheduler jobs to trigger BigQuery Scheduled Queries
PROJECT_ID="chicago-taxi-analytics"
REGION="us-central1"

# Job 1: Bronze Incremental Load — runs daily at 1:00 AM
gcloud scheduler jobs create http bronze-incremental-load \
  --location=$REGION \
  --schedule="0 1 * * *" \
  --uri="https://bigquerydatatransfer.googleapis.com/v1/projects/${PROJECT_ID}/locations/us/transferConfigs/BRONZE_TRANSFER_ID:startManualRuns" \
  --http-method=POST \
  --oauth-service-account-email="bq-bronze-ingestor@${PROJECT_ID}.iam.gserviceaccount.com" \
  --message-body='{"requestedRunTime": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"}'

# Job 2: Silver Transform — runs daily at 3:00 AM (after Bronze finishes)
gcloud scheduler jobs create http silver-transform \
  --location=$REGION \
  --schedule="0 3 * * *" \
  --uri="https://bigquerydatatransfer.googleapis.com/v1/projects/${PROJECT_ID}/locations/us/transferConfigs/SILVER_TRANSFER_ID:startManualRuns" \
  --http-method=POST \
  --oauth-service-account-email="bq-transformer@${PROJECT_ID}.iam.gserviceaccount.com"

# Job 3: Gold Aggregation — runs daily at 4:30 AM
gcloud scheduler jobs create http gold-aggregation \
  --location=$REGION \
  --schedule="30 4 * * *" \
  --uri="https://bigquerydatatransfer.googleapis.com/v1/projects/${PROJECT_ID}/locations/us/transferConfigs/GOLD_TRANSFER_ID:startManualRuns" \
  --http-method=POST \
  --oauth-service-account-email="bq-transformer@${PROJECT_ID}.iam.gserviceaccount.com"

echo "✅ Cloud Scheduler jobs created"
```

---

### Step 8.2 — Airflow / Cloud Composer DAG (Production Option)

```python
# File: dags/medallion_pipeline_dag.py
# Deploy to Cloud Composer (managed Airflow on GCP)

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

PROJECT_ID = "chicago-taxi-analytics"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": True,
    "email":            ["data-alerts@yourcompany.com"],
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Chicago Taxi Medallion ETL: Bronze → Silver → Gold",
    schedule_interval="0 1 * * *",   # Daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["medallion", "bigquery", "chicago-taxi"],
) as dag:

    # ── BRONZE LAYER ────────────────────────────────────────
    bronze_load = BigQueryInsertJobOperator(
        task_id="bronze_incremental_load",
        configuration={
            "query": {
                "query": "{% include 'sql/bronze_incremental_load.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        project_id=PROJECT_ID,
        location="US",
    )

    bronze_validate = BigQueryInsertJobOperator(
        task_id="bronze_row_count_check",
        configuration={
            "query": {
                "query": f"""
                SELECT IF(
                    (SELECT COUNT(*) FROM `{PROJECT_ID}.bronze.taxi_trips_raw`
                     WHERE _ingestion_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) > 0,
                    'PASS', ERROR('Bronze validation failed: 0 rows loaded')
                ) AS validation_result
                """,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
    )

    # ── SILVER LAYER ─────────────────────────────────────────
    silver_weather_load = BigQueryInsertJobOperator(
        task_id="silver_weather_transform",
        configuration={
            "query": {
                "query": "{% include 'sql/silver_weather_transform.sql' %}",
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
    )

    silver_trips_load = BigQueryInsertJobOperator(
        task_id="silver_trips_transform",
        configuration={
            "query": {
                "query": "{% include 'sql/silver_trips_transform.sql' %}",
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
    )

    silver_validate = BigQueryInsertJobOperator(
        task_id="silver_dq_check",
        configuration={
            "query": {
                "query": f"""
                SELECT IF(
                    (SELECT AVG(dq_score) FROM `{PROJECT_ID}.silver.taxi_trips`
                     WHERE trip_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) >= 65,
                    'PASS', ERROR('Silver DQ check failed: avg quality score below threshold')
                )
                """,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
    )

    # ── GOLD LAYER ───────────────────────────────────────────
    gold_merge = BigQueryInsertJobOperator(
        task_id="gold_fact_merge",
        configuration={
            "query": {
                "query": "{% include 'sql/gold_merge.sql' %}",
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
    )

    gold_validate = BigQueryInsertJobOperator(
        task_id="gold_reconciliation_check",
        configuration={
            "query": {
                "query": "{% include 'sql/cross_layer_reconciliation.sql' %}",
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
    )

    # ── ML LAYER ─────────────────────────────────────────────
    ml_score = BigQueryInsertJobOperator(
        task_id="ml_daily_scoring",
        configuration={
            "query": {
                "query": "{% include 'sql/ml_daily_scoring.sql' %}",
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
    )

    # ── PIPELINE DEPENDENCIES ────────────────────────────────
    (
        bronze_load
        >> bronze_validate
        >> [silver_weather_load, silver_trips_load]   # parallel Silver loads
        >> silver_validate
        >> gold_merge
        >> gold_validate
        >> ml_score
    )
```

---

