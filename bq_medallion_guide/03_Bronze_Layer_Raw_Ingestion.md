<!-- Part of: BigQuery Medallion Architecture Guide | File: 03_Bronze_Layer_Raw_Ingestion.md -->

## SECTION 4: BRONZE LAYER — RAW DATA INGESTION

### Overview
The Bronze layer is the **immutable raw landing zone**. Data is copied exactly as-is from the public dataset. No transformations, no filtering. Think of it as your "source of truth" backup.

**Principles:**
- Append-only (never UPDATE or DELETE)
- Partitioned by `_ingestion_date` (when data was loaded)
- Includes metadata columns: `_source_system`, `_load_timestamp`, `_batch_id`
- Stored in BigQuery native format

---

### Step 4.1 — Create Bronze Tables with Schema

```sql
-- ═══════════════════════════════════════════════════
-- BRONZE TABLE 1: Raw Taxi Trips
-- Source: bigquery-public-data.chicago_taxi_trips.taxi_trips
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.bronze.taxi_trips_raw`
(
  -- Original source columns (exact copy, no type changes)
  unique_key              STRING,
  taxi_id                 STRING,
  trip_start_timestamp    TIMESTAMP,
  trip_end_timestamp      TIMESTAMP,
  trip_seconds            INT64,
  trip_miles              FLOAT64,
  pickup_census_tract     INT64,
  dropoff_census_tract    INT64,
  pickup_community_area   INT64,
  dropoff_community_area  INT64,
  fare                    FLOAT64,
  tips                    FLOAT64,
  tolls                   FLOAT64,
  extras                  FLOAT64,
  trip_total              FLOAT64,
  payment_type            STRING,
  company                 STRING,
  pickup_centroid_latitude  FLOAT64,
  pickup_centroid_longitude FLOAT64,
  pickup_centroid_location  GEOGRAPHY,
  dropoff_centroid_latitude  FLOAT64,
  dropoff_centroid_longitude FLOAT64,
  dropoff_centroid_location  GEOGRAPHY,

  -- Medallion metadata columns
  _ingestion_date         DATE,       -- partition key
  _load_timestamp         TIMESTAMP,  -- when this row was loaded
  _source_system          STRING,     -- 'bigquery-public-data.chicago_taxi_trips'
  _batch_id               STRING,     -- unique load batch identifier
  _source_row_hash        STRING      -- MD5 of all source columns (dedup key)
)
PARTITION BY _ingestion_date
CLUSTER BY taxi_id, pickup_community_area
OPTIONS (
  description = 'Bronze: Raw Chicago taxi trips. Immutable. Source of truth.',
  require_partition_filter = FALSE
);


-- ═══════════════════════════════════════════════════
-- BRONZE TABLE 2: Raw Weather Data
-- Source: bigquery-public-data.noaa_gsod
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.bronze.weather_raw`
(
  stn          STRING,
  wban         STRING,
  year         STRING,
  mo           STRING,
  da           STRING,
  temp         FLOAT64,
  dewp         FLOAT64,
  slp          FLOAT64,
  visib        FLOAT64,
  wdsp         FLOAT64,
  mxpsd        FLOAT64,
  gust         FLOAT64,
  max          FLOAT64,
  min          FLOAT64,
  prcp         FLOAT64,
  sndp         FLOAT64,
  fog          STRING,
  rain_drizzle STRING,
  snow_ice     STRING,
  thunder      STRING,
  tornado      STRING,

  -- Medallion metadata
  _ingestion_date   DATE,
  _load_timestamp   TIMESTAMP,
  _source_system    STRING,
  _batch_id         STRING
)
PARTITION BY _ingestion_date
CLUSTER BY stn, year
OPTIONS (description = 'Bronze: Raw NOAA weather data for Chicago stations.');


-- ═══════════════════════════════════════════════════
-- BRONZE TABLE 3: Community Area Reference
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.bronze.community_areas_raw`
(
  area_numbe   STRING,
  community    STRING,
  shape_area   FLOAT64,
  shape_len    FLOAT64,
  the_geom     GEOGRAPHY,

  _ingestion_date   DATE,
  _load_timestamp   TIMESTAMP,
  _source_system    STRING,
  _batch_id         STRING
)
PARTITION BY _ingestion_date
OPTIONS (description = 'Bronze: Chicago community area boundaries and names.');


-- ═══════════════════════════════════════════════════
-- BRONZE TABLE 4: Streaming Trip Events (real-time)
-- Populated via Pub/Sub → BigQuery subscription
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.bronze.trip_events_stream`
(
  event_id           STRING,
  event_type         STRING,   -- 'trip_started','trip_ended','payment_processed'
  taxi_id            STRING,
  trip_id            STRING,
  event_timestamp    TIMESTAMP,
  latitude           FLOAT64,
  longitude          FLOAT64,
  speed_mph          FLOAT64,
  passenger_count    INT64,
  meter_amount       FLOAT64,
  event_payload      JSON,     -- full event as JSON for schema flexibility

  -- Streaming metadata
  _pub_sub_message_id  STRING,
  _ingestion_timestamp TIMESTAMP,
  _processing_date     DATE
)
PARTITION BY _processing_date
CLUSTER BY taxi_id, event_type
OPTIONS (description = 'Bronze: Real-time streaming trip events from Pub/Sub.');
```

---

### Step 4.2 — Load Historical Data into Bronze

```bash
# ════════════════════════════════════════════════
# METHOD A: Direct BigQuery-to-BigQuery copy
# Fastest, no GCS needed, stays within BigQuery infrastructure
# ════════════════════════════════════════════════

# Load ALL historical taxi trips (2013 to present, ~210M rows)
bq query \
  --use_legacy_sql=false \
  --destination_table='chicago-taxi-analytics:bronze.taxi_trips_raw' \
  --append_table \
  --batch \
  "
  SELECT
    unique_key,
    taxi_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_seconds,
    trip_miles,
    pickup_census_tract,
    dropoff_census_tract,
    pickup_community_area,
    dropoff_community_area,
    fare,
    tips,
    tolls,
    extras,
    trip_total,
    payment_type,
    company,
    pickup_centroid_latitude,
    pickup_centroid_longitude,
    pickup_centroid_location,
    dropoff_centroid_latitude,
    dropoff_centroid_longitude,
    dropoff_centroid_location,
    -- Medallion metadata
    CURRENT_DATE()          AS _ingestion_date,
    CURRENT_TIMESTAMP()     AS _load_timestamp,
    'bigquery-public-data.chicago_taxi_trips.taxi_trips' AS _source_system,
    GENERATE_UUID()         AS _batch_id,
    MD5(CONCAT(
      COALESCE(unique_key,''),
      COALESCE(CAST(trip_start_timestamp AS STRING),''),
      COALESCE(CAST(trip_total AS STRING),'')
    ))                      AS _source_row_hash
  FROM \`bigquery-public-data.chicago_taxi_trips.taxi_trips\`
  WHERE trip_start_timestamp IS NOT NULL
  "

echo "✅ Historical taxi trips loaded to Bronze"
```

```bash
# Load Weather Data for Chicago (Station: 725300 = O'Hare, 725340 = Midway)
bq query \
  --use_legacy_sql=false \
  --destination_table='chicago-taxi-analytics:bronze.weather_raw' \
  --append_table \
  "
  SELECT
    stn, wban, year, mo, da,
    temp, dewp, slp, visib, wdsp, mxpsd, gust,
    max, min, prcp, sndp,
    fog, rain_drizzle, snow_ice_pellets AS snow_ice, thunder, tornado,
    CURRENT_DATE()      AS _ingestion_date,
    CURRENT_TIMESTAMP() AS _load_timestamp,
    'bigquery-public-data.noaa_gsod' AS _source_system,
    GENERATE_UUID()     AS _batch_id
  FROM \`bigquery-public-data.noaa_gsod.gsod20*\`
  WHERE stn IN ('725300', '725340')   -- Chicago O'Hare and Midway
    AND _TABLE_SUFFIX BETWEEN '13' AND '24'  -- 2013–2024
  "

echo "✅ Weather data loaded to Bronze"
```

```python
# ════════════════════════════════════════════════
# METHOD B: Python SDK for incremental daily loads
# Use for production pipelines
# ════════════════════════════════════════════════
# File: bronze_incremental_loader.py

from google.cloud import bigquery
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_bronze_incremental(load_date: str = None):
    """Load yesterday's taxi trips into Bronze layer."""
    client = bigquery.Client(project="chicago-taxi-analytics")

    if load_date is None:
        load_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Loading Bronze data for date: {load_date}")

    query = f"""
    INSERT INTO `chicago-taxi-analytics.bronze.taxi_trips_raw`
    SELECT
        unique_key,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        pickup_census_tract,
        dropoff_census_tract,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        payment_type,
        company,
        pickup_centroid_latitude,
        pickup_centroid_longitude,
        pickup_centroid_location,
        dropoff_centroid_latitude,
        dropoff_centroid_longitude,
        dropoff_centroid_location,
        DATE('{load_date}')             AS _ingestion_date,
        CURRENT_TIMESTAMP()             AS _load_timestamp,
        'chicago_taxi_trips.taxi_trips' AS _source_system,
        GENERATE_UUID()                 AS _batch_id,
        MD5(CONCAT(
            COALESCE(unique_key,''),
            COALESCE(CAST(trip_start_timestamp AS STRING),''),
            COALESCE(CAST(trip_total AS STRING),'')
        ))                              AS _source_row_hash
    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
    WHERE DATE(trip_start_timestamp) = DATE('{load_date}')
      -- Avoid reloading rows already in Bronze
      AND unique_key NOT IN (
        SELECT unique_key FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
        WHERE _ingestion_date = DATE('{load_date}')
      )
    """

    job_config = bigquery.QueryJobConfig(
        priority=bigquery.QueryPriority.BATCH,
        use_query_cache=False
    )

    job = client.query(query, job_config=job_config)
    job.result()  # Wait for completion

    logger.info(f"✅ Bronze load complete. Rows loaded: {job.num_dml_affected_rows}")
    return job.num_dml_affected_rows


if __name__ == "__main__":
    load_bronze_incremental()
```

---

### Step 4.3 — Bronze Data Validation Queries

```sql
-- ════════════════════════════════════════════════
-- VALIDATE: Bronze layer data quality checks
-- Run after every load batch
-- ════════════════════════════════════════════════

-- Check 1: Row counts by ingestion date
SELECT
  _ingestion_date,
  COUNT(*)                              AS total_rows,
  COUNT(DISTINCT unique_key)            AS unique_trips,
  COUNT(DISTINCT taxi_id)               AS unique_taxis,
  MIN(trip_start_timestamp)             AS earliest_trip,
  MAX(trip_start_timestamp)             AS latest_trip,
  SUM(trip_total)                       AS total_revenue
FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;


-- Check 2: Duplicate detection (using row hash)
SELECT
  _source_row_hash,
  COUNT(*) AS duplicate_count
FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
WHERE _ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;


-- Check 3: Schema validation — unexpected nulls in critical columns
SELECT
  'taxi_id'              AS column_name,
  COUNTIF(taxi_id IS NULL) AS null_count,
  COUNT(*)               AS total_rows,
  ROUND(COUNTIF(taxi_id IS NULL) / COUNT(*) * 100, 2) AS null_pct
FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
WHERE _ingestion_date = CURRENT_DATE() - 1

UNION ALL

SELECT 'trip_start_timestamp', COUNTIF(trip_start_timestamp IS NULL), COUNT(*),
  ROUND(COUNTIF(trip_start_timestamp IS NULL) / COUNT(*) * 100, 2)
FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
WHERE _ingestion_date = CURRENT_DATE() - 1

UNION ALL

SELECT 'trip_total', COUNTIF(trip_total IS NULL), COUNT(*),
  ROUND(COUNTIF(trip_total IS NULL) / COUNT(*) * 100, 2)
FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
WHERE _ingestion_date = CURRENT_DATE() - 1

UNION ALL

SELECT 'fare', COUNTIF(fare IS NULL), COUNT(*),
  ROUND(COUNTIF(fare IS NULL) / COUNT(*) * 100, 2)
FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
WHERE _ingestion_date = CURRENT_DATE() - 1;


-- Check 4: Bronze storage summary
SELECT
  table_name,
  ROUND(total_logical_bytes / POW(1024, 3), 2)     AS size_gb,
  ROUND(active_logical_bytes / POW(1024, 3), 2)    AS active_gb,
  row_count,
  creation_time
FROM `chicago-taxi-analytics`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE table_schema = 'bronze'
ORDER BY total_logical_bytes DESC;
```

---

### Step 4.4 — Set Up Pub/Sub for Real-Time Streaming (Bronze Streaming)

```bash
PROJECT_ID="chicago-taxi-analytics"

# Create Pub/Sub topic for real-time trip events
gcloud pubsub topics create chicago-taxi-live-events \
  --project=$PROJECT_ID

# Create BigQuery subscription (direct stream to BQ — no Dataflow needed!)
gcloud pubsub subscriptions create taxi-events-to-bq \
  --topic=chicago-taxi-live-events \
  --bigquery-table="${PROJECT_ID}:bronze.trip_events_stream" \
  --use-topic-schema \
  --write-metadata \
  --project=$PROJECT_ID

echo "✅ Pub/Sub → BigQuery streaming pipeline created"
```

```python
# ════════════════════════════════════════════════
# Streaming simulator: publishes real-time trip events
# In production, replace with actual taxi dispatch system
# File: streaming_simulator.py
# ════════════════════════════════════════════════

from google.cloud import pubsub_v1
import json
import time
import random
import uuid
from datetime import datetime

PROJECT_ID = "chicago-taxi-analytics"
TOPIC_ID = "chicago-taxi-live-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Chicago community areas for realistic simulation
COMMUNITY_AREAS = list(range(1, 78))
PAYMENT_TYPES = ["Credit Card", "Cash", "Mobile"]
COMPANIES = ["Flash Cab", "Taxi Affiliation Services", "Sun Taxi", "City Service"]

def generate_trip_event():
    """Generate a realistic taxi trip event."""
    event_types = ["trip_started", "trip_updated", "trip_ended", "payment_processed"]
    event_type = random.choice(event_types)

    # Chicago bounds: lat 41.6 to 42.1, lng -87.9 to -87.5
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "taxi_id": f"TAXI_{random.randint(1000, 9999):04d}",
        "trip_id": str(uuid.uuid4()),
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
        "latitude": round(random.uniform(41.65, 42.05), 6),
        "longitude": round(random.uniform(-87.85, -87.52), 6),
        "speed_mph": round(random.uniform(0, 45), 1),
        "passenger_count": random.randint(1, 4),
        "meter_amount": round(random.uniform(3.25, 75.00), 2),
        "community_area": random.choice(COMMUNITY_AREAS),
        "payment_type": random.choice(PAYMENT_TYPES),
        "company": random.choice(COMPANIES)
    }

def stream_events(events_per_second: int = 50, duration_minutes: int = 60):
    """Publish events to Pub/Sub at specified rate."""
    print(f"🚀 Starting stream: {events_per_second} events/sec for {duration_minutes} min")
    total_published = 0
    end_time = time.time() + (duration_minutes * 60)

    while time.time() < end_time:
        batch_start = time.time()

        for _ in range(events_per_second):
            event = generate_trip_event()
            message_data = json.dumps(event).encode("utf-8")

            # Publish with attributes for routing
            future = publisher.publish(
                topic_path,
                data=message_data,
                event_type=event["event_type"],
                source="taxi-simulator"
            )
            total_published += 1

        # Rate limiting
        elapsed = time.time() - batch_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        if total_published % 1000 == 0:
            print(f"  Published {total_published:,} events")

    print(f"✅ Stream complete. Total events published: {total_published:,}")


if __name__ == "__main__":
    stream_events(events_per_second=100, duration_minutes=60)
```

---

### Step 4.5 — Bronze Layer Monitoring Table

```sql
-- Create audit/monitoring log for Bronze loads
CREATE TABLE IF NOT EXISTS `chicago-taxi-analytics.monitoring.bronze_load_log`
(
  log_id              STRING DEFAULT (GENERATE_UUID()),
  batch_id            STRING,
  source_system       STRING,
  target_table        STRING,
  load_date           DATE,
  load_start_ts       TIMESTAMP,
  load_end_ts         TIMESTAMP,
  rows_loaded         INT64,
  rows_rejected       INT64,
  bytes_processed     INT64,
  load_status         STRING,   -- 'SUCCESS','FAILED','PARTIAL'
  error_message       STRING,
  created_at          TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
PARTITION BY load_date
OPTIONS (description = 'Audit log for all Bronze layer data loads.');


-- Insert a load record (run after each batch)
INSERT INTO `chicago-taxi-analytics.monitoring.bronze_load_log`
  (batch_id, source_system, target_table, load_date,
   load_start_ts, load_end_ts, rows_loaded, rows_rejected,
   bytes_processed, load_status)
VALUES
  (
    GENERATE_UUID(),
    'chicago_taxi_trips.taxi_trips',
    'bronze.taxi_trips_raw',
    CURRENT_DATE(),
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE),
    CURRENT_TIMESTAMP(),
    (SELECT COUNT(*) FROM `chicago-taxi-analytics.bronze.taxi_trips_raw`
     WHERE _ingestion_date = CURRENT_DATE()),
    0,
    0,
    'SUCCESS'
  );
```


---
