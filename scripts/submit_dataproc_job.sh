#!/bin/bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-your-project}"
REGION="${GCP_REGION:-us-central1}"
CLUSTER="taxi-processing-dev"
RAW_BUCKET="gs://taxi-raw-dev"
PROCESSED_BUCKET="gs://taxi-processed-dev"

echo "=== Submitting transform job ==="
gcloud dataproc jobs submit pyspark \
    spark_jobs/transform_trips.py \
    --cluster="$CLUSTER" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    -- "$RAW_BUCKET/yellow/**/*.parquet" "$PROCESSED_BUCKET/yellow_trips/"

echo "=== Submitting aggregation job ==="
gcloud dataproc jobs submit pyspark \
    spark_jobs/aggregate_zones.py \
    --cluster="$CLUSTER" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    -- "$PROCESSED_BUCKET/yellow_trips/" "$PROCESSED_BUCKET/zone_aggregates/"

echo "=== All jobs complete ==="
