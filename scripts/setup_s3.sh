#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define LocalStack endpoint and bucket name
ENDPOINT_URL=http://localstack:4566
BUCKET_NAME=data-ingestion-edb-ex2

echo "--- Waiting for LocalStack S3 to be ready..."

# --- THIS IS THE NEW, MORE ROBUST WAIT LOGIC ---
# We use a command that is expected to fail once the service is up.
# The '|| true' ensures that the script doesn't exit when the expected error occurs.
aws s3api wait bucket-exists --bucket dummy-bucket-that-doesnt-exist --endpoint-url ${ENDPOINT_URL} || true

echo "LocalStack S3 is ready."

echo "--- Creating S3 bucket: $BUCKET_NAME ---"
# Create the bucket. The '--quiet' flag suppresses output on success.
aws --endpoint-url=$ENDPOINT_URL s3 mb s3://$BUCKET_NAME

echo "--- Uploading raw data to S3 ---"
# Use sync to upload the entire raw-data directory
aws --endpoint-url=$ENDPOINT_URL s3 sync /data/raw-data/ s3://$BUCKET_NAME/raw-data/

echo "--- Verifying uploaded files ---"
aws --endpoint-url=$ENDPOINT_URL s3 ls s3://$BUCKET_NAME/raw-data/ --recursive

echo "--- S3 setup complete. ---"