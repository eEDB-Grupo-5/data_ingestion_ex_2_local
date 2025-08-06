import os

# --- Environment Configuration ---
S3_BUCKET = os.environ.get("S3_BUCKET")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", 5432)

# --- S3 Path Configuration ---
RAW_DATA_PATH = f"s3://{S3_BUCKET}/raw-data"
TRUSTED_DATA_PATH = f"s3://{S3_BUCKET}/trust-data"
DELIVERY_DATA_PATH = f"s3://{S3_BUCKET}/delivery-data"

# --- LocalStack/Boto3 Configuration ---
# This dictionary tells Polars how to connect to our local S3 (LocalStack)
STORAGE_OPTIONS = {
    "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    "aws_endpoint_url": os.environ.get("AWS_ENDPOINT_URL"),
    "aws_region": os.environ.get("AWS_DEFAULT_REGION")
}
