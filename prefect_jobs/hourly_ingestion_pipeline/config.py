"""Configuration for hourly ingestion pipeline job."""

import os
from pathlib import Path

ENV = os.environ.get("ENV", "local")

# Default database path (outside project)
_DEFAULT_DB_PATH = Path.home() / "data" / "lemonade" / "metadata.db"
_DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_DEFAULT_DB_URL = f"sqlite:///{_DEFAULT_DB_PATH}"

# File paths (resolve to absolute paths)
# New structure: incoming/ for all incoming files, source_data/ for processing/failed/error
_INCOMING_FOLDER_STR = os.environ.get("INCOMING_FOLDER", "./data/incoming")
INCOMING_FOLDER = Path(_INCOMING_FOLDER_STR).resolve()
_SOURCE_DATA_BASE_STR = os.environ.get("SOURCE_DATA_BASE", "./data/source_data")
SOURCE_DATA_BASE = Path(_SOURCE_DATA_BASE_STR).resolve()
PROCESSING_FOLDER = SOURCE_DATA_BASE / "processing"
FAILED_FOLDER = SOURCE_DATA_BASE / "failed"
ERROR_FOLDER = SOURCE_DATA_BASE / "error"
_PROCESSED_FOLDER_STR = os.environ.get("PROCESSED_FOLDER", "./data/processed")
PROCESSED_FOLDER = Path(_PROCESSED_FOLDER_STR).resolve()

# File type patterns
EVENTS_FILE_PATTERN = "vehicles_events_*.json"
STATUS_FILE_PATTERN = "vehicles_status_*.json"

# Datalake configuration
# Local: Parquet files in local filesystem
# Production: Iceberg tables in S3, managed by AWS Glue Catalog
if ENV == "production":
    DATALAKE_BASE = Path(os.environ.get("DATALAKE_BASE", "s3://lemonade-datalake"))
else:
    DATALAKE_BASE = Path(os.environ.get("DATALAKE_BASE", "./data/datalake"))
DATALAKE_EVENTS_TABLE = DATALAKE_BASE / "raw_data" / "vehicle_events"
DATALAKE_STATUS_TABLE = DATALAKE_BASE / "raw_data" / "vehicle_status"

# Iceberg configuration (production - not used in this challenge)
# In production: Would use Iceberg tables in S3 with AWS Glue Catalog
# ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI")
# ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE")
# ICEBERG_NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "lemonade")

# Spark configuration
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME", "lemonade-data-ingestion")

# EMR configuration (production - not used in this challenge)
# EMR_CLUSTER_ID = os.environ.get("EMR_CLUSTER_ID")
# EMR_STEP_TYPE = os.environ.get("EMR_STEP_TYPE", "spark-submit")

# Database
# This project uses SQLite only for metadata storage
# In production:
#   - Metadata: PostgreSQL
#   - Raw events: Iceberg tables in S3
#   - Aggregation layer: Snowflake
if ENV == "production":
    DATABASE_URL = os.environ.get("DATABASE_URL")  # PostgreSQL in production
else:
    DATABASE_URL = os.environ.get("DATABASE_URL", _DEFAULT_DB_URL)

