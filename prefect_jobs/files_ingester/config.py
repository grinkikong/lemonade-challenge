"""Configuration for files ingestion pipeline job."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# TODO v2: Add metadata table with full schema validation:
# - Store schemas in database (PostgreSQL in production)
# - Support dynamic file types without code changes
# - Full schema validation (data types, value ranges, required fields)
# - Status management (active/inactive) for file types
# - Processing config per file type

# Hardcoded file type definitions (v1 - simplified)
# Supported file types: vehicle_events and vehicle_status
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Schema definitions for validation
# These schemas enforce data types and catch schema changes early
VEHICLE_EVENTS_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), nullable=False),
    StructField("event_time", StringType(), nullable=False),  # ISO format string, validated via to_timestamp()
    StructField("event_source", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_value", StringType(), nullable=True),
    StructField("event_extra_data", MapType(StringType(), StringType()), nullable=True),
])

VEHICLE_STATUS_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), nullable=False),
    StructField("report_time", StringType(), nullable=False),  # ISO format string, validated via to_timestamp()
    StructField("status_source", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
])

FILE_TYPE_CONFIG = {
    "vehicle_events": {
        "required_columns": ["vehicle_id", "event_time", "event_source", "event_type"],
        "root_keys": ["vehicles_events", "vehicle_events"],  # Support both plural and singular
        "partition_column": "date",
        "schema": VEHICLE_EVENTS_SCHEMA,  # Explicit schema for validation
    },
    "vehicle_status": {
        "required_columns": ["vehicle_id", "report_time", "status_source", "status"],
        "root_keys": ["vehicles_status", "vehicle_status"],  # Support both plural and singular
        "partition_column": "date",
        "schema": VEHICLE_STATUS_SCHEMA,  # Explicit schema for validation
    },
}

# List of supported file types
SUPPORTED_FILE_TYPES = list(FILE_TYPE_CONFIG.keys())

ENV = os.environ.get("ENV", "local")

# Default database path (outside project)
_DEFAULT_DB_PATH = Path.home() / "data" / "lemonade" / "metadata.db"
_DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_DEFAULT_DB_URL = f"sqlite:///{_DEFAULT_DB_PATH}"

# File paths (resolve to absolute paths relative to project root)
# Structure: source_data/ contains incoming, processing, failed, processed
# Get project root (parent of prefect_jobs folder)
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_SOURCE_DATA_BASE_STR = os.environ.get("SOURCE_DATA_BASE", "./data/source_data")
if Path(_SOURCE_DATA_BASE_STR).is_absolute():
    SOURCE_DATA_BASE = Path(_SOURCE_DATA_BASE_STR)
else:
    SOURCE_DATA_BASE = (_PROJECT_ROOT / _SOURCE_DATA_BASE_STR).resolve()
INCOMING_FOLDER = SOURCE_DATA_BASE / "incoming"
PROCESSING_FOLDER = SOURCE_DATA_BASE / "processing"
FAILED_FOLDER = SOURCE_DATA_BASE / "failed"
PROCESSED_FOLDER = SOURCE_DATA_BASE / "processed"

# File type patterns
EVENTS_FILE_PATTERN = "vehicles_events_*.json"
STATUS_FILE_PATTERN = "vehicles_status_*.json"

# Datalake configuration
# Local: Parquet files in local filesystem
# Production: Iceberg tables in S3, managed by AWS Glue Catalog
if ENV == "production":
    DATALAKE_BASE = Path(os.environ.get("DATALAKE_BASE", "s3://lemonade-datalake"))
else:
    _DATALAKE_BASE_STR = os.environ.get("DATALAKE_BASE", "./data/datalake")
    if Path(_DATALAKE_BASE_STR).is_absolute():
        DATALAKE_BASE = Path(_DATALAKE_BASE_STR)
    else:
        DATALAKE_BASE = (_PROJECT_ROOT / _DATALAKE_BASE_STR).resolve()
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

