"""Configuration for daily summary aggregation job."""

import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, StringType

# Load environment variables from .env file
load_dotenv()

# Schema validation for input data (vehicle_events)
# Expected columns when reading from Parquet datalake
VEHICLE_EVENTS_INPUT_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), nullable=False),
    StructField("event_time", StringType(), nullable=False),  # Will be converted to timestamp
    StructField("event_type", StringType(), nullable=False),
    # Other fields may exist but are not required for aggregation
])

ENV = os.environ.get("ENV", "local")

# Datalake configuration
# Local: Parquet files in local filesystem
# Production: Iceberg tables in S3, managed by AWS Glue Catalog
# Get project root (parent of prefect_jobs folder) to ensure correct path resolution
_PROJECT_ROOT = Path(__file__).parent.parent.parent

if ENV == "production":
    DATALAKE_BASE = Path(os.environ.get("DATALAKE_BASE", "s3://lemonade-datalake"))
else:
    _DATALAKE_BASE_STR = os.environ.get("DATALAKE_BASE", "./data/datalake")
    if Path(_DATALAKE_BASE_STR).is_absolute():
        DATALAKE_BASE = Path(_DATALAKE_BASE_STR)
    else:
        DATALAKE_BASE = (_PROJECT_ROOT / _DATALAKE_BASE_STR).resolve()
DATALAKE_EVENTS_TABLE = DATALAKE_BASE / "raw_data" / "vehicle_events"
DATALAKE_SUMMARY_TABLE = DATALAKE_BASE / "reports" / "daily_summary"

# Iceberg configuration (production - not used in this challenge)
# In production: Would use Iceberg tables in S3 with AWS Glue Catalog
# ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI")
# ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE")
# ICEBERG_NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "lemonade")
# ICEBERG_SUMMARY_TABLE = f"{ICEBERG_NAMESPACE}.daily_summary"

# Database (for local dev summary table)
# This project uses SQLite only
# In production:
#   - Metadata: PostgreSQL
#   - Raw events: Iceberg tables in S3
#   - Aggregation layer: Snowflake (daily summary tables)
_DEFAULT_DB_PATH = Path.home() / "data" / "lemonade" / "metadata.db"
_DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_DEFAULT_DB_URL = f"sqlite:///{_DEFAULT_DB_PATH}"

if ENV == "production":
    DATABASE_URL = os.environ.get("DATABASE_URL")  # PostgreSQL in production
else:
    DATABASE_URL = os.environ.get("DATABASE_URL", _DEFAULT_DB_URL)

