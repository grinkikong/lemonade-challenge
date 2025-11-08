"""Configuration for daily summary aggregation job."""

import os
from pathlib import Path

ENV = os.environ.get("ENV", "local")

# Default database path (outside project)
_DEFAULT_DB_PATH = Path.home() / "data" / "lemonade" / "metadata.db"
_DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_DEFAULT_DB_URL = f"sqlite:///{_DEFAULT_DB_PATH}"

# Datalake configuration
# Local: Parquet files in local filesystem
# Production: Iceberg tables in S3, managed by AWS Glue Catalog
if ENV == "production":
    DATALAKE_BASE = Path(os.environ.get("DATALAKE_BASE", "s3://lemonade-datalake"))
else:
    DATALAKE_BASE = Path(os.environ.get("DATALAKE_BASE", "./data/datalake"))
DATALAKE_EVENTS_TABLE = DATALAKE_BASE / "raw_data" / "vehicle_events"
DATALAKE_SUMMARY_TABLE = DATALAKE_BASE / "summary" / "daily_summary"

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
if ENV == "production":
    DATABASE_URL = os.environ.get("DATABASE_URL")  # PostgreSQL in production
else:
    DATABASE_URL = os.environ.get("DATABASE_URL", _DEFAULT_DB_URL)

