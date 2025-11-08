#!/usr/bin/env python3
"""Script to create database tables for metadata management.

Usage:
    python scripts/create_tables.py

Creates SQLite tables for managing processing schemas and file metadata.
Table definitions are in data_utils.database.models.

Note: This project uses SQLite only. In production:
  - Metadata: PostgreSQL
  - Raw events: Iceberg tables in S3
  - Aggregation layer: Snowflake
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_utils.database.drivers import db_driver
from project_utils import ENV


def main():
    """Create all database tables."""
    print(f"Creating database tables (Environment: {ENV})")
    print(f"Database URL: {db_driver.database_url}")

    # Ensure data directory exists for SQLite
    if db_driver.database_url.startswith("sqlite"):
        db_path = Path(db_driver.database_url.replace("sqlite:///", ""))
        db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create tables
    db_driver.create_tables()
    print("✓ Tables created successfully!")

    print("\nCreated tables:")
    print("  - table_metadata (stores table schema definitions and Spark processing config)")


if __name__ == "__main__":
    main()

