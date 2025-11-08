#!/usr/bin/env python3
"""Script to create database table schemas and folder structure.

**Note**: This script is for development/demonstration purposes only.
In production, data is stored as Parquet files in S3, with table schemas 
predefined in AWS Glue Catalog as Iceberg tables.

This script:
1. Creates folder structure for all tables
2. Defines table schemas (as they would be in production with Iceberg)
3. Creates DuckDB views for querying data (dev only)

Usage:
    python scripts/create_tables.py

Defines schemas for 3 tables:
1. raw_data.vehicle_events - Vehicle events table (partitioned by date)
2. raw_data.vehicle_status - Vehicle status table (partitioned by date)
3. reports.daily_summary - Daily summary table (partitioned by day)

In production:
- Data: Parquet files in S3 (same format as dev, just different location)
- Table definitions: Iceberg tables in AWS Glue Catalog (predefined schemas)
"""

import sys
import os
import duckdb
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Get project root (parent of scripts folder)
_PROJECT_ROOT = Path(__file__).parent.parent

# Get datalake base path from config or environment
ENV = os.environ.get("ENV", "local")
if ENV == "production":
    DATALAKE_BASE = Path(os.environ.get("DATALAKE_BASE", "s3://lemonade-datalake"))
else:
    _DATALAKE_BASE_STR = os.environ.get("DATALAKE_BASE", "./data/datalake")
    if Path(_DATALAKE_BASE_STR).is_absolute():
        DATALAKE_BASE = Path(_DATALAKE_BASE_STR)
    else:
        DATALAKE_BASE = (_PROJECT_ROOT / _DATALAKE_BASE_STR).resolve()


# Table schema definitions (as they would be in production with Iceberg)
# These schemas match what would be defined in AWS Glue Catalog for Iceberg tables
TABLE_SCHEMAS = {
    "raw_data.vehicle_events": {
        "columns": [
            "vehicle_id STRING",
            "event_time TIMESTAMP",
            "event_source STRING",
            "event_type STRING",
            "event_value STRING",
            "event_extra_data STRUCT<note: STRING, boot_time: INT, emergency_call: BOOLEAN, severity: STRING>",
            "date STRING"
        ],
        "partition": "date",
        "description": "Vehicle events table - stores all vehicle events",
        "location": "data/datalake/raw_data/vehicle_events/",
        "iceberg_location": "s3://lemonade-datalake/raw_data/vehicle_events/",
        "iceberg_table": "raw_data.vehicle_events"
    },
    "raw_data.vehicle_status": {
        "columns": [
            "vehicle_id STRING",
            "report_time TIMESTAMP",
            "status_source STRING",
            "status STRING",
            "date STRING"
        ],
        "partition": "date",
        "description": "Vehicle status table - stores vehicle status reports",
        "location": "data/datalake/raw_data/vehicle_status/",
        "iceberg_location": "s3://lemonade-datalake/raw_data/vehicle_status/",
        "iceberg_table": "raw_data.vehicle_status"
    },
    "reports.daily_summary": {
        "columns": [
            "vehicle_id STRING",
            "day STRING",
            "last_event_time STRING",
            "last_event_type STRING"
        ],
        "partition": "day",
        "description": "Daily summary table - aggregated vehicle events by day",
        "location": "data/datalake/reports/daily_summary/",
        "iceberg_location": "s3://lemonade-datalake/reports/daily_summary/",
        "iceberg_table": "reports.daily_summary"
    }
}


def create_folder_paths():
    """Create all folder paths for tables if they don't exist."""
    print("=" * 70)
    print("CREATING FOLDER STRUCTURE")
    print("=" * 70)
    
    if ENV == "production":
        print("\n⚠️  Production environment detected (S3).")
        print("   Skipping local folder creation (S3 paths are handled by AWS).\n")
        return
    
    created_folders = []
    existing_folders = []
    
    for table_name, schema in TABLE_SCHEMAS.items():
        # Resolve folder path relative to project root
        folder_path = DATALAKE_BASE / schema["location"].replace("data/datalake/", "")
        
        if folder_path.exists():
            existing_folders.append(folder_path)
            print(f"  ✓ {folder_path} (already exists)")
        else:
            folder_path.mkdir(parents=True, exist_ok=True)
            created_folders.append(folder_path)
            print(f"  ✓ Created: {folder_path}")
    
    print(f"\n✓ Folder structure ready:")
    print(f"  - Created: {len(created_folders)} folder(s)")
    print(f"  - Existing: {len(existing_folders)} folder(s)")
    print()


def create_duckdb_views():
    """Create DuckDB views for querying Parquet files (dev only)."""
    print("=" * 70)
    print("CREATING DUCKDB VIEWS")
    print("=" * 70)
    
    if ENV == "production":
        print("\n⚠️  Production environment detected (S3).")
        print("   Skipping DuckDB view creation (production uses Iceberg tables in Glue Catalog).\n")
        return
    
    db_path = DATALAKE_BASE / "lemonade.duckdb"
    print(f"\nDatalake base: {DATALAKE_BASE}")
    print(f"Database: {db_path}\n")
    
    conn = duckdb.connect(str(db_path))
    
    # Clean up old views and schemas
    print("Cleaning up old views and schemas...")
    try:
        conn.execute("DROP VIEW IF EXISTS dwh.daily_summary")
        conn.execute("DROP VIEW IF EXISTS dwh.main")
        conn.execute("DROP TABLE IF EXISTS dwh.main")
        conn.execute("DROP SCHEMA IF EXISTS dwh")
        conn.execute("DROP VIEW IF EXISTS main.vehicle_events")
        conn.execute("DROP VIEW IF EXISTS main.vehicle_status")
        conn.execute("DROP VIEW IF EXISTS main.daily_summary")
        conn.execute("DROP VIEW IF EXISTS vehicle_events")
        conn.execute("DROP VIEW IF EXISTS vehicle_status")
        conn.execute("DROP VIEW IF EXISTS daily_summary")
        conn.execute("DROP VIEW IF EXISTS reports.main")
        conn.execute("DROP TABLE IF EXISTS reports.main")
        print("✓ Cleaned up old views and schemas\n")
    except Exception as e:
        print(f"  Note: {e}\n")
    
    # Create schemas
    print("Creating schemas...")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    conn.execute("CREATE SCHEMA IF NOT EXISTS reports")
    print("✓ Created schemas: raw_data, reports\n")
    
    # Column order for logical presentation
    column_orders = {
        "raw_data.vehicle_events": [
            "vehicle_id",
            "event_time",
            "event_type",
            "event_source",
            "event_value",
            "event_extra_data",
            "date"
        ],
        "raw_data.vehicle_status": [
            "vehicle_id",
            "report_time",
            "status",
            "status_source",
            "date"
        ],
        "reports.daily_summary": [
            "vehicle_id",
            "day",
            "last_event_time",
            "last_event_type"
        ]
    }
    
    # Create views for each table
    for table_name, schema in TABLE_SCHEMAS.items():
        print(f"Creating view: {table_name}")
        
        base_path = DATALAKE_BASE / schema["location"].replace("data/datalake/", "")
        base_path.mkdir(parents=True, exist_ok=True)
        
        # Use glob pattern to automatically discover Parquet files
        glob_pattern = str(base_path / "**" / "*.parquet")
        
        # Check if files exist
        parquet_files = list(base_path.rglob("*.parquet")) if base_path.exists() else []
        
        # Get column order
        column_order = column_orders.get(table_name, [col.split()[0] for col in schema["columns"]])
        ordered_cols = ", ".join(column_order)
        
        # Create view with glob pattern (automatically discovers new files)
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT {ordered_cols}
        FROM read_parquet('{glob_pattern}')
        """
        conn.execute(create_view_sql)
        
        if parquet_files:
            print(f"  ✓ Created view: {table_name} ({len(parquet_files)} Parquet files found)")
            print(f"    ✅ New files matching pattern will be automatically discovered")
        else:
            print(f"  ✓ Created view: {table_name} (no files yet, will discover when data arrives)")
        print()
    
    # Verify views were created
    print("Verifying views...")
    tables = conn.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('raw_data', 'reports')
        ORDER BY table_schema, table_name
    """).fetchall()
    
    print(f"✓ Found {len(tables)} view(s):")
    for schema, table in tables:
        print(f"  - {schema}.{table}")
    
    conn.close()
    print()


def print_schema_definitions():
    """Print all table schema definitions (as they would be in production with Iceberg)."""
    print("=" * 70)
    print("TABLE SCHEMA DEFINITIONS")
    print("=" * 70)
    print("\nThese schemas match what would be predefined in AWS Glue Catalog for Iceberg tables.")
    print("In production, data is stored as Parquet files in S3 (same format as dev),")
    print("with these schemas predefined in AWS Glue Catalog as Iceberg tables.\n")
    
    for table_name, schema in TABLE_SCHEMAS.items():
        print(f"\n{table_name}")
        print("-" * 70)
        print(f"Description: {schema['description']}")
        print(f"Partition: {schema['partition']}")
        print(f"Location (dev): {schema['location']}")
        print(f"Location (production): {schema['iceberg_location']}")
        print(f"Iceberg Table: {schema['iceberg_table']}")
        print(f"\nSchema ({len(schema['columns'])} columns):")
        for col in schema['columns']:
            print(f"  - {col}")
    
    print("\n" + "=" * 70)
    print("ICEBERG TABLE DDL (For Production Reference)")
    print("=" * 70)
    print("\n-- Example Iceberg table creation (for production with AWS Glue Catalog)")
    print("-- Data is stored as Parquet files in S3, with schemas predefined in Glue Catalog")
    print("-- These would be created using Spark or AWS Glue APIs\n")
    
    for table_name, schema in TABLE_SCHEMAS.items():
        schema_name, table = table_name.split(".")
        print(f"\n-- {schema['description']}")
        print(f"-- Schema: {schema_name}, Table: {table}")
        print(f"CREATE TABLE {schema['iceberg_table']}")
        print("USING ICEBERG")
        print("LOCATION '{s3://lemonade-datalake/" + schema['location'].replace("data/datalake/", "") + "}'")
        print("PARTITIONED BY ({schema['partition']})")
        print("AS SELECT")
        col_list = ",\n    ".join([col.split()[0] for col in schema['columns']])
        print(f"    {col_list}")
        print("FROM ...")
        print()


def main():
    """Create folder structure, DuckDB views, and print schema definitions."""
    # Create folder paths first
    create_folder_paths()
    
    # Create DuckDB views (dev only)
    create_duckdb_views()
    
    # Print schema definitions
    print_schema_definitions()
    
    print("=" * 70)
    print("✅ Schema definitions, folder structure, and DuckDB views complete!")
    print("=" * 70)
    print("\nYou can now:")
    print("  1. Query the tables using DuckDB UI (open lemonade.duckdb)")
    print("  2. Views will automatically discover new Parquet files as they are created")
    print("  3. Run: SELECT * FROM raw_data.vehicle_events LIMIT 10;")


if __name__ == "__main__":
    main()
