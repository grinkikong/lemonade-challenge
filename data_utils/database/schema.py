"""Database schema creation SQL and table definitions.

This module contains SQL statements for creating database tables.
This project uses SQLite only. Table creation is handled by SQLAlchemy models.

Note: In production, this would use:
- Metadata: PostgreSQL
- Raw events: Iceberg tables in S3
- Aggregation layer: Snowflake
"""

# Table creation is handled by SQLAlchemy models in models.py
# The create_tables() method in drivers.py uses Base.metadata.create_all()
# which generates appropriate SQL for SQLite

# SQLite table creation SQL (used in this project)
CREATE_TABLE_METADATA_SQLITE = """
CREATE TABLE IF NOT EXISTS table_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    file_type VARCHAR(50) NOT NULL,
    partition_column VARCHAR(50) DEFAULT 'date',
    processing_folder_path VARCHAR(255),
    schema_definition TEXT NOT NULL,
    processing_config TEXT,
    status VARCHAR(20) DEFAULT 'active' NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_table_metadata_file_type ON table_metadata(file_type);
CREATE INDEX IF NOT EXISTS idx_table_metadata_status ON table_metadata(status);
"""

# Production architecture (for reference only - not used in this project)
# ========================================================================
# 
# 1. Metadata: PostgreSQL
# CREATE_TABLE_METADATA_POSTGRES = """
# CREATE TABLE IF NOT EXISTS table_metadata (
#     id SERIAL PRIMARY KEY,
#     table_name VARCHAR(100) NOT NULL UNIQUE,
#     file_type VARCHAR(50) NOT NULL,
#     partition_column VARCHAR(50) DEFAULT 'date',
#     processing_folder_path VARCHAR(255),
#     schema_definition JSONB NOT NULL,
#     processing_config JSONB,
#     status VARCHAR(20) DEFAULT 'active' NOT NULL,
#     description TEXT,
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );
# CREATE INDEX IF NOT EXISTS idx_table_metadata_file_type ON table_metadata(file_type);
# CREATE INDEX IF NOT EXISTS idx_table_metadata_status ON table_metadata(status);
# """
#
# 2. Raw events: Iceberg tables in S3 (partitioned by date)
#    - Managed by Spark/Iceberg catalog
#    - Partitioned by date column
#    - Stored as Parquet files in S3
#
# 3. Aggregation layer: Snowflake
#    - Daily summary tables
#    - Aggregated metrics and analytics
#    - Can query Iceberg tables via external tables or data sharing
# ========================================================================
