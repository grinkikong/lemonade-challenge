# Lemonade Data Ingestion Pipeline

A scalable data ingestion pipeline using Prefect as scheduler, Spark for heavy lifting of mass events for each chunk of files, and Iceberg tables as datalake. Currently supports `vehicles_events_[time].json` and `vehicles_status_[time].json` file types, and can be extended to support any new file types through the `table_metadata` table in SQLite (this project). No code changes required to add new file types - just register metadata in the database.

**Note:** This project uses SQLite only for local development. In production:
- **Metadata**: PostgreSQL
- **Raw events**: Iceberg tables in S3
- **Aggregation layer**: Snowflake

## Architecture Overview

The pipeline consists of two Prefect jobs that work together to ingest, process, and aggregate vehicle data:

### 1. Hourly Ingestion Pipeline (Combined Job with 2 Dependent Tasks)
- **Schedule**: Every hour at :00
- **Task 1 - New File Type Discovery**:
  - Scans incoming folder for new JSON files
  - Detects file types from filenames (pattern: `{file_type}_{timestamp}.json`)
  - Creates inactive metadata records in `table_metadata` for any new file types not yet registered
  - Admin can then configure `schema_definition` and `processing_config` and set status to "active"
- **Task 2 - Spark Processing** (depends on Task 1):
  - Automatically discovers all folders in processing directory
  - Loops through each folder and processes all files
  - Retrieves table metadata (schema and processing config) from database
  - Submits Spark jobs (EMR or local) for each file using metadata
  - Writes processed data to datalake (S3 as Parquet files in Iceberg format)
  - Moves successfully processed files to processed/ folder
  - Moves failed files to failed/ folder

### 2. Daily Summary Aggregation (Job)
- **Schedule**: Daily at 1:00 AM UTC
- **Function**: Creates daily summary table in Iceberg format
- **Actions**:
  - Aggregates data from vehicle_events table
  - Creates summary table with: vehicle_id, day, last_event_time, last_event_type
  - Managed by AWS Glue Catalog

## Data Flow

```
Incoming Files (JSON) in incoming/
    ↓
Hourly Pipeline - Task 1: File Discovery & Movement
    ↓
Validate metadata → Move to source_data/processing/ or source_data/failed/
    ↓
Hourly Pipeline - Task 2: Auto-discover folders & Spark Processing
    ↓
Datalake (Parquet files)
    ↓
Daily Job: Summary Aggregation
    ↓
Summary Table (Iceberg)
```

## Project Structure

```
lemonade-challenge/
├── prefect_jobs/                    # Prefect job definitions
│   ├── hourly_ingestion_pipeline/  # Combined job with 2 tasks
│   │   ├── main.py                 # Task 1: File discovery, Task 2: Spark processing
│   │   ├── config.py
│   │   └── utils.py
│   └── daily_summary_aggregation/   # Daily summary job
│       ├── main.py
│       ├── config.py
│       └── utils.py
├── data_utils/                      # Shared utilities
│   ├── database/                    # Database drivers and models
│   │   ├── drivers.py
│   │   └── models.py
│   └── aws/                         # AWS/S3 drivers
│       └── s3_driver.py
├── data/                            # Data folders
│   ├── incoming/                    # All incoming files land here
│   ├── source_data/                 # Processing and error handling
│   │   ├── processing/              # Files being processed
│   │   │   ├── vehicle_events/
│   │   │   └── vehicle_status/
│   │   ├── failed/                  # Failed files
│   │   └── error/                   # Schema validation errors
│   ├── processed/                   # Successfully processed files
│   └── datalake/                    # Processed data (Parquet format)
│       └── raw_data/
│           ├── vehicle_events/      # Partitioned by date
│           └── vehicle_status/      # Partitioned by date
├── scripts/                         # Utility scripts
│   ├── create_tables.py
│   └── generate_test_data.py
├── prefect.yaml                     # Prefect deployments
├── pyproject.toml                   # Poetry dependencies
└── README.md
```

## Setup Instructions (macOS)

### Prerequisites

1. **Python 3.9+ (3.11 recommended)**: Check with `python3 --version`
2. **Poetry**: Install with:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

### Step-by-Step Installation

#### Step 1: Install Dependencies with Poetry

```bash
# Install Poetry if not already installed
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies (creates virtual environment automatically)
poetry install

# Activate the virtual environment
poetry shell
```

#### Step 2: Set Up Prefect Server and Worker

In separate terminal windows/tabs, run:

**Terminal 1 - Start Prefect Server:**
```bash
poetry run prefect server start
```
The Prefect UI will be available at: **http://127.0.0.1:4200**

**Terminal 2 - Start Prefect Worker:**
```bash
# Create work pool (if not exists)
poetry run prefect work-pool create --type process default-agent-pool

# Start worker to execute scheduled jobs
poetry run prefect worker start --pool default-agent-pool
```

**Terminal 3 - Deploy Scheduled Jobs:**
```bash
# Deploy all scheduled jobs
poetry run prefect deploy --all
```

#### Step 3: Initialize Test Data

```bash
# Generate test data and set up metadata (cleans all existing data first)
poetry run python scripts/generate_test_data.py --num-files 5
```

This will:
- Clean all existing data (incoming, source_data, processed, datalake folders)
- Clean the database
- Create fresh database tables
- Set up active metadata for `vehicle_events` and `vehicle_status`
- Generate 5 test files of each type (10 files total) in `./data/incoming/`

#### Step 4: Verify Setup

1. **Check Prefect UI**: Open http://127.0.0.1:4200
   - You should see both deployments: `hourly_ingestion_pipeline` and `daily_summary_aggregation`
   - Schedules should be active

2. **Manually trigger a test run** (optional):
   ```bash
   poetry run prefect deployment run 'hourly_ingestion_pipeline_flow/hourly_ingestion_pipeline'
   ```

3. **Check generated files**:
   ```bash
   ls -la ./data/incoming/*.json
   ls -la ./data/processed/*/
   ls -la ./data/datalake/raw_data/*/
   ```

4. **Query the datalake** (verify Parquet files were created):
   ```bash
   # Interactive SQL CLI
   poetry run python scripts/query_interactive.py
   # Then type: SELECT * FROM vehicle_events LIMIT 10;
   ```

### Quick Start (After Installation)

Once you've completed the setup steps above, the pipeline is ready to run:

**Scheduled jobs** will run automatically:
- Hourly ingestion pipeline: Every hour at :00
- Daily summary aggregation: Daily at 1:00 AM UTC

**Manual execution**:
```bash
# Run hourly pipeline manually
poetry run python prefect_jobs/hourly_ingestion_pipeline/main.py

# Or trigger via deployment
poetry run prefect deployment run 'hourly_ingestion_pipeline_flow/hourly_ingestion_pipeline'
```

**Generate more test data**:
```bash
# Generate 10 files of each type
poetry run python scripts/generate_test_data.py --num-files 10

# Generate continuously every 10 seconds
poetry run python scripts/generate_test_data.py --interval 10 --continuous

# Skip cleanup (add to existing data)
poetry run python scripts/generate_test_data.py --num-files 5 --no-cleanup
```

## Configuration

Configuration is managed through environment variables (see `.env.example`):

- `ENV`: Environment (`local` or `production`)
- `DATABASE_URL`: Database connection string
- `INCOMING_FOLDER`: Path to incoming folder (default: `./data/incoming`)
- `SOURCE_DATA_BASE`: Base path for source data (default: `./data/source_data`)
- `PROCESSED_FOLDER`: Path to processed folder (default: `./data/processed`)
- `DATALAKE_BASE`: Base path for datalake
- `ICEBERG_CATALOG_URI`: Iceberg catalog URI (production)
- `ICEBERG_WAREHOUSE`: Iceberg warehouse path (production)
- `S3_BUCKET`: S3 bucket name (production)

## File Formats

### Supported File Types

The system supports **dynamic file types** managed through the `table_metadata` table in SQLite (this project). File type definitions (schema structure and processing configuration) are stored in the database, allowing new file types to be added without code changes.

**Note:** In production, metadata would be stored in PostgreSQL.

#### Current File Types

The system currently has 2 file types configured in the `table_metadata` table:

**vehicles_events Files**
- **Pattern**: `vehicles_events_YYYYMMDDHHMMSS.json`
- **Content**: JSON array with event records
- **Fields**: vehicle_id, event_time, event_source, event_type, event_value, event_extra_data
- **Managed in**: `table_metadata` table (table_name: "vehicle_events", file_type: "vehicle_events")

**vehicles_status Files**
- **Pattern**: `vehicles_status_YYYYMMDDHHMMSS.json`
- **Content**: JSON array with status records
- **Fields**: vehicle_id, report_time, status_source, status
- **Managed in**: `table_metadata` table (table_name: "vehicle_status", file_type: "vehicle_status")

### Adding New File Types Dynamically

The system is designed to support any new file type with different payloads. To add a new file type:

1. **Register table metadata in database**:
   ```python
   from data_utils.database.drivers import db_driver
   
   # Define schema for your new file type
   new_schema = {
       "fields": [
           {"name": "id", "type": "string", "nullable": False},
           {"name": "timestamp", "type": "timestamp", "nullable": False},
           {"name": "data", "type": "string", "nullable": True},
           # ... add your fields
       ],
       "primary_key": ["id", "timestamp"],
   }
   
   # Define processing config
   processing_config = {
       "partition_by": "date",
       "format": "iceberg",
       "write_mode": "append",
   }
   
   # Register metadata
   db_driver.get_or_create_table_metadata(
       table_name="new_table_name",
       file_type="new_file_type",
       schema_definition=new_schema,
       partition_column="date",
       processing_config=processing_config,
       description="Description of new file type",
   )
   ```

2. **File naming**:
   - File name should contain the `file_type` string (e.g., `new_file_type_20240115120000.json`)
   - System automatically detects file type by matching filename with registered types
   - Processing folder `data/source_data/processing/new_file_type/` will be created automatically

3. **That's it!** The system will automatically:
   - Discover files with the new file type (by matching filename with registered types)
   - Move them to the appropriate processing folder (created automatically)
   - Retrieve schema and processing config from `table_metadata` table
   - Process them with Spark using the defined schema and config

**No code changes required** - just register new file type metadata in the database and the system will handle it automatically.

## Querying the Datalake

After running the pipeline, you can query the Parquet files in the datalake using the interactive SQL CLI:

```bash
poetry run python scripts/query_interactive.py
```

**Example queries:**
```sql
SQL> SELECT * FROM vehicle_events LIMIT 10;
SQL> SELECT COUNT(*) FROM vehicle_events;
SQL> SELECT event_type, COUNT(*) FROM vehicle_events GROUP BY event_type;
SQL> SELECT * FROM vehicle_status WHERE status = 'driving';
SQL> exit
```

**Available tables:**
- `vehicle_events` - All vehicle events from Parquet files
- `vehicle_status` - All vehicle statuses from Parquet files

The CLI automatically:
- Finds all Parquet files in the datalake
- Registers them as SQL tables
- Allows you to query them with standard SQL

**Alternative query methods:**
- **Spark SQL**: `poetry run python scripts/query_datalake_spark.py --table vehicle_events`
- **DuckDB CLI**: `poetry run python scripts/query_datalake.py --table vehicle_events`

## Database Access

The project uses a global `db_driver` instance for database access:

```python
from data_utils.database.drivers import db_driver
from data_utils.database.models import TableMetadata

# Use with context manager
with db_driver.session() as session:
    metadata = session.query(TableMetadata).filter(...).first()
```

**Database location**: `~/data/lemonade/metadata.db` (SQLite)

## Database Schema

### Metadata Table (SQLite - this project only)

**table_metadata**: Stores table schema definitions and Spark processing configuration
- table_name: Name of the table (e.g., "vehicle_events", "vehicle_status")
- file_type: Type of file (vehicle_events or vehicle_status)
- partition_column: Column name for partitioning (default: "date")
- schema_definition: JSON schema defining the structure/fields for Spark processing
- processing_config: JSON configuration for Spark processing (partition_by, format, write_mode, etc.)
- description: Optional description of the table

Each row represents metadata for a table name, defining how Spark should process the payload/events.

**Dynamic Support**: New file types can be added by inserting a new row in this table with:
- Unique `table_name` and `file_type`
- `schema_definition` describing the payload structure
- `processing_config` for Spark processing options

The system will automatically discover and process files for any registered file type without code changes.

### Datalake Tables (Iceberg)

**vehicle_events**: Partitioned by date (date=YYYY-MM-DD/)
- vehicle_id, event_time, event_source, event_type, event_value, event_extra_data

**vehicle_status**: Partitioned by date (date=YYYY-MM-DD/)
- vehicle_id, report_time, status_source, status

**daily_summary**: Daily aggregation table
- vehicle_id, day, last_event_time, last_event_type

## Production Considerations

### Spark Integration
- In production, Spark jobs would be submitted to EMR or Spark cluster
- Current implementation simulates Spark processing for local development
- See `prefect_jobs/hourly_ingestion_pipeline/utils.py` for Spark job submission example

### Iceberg Tables
- Production uses PyIceberg with AWS Glue Catalog
- Tables are partitioned by date for efficient queries
- Supports ACID transactions, schema evolution, and time travel

### S3 Integration
- Local development uses filesystem
- Production uses boto3 for S3 operations
- Files are organized in S3 with prefixes matching folder structure

## Troubleshooting

### Database Connection Issues
- Ensure SQLite database file path is writable
- Check SQLite database file exists (this project only)
- In production: Would check PostgreSQL connection for metadata
- Verify database exists and user has permissions

### File Processing Failures
- Check file format matches expected JSON structure
- Verify file naming pattern
- Check database metadata table for error messages
- Inspect failed/ folder for problematic files

### Prefect Issues
- Ensure Prefect server is running (if using agent)
- Check Prefect logs for errors
- Verify flow deployment with `prefect deployment ls`

## Assumptions

1. **File Naming**: Files follow pattern `{type}_{timestamp}.json` where timestamp is `YYYYMMDDHHMMSS`
   - For new file types, file name should contain the `file_type` string for automatic detection
2. **JSON Structure**: Files contain root keys matching the file type (e.g., `vehicles_events`, `vehicle_status`)
   - For new file types, define the structure in `schema_definition` when registering metadata
3. **Dynamic File Types**: New file types are supported by registering metadata in `table_metadata` table
   - System automatically discovers and processes any registered file type
4. **Partitioning**: Data is partitioned by date extracted from filename timestamp (configurable per table)
5. **Processing Order**: Files can arrive out of chronological order (retroactive processing supported)
6. **Local Development**: Uses SQLite and local filesystem
7. **Production**: Would use PostgreSQL for metadata, Iceberg tables in S3 for raw events, and Snowflake for aggregation layer (this project uses SQLite only)

## Alerts and Monitoring

### Failed File Alerts

The system logs alerts when files fail processing and are moved to the `failed/` folder:

- **Individual File Failures**: Each failed file logs an ERROR alert with file details, error message, and destination path
- **Batch Failures**: Summary alerts are logged when a folder or pipeline run completes with failures
- **Alert Details Include**:
  - File name and type
  - Error message or exception
  - Destination path in failed folder
  - Processing statistics (processed vs failed counts)

### Future Improvements for Error Handling

**TODO: Per-File Error Handling Instead of Failing Entire Chunks**

Currently, if a file fails, it's moved to the failed folder and processing continues. Future improvements should include:

1. **Granular Error Classification**:
   - Categorize errors by type (schema mismatch, data quality, parsing errors, etc.)
   - Different handling strategies per error type
   - Retry logic for transient errors

2. **Partial Success Handling**:
   - Process valid records from a file even if some records fail
   - Move only failed records to a separate error table/partition
   - Continue processing remaining files in chunk even if one fails

3. **Error Recovery**:
   - Automatic retry for transient errors (network, temporary Spark issues)
   - Dead letter queue for permanently failed files
   - Manual reprocessing capability for failed files

4. **Enhanced Alerting**:
   - Integration with monitoring systems (PagerDuty, Slack, email)
   - Alert severity levels based on failure rate
   - Dashboard for tracking failure patterns
   - Automated escalation for critical failures

5. **Error Analysis**:
   - Track error patterns by file type, time, or source
   - Identify systemic issues vs one-off failures
   - Generate reports for data quality monitoring

## What Would Be Done Differently with More Time

1. **Full Spark Integration**: Complete Spark job submission to EMR/cluster
2. **Iceberg Implementation**: Full PyIceberg integration with Glue Catalog
3. **Per-File Error Handling**: Implement granular error handling as described above
4. **Testing**: Comprehensive unit and integration tests
5. **Monitoring**: Metrics, dashboards, and alerting
6. **Error Recovery**: Automatic retry logic and dead letter queues
7. **Data Validation**: Schema validation and data quality checks
8. **Performance Optimization**: Connection pooling, query optimization
9. **CI/CD**: Automated testing and deployment pipelines
10. **Documentation**: API documentation and architecture diagrams
11. **Scalability**: Distributed processing with Dask/Kubernetes

## License

This project is part of the Lemonade Data Engineering Challenge.
