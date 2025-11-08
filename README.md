# Lemonade Data Engineering Challenge

Data ingestion pipeline for vehicle events and status data, following a "Prefect style" architecture. The pipeline handles unbounded data streams, processes files from S3 (or local folders for development), and generates daily summaries.

## Architecture

### Data Flow

```
Incoming Files
    ↓
[Task 1: File Discovery from incoming folder] → Move to processing/ or failed/
    ↓
[Task 2: Spark Processing] → Read from processing/, validate, write to Parquet datalake
    ↓
[Task 3: Cleanup] → Move successfully processed files to processed/
    ↓
Parquet Datalake (partitioned by date)
    ↓
[Daily Summary Job] → Aggregate and create daily summary tables
```

### Technology Stack

- **Orchestration**: Prefect 2.x
- **Processing**: Apache Spark (PySpark) - local mode for development
- **Storage**: 
  - Local: Parquet files in filesystem
  - Production: Iceberg tables in S3 (planned)
- **Metadata**: SQLite (local) / PostgreSQL (production)
- **Query Engine**: DuckDB for local exploration
- **Language**: Python 3.11

## Project Structure

```
lemonade-challenge/
├── prefect_jobs/                    # Prefect job definitions
│   ├── files_ingester/              # Files ingestion pipeline
│   │   ├── main.py                  # Main flow definition
│   │   ├── config.py                # Job configuration
│   │   ├── tasks/                   # Prefect tasks
│   │   │   ├── file_discovery.py    # Task 1: Discover and move files
│   │   │   ├── spark_processing.py  # Task 2: Process with Spark
│   │   │   └── cleanup.py           # Task 3: Cleanup processed files
│   │   └── utils/                    # Utility functions
│   │       └── spark_batch_ingestion.py
│   └── daily_summary/                # Daily summary aggregation
│       ├── main.py                  # Main flow definition
│       ├── config.py                # Job configuration
│       └── utils/                   # Utility functions
│           └── spark_aggregator.py
├── data_utils/                       # Shared utilities
│   ├── aws/                         # AWS/S3 drivers
│   │   └── s3_driver.py            # Unified S3/local file operations
│   └── database/                    # Database drivers
│       ├── drivers.py              # SQLite/PostgreSQL driver
│       └── models.py                # SQLAlchemy models
├── scripts/                         # Utility scripts
│   ├── create_tables.py             # Create table schemas and DuckDB views
│   ├── generate_data.py             # Generate test data files
│   └── backfill_daily_summary.py    # Backfill historical daily summaries (TODO)
├── data/                            # Local data storage
│   ├── source_data/                 # Source data folders
│   │   ├── incoming/               # New files arrive here
│   │   ├── processing/            # Files being processed
│   │   ├── failed/                 # Failed files (with error details)
│   │   └── processed/              # Successfully processed files
│   └── datalake/                    # Parquet datalake
│       ├── raw_data/                # Raw ingested data
│       │   ├── vehicle_events/     # Partitioned by date
│       │   └── vehicle_status/      # Partitioned by date
│       └── reports/                 # Reports (aggregated/transformed data)
│           └── daily_summary/       # Daily aggregated summaries
├── .env                             # Environment variables (local, gitignored)
├── .env.example                     # Environment variables template
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
3. **DuckDB CLI**: Install with:
   ```bash
   brew install duckdb
   ```
4. **Java Development Kit (JDK)**: Required for PySpark. Install OpenJDK 21 (LTS) with:
   ```bash
   brew install openjdk@21
   # Set JAVA_HOME permanently (add to ~/.zshrc or ~/.bashrc)
   echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@21"' >> ~/.zshrc
   echo 'export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc # Apply changes to current session
   ```
   
   **Note**: Ensure `JAVA_HOME` is set in your environment. Java must be installed and accessible.

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

#### Step 2: Configure Environment Variables

```bash
# Copy the example .env file (if not already exists)
cp .env.example .env

# Edit .env to customize settings (optional - defaults work for local dev)
# The .env file is automatically loaded by the application
```

#### Step 3: Set Up Prefect Server and Worker

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

#### Step 4: Initialize Test Data

```bash
# Generate test data and set up metadata (cleans all existing data except DB first)
poetry run python scripts/generate_data.py --num-files 5 --cleanup --clean-datalake
```

This will:
- Clean all existing data folders (`data/source_data/incoming`, `processing`, `failed`, `processed`)
- Clean the datalake (`data/datalake`)
- **Keep the database** (`~/data/lemonade/metadata.db`) and its metadata
- Create fresh database tables (if not exist)
- Set up active metadata for `vehicle_events` and `vehicle_status`
- Generate 5 test files of each type (10 files total) in `./data/source_data/incoming/`

#### Step 5: Verify Setup

1. **Check Prefect UI**: Open http://127.0.0.1:4200
   - You should see both deployments: `files_ingester` and `daily_summary`
   - Schedules should be active

2. **Manually trigger a test run** (optional):
   ```bash
   poetry run prefect deployment run 'files_ingester_flow/files_ingester'
   ```

3. **Check generated files**:
   ```bash
   ls -la ./data/source_data/incoming/*.json
   ls -la ./data/source_data/processed/*/
   ls -la ./data/datalake/raw_data/*/
   ```

4. **Query the datalake** (verify Parquet files were created):
   ```bash
   # After first data ingestion, create DuckDB catalog:
   poetry run python scripts/create_tables.py
   
   # Query the datalake using DuckDB UI (opens in browser)
   duckdb data/datalake/lemonade.duckdb -ui
   
   # Or use DuckDB CLI (if installed: brew install duckdb)
   duckdb data/datalake/lemonade.duckdb
   # Then type: SELECT * FROM raw_data.vehicle_events LIMIT 10;
   ```

### Quick Start (After Installation)

Once you've completed the setup steps above, the pipeline is ready to run:

**Scheduled jobs** will run automatically:
- Hourly ingestion pipeline: Every hour at :00
- Daily summary aggregation: Daily at 1:00 AM UTC (with 2-day lookback and event-time filtering)

**Manual execution**:
```bash
# Run files ingestion pipeline manually
poetry run python prefect_jobs/files_ingester/main.py

# Or trigger via deployment
poetry run prefect deployment run 'files_ingester_flow/files_ingester'

# Run daily summary for a specific date
poetry run python prefect_jobs/daily_summary/main.py 2025-11-08

# Or trigger via deployment
poetry run prefect deployment run 'daily_summary_aggregation_flow/daily_summary'
```

**Backfill historical data**:
For historical data outside the 2-day lookback window, use the backfill script:
```bash
# Backfill daily summaries for a date range
poetry run python scripts/backfill_daily_summary.py --start-date 2025-01-01 --end-date 2025-11-07
```

**Generate more test data**:
```bash
# Generate 10 files of each type
poetry run python scripts/generate_data.py --num-files 10

# Generate continuously every 10 seconds
poetry run python scripts/generate_data.py --interval 10 --continuous

# Skip cleanup (add to existing data)
poetry run python scripts/generate_data.py --num-files 5 --no-cleanup
```

## Configuration

Configuration is managed through environment variables (see `.env.example`):

- `ENV`: Environment (`local` or `production`)
- `DATABASE_URL`: Database connection string
- `SOURCE_DATA_BASE`: Base path for source data (default: `./data/source_data`)
- `DATALAKE_BASE`: Base path for datalake (default: `./data/datalake`)
- `SPARK_MASTER`: Spark master URL (default: `local[*]`)
- `SPARK_APP_NAME`: Spark application name
- `AWS_REGION`: AWS region (production)
- `S3_BUCKET`: S3 bucket name (production)

**Note**: Ensure `JAVA_HOME` is set in your environment. Java is required for Spark to run.

## File Formats

### Supported File Types

The system supports **dynamic file types** managed through the `table_metadata` table in SQLite (this project). File type definitions (schema structure and processing configuration) are stored in the database, allowing new file types to be added without code changes.

**Currently supported file types (v1 - hardcoded):**
- `vehicle_events`: Vehicle event data
- `vehicle_status`: Vehicle status data

**TODO v2**: Reintroduce dynamic metadata table support for full schema validation and dynamic file type management.

### Vehicle Events Format

```json
{
  "vehicles_events": [
    {
      "vehicle_id": "V123",
      "event_time": "2025-11-08T10:30:00Z",
      "event_source": "sensor",
      "event_type": "acceleration"
    }
  ]
}
```

### Vehicle Status Format

```json
{
  "vehicles_status": [
    {
      "vehicle_id": "V123",
      "report_time": "2025-11-08T10:30:00Z",
      "status_source": "api",
      "status": "active"
    }
  ]
}
```

## Prefect Jobs

### Files Ingestion Pipeline (`files_ingester`)

**Schedule**: Every hour at :00

**Tasks**:
1. **File Discovery** (`discover_and_move_files`): Scans `incoming/` folder, detects file types, moves to `processing/` or `failed/`
2. **Spark Processing** (`process_all_folders_with_spark`): Processes all files in `processing/` folders, validates schemas, writes to Parquet datalake
3. **Cleanup** (`cleanup_processed_files`): Moves successfully processed files from `processing/` to `processed/`

**After first ingestion**: Run `poetry run python scripts/create_tables.py` to create DuckDB views. Views will automatically discover new Parquet files as they are created.

**Entry Point**: `prefect_jobs/files_ingester/main.py:files_ingester_flow`

### Daily Summary Aggregation (`daily_summary`)

**Schedule**: Daily at 1:00 AM UTC (runs after 00:00 UTC to process the day that just ended)

**Overview**: 
The daily summary job processes events with a **2-day lookback window** and **event-time filtering** to handle late-arriving data. It reads from the last 2 days of partitions but filters events by their actual `event_time` field (not partition date), ensuring late data is captured correctly.

**How it works**:
1. **Reads 2 days back**: Reads partitions for `date={day_before_yesterday}` and `date={yesterday}`
2. **Event-time filtering**: Filters events where `event_time` is between 2 days ago 00:00 UTC and yesterday 23:59:59 UTC
3. **Partition by event date**: Summary table is partitioned by `day` (derived from `event_time`), not processing date
4. **Merge/upsert logic**: If late data arrives, existing summaries for previous days are updated (not overwritten)

**Tasks**:
1. **Create Daily Summary** (`create_daily_summary`): 
   - Reads from last 2 days of partitions
   - Filters by `event_time` field
   - Aggregates to get last event per vehicle per day
   - Creates/updates summaries partitioned by event date
   - Summary columns:
     - `vehicle_id`
     - `day` (from `event_time`, not partition date)
     - `last_event_time`
     - `last_event_type`

**Key Assumptions**:
1. **Late data updates**: Reports for previous days may be updated when late data arrives (within 48-hour window)
2. **Max 48-hour delay**: Assumes maximum 48 hours delay for late data (2-day lookback is sufficient)
3. **Historical backfill**: Historical data outside the 2-day window requires a separate backfill process (see Backfill section below)

**Example Scenario**:
- Event from 2025-11-08 23:30 arrives at 2025-11-09 02:00 (in partition `date=2025-11-09`)
- Job runs at 2025-11-09 01:00 UTC
- Reads partitions: `date=2025-11-07` and `date=2025-11-08`
- Filters: `event_time` between 2025-11-07 00:00 and 2025-11-08 23:59:59
- **Result**: Event is included in summary for `day=2025-11-08` (based on `event_time`)

**Entry Point**: `prefect_jobs/daily_summary/main.py:daily_summary_aggregation_flow`

**Backfill Process**:
For historical data that doesn't fall within the 2-day lookback window, a separate backfill process is required. The backfill script (`scripts/backfill_daily_summary.py`) processes a date range and creates/updates summaries for each day, reading from all relevant partitions and filtering by `event_time`.

**Usage**:
```bash
# Backfill summaries for a date range
poetry run python scripts/backfill_daily_summary.py --start-date 2025-01-01 --end-date 2025-11-07
```

**Note**: The backfill process uses the same aggregation logic as the daily job but processes multiple days in sequence, making it suitable for initial data loads or reprocessing historical periods.

## Data Lake Structure

**Note**: This implementation uses local Parquet files with DuckDB views for **development/demonstration purposes only**. In production, data is stored as **Parquet files in S3** (same format), with **table schemas predefined in AWS Glue Catalog as Iceberg tables** for better scalability, ACID transactions, and schema evolution.

### Raw Data (`raw_data`)

- **Location**: `data/datalake/raw_data/` (local) or `s3://lemonade-datalake/raw_data/` (production)
- **Format**: Parquet files (same format in dev and production), partitioned by date
- **Table Definition**: Local DuckDB views (dev) or Iceberg tables in AWS Glue Catalog (production)
- **Tables**:
  - `vehicle_events/`: Partitioned by `date` column
  - `vehicle_status/`: Partitioned by `date` column

### Reports (`reports`)

- **Location**: `data/datalake/reports/` (local) or `s3://lemonade-datalake/reports/` (production)
- **Format**: Parquet files (same format in dev and production), partitioned by day
- **Table Definition**: Local DuckDB views (dev) or Iceberg tables in AWS Glue Catalog (production)
- **Tables**:
  - `daily_summary/`: Daily aggregated summaries, partitioned by `day` (derived from `event_time`, not processing date)

## Querying the Data Lake

**Note**: The DuckDB approach below is for **development/demonstration only**. In production, Parquet files in S3 are managed as Iceberg tables in AWS Glue Catalog (with predefined schemas) and queried directly via Spark, Athena, or other query engines.

### Using DuckDB (Development/Demo Only)

1. **Set up the catalog** (run after first data ingestion):
   ```bash
   poetry run python scripts/create_tables.py
   ```
   
   **Note**: Run this script after your first data ingestion to create DuckDB views. The views will automatically discover new Parquet files as they are created.

2. **Query using DuckDB UI**:
   ```bash
   duckdb data/datalake/lemonade.duckdb -ui
   ```

3. **Query using DuckDB CLI**:
   ```bash
   duckdb data/datalake/lemonade.duckdb
   ```
   Then run SQL queries:
   ```sql
   SELECT * FROM raw_data.vehicle_events LIMIT 10;
   SELECT * FROM raw_data.vehicle_status LIMIT 10;
   SELECT * FROM reports.daily_summary WHERE day = '2025-11-08';
   ```

**Note**: The `daily_summary` table is partitioned by `day` (derived from `event_time`), not by processing date. This ensures that late-arriving data is correctly associated with the day the event actually occurred, not when it was processed.

## Troubleshooting

### Java/Spark Issues

**Error**: `Java gateway process exited before sending its port number`

**Solution**:
1. Verify Java is installed: `java -version`
2. Ensure `JAVA_HOME` is set. If it's not set:
   - Ensure Java is installed: `brew install openjdk@21`
   - If Java is in a non-standard location, set `JAVA_HOME` manually:
     ```bash
     export JAVA_HOME="/path/to/your/java"
     export PATH="$JAVA_HOME/bin:$PATH"
     ```
3. For PyCharm/IDE: The automatic detection should work, but you can also set `JAVA_HOME` in Run Configuration → Environment Variables if needed

### Prefect UI Not Accessible

**Error**: Cannot connect to Prefect UI at http://127.0.0.1:4200

**Solution**:
1. Ensure Prefect server is running: `poetry run prefect server start`
2. Check if port 4200 is already in use
3. Access the UI in a browser at http://127.0.0.1:4200

### Files Not Processing

**Check**:
1. Files are in `data/source_data/incoming/` folder
2. File names match expected patterns (`vehicles_events_*.json` or `vehicles_status_*.json`)
3. Files are valid JSON
4. Check `data/source_data/failed/` for error details

### DuckDB Schema Errors

**Error**: `types don't match` or `Binder Error`

**Solution**:
1. Regenerate the DuckDB catalog: `poetry run python scripts/create_tables.py`
2. Ensure Parquet files exist and are valid
3. Check for old Parquet files with different schemas (delete if found)

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black .
poetry run ruff check .
```

### Adding New File Types

**v1 (Current)**: File types are hardcoded in `prefect_jobs/files_ingester/config.py` in the `FILE_TYPE_CONFIG` dictionary.

**v2 (Planned)**: Dynamic file type support via metadata table:
1. Add new file type to `table_metadata` table
2. Configure schema definition and processing config
3. System automatically detects and processes new file type

## Production Deployment

### Architecture

- **Metadata**: PostgreSQL
- **Raw Events**: Iceberg tables in S3
- **Aggregation Layer**: Snowflake
- **Orchestration**: Prefect Cloud or self-hosted Prefect server
- **Compute**: EMR clusters for Spark processing (or Kubernetes with Spark operator)

### Environment Variables

Set the following in your production environment:

```bash
ENV=production
DATABASE_URL=postgresql://user:pass@host:5432/dbname
DATALAKE_BASE=s3://lemonade-datalake
S3_BUCKET=lemonade-datalake
AWS_REGION=us-east-1
ICEBERG_CATALOG_URI=...
ICEBERG_WAREHOUSE=s3://lemonade-datalake/warehouse
```

## License

This project is part of the Lemonade Data Engineering Challenge.
