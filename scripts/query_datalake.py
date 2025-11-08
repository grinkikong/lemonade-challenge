#!/usr/bin/env python3
"""Query the datalake using DuckDB.

This script demonstrates how to query Parquet files in the datalake using DuckDB.
DuckDB can query Parquet files directly without importing data.

Usage:
    # Query vehicle events
    poetry run python scripts/query_datalake.py --table vehicle_events

    # Query with date filter
    poetry run python scripts/query_datalake.py --table vehicle_events --date 2024-01-15

    # Custom SQL query
    poetry run python scripts/query_datalake.py --sql "SELECT COUNT(*) FROM vehicle_events"
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from prefect_jobs.hourly_ingestion_pipeline.config import DATALAKE_BASE


def query_vehicle_events(date_filter: str = None, limit: int = 10):
    """Query vehicle_events table."""
    import duckdb
    from pathlib import Path
    
    conn = duckdb.connect()
    
    table_path = DATALAKE_BASE / "raw_data" / "vehicle_events"
    
    # Check if Parquet files exist, otherwise use JSON
    parquet_files = list(table_path.glob("**/*.parquet"))
    json_files = list(table_path.glob("**/*.json"))
    
    if parquet_files:
        file_pattern = str(table_path / "**" / "*.parquet")
        query = f"SELECT * FROM read_parquet('{file_pattern}')"
    elif json_files:
        # For JSON files, read them directly and use DuckDB
        import json
        all_records = []
        for json_file in json_files:
            if date_filter and date_filter not in str(json_file):
                continue
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    # Extract records from nested structure
                    records = data.get("vehicles_events", []) or data.get("vehicle_events", [])
                    if not records:
                        records = list(data.values())[0] if data else []
                    all_records.extend(records)
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                continue
        
        if not all_records:
            print("No records found")
            return
        
        # Use DuckDB to query the records
        # Convert to list of dicts and use DuckDB's from_json
        import json as json_module
        json_strings = [json_module.dumps(record) for record in all_records]
        # Create a temporary table
        conn.execute("CREATE TEMP TABLE temp_events AS SELECT unnest(?) as json_data", [json_strings])
        query = """
        SELECT 
            json_extract_string(json_data, '$.vehicle_id') as vehicle_id,
            json_extract_string(json_data, '$.event_time') as event_time,
            json_extract_string(json_data, '$.event_source') as event_source,
            json_extract_string(json_data, '$.event_type') as event_type,
            json_extract_string(json_data, '$.event_value') as event_value,
            json_extract(json_data, '$.event_extra_data') as event_extra_data
        FROM temp_events
        """
    else:
        print(f"No data files found in {table_path}")
        return
    
    if date_filter:
        query += f" WHERE date = '{date_filter}'"
    
    query += f" LIMIT {limit}"
    
    print(f"Query: {query}\n")
    result = conn.execute(query).fetchall()
    columns = conn.execute(query).description
    
    # Print results
    if columns:
        col_names = [col[0] for col in columns]
        print(f"Columns: {', '.join(col_names)}\n")
    
    for row in result:
        print(row)
    
    # Get count
    if parquet_files:
        count_query = f"SELECT COUNT(*) FROM read_parquet('{file_pattern}')"
        if date_filter:
            count_query += f" WHERE date = '{date_filter}'"
    else:
        # For JSON, count from temp table
        count_query = "SELECT COUNT(*) FROM temp_events"
    total = conn.execute(count_query).fetchone()[0]
    print(f"\nTotal records: {total}")
    
    conn.close()


def query_vehicle_status(date_filter: str = None, limit: int = 10):
    """Query vehicle_status table."""
    import duckdb
    from pathlib import Path
    import json
    
    conn = duckdb.connect()
    
    table_path = DATALAKE_BASE / "raw_data" / "vehicle_status"
    
    # Check if Parquet files exist, otherwise use JSON
    parquet_files = list(table_path.glob("**/*.parquet"))
    json_files = list(table_path.glob("**/*.json"))
    
    if parquet_files:
        file_pattern = str(table_path / "**" / "*.parquet")
        query = f"SELECT * FROM read_parquet('{file_pattern}')"
    elif json_files:
        # For JSON files, read them directly and use DuckDB
        all_records = []
        for json_file in json_files:
            if date_filter and date_filter not in str(json_file):
                continue
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    # Extract records from nested structure
                    records = data.get("vehicle_status", []) or data.get("vehicles_status", [])
                    if not records:
                        records = list(data.values())[0] if data else []
                    all_records.extend(records)
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                continue
        
        if not all_records:
            print("No records found")
            return
        
        # Use DuckDB to query the records
        import json as json_module
        json_strings = [json_module.dumps(record) for record in all_records]
        # Create a temporary table
        conn.execute("CREATE TEMP TABLE temp_status AS SELECT unnest(?) as json_data", [json_strings])
        query = """
        SELECT 
            json_extract_string(json_data, '$.vehicle_id') as vehicle_id,
            json_extract_string(json_data, '$.report_time') as report_time,
            json_extract_string(json_data, '$.status_source') as status_source,
            json_extract_string(json_data, '$.status') as status
        FROM temp_status
        """
    else:
        print(f"No data files found in {table_path}")
        return
    
    if date_filter:
        query += f" WHERE date = '{date_filter}'"
    
    query += f" LIMIT {limit}"
    
    print(f"Query: {query}\n")
    result = conn.execute(query).fetchall()
    columns = conn.execute(query).description
    
    # Print results
    if columns:
        col_names = [col[0] for col in columns]
        print(f"Columns: {', '.join(col_names)}\n")
    
    for row in result:
        print(row)
    
    # Get count
    if parquet_files:
        count_query = f"SELECT COUNT(*) FROM read_parquet('{file_pattern}')"
        if date_filter:
            count_query += f" WHERE date = '{date_filter}'"
    else:
        # For JSON, count from temp table
        count_query = "SELECT COUNT(*) FROM temp_status"
    total = conn.execute(count_query).fetchone()[0]
    print(f"\nTotal records: {total}")
    
    conn.close()


def execute_custom_sql(sql: str):
    """Execute custom SQL query."""
    import duckdb
    
    conn = duckdb.connect()
    
    # Replace table names with actual Parquet paths
    sql = sql.replace(
        "vehicle_events",
        f"read_parquet('{DATALAKE_BASE / 'raw_data' / 'vehicle_events' / '**' / '*.parquet'}')"
    )
    sql = sql.replace(
        "vehicle_status",
        f"read_parquet('{DATALAKE_BASE / 'raw_data' / 'vehicle_status' / '**' / '*.parquet'}')"
    )
    
    print(f"Executing SQL:\n{sql}\n")
    result = conn.execute(sql).fetchall()
    columns = conn.execute(sql).description
    
    # Print results
    if columns:
        col_names = [col[0] for col in columns]
        print(f"Columns: {', '.join(col_names)}\n")
    
    for row in result:
        print(row)
    
    conn.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Query datalake using DuckDB")
    parser.add_argument(
        "--table",
        choices=["vehicle_events", "vehicle_status"],
        help="Table to query"
    )
    parser.add_argument(
        "--date",
        help="Filter by date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit number of results (default: 10)"
    )
    parser.add_argument(
        "--sql",
        help="Custom SQL query (use table names: vehicle_events, vehicle_status)"
    )
    
    args = parser.parse_args()
    
    if args.sql:
        execute_custom_sql(args.sql)
    elif args.table == "vehicle_events":
        query_vehicle_events(args.date, args.limit)
    elif args.table == "vehicle_status":
        query_vehicle_status(args.date, args.limit)
    else:
        print("Please specify --table or --sql")
        parser.print_help()


if __name__ == "__main__":
    main()

