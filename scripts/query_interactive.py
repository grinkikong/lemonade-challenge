#!/usr/bin/env python3
"""Interactive SQL query interface for the datalake (similar to Zeppelin).

Query Parquet files using SQL syntax. Type SQL queries and see results.

Usage:
    poetry run python scripts/query_interactive.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from prefect_jobs.hourly_ingestion_pipeline.config import DATALAKE_BASE
import duckdb


def setup_tables(conn):
    """Register Parquet files as tables."""
    # Resolve to absolute path from project root
    # DATALAKE_BASE is relative to project root, not script location
    project_root = Path(__file__).parent.parent
    if Path(DATALAKE_BASE).is_absolute():
        datalake_base = Path(DATALAKE_BASE)
    else:
        datalake_base = project_root / DATALAKE_BASE
    
    # Register vehicle_events
    events_path = datalake_base / "raw_data" / "vehicle_events"
    parquet_files = list(events_path.glob("**/*.parquet"))
    
    if parquet_files:
        try:
            # Use list of file paths - DuckDB can read multiple files
            file_paths = [str(f.resolve()) for f in parquet_files]
            # Create a list string for DuckDB
            file_list = "', '".join(file_paths)
            conn.execute(f"""
                CREATE OR REPLACE VIEW vehicle_events AS 
                SELECT * FROM read_parquet(['{file_list}'])
            """)
            print(f"✓ Registered: vehicle_events ({len(parquet_files)} file(s))")
        except Exception as e:
            print(f"⚠ vehicle_events: {e}")
            # Fallback: try with first file to see the error
            if parquet_files:
                print(f"   Trying single file: {parquet_files[0]}")
    else:
        print(f"⚠ vehicle_events: No Parquet files found in {events_path}")
    
    # Register vehicle_status
    status_path = datalake_base / "raw_data" / "vehicle_status"
    parquet_files = list(status_path.glob("**/*.parquet"))
    
    if parquet_files:
        try:
            # Use list of file paths - DuckDB can read multiple files
            file_paths = [str(f.resolve()) for f in parquet_files]
            # Create a list string for DuckDB
            file_list = "', '".join(file_paths)
            conn.execute(f"""
                CREATE OR REPLACE VIEW vehicle_status AS 
                SELECT * FROM read_parquet(['{file_list}'])
            """)
            print(f"✓ Registered: vehicle_status ({len(parquet_files)} file(s))")
        except Exception as e:
            print(f"⚠ vehicle_status: {e}")
            # Fallback: try with first file to see the error
            if parquet_files:
                print(f"   Trying single file: {parquet_files[0]}")
    else:
        print(f"⚠ vehicle_status: No Parquet files found in {status_path}")


def main():
    """Interactive SQL query interface."""
    print("=" * 60)
    print("Datalake Interactive SQL Query (DuckDB)")
    print("=" * 60)
    print(f"Datalake: {DATALAKE_BASE}")
    print("\nType SQL queries (or 'exit' to quit):")
    print("Examples:")
    print("  SELECT * FROM vehicle_events LIMIT 10;")
    print("  SELECT COUNT(*) FROM vehicle_events;")
    print("  SELECT event_type, COUNT(*) FROM vehicle_events GROUP BY event_type;")
    print("-" * 60)
    
    conn = duckdb.connect()
    setup_tables(conn)
    print("-" * 60)
    
    while True:
        try:
            query = input("\nSQL> ").strip()
            
            if not query:
                continue
            
            if query.lower() in ['exit', 'quit', 'q']:
                print("Goodbye!")
                break
            
            if query.lower() == 'help':
                print("\nAvailable commands:")
                print("  SELECT * FROM vehicle_events LIMIT 10;")
                print("  SELECT * FROM vehicle_status LIMIT 10;")
                print("  SELECT COUNT(*) FROM vehicle_events;")
                print("  exit - quit the interface")
                continue
            
            # Execute query
            result = conn.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in result.description] if result.description else []
            
            # Fetch results
            rows = result.fetchall()
            
            # Display results
            if columns:
                # Print header
                print("\n" + " | ".join(columns))
                print("-" * 80)
                
                # Print rows
                for row in rows:
                    print(" | ".join(str(val) for val in row))
                
                print(f"\n({len(rows)} row(s))")
            else:
                print("Query executed successfully.")
                
        except (KeyboardInterrupt, EOFError):
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")
    
    conn.close()


if __name__ == "__main__":
    main()

