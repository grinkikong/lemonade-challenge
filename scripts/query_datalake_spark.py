#!/usr/bin/env python3
"""Query the datalake using Spark SQL.

This script demonstrates how to query Parquet files in the datalake using Spark SQL.

Usage:
    # Query vehicle events
    poetry run python scripts/query_datalake_spark.py --table vehicle_events

    # Query with date filter
    poetry run python scripts/query_datalake_spark.py --table vehicle_events --date 2024-01-15

    # Custom SQL query
    poetry run python scripts/query_datalake_spark.py --sql "SELECT COUNT(*) FROM vehicle_events"
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from prefect_jobs.hourly_ingestion_pipeline.config import DATALAKE_BASE, SPARK_MASTER, SPARK_APP_NAME


def query_with_spark(table_name: str, date_filter: str = None, limit: int = 10):
    """Query Parquet files using Spark SQL."""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName(f"{SPARK_APP_NAME}-query") \
            .master(SPARK_MASTER) \
            .getOrCreate()
        
        table_path = DATALAKE_BASE / "raw_data" / table_name
        parquet_path = str(table_path)
        
        # Read Parquet files
        df = spark.read.parquet(parquet_path)
        
        # Create temporary view
        df.createOrReplaceTempView(table_name)
        
        # Build query
        query = f"SELECT * FROM {table_name}"
        if date_filter:
            query += f" WHERE date = '{date_filter}'"
        query += f" LIMIT {limit}"
        
        print(f"Query: {query}\n")
        result = spark.sql(query)
        
        # Show results
        result.show(truncate=False)
        
        # Get count
        count_query = f"SELECT COUNT(*) as total FROM {table_name}"
        if date_filter:
            count_query += f" WHERE date = '{date_filter}'"
        total = spark.sql(count_query).collect()[0]["total"]
        print(f"\nTotal records: {total}")
        
        spark.stop()
        
    except ImportError:
        print("PySpark not available. Install with: poetry install --extras pyspark")
    except Exception as e:
        print(f"Error: {e}")


def execute_custom_sql(sql: str):
    """Execute custom SQL query using Spark SQL."""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName(f"{SPARK_APP_NAME}-query") \
            .master(SPARK_MASTER) \
            .getOrCreate()
        
        # Register tables
        for table_name in ["vehicle_events", "vehicle_status"]:
            table_path = DATALAKE_BASE / "raw_data" / table_name
            if Path(table_path).exists():
                df = spark.read.parquet(str(table_path))
                df.createOrReplaceTempView(table_name)
        
        print(f"Executing SQL:\n{sql}\n")
        result = spark.sql(sql)
        result.show(truncate=False)
        
        spark.stop()
        
    except ImportError:
        print("PySpark not available. Install with: poetry install --extras pyspark")
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Query datalake using Spark SQL")
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
    elif args.table:
        query_with_spark(args.table, args.date, args.limit)
    else:
        print("Please specify --table or --sql")
        parser.print_help()


if __name__ == "__main__":
    main()

