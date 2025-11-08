"""Spark daily aggregation - simple and straightforward."""

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number, lit, date_format
from pyspark.sql.window import Window
from prefect_jobs.daily_summary.config import (
    DATALAKE_EVENTS_TABLE,
    DATALAKE_SUMMARY_TABLE,
    VEHICLE_EVENTS_INPUT_SCHEMA,
)
from prefect_jobs.files_ingester.config import SPARK_MASTER, SPARK_APP_NAME


def spark_daily_aggregation(target_date: date) -> Dict[str, Any]:
    """Simple Spark daily aggregation - no fallbacks, no tricks.
    
    Args:
        target_date: Date to aggregate events for
        
    Returns:
        Dict with status, records_count, error
    """
    # Set JAVA_HOME if not already set (check common locations)
    if not os.environ.get("JAVA_HOME"):
        java_home_candidates = [
            "/opt/homebrew/opt/openjdk@21",
            "/opt/homebrew/opt/openjdk@17",
            "/opt/homebrew/opt/openjdk",
            "/usr/local/opt/openjdk@21",
            "/usr/local/opt/openjdk@17",
        ]
        for candidate in java_home_candidates:
            if os.path.exists(candidate):
                os.environ["JAVA_HOME"] = candidate
                break
        else:
            # No Java found - fail with clear error
            error_msg = (
                "JAVA_HOME environment variable is not set and Java could not be found. "
                "Spark requires Java to run. "
                "Please install Java or set JAVA_HOME in your environment/PyCharm run configuration."
            )
            return {
                "status": "failed",
                "error": error_msg,
                "records_count": 0,
            }
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .getOrCreate()
    
    # Suppress FileStreamSink warnings (Spark checks for streaming metadata, we're using batch)
    spark.sparkContext.setLogLevel("ERROR")  # Only show errors, suppress warnings
    
    try:
        # Read from 2 days back partitions for fast querying (yesterday-1 and yesterday)
        # This allows us to catch late-arriving data efficiently
        day_before_yesterday = target_date - timedelta(days=1)
        partition_paths = [
            f"{str(DATALAKE_EVENTS_TABLE)}/date={day_before_yesterday.strftime('%Y-%m-%d')}",
            f"{str(DATALAKE_EVENTS_TABLE)}/date={target_date.strftime('%Y-%m-%d')}",
        ]
        
        # Check which partitions exist before reading (Spark doesn't handle missing partitions gracefully)
        existing_partitions = []
        for path_str in partition_paths:
            path = Path(path_str)
            if path.exists() and any(path.iterdir()):  # Check if path exists and has files
                existing_partitions.append(path_str)
        
        if not existing_partitions:
            return {
                "status": "no_events",
                "records_count": 0,
                "message": f"No partitions found for {target_date}. Checked: {partition_paths}",
            }
        
        # Read from existing partitions only
        if len(existing_partitions) == 1:
            df = spark.read.parquet(existing_partitions[0])
        else:
            df = spark.read.parquet(*existing_partitions)
        
        initial_count = df.count()
        if initial_count == 0:
            return {
                "status": "no_events",
                "records_count": 0,
                "message": f"No events found in partitions: {existing_partitions}",
            }
        
        # Convert event_time to timestamp for filtering
        df = df.withColumn("event_timestamp", to_timestamp(col("event_time")))
        
        # Calculate 48 hours ago timestamp for filtering
        # Only process events from the last 48 hours to avoid very old late events
        # This prevents creating partitions with almost empty data from one very old event
        now_utc = datetime.utcnow()
        cutoff_time = now_utc - timedelta(hours=48)
        
        # Process events from the day BEFORE target_date (yesterday)
        # When processing target_date = 2025-11-09, we process events from 2025-11-08
        # This is because the job runs after 00:00 UTC to process the day that just ended
        yesterday_date = target_date - timedelta(days=1)
        yesterday_start = datetime.combine(yesterday_date, datetime.min.time())
        yesterday_end = datetime.combine(yesterday_date, datetime.max.time())
        
        # Debug: Show date range being filtered
        print(f"DEBUG: Reading from partitions: {existing_partitions}")
        print(f"DEBUG: Initial event count: {initial_count}")
        print(f"DEBUG: Processing target_date: {target_date}")
        print(f"DEBUG: Filtering for yesterday (event_time): {yesterday_date} ({yesterday_start} to {yesterday_end})")
        print(f"DEBUG: 48-hour cutoff: {cutoff_time} (now: {now_utc})")
        
        # Filter: Only events from last 48 hours AND within yesterday (event_time)
        # This prevents processing very old late events that might be in those partitions
        # Example: When processing 2025-11-09, process events from 2025-11-08, but exclude old ones from 2025-11-07
        df = df.filter(
            (col("event_timestamp") >= lit(cutoff_time)) &
            (col("event_timestamp") >= lit(yesterday_start)) &
            (col("event_timestamp") <= lit(yesterday_end))
        )
        
        filtered_count = df.count()
        print(f"DEBUG: Filtered event count: {filtered_count}")
        
        if filtered_count == 0:
            # Show sample event_times to help debug
            sample_df = spark.read.parquet(*existing_partitions) if len(existing_partitions) > 1 else spark.read.parquet(existing_partitions[0])
            sample_df = sample_df.withColumn("event_timestamp", to_timestamp(col("event_time")))
            sample_times = sample_df.select("event_timestamp").limit(5).collect()
            print(f"DEBUG: Sample event_times in partitions: {[row.event_timestamp for row in sample_times]}")
            return {
                "status": "no_events",
                "records_count": 0,
                "message": f"No events found matching criteria. Initial count: {initial_count}, After filter: {filtered_count}. Target date: {target_date}",
            }
        
        # Validate required columns exist
        required_columns = ["vehicle_id", "event_time", "event_type"]
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            error_msg = (
                f"SCHEMA VALIDATION FAILED - Missing required columns in Parquet data: {missing_cols}. "
                f"Date: {target_date}. This indicates a schema change in the datalake. "
                f"Data engineer action required: Investigate and fix schema mismatch."
            )
            raise ValueError(error_msg)
        
        # Validate data types and nullable constraints
        try:
            # Check for nulls in required fields
            for field in VEHICLE_EVENTS_INPUT_SCHEMA.fields:
                if not field.nullable and field.name in df.columns:
                    null_count = df.filter(col(field.name).isNull()).count()
                    if null_count > 0:
                        error_msg = (
                            f"SCHEMA VALIDATION FAILED - Required field '{field.name}' contains {null_count} null value(s). "
                            f"Date: {target_date}. This indicates data quality issues in the datalake. "
                            f"Data engineer action required: Investigate and fix null values."
                        )
                        raise ValueError(error_msg)
        except Exception as e:
            # Re-raise with clearer error message if it's already a validation error
            if "SCHEMA VALIDATION FAILED" in str(e):
                raise
            # Otherwise, wrap it as a validation error
            error_msg = (
                f"SCHEMA VALIDATION FAILED - Data type or format mismatch: {str(e)}. "
                f"Date: {target_date}. This indicates a schema change in the datalake. "
                f"Data engineer action required: Investigate and fix schema mismatch."
            )
            raise ValueError(error_msg)
        
        # Add day column based on event_time (not partition date)
        # This ensures the summary is partitioned by the actual event date
        df = df.withColumn("day", date_format(col("event_timestamp"), "yyyy-MM-dd"))
        
        # Use window function to get last event per vehicle per day
        # Partition by vehicle_id and day (derived from event_time) to get last event per vehicle per day
        window_spec = Window.partitionBy("vehicle_id", "day").orderBy(col("event_timestamp").desc())
        df_with_rank = df.withColumn("rn", row_number().over(window_spec))
        
        # Get only the last event for each vehicle-day combination
        # Filter to only include events for yesterday (the day we're processing)
        # When processing target_date = 2025-11-09, we process events from 2025-11-08
        summary_df = df_with_rank.filter(
            (col("rn") == 1) & (col("day") == lit(yesterday_date.strftime('%Y-%m-%d')))
        ) \
            .select(
                "vehicle_id",
                "day",
                col("event_timestamp").alias("last_event_time"),
                "event_type"
            ) \
            .withColumnRenamed("event_type", "last_event_type")
        
        # Write summary to Parquet (partitioned by day)
        # Use day column (derived from event_time) for partitioning
        output_path = f"{str(DATALAKE_SUMMARY_TABLE)}"
        summary_df.write \
            .mode("overwrite") \
            .partitionBy("day") \
            .parquet(output_path)
        
        records_count = summary_df.count()
        
        return {
            "status": "success",
            "records_count": records_count,
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "records_count": 0,
        }
    finally:
        spark.stop()

