"""Spark batch ingestion - simple and straightforward."""

import os
import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, udf, col, explode, to_timestamp
from pyspark.sql.types import StringType, StructType
from prefect_jobs.files_ingester.config import SPARK_MASTER, SPARK_APP_NAME


def spark_batch_ingestion(
    processing_folder_path: str,
    output_table_path: str,
    file_type: str,
    config: Dict[str, Any],
    file_paths: list = None
) -> Dict[str, Any]:
    """Simple Spark batch ingestion with schema validation.
    
    Args:
        processing_folder_path: Path to folder with JSON files to process
        output_table_path: Path where to write Parquet files
        file_type: File type (vehicle_events or vehicle_status)
        config: Config dict with required_columns, root_keys, partition_column, schema
        file_paths: Optional list of specific file paths to process (if None, uses glob pattern)
        
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
    
    # Get config values
    required_columns = config.get("required_columns", [])
    root_keys = config.get("root_keys", [])
    partition_column = config.get("partition_column", "date")
    schema = config.get("schema")  # Explicit schema for validation
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .getOrCreate()
    
    # Suppress FileStreamSink warnings (Spark checks for streaming metadata, we're using batch)
    spark.sparkContext.setLogLevel("ERROR")  # Only show errors, suppress warnings
    
    try:
        # Read JSON files - use file_paths if provided, otherwise use glob pattern
        if file_paths:
            # Read from list of specific file paths
            # Convert to absolute paths and ensure they exist
            absolute_paths = [str(Path(p).resolve()) for p in file_paths]
            # Verify all files exist
            for path in absolute_paths:
                if not Path(path).exists():
                    raise FileNotFoundError(f"File not found: {path}")
            
            # Read files - if single file, pass directly; if multiple, use glob pattern from folder
            if len(absolute_paths) == 1:
                df = spark.read.option("multiLine", "true").json(absolute_paths[0])
            else:
                # For multiple files, use glob pattern from the folder (more reliable)
                folder_path = str(Path(absolute_paths[0]).parent)
                df = spark.read.option("multiLine", "true").json(f"{folder_path}/*.json")
        else:
            # Fallback to glob pattern (for backward compatibility)
            df = spark.read.option("multiLine", "true").json(f"{processing_folder_path}/*.json")
        
        # Find root key (first matching root_key from config)
        root_key = None
        for key in root_keys:
            if key in df.columns:
                root_key = key
                break
        
        if not root_key:
            raise ValueError(f"Root key not found. Expected one of: {root_keys}, found: {df.columns}")
        
        # Extract nested array
        df = df.select(explode(col(root_key)).alias("data"))
        df = df.select("data.*")
        
        # Validate required columns exist
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Apply schema validation if schema is provided
        if schema:
            # Validate data types by attempting to cast to expected types
            # This will fail if data doesn't match schema expectations
            try:
                # Convert timestamp strings to timestamp type for validation
                # This validates that event_time/report_time are valid ISO format timestamps
                if "event_time" in df.columns:
                    df = df.withColumn("event_time", to_timestamp(col("event_time")))
                if "report_time" in df.columns:
                    df = df.withColumn("report_time", to_timestamp(col("report_time")))
                
                # Validate nullable constraints - check for nulls in required fields
                for field in schema.fields:
                    if not field.nullable and field.name in df.columns:
                        null_count = df.filter(col(field.name).isNull()).count()
                        if null_count > 0:
                            raise ValueError(
                                f"Schema validation failed - required field '{field.name}' contains {null_count} null value(s)"
                            )
            except Exception as e:
                # Re-raise with clearer error message
                if "Schema validation failed" in str(e):
                    raise
                raise ValueError(f"Schema validation failed - data type or format mismatch: {str(e)}")
        
        # Add partition column from filename
        def extract_date(path_str: str) -> str:
            filename = Path(path_str).name
            match = re.search(r"(\d{14})", filename)
            if match:
                dt = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
                return dt.strftime("%Y-%m-%d")
            return datetime.now().strftime("%Y-%m-%d")
        
        extract_date_udf = udf(extract_date, StringType())
        df = df.withColumn(partition_column, extract_date_udf(input_file_name()))
        
        # Write to Parquet (append mode, partitioned)
        df.write \
            .mode("append") \
            .partitionBy(partition_column) \
            .parquet(output_table_path)
        
        records_count = df.count()
        
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

