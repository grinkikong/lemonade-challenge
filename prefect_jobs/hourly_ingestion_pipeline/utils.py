"""Utilities for hourly ingestion pipeline."""

import json
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from data_utils.aws.s3_driver import s3_driver
from prefect_jobs.hourly_ingestion_pipeline.config import SPARK_MASTER, SPARK_APP_NAME


def extract_date_from_filename(filename: str) -> str:
    """Extract date from filename timestamp.

    Args:
        filename: Filename like vehicles_events_20240115120000.json

    Returns:
        Date string in YYYY-MM-DD format.
    """
    match = re.search(r"(\d{14})", filename)
    if match:
        timestamp_str = match.group(1)
        dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        return dt.strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


def read_json_file(file_path: str) -> Dict[str, Any]:
    """Read and parse JSON file.

    Args:
        file_path: Path to JSON file.

    Returns:
        Parsed JSON data as dictionary.
    """
    content = s3_driver.read_file(file_path)
    return json.loads(content.decode("utf-8"))


def convert_json_schema_to_spark_schema(json_schema: Dict[str, Any]):
    """Convert JSON Schema to PySpark StructType.
    
    Args:
        json_schema: JSON Schema definition from metadata
        
    Returns:
        PySpark StructType schema or None if PySpark not available
    """
    try:
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, 
            DoubleType, BooleanType, TimestampType, MapType, ArrayType
        )
        
        def json_type_to_spark_type(prop: Dict[str, Any], field_name: str):
            """Convert JSON Schema type to Spark type."""
            prop_type = prop.get("type")
            
            if prop_type == "string":
                if prop.get("format") == "date-time":
                    return TimestampType()
                return StringType()
            elif prop_type == "integer":
                return IntegerType()
            elif prop_type == "number":
                return DoubleType()
            elif prop_type == "boolean":
                return BooleanType()
            elif prop_type == "object":
                # For nested objects, use MapType - can be enhanced to use StructType
                return MapType(StringType(), StringType())
            elif prop_type == "array":
                return ArrayType(StringType())
            else:
                return StringType()
        
        fields = []
        properties = json_schema.get("properties", {})
        required = json_schema.get("required", [])
        
        for field_name, field_def in properties.items():
            spark_type = json_type_to_spark_type(field_def, field_name)
            nullable = field_name not in required
            fields.append(StructField(field_name, spark_type, nullable=nullable))
        
        return StructType(fields)
    except ImportError:
        return None


def validate_data_against_schema(data: Dict[str, Any], schema_def: Dict[str, Any], file_type: str) -> Tuple[bool, Optional[str]]:
    """Validate data against JSON schema definition.
    
    Args:
        data: Parsed JSON data
        schema_def: JSON schema definition from metadata
        file_type: Type of file (for extracting records)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Extract records from data
    records = data.get(file_type, []) or data.get(f"{file_type}s", [])
    if not records:
        records = list(data.values())[0] if data else []
    
    if not records:
        return False, "No records found in file"
    
    required_fields = schema_def.get("required", [])
    properties = schema_def.get("properties", {})
    
    for idx, record in enumerate(records):
        # Check required fields
        for field in required_fields:
            if field not in record:
                return False, f"Record {idx}: Missing required field '{field}'"
        
        # Check field types (basic validation)
        for field_name, field_value in record.items():
            if field_name in properties:
                field_def = properties[field_name]
                expected_type = field_def.get("type")
                
                # Handle null values
                if field_value is None:
                    if field_name in required_fields:
                        return False, f"Record {idx}: Required field '{field_name}' is null"
                    continue  # Nullable fields can be None
                
                # Type validation
                if expected_type == "string" and not isinstance(field_value, str):
                    return False, f"Record {idx}: Field '{field_name}' should be string, got {type(field_value).__name__}"
                elif expected_type == "integer" and not isinstance(field_value, int):
                    return False, f"Record {idx}: Field '{field_name}' should be integer, got {type(field_value).__name__}"
                elif expected_type == "number" and not isinstance(field_value, (int, float)):
                    return False, f"Record {idx}: Field '{field_name}' should be number, got {type(field_value).__name__}"
                elif expected_type == "boolean" and not isinstance(field_value, bool):
                    return False, f"Record {idx}: Field '{field_name}' should be boolean, got {type(field_value).__name__}"
                elif expected_type == "object" and not isinstance(field_value, dict):
                    return False, f"Record {idx}: Field '{field_name}' should be object, got {type(field_value).__name__}"
                elif expected_type == "array" and not isinstance(field_value, list):
                    return False, f"Record {idx}: Field '{field_name}' should be array, got {type(field_value).__name__}"
    
    return True, None


def process_files_with_pyspark(
    file_paths: List[str], file_type: str, output_table: Path, metadata_dict=None
) -> Dict[str, Any]:
    """Process files with PySpark locally - validates schema and writes Parquet files."""
    try:
        import subprocess
        import sys
        import os
        
        # Check if Java is available before attempting to use PySpark
        # This prevents error messages from being printed to stderr
        java_home = os.environ.get('JAVA_HOME')
        
        # If JAVA_HOME not set, try common Homebrew locations (prefer Java 21 LTS for Spark compatibility)
        if not java_home:
            # Try Java 21 first (LTS, better Spark compatibility)
            java21_home = '/opt/homebrew/opt/openjdk@21'
            if os.path.exists(os.path.join(java21_home, 'bin', 'java')):
                java_home = java21_home
                os.environ['JAVA_HOME'] = java_home
            else:
                # Fallback to Java 25
                common_java_home = '/opt/homebrew/opt/openjdk'
                if os.path.exists(os.path.join(common_java_home, 'bin', 'java')):
                    java_home = common_java_home
                    os.environ['JAVA_HOME'] = java_home
        
        # Try to find Java - check JAVA_HOME first, then PATH
        java_cmd = 'java'
        if java_home:
            java_cmd_path = os.path.join(java_home, 'bin', 'java')
            if os.path.exists(java_cmd_path):
                java_cmd = java_cmd_path
        
        try:
            # Suppress stderr to avoid Java error messages
            with open(os.devnull, 'w') as devnull:
                result = subprocess.run(
                    [java_cmd, '-version'],
                    stderr=devnull,
                    stdout=devnull,
                    timeout=2,
                    env=os.environ.copy()  # Preserve environment including JAVA_HOME
                )
            if result.returncode != 0:
                raise Exception("Java not available")
        except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
            # Java not found or not working - check if JAVA_HOME points to valid Java
            if java_home and os.path.exists(os.path.join(java_home, 'bin', 'java')):
                # JAVA_HOME is set and Java exists there - set it and continue
                os.environ['JAVA_HOME'] = java_home
            else:
                # Java not found - skip PySpark
                raise ImportError("Java runtime not available")
        
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import input_file_name, udf, col, explode
        from pyspark.sql.types import StringType
        from pyspark.sql.utils import AnalysisException

        # Set JAVA_HOME for Spark if not already set
        if java_home:
            os.environ['JAVA_HOME'] = java_home
        
        # Create SparkSession
        # Java 21 (LTS) works without compatibility flags
        # Java 25+ would need additional --add-opens flags, but we prefer Java 21 for Spark
        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .master(SPARK_MASTER) \
            .getOrCreate()

        partition_col = metadata_dict.get("partition_column", "date") if metadata_dict else "date"
        schema_def = metadata_dict.get("schema_definition") if metadata_dict else None
        
        # Convert JSON schema to Spark schema if available (for validation after extraction)
        spark_schema = None
        if schema_def:
            spark_schema = convert_json_schema_to_spark_schema(schema_def)
        
        # Read JSON files WITHOUT schema first (because JSON has nested structure)
        # We'll extract the nested data first, then validate
        # Use multiLine=True because our JSON files have arrays of objects
        try:
            df = spark.read.option("multiLine", "true").json(file_paths)
        except AnalysisException as e:
            spark.stop()
            return {
                "status": "schema_validation_failed",
                "error": f"Failed to read JSON files: {str(e)}",
                "records_count": 0,
            }
        
        # Extract records from nested structure (e.g., {"vehicles_events": [...]})
        # Flatten if needed - handle both singular and plural forms
        columns = df.columns
        
        # Try to find the array column (could be file_type or plural version)
        array_column = None
        if file_type in columns:
            array_column = file_type
        elif f"{file_type}s" in columns:  # e.g., vehicle_events -> vehicles_events
            array_column = f"{file_type}s"
        elif len(columns) == 1 and columns[0] != "_corrupt_record":
            # Only one column - use it (but not _corrupt_record)
            array_column = columns[0]
        
        if not array_column:
            spark.stop()
            return {
                "status": "schema_validation_failed",
                "error": f"Could not find array column for file_type '{file_type}' in columns: {columns}",
                "records_count": 0,
            }
        
        # Data is nested like {"vehicles_events": [...]} or {"vehicle_events": [...]}
        df = df.select(explode(col(array_column)).alias("data"))
        df = df.select("data.*")
        
        # Now validate against schema if provided (after extraction)
        if spark_schema:
            # Check if extracted columns match expected schema
            expected_cols = set([field.name for field in spark_schema.fields])
            actual_cols = set(df.columns)
            missing_cols = expected_cols - actual_cols
            if missing_cols:
                spark.stop()
                return {
                    "status": "schema_validation_failed",
                    "error": f"Missing columns after extraction: {missing_cols}",
                    "records_count": 0,
                }
        
        # Extract date from filename using UDF
        def extract_date_from_path(path_str):
            if not path_str:
                return datetime.now().strftime("%Y-%m-%d")
            filename = path_str.split("/")[-1]
            match = re.search(r"(\d{14})", filename)
            if match:
                timestamp_str = match.group(1)
                dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                return dt.strftime("%Y-%m-%d")
            return datetime.now().strftime("%Y-%m-%d")
        
        extract_date_udf = udf(extract_date_from_path, StringType())
        
        # Add partition column from filename
        df = df.withColumn("file_path", input_file_name())
        df = df.withColumn(partition_col, extract_date_udf("file_path"))
        df = df.drop("file_path")
        
        # Write to datalake as Parquet (partitioned by date)
        # This creates queryable Parquet files that can be used with DuckDB, Polars, Spark SQL, etc.
        output_path = str(output_table)
        df.write \
            .mode("append") \
            .partitionBy(partition_col) \
            .parquet(output_path)
        
        records_count = df.count()
        
        spark.stop()
        
        return {
            "status": "success",
            "records_count": records_count,
            "output_path": output_path,
            "files_processed": len(file_paths),
        }

    except ImportError:
        return {"status": "failed", "error": "PySpark not installed", "records_count": 0}
    except Exception as e:
        return {"status": "failed", "error": str(e), "records_count": 0}


# In production (non-challenge): Would submit Spark job to EMR cluster
# EMR would read all files from processing folder in s3, apply schema from metadata,
# transform data, and write Parquet files to Iceberg table partitioned by date
