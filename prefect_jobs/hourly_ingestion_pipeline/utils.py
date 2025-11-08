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
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import input_file_name, udf, col, explode
        from pyspark.sql.types import StringType
        from pyspark.sql.utils import AnalysisException

        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .master(SPARK_MASTER) \
            .getOrCreate()

        partition_col = metadata_dict.get("partition_column", "date") if metadata_dict else "date"
        schema_def = metadata_dict.get("schema_definition") if metadata_dict else None
        
        # Convert JSON schema to Spark schema if available
        spark_schema = None
        if schema_def:
            spark_schema = convert_json_schema_to_spark_schema(schema_def)
        
        # Read JSON files with schema validation
        try:
            if spark_schema:
                # Read with schema - will fail if data doesn't match
                df = spark.read.schema(spark_schema).json(file_paths)
            else:
                # Fallback: read without schema
                df = spark.read.json(file_paths)
        except AnalysisException as e:
            spark.stop()
            return {
                "status": "schema_validation_failed",
                "error": f"Schema validation failed: {str(e)}",
                "records_count": 0,
            }
        
        # Extract records from nested structure (e.g., {"vehicles_events": [...]})
        # Flatten if needed
        columns = df.columns
        if len(columns) == 1 and file_type in columns:
            # Data is nested like {"vehicle_events": [...]}
            df = df.select(explode(col(file_type)).alias("data"))
            df = df.select("data.*")
        elif file_type in columns:
            # Data has file_type as a column with array
            df = df.select(explode(col(file_type)).alias("data"))
            df = df.select("data.*")
        
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
        return process_files_simulation(file_paths, file_type, output_table, metadata_dict)
    except Exception as e:
        return {"status": "failed", "error": str(e), "records_count": 0}


def process_files_simulation(
    file_paths: List[str], file_type: str, output_table: Path, metadata_dict=None
) -> Dict[str, Any]:
    """Fallback simulation with schema validation - writes Parquet files using pyarrow."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except (ImportError, AttributeError, RuntimeError) as e:
        # If pyarrow not available or has compatibility issues (e.g., NumPy version), fall back to JSON
        return process_files_simulation_json(file_paths, file_type, output_table, metadata_dict)
    
    total_records = 0
    partition_col = metadata_dict.get("partition_column", "date") if metadata_dict else "date"
    schema_def = metadata_dict.get("schema_definition") if metadata_dict else None
    
    # Group files by partition date
    files_by_partition = {}
    
    for file_path in file_paths:
        data = read_json_file(file_path)
        
        # Validate schema if available
        if schema_def:
            is_valid, error_msg = validate_data_against_schema(data, schema_def, file_type)
            if not is_valid:
                return {
                    "status": "schema_validation_failed",
                    "error": error_msg,
                    "records_count": 0,
                    "file_path": file_path,
                }
        
        # Extract records
        records = data.get(file_type, []) or data.get(f"{file_type}s", [])
        if not records:
            records = list(data.values())[0] if data else []
        
        filename = Path(file_path).name
        partition_date = extract_date_from_filename(filename)
        
        if partition_date not in files_by_partition:
            files_by_partition[partition_date] = []
        files_by_partition[partition_date].extend(records)
        total_records += len(records)
    
    # Write Parquet files partitioned by date
    for partition_date, records in files_by_partition.items():
        if not records:
            continue
            
        # Convert to PyArrow table
        table = pa.Table.from_pylist(records)
        
        # Write Parquet file
        partition_path = output_table / f"{partition_col}={partition_date}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        parquet_file = partition_path / f"data_{partition_date}.parquet"
        pq.write_table(table, str(parquet_file))
    
    return {
        "status": "success",
        "records_count": total_records,
        "files_processed": len(file_paths),
    }


def process_files_simulation_json(
    file_paths: List[str], file_type: str, output_table: Path, metadata_dict=None
) -> Dict[str, Any]:
    """Fallback if pyarrow not available - validates but writes JSON."""
    total_records = 0
    partition_col = metadata_dict.get("partition_column", "date") if metadata_dict else "date"
    schema_def = metadata_dict.get("schema_definition") if metadata_dict else None
    
    for file_path in file_paths:
        data = read_json_file(file_path)
        
        # Validate schema if available
        if schema_def:
            is_valid, error_msg = validate_data_against_schema(data, schema_def, file_type)
            if not is_valid:
                return {
                    "status": "schema_validation_failed",
                    "error": error_msg,
                    "records_count": 0,
                    "file_path": file_path,
                }
        
        records = data.get(file_type, []) or data.get(f"{file_type}s", [])
        if not records:
            records = list(data.values())[0] if data else []
        
        filename = Path(file_path).name
        partition_date = extract_date_from_filename(filename)
        output_path = output_table / f"{partition_col}={partition_date}" / filename
        s3_driver.write_file(str(output_path), json.dumps(data).encode("utf-8"))
        total_records += len(records)
    
    return {
        "status": "success",
        "records_count": total_records,
        "files_processed": len(file_paths),
    }

# In production (non-challenge): Would submit Spark job to EMR cluster
# EMR would read all files from processing folder in s3, apply schema from metadata,
# transform data, and write Parquet files to Iceberg table partitioned by date
