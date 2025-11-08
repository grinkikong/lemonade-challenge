"""Spark processing: reads files from processing folders, writes to Iceberg datalake."""

import traceback
from pathlib import Path
from prefect import task, get_run_logger
from data_utils.aws.s3_driver import s3_driver
from data_utils.database.drivers import db_driver
from data_utils.database.models import TableMetadata
from prefect_jobs.hourly_ingestion_pipeline.config import (
    PROCESSING_FOLDER,
    FAILED_FOLDER,
    DATALAKE_BASE
)
from prefect_jobs.hourly_ingestion_pipeline.utils import (
    process_files_with_pyspark,
)


def discover_processing_folders() -> list:
    """Discover all folders in processing directory that contain files.

    Returns:
        List of folder paths that need processing.
    """
    logger = get_run_logger()
    logger.info(f"Discovering processing folders in {PROCESSING_FOLDER}")
    processing_folders = []

    # Get all active registered file types from database
    with db_driver.session() as session:
        active_metadata = (
            session.query(TableMetadata)
            .filter(TableMetadata.status == "active")
            .all()
        )
        for metadata in active_metadata:
            # Use processing_folder_path from metadata, or default to file_type
            processing_folder_path = metadata.processing_folder_path or metadata.file_type
            folder_path = PROCESSING_FOLDER / processing_folder_path
            folder_str = str(folder_path)
            
            # Check if folder has files
            files = s3_driver.list_files(folder_str)
            if files:
                processing_folders.append(folder_str)
                logger.info(f"Found {len(files)} files in {folder_str}")

    logger.info(f"Found {len(processing_folders)} processing folders with files")
    return processing_folders


def get_metadata_for_folder(processing_folder: str) -> tuple:
    """Get table metadata for processing folder and extract values while in session."""
    folder_path = Path(processing_folder)
    processing_folder_name = folder_path.name
    
    with db_driver.session() as session:
        metadata_by_folder = (
            session.query(TableMetadata)
            .filter(TableMetadata.processing_folder_path == processing_folder_name)
            .filter(TableMetadata.status == "active")
            .first()
        )
        if metadata_by_folder:
            # Extract values while in session
            metadata_dict = {
                "file_type": metadata_by_folder.file_type,
                "partition_column": metadata_by_folder.partition_column,
                "processing_folder_path": metadata_by_folder.processing_folder_path,
                "schema_definition": metadata_by_folder.schema_definition,
                "processing_config": metadata_by_folder.processing_config,
            }
            return metadata_dict, metadata_by_folder.file_type
        
        metadata_by_type = (
            session.query(TableMetadata)
            .filter(TableMetadata.file_type == processing_folder_name)
            .filter(TableMetadata.status == "active")
            .first()
        )
        if metadata_by_type:
            # Extract values while in session
            metadata_dict = {
                "file_type": metadata_by_type.file_type,
                "partition_column": metadata_by_type.partition_column,
                "processing_folder_path": metadata_by_type.processing_folder_path,
                "schema_definition": metadata_by_type.schema_definition,
                "processing_config": metadata_by_type.processing_config,
            }
            return metadata_dict, metadata_by_type.file_type
    
    return None, processing_folder_name


def process_files_batch_with_spark(
    file_paths: list,
    file_type: str,
    output_table: Path,
    metadata_dict: dict,
) -> dict:
    """Process batch of files from processing folder with Spark and write to datalake.

    Args:
        file_paths: List of file paths to process.
        file_type: Type of file.
        output_table: Output table path (Iceberg table location).
        table_metadata: TableMetadata instance with schema and config.

    Returns:
        Processing result dictionary with records_count and status.
    """
    logger = get_run_logger()
    logger.info(f"Processing {len(file_paths)} {file_type} files with Spark")

    try:
        # Use PySpark to process all files (local)
        # In production (non-challenge): Would submit Spark job to EMR cluster
        result = process_files_with_pyspark(
            file_paths, file_type, output_table, metadata_dict
        )
        return result

    except Exception as e:
        logger.error(f"Spark processing failed: {e}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        return {"status": "failed", "error": str(e), "records_count": 0}


@task
def process_folder_with_spark(processing_folder: str) -> dict:
    """Process all files in folder with Spark: read from processing, write to Iceberg datalake."""
    logger = get_run_logger()
    logger.info(f"Processing folder: {processing_folder}")

    files = s3_driver.list_files(processing_folder)
    if not files:
        return {"processed": 0, "failed": 0, "folder": processing_folder}

    metadata_dict, file_type = get_metadata_for_folder(processing_folder)
    
    if not metadata_dict:
        logger.error(f"No metadata for {file_type} in {processing_folder}")
        for file_path in files:
            file_name = Path(file_path).name
            dest_path = FAILED_FOLDER / file_type / file_name
            try:
                s3_driver.move_file(file_path, str(dest_path))
            except Exception as move_error:
                logger.error(f"Failed to move {file_name} to failed: {move_error}")
        raise Exception(f"No active metadata for {file_type}. Files moved to failed.")
    
    output_table = DATALAKE_BASE / "raw_data" / file_type

    # Process all files and write to Parquet datalake
    result = process_files_batch_with_spark(
        files, file_type, output_table, metadata_dict
    )
    
    # Task 2 only processes and writes to Parquet - cleanup is done in Task 3
    if result["status"] == "success":
        # All files successfully loaded to Parquet
        # Files remain in processing/ folder - Task 3 will move them to processed/
        results = {
            "processed": len(files),
            "failed": 0,
            "records_count": result.get("records_count", 0)
        }
        logger.info(
            f"✓ Successfully processed {len(files)} files: {result.get('records_count', 0)} records written to Parquet. "
            f"Files remain in processing folder - Task 3 will move to processed/ after verification."
        )
    else:
        # Any error (schema validation, processing, etc.) - move ALL files from this chunk to failed
        # One error = entire chunk fails (atomic operation)
        error_msg = result.get('error', 'Unknown error')
        error_type = result.get("status", "failed")
        
        logger.error(
            f"Processing failed for chunk {file_type} in {processing_folder}. "
            f"Error type: {error_type}, Error: {error_msg}. "
            f"Moving all {len(files)} files to failed folder."
        )
        
        for file_path in files:
            file_name = Path(file_path).name
            dest_path = FAILED_FOLDER / file_type / file_name
            try:
                s3_driver.move_file(file_path, str(dest_path))
            except Exception as move_error:
                logger.error(f"Failed to move {file_name} to failed folder: {move_error}")
        
        # Track schema errors separately for reporting
        schema_errors = len(files) if error_type == "schema_validation_failed" else 0
        results = {
            "processed": 0,
            "failed": len(files),
            "schema_errors": schema_errors
        }
        logger.error(
            f"ALERT: Chunk processing failed - {len(files)} files moved to failed folder. "
            f"Error: {error_msg}"
        )

    if results.get("schema_errors", 0) > 0:
        alert_message = (
            f"SCHEMA VALIDATION FAILED - Folder: {processing_folder}, "
            f"Schema errors: {results['schema_errors']}, Processed: {results['processed']}"
        )
        logger.error(alert_message)
        raise Exception(alert_message)
    
    if results["failed"] > 0:
        alert_message = (
            f"BATCH PROCESSING FAILED - Folder: {processing_folder}, "
            f"Failed: {results['failed']}, Processed: {results['processed']}"
        )
        logger.error(alert_message)
        raise Exception(alert_message)
    
    return results


@task
def process_all_folders_with_spark() -> dict:
    """Process all folders in parallel: Spark reads from processing, writes to Iceberg datalake."""
    logger = get_run_logger()
    logger.info("Starting Spark processing")

    processing_folders = discover_processing_folders()
    if not processing_folders:
        return {"folders_processed": 0, "total_processed": 0, "total_failed": 0, "total_schema_errors": 0, "folder_results": []}

    # Process folders in parallel using Prefect task mapping
    folder_results = process_folder_with_spark.map(processing_folders)
    
    # Aggregate results
    total_processed = 0
    total_failed = 0
    total_schema_errors = 0
    folder_errors = []
    successful_results = []

    for folder_path, folder_result in zip(processing_folders, folder_results):
        try:
            result = folder_result.result()
            successful_results.append(result)
            total_processed += result.get("processed", 0)
            total_failed += result.get("failed", 0)
            total_schema_errors += result.get("schema_errors", 0)
        except Exception as folder_error:
            logger.error(f"Folder processing failed: {folder_path}: {folder_error}")
            folder_errors.append({"folder": folder_path, "error": str(folder_error)})
            total_failed += 1

    logger.info(f"Complete: {len(successful_results)} folders, {total_processed} processed, {total_failed} failed, {total_schema_errors} schema errors")

    return {
        "folders_processed": len(successful_results),
        "total_processed": total_processed,
        "total_failed": total_failed,
        "total_schema_errors": total_schema_errors,
        "folder_results": successful_results,
        "folder_errors": folder_errors,
    }

