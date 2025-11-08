"""Task 3: Move successfully processed files to processed folder.

This task runs after Spark processing (Task 2) and moves files from processing/
to processed/ only for chunks that were successfully loaded to Parquet.
"""

from pathlib import Path
from prefect import task, get_run_logger
from data_utils.aws.s3_driver import s3_driver
from prefect_jobs.files_ingester.config import (
    PROCESSING_FOLDER,
    PROCESSED_FOLDER,
    DATALAKE_BASE,
)


def discover_processing_folders() -> list:
    """Discover all folders in processing directory that contain files.
    
    Returns:
        List of folder paths that need cleanup.
    """
    logger = get_run_logger()
    logger.info(f"Discovering processing folders in {PROCESSING_FOLDER}")
    processing_folders = []
    
    if not s3_driver.exists(str(PROCESSING_FOLDER)):
        logger.info(f"Processing folder does not exist: {PROCESSING_FOLDER}")
        return processing_folders
    
    # List all items in processing folder (non-recursive)
    try:
        # Get all items in processing folder
        processing_folder_str = str(PROCESSING_FOLDER)
        all_items = []
        
        # Check each potential subfolder (vehicle_events, vehicle_status, etc.)
        # by checking if the folder exists and has files
        for potential_folder in PROCESSING_FOLDER.iterdir():
            if s3_driver.exists(str(potential_folder)) and s3_driver.is_dir(str(potential_folder)):
                files = s3_driver.list_files(str(potential_folder))
                if files:  # Only include folders with files
                    processing_folders.append(str(potential_folder))
    except Exception as e:
        logger.error(f"Error discovering processing folders: {e}")
    
    logger.info(f"Found {len(processing_folders)} processing folders with files")
    return processing_folders


def verify_parquet_files_exist(file_type: str) -> bool:
    """Verify that Parquet files exist in datalake for this file type.
    
    Args:
        file_type: Type of file (e.g., 'vehicle_events')
        
    Returns:
        True if Parquet files exist, False otherwise
    """
    output_table = DATALAKE_BASE / "raw_data" / file_type
    try:
        if output_table.exists():
            parquet_files = list(output_table.glob("**/*.parquet"))
            return len(parquet_files) > 0
    except Exception:
        pass
    return False


@task
def cleanup_processed_files() -> dict:
    """Move successfully processed files from processing/ to processed/ folder.
    
    This task:
    1. Discovers all processing folders
    2. For each folder, verifies Parquet files exist in datalake
    3. If Parquet files exist, moves all files from processing/ to processed/
    4. This is the final cleanup step after successful data load
    
    Returns:
        Dictionary with cleanup results
    """
    logger = get_run_logger()
    logger.info("Starting cleanup of processed files (Task 3)")
    
    processing_folders = discover_processing_folders()
    
    if not processing_folders:
        logger.info("No processing folders found - nothing to cleanup")
        return {
            "folders_cleaned": 0,
            "files_moved": 0,
            "folders_skipped": 0,
        }
    
    folders_cleaned = 0
    files_moved = 0
    folders_skipped = 0
    
    for processing_folder in processing_folders:
        try:
            # Extract file_type from folder path (e.g., processing/vehicle_events -> vehicle_events)
            folder_name = Path(processing_folder).name
            file_type = folder_name
            
            # Verify Parquet files exist for this file type
            if not verify_parquet_files_exist(file_type):
                logger.warning(
                    f"Skipping {processing_folder}: No Parquet files found in datalake. "
                    f"Files may still be processing or failed."
                )
                folders_skipped += 1
                continue
            
            # Parquet files exist - this chunk was successfully processed
            # Move all files from processing/ to processed/
            files = s3_driver.list_files(processing_folder)
            if not files:
                continue
            
            logger.info(
                f"Moving {len(files)} files from {processing_folder} to processed/{file_type}/"
            )
            
            moved_count = 0
            for file_path in files:
                file_name = Path(file_path).name
                dest_path = PROCESSED_FOLDER / file_type / file_name
                try:
                    s3_driver.move_file(file_path, str(dest_path))
                    moved_count += 1
                    logger.debug(f"Moved {file_name} to processed folder")
                except Exception as move_error:
                    logger.error(f"Failed to move {file_name} to processed folder: {move_error}")
            
            if moved_count == len(files):
                folders_cleaned += 1
                files_moved += moved_count
                logger.info(
                    f"✓ Successfully moved {moved_count} files from {processing_folder} to processed/"
                )
            else:
                logger.warning(
                    f"Partially moved files from {processing_folder}: {moved_count}/{len(files)}"
                )
                
        except Exception as e:
            logger.error(f"Error cleaning up folder {processing_folder}: {e}")
            folders_skipped += 1
    
    logger.info(
        f"Cleanup complete: {folders_cleaned} folders cleaned, "
        f"{files_moved} files moved, {folders_skipped} folders skipped"
    )
    
    return {
        "folders_cleaned": folders_cleaned,
        "files_moved": files_moved,
        "folders_skipped": folders_skipped,
    }

