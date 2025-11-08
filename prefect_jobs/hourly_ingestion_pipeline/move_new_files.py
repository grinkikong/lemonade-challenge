"""Move new files to processing folders based on metadata validation."""

import re
from pathlib import Path
from typing import Optional
from prefect import task, get_run_logger
from data_utils.aws.s3_driver import s3_driver
from data_utils.database.drivers import db_driver
from data_utils.database.models import TableMetadata
from prefect_jobs.hourly_ingestion_pipeline.config import (
    INCOMING_FOLDER,
    PROCESSING_FOLDER,
    FAILED_FOLDER,
)


def detect_file_type_from_filename(file_name: str, registered_file_types: list) -> str:
    """Detect file type from filename pattern: {file_type}_{timestamp}.json"""
    match = re.match(r"^([a-z_]+)_\d{14}\.json$", file_name.lower())
    if match:
        detected_type = match.group(1)
        # Normalize plural to singular (vehicles_events -> vehicle_events)
        if detected_type.startswith("vehicles_"):
            detected_type = detected_type.replace("vehicles_", "vehicle_", 1)
        return detected_type
    
    for registered_type in registered_file_types:
        if registered_type in file_name.lower():
            return registered_type
    
    return "unknown"


def group_files_by_type(files: list, registered_file_types: list) -> dict:
    """Group files by detected file type."""
    file_type_map = {}
    
    for file_path in files:
        file_name = Path(file_path).name
        file_type = detect_file_type_from_filename(file_name, registered_file_types)
        
        if file_type not in file_type_map:
            file_type_map[file_type] = []
        file_type_map[file_type].append((file_path, file_name))
    
    return file_type_map


def handle_unknown_files(file_list: list, moved_to_failed: list):
    """Move unknown file types to failed folder."""
    logger = get_run_logger()
    
    for file_path, file_name in file_list:
        logger.error(f"Unknown file type: {file_name}. Moving to failed folder.")
        dest_path = FAILED_FOLDER / "unknown" / file_name
        try:
            s3_driver.move_file(file_path, str(dest_path))
            moved_to_failed.append({"file": file_name, "reason": "Unknown file type"})
        except Exception as e:
            logger.error(f"Failed to move {file_name} to failed folder: {e}")


def handle_invalid_metadata(
    file_type: str, table_metadata: Optional[TableMetadata], file_list: list, moved_to_failed: list
):
    """Move files to failed folder when metadata is missing or inactive."""
    logger = get_run_logger()
    
    if not table_metadata:
        reason = "No metadata found"
        logger.info(f"No metadata found for file type '{file_type}'. Moving {len(file_list)} file(s) to failed folder.")
    else:
        reason = f"Metadata status: {table_metadata.status}"
        logger.info(
            f"Metadata for file type '{file_type}' is not active (status: {table_metadata.status}). "
            f"Moving {len(file_list)} file(s) to failed folder."
        )
    
    for file_path, file_name in file_list:
        dest_path = FAILED_FOLDER / file_type / file_name
        try:
            s3_driver.move_file(file_path, str(dest_path))
            moved_to_failed.append({
                "file": file_name,
                "file_type": file_type,
                "reason": reason
            })
        except Exception as e:
            logger.error(f"Failed to move {file_name} to failed folder: {e}")


def move_files_to_processing(
    file_type: str,
    processing_folder: Path,
    file_list: list,
    moved_to_processing: list,
    moved_to_failed: list,
):
    """Move files to processing folder."""
    logger = get_run_logger()
    
    for file_path, file_name in file_list:
        dest_path = processing_folder / file_name
        try:
            s3_driver.move_file(file_path, str(dest_path))
            moved_to_processing.append(str(dest_path))
            logger.info(f"Moved {file_name} to processing folder: {processing_folder}")
        except Exception as e:
            logger.error(f"Failed to move {file_name} to processing folder: {e}")
            failed_dest = FAILED_FOLDER / file_type / file_name
            try:
                s3_driver.move_file(file_path, str(failed_dest))
                moved_to_failed.append({
                    "file": file_name,
                    "file_type": file_type,
                    "reason": f"Failed to move to processing: {str(e)}"
                })
            except:
                pass


@task
def discover_and_move_files() -> dict:
    """Discover new files and move to processing folders based on metadata validation."""
    logger = get_run_logger()
    logger.info("Starting file discovery and movement")

    # Convert Path to string for list_files (INCOMING_FOLDER is already resolved to absolute path)
    incoming_path_str = str(INCOMING_FOLDER)
    logger.info(f"Scanning incoming folder: {incoming_path_str}")
    files = s3_driver.list_files(incoming_path_str)
    logger.info(f"Found {len(files)} files in incoming folder")

    if not files:
        return {"moved_to_processing": 0, "moved_to_failed": 0, "files": []}

    # Get registered file types and metadata within session context
    with db_driver.session() as session:
        registered_types = session.query(TableMetadata.file_type).distinct().all()
        registered_file_types = [ft[0] for ft in registered_types]
        
        # Get all metadata and extract values while in session to avoid detached instance errors
        all_metadata = session.query(TableMetadata).all()
        metadata_by_file_type = {}
        for m in all_metadata:
            metadata_by_file_type[m.file_type] = {
                "status": m.status,
                "processing_folder_path": m.processing_folder_path,
                "metadata_obj": m  # Keep reference for handle_invalid_metadata if needed
            }

    file_type_map = group_files_by_type(files, registered_file_types)
    moved_to_processing = []
    moved_to_failed = []
    
    for file_type, file_list in file_type_map.items():
        if file_type == "unknown":
            handle_unknown_files(file_list, moved_to_failed)
            continue

        metadata_info = metadata_by_file_type.get(file_type)
        
        if not metadata_info or metadata_info["status"] != "active":
            metadata_obj = metadata_info["metadata_obj"] if metadata_info else None
            handle_invalid_metadata(file_type, metadata_obj, file_list, moved_to_failed)
            continue

        # Use extracted values
        processing_folder_path = metadata_info["processing_folder_path"] or file_type
        processing_folder = PROCESSING_FOLDER / processing_folder_path
        processing_folder_str = str(processing_folder)
        
        if not s3_driver.exists(processing_folder_str) and not s3_driver.is_dir(processing_folder_str):
            try:
                s3_driver.create_folder(processing_folder_str)
                logger.info(f"Created processing folder: {processing_folder_str}")
            except Exception as e:
                logger.error(f"Failed to create processing folder for '{file_type}': {e}")
                for file_path, file_name in file_list:
                    dest_path = FAILED_FOLDER / file_type / file_name
                    try:
                        s3_driver.move_file(file_path, str(dest_path))
                        moved_to_failed.append({
                            "file": file_name,
                            "file_type": file_type,
                            "reason": f"Failed to create processing folder: {str(e)}"
                        })
                    except Exception as move_error:
                        logger.error(f"Failed to move {file_name} to failed folder: {move_error}")
                continue
        
        move_files_to_processing(
            file_type, processing_folder, file_list, moved_to_processing, moved_to_failed
        )

    logger.info(
        f"Complete: {len(moved_to_processing)} to processing, {len(moved_to_failed)} to failed"
    )
    
    return {
        "moved_to_processing": len(moved_to_processing),
        "moved_to_failed": len(moved_to_failed),
        "files": moved_to_processing,
        "failed_files": moved_to_failed,
    }

