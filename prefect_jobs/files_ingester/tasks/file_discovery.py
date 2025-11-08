"""Task 1: Discover and move files to processing folders based on file type detection.

TODO v2: Add metadata table validation:
- Check metadata status (active/inactive)
- Validate file types against database
- Support dynamic file types
"""

import re
from pathlib import Path
from prefect import task, get_run_logger
from data_utils.aws.s3_driver import s3_driver
from prefect_jobs.files_ingester.config import (
    INCOMING_FOLDER,
    PROCESSING_FOLDER,
    FAILED_FOLDER,
    SUPPORTED_FILE_TYPES,
)


def detect_file_type_from_filename(file_name: str) -> str:
    """Detect file type from filename pattern: {file_type}_{timestamp}.json
    
    Returns:
        Detected file type (vehicle_events, vehicle_status) or "unknown"
    """
    match = re.match(r"^([a-z_]+)_\d{14}\.json$", file_name.lower())
    if match:
        detected_type = match.group(1)
        # Normalize plural to singular (vehicles_events -> vehicle_events)
        if detected_type.startswith("vehicles_"):
            detected_type = detected_type.replace("vehicles_", "vehicle_", 1)
        
        # Check if it's a supported file type
        if detected_type in SUPPORTED_FILE_TYPES:
            return detected_type
    
    # Fallback: check if filename contains any supported file type
    for file_type in SUPPORTED_FILE_TYPES:
        if file_type in file_name.lower():
            return file_type
    
    return "unknown"


def group_files_by_type(files: list) -> dict:
    """Group files by detected file type."""
    file_type_map = {}
    
    for file_path in files:
        file_name = Path(file_path).name
        file_type = detect_file_type_from_filename(file_name)
        
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
    """Discover new files and move to processing folders based on file type detection.
    
    Files go to failed/ folder if:
    - Unknown file type (not vehicle_events or vehicle_status)
    - I/O errors when moving to processing folder
    """
    logger = get_run_logger()
    logger.info("Starting file discovery and movement")

    # Convert Path to string for list_files (INCOMING_FOLDER is already resolved to absolute path)
    incoming_path_str = str(INCOMING_FOLDER)
    logger.info(f"Scanning incoming folder: {incoming_path_str}")
    files = s3_driver.list_files(incoming_path_str)
    logger.info(f"Found {len(files)} files in incoming folder")

    if not files:
        return {"moved_to_processing": 0, "moved_to_failed": 0, "files": []}

    file_type_map = group_files_by_type(files)
    moved_to_processing = []
    moved_to_failed = []
    
    for file_type, file_list in file_type_map.items():
        if file_type == "unknown":
            handle_unknown_files(file_list, moved_to_failed)
            continue

        # Create processing folder for this file type
        processing_folder = PROCESSING_FOLDER / file_type
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

