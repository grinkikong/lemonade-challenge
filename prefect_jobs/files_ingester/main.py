"""Files Ingestion Pipeline: Combined Prefect Job.

This job consists of three dependent tasks:
1. Task 1: Discovers new files in incoming folder, validates metadata, and moves to processing folders
2. Task 2: Automatically discover and process all folders in processing directory with Spark jobs (writes to Parquet)
3. Task 3: Cleanup - Move successfully processed files from processing/ to processed/ folder
"""

from prefect import flow, get_run_logger
from prefect_jobs.files_ingester.tasks.file_discovery import discover_and_move_files
from prefect_jobs.files_ingester.tasks.spark_processing import process_all_folders_with_spark
from prefect_jobs.files_ingester.tasks.cleanup import cleanup_processed_files


@flow(name="files_ingester_flow", log_prints=True)
def files_ingester_flow():
    """Files Ingestion Pipeline: Combined flow with three dependent tasks.

    Task 1: Discovers new files in incoming folder, validates metadata, and moves to processing folders
    Task 2: Discovers processing folders and processes all files with Spark jobs (writes to Parquet)
    Task 3: Cleanup - Moves successfully processed files from processing/ to processed/ folder
    """
    logger = get_run_logger()
    logger.info("Starting files ingestion pipeline flow")

    try:
        # Task 1: Discover and move files
        task1_result = discover_and_move_files()
        
        # Task 2: Process files with Spark (depends on Task 1)
        # Only run Task 2 if Task 1 moved files to processing
        task2_result = None
        if task1_result.get("moved_to_processing", 0) > 0:
            task2_result = process_all_folders_with_spark()
        else:
            logger.info("No files moved to processing - skipping Spark processing")
            task2_result = {"folders_processed": 0, "total_processed": 0, "total_failed": 0, "total_schema_errors": 0, "folder_results": [], "folder_errors": []}
        
        # Ensure Task 2 is fully complete before proceeding
        # This ensures all mapped sub-tasks within Task 2 have finished
        logger.info(f"Task 2 completed: {task2_result.get('folders_processed', 0)} folders processed")
        
        # Task 3: Cleanup processed files (depends on Task 2 completing)
        # Always run Task 3 after Task 2 completes (success or failure)
        # The cleanup task itself will only move files that were successfully processed
        # (it verifies Parquet files exist before moving)
        logger.info("Starting Task 3: Cleanup processed files (after Task 2 completion)")
        task3_result = cleanup_processed_files()

        # Alert if any files failed or schema validation failed - raise exception to ensure visibility
        if task2_result.get("total_failed", 0) > 0 or task2_result.get("total_schema_errors", 0) > 0 or task2_result.get("folder_errors"):
            alert_message = (
                f"PIPELINE PROCESSING FAILED - Total failed files: {task2_result.get('total_failed', 0)}, "
                f"Total schema errors: {task2_result.get('total_schema_errors', 0)}, "
                f"Total processed: {task2_result.get('total_processed', 0)}, "
                f"Folders processed: {task2_result.get('folders_processed', 0)}, "
                f"Folder errors: {len(task2_result.get('folder_errors', []))}. "
                f"Data engineer action required: Check failed folder for details and investigate failures."
            )
            logger.error(alert_message)
            logger.error(
                f"ALERT: Pipeline completed with {task2_result.get('total_failed', 0)} failed file(s) "
                f"and {task2_result.get('total_schema_errors', 0)} schema error(s). "
                f"Check failed folder for details. Processed: {task2_result.get('total_processed', 0)}, "
                f"Failed: {task2_result.get('total_failed', 0)}, Schema errors: {task2_result.get('total_schema_errors', 0)}"
            )
            if task2_result.get("folder_errors"):
                for folder_error in task2_result.get("folder_errors", []):
                    logger.error(f"Folder error: {folder_error['folder']} - {folder_error['error']}")
            # Raise exception to ensure Prefect flow shows failure state
            raise Exception(alert_message)

        return {
            "task1": {
                "moved_to_processing": task1_result.get("moved_to_processing", 0),
                "moved_to_failed": task1_result.get("moved_to_failed", 0),
                "failed_files": task1_result.get("failed_files", []),
            },
            "task2": task2_result,
            "task3": task3_result,
        }

    except Exception as e:
        logger.error(f"Error in files ingestion pipeline flow: {e}")
        raise


if __name__ == "__main__":
    files_ingester_flow()

