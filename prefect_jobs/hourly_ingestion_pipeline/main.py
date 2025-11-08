"""Hourly Ingestion Pipeline: Combined Prefect Job.

This job consists of two dependent tasks:
1. Task 1: Move files to processing folders and ensure table metadata exists
2. Task 2: Automatically discover and process all folders in processing directory with Spark jobs
"""

from prefect import flow, get_run_logger
from prefect_jobs.hourly_ingestion_pipeline.move_new_files import discover_and_move_files
from prefect_jobs.hourly_ingestion_pipeline.spark_processing import process_all_folders_with_spark


@flow(name="hourly_ingestion_pipeline_flow", log_prints=True)
def hourly_ingestion_pipeline_flow():
    """Hourly Ingestion Pipeline: Combined flow with two dependent tasks.

    Task 1: Discovers new files in incoming folder, validates metadata, and moves to processing folders
    Task 2: Discovers processing folders and processes all files with Spark jobs
    """
    logger = get_run_logger()
    logger.info("Starting hourly ingestion pipeline flow")

    try:
        task1_result = discover_and_move_files()
        task2_result = process_all_folders_with_spark()

        # Alert if any files failed or schema validation failed - raise exception to ensure visibility
        if task2_result.get("total_failed", 0) > 0 or task2_result.get("total_schema_errors", 0) > 0 or task2_result.get("folder_errors"):
            alert_message = (
                f"PIPELINE PROCESSING FAILED - Total failed files: {task2_result.get('total_failed', 0)}, "
                f"Total schema errors: {task2_result.get('total_schema_errors', 0)}, "
                f"Total processed: {task2_result.get('total_processed', 0)}, "
                f"Folders processed: {task2_result.get('folders_processed', 0)}, "
                f"Folder errors: {len(task2_result.get('folder_errors', []))}. "
                f"Data engineer action required: Check failed/error folders for details and investigate failures."
            )
            logger.error(alert_message)
            logger.error(
                f"ALERT: Pipeline completed with {task2_result.get('total_failed', 0)} failed file(s) "
                f"and {task2_result.get('total_schema_errors', 0)} schema error(s). "
                f"Check failed/error folders for details. Processed: {task2_result.get('total_processed', 0)}, "
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
        }

    except Exception as e:
        logger.error(f"Error in hourly ingestion pipeline flow: {e}")
        raise


if __name__ == "__main__":
    hourly_ingestion_pipeline_flow()
