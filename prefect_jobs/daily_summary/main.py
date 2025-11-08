"""Daily Prefect Job: Summary Table Creation.

Creates daily summary table using Spark aggregation.
Aggregates data from vehicle_events table to create summary with:
- vehicle_id
- day
- last_event_time
- last_event_type
"""

from datetime import date, datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect_jobs.daily_summary.utils.spark_aggregator import (
    spark_daily_aggregation,
)


@task
def create_daily_summary(target_date: date) -> dict:
    """Create daily summary using Spark aggregation."""
    logger = get_run_logger()
    logger.info(f"Aggregating summary for {target_date}")
    
    result = spark_daily_aggregation(target_date)
    
    if result["status"] == "no_events":
        message = result.get("message", f"No events found for {target_date}")
        logger.warning(f"No events found for {target_date}: {message}")
    elif result["status"] == "success":
        logger.info(f"Created summary: {result['records_count']} records")
    else:
        # Schema validation or processing failure - alert only (no files to move)
        error_msg = result.get('error', 'Unknown error')
        logger.error(f"SCHEMA VALIDATION FAILED - Daily summary aggregation failed: {error_msg}")
        logger.error("This is an alert - no files to move (reading from Parquet datalake)")
        raise Exception(f"Aggregation failed: {error_msg}")
    
    return result


@flow(name="daily_summary_aggregation_flow", log_prints=True)
def daily_summary_aggregation_flow(target_date: str | None = None):
    """Daily Prefect Job: Create summary table from events using Spark.

    Args:
        target_date: Date to process (YYYY-MM-DD format). If None, uses yesterday.
    """
    logger = get_run_logger()
    logger.info("Starting daily summary aggregation flow")

    try:
        # Determine target date
        if target_date:
            target = datetime.strptime(target_date, "%Y-%m-%d").date()
        else:
            # Default to yesterday
            target = (datetime.now() - timedelta(days=1)).date()

        logger.info(f"Processing summary for date: {target}")

        # Create summary using Spark
        result = create_daily_summary(target)

        logger.info(f"Successfully completed summary aggregation for {target}")
        return {
            "status": result["status"],
            "date": str(target),
            "summary_records": result.get("records_count", 0),
        }

    except Exception as e:
        logger.error(f"Error in daily summary aggregation flow: {e}")
        raise


if __name__ == "__main__":
    import sys
    
    # Allow passing date as command line argument
    # Usage: python main.py [YYYY-MM-DD]
    # Example: python main.py 2025-11-08
    target_date = sys.argv[1] if len(sys.argv) > 1 else '2025-11-09'
    daily_summary_aggregation_flow(target_date=target_date)

