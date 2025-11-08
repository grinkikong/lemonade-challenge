"""Daily Prefect Job: Summary Table Creation.

Creates daily summary table in Iceberg format (managed by Glue Catalog).
Aggregates data from vehicle_events table to create summary with:
- vehicle_id
- day
- last_event_time
- last_event_type
"""

from datetime import date, datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect_jobs.daily_summary_aggregation.config import DATALAKE_SUMMARY_TABLE
from prefect_jobs.daily_summary_aggregation.utils import (
    read_events_from_datalake,
    aggregate_daily_summary,
    write_summary_to_iceberg,
)


@task
def read_events_for_date(target_date: date) -> list:
    """Read events from datalake for a specific date."""
    logger = get_run_logger()
    logger.info(f"Reading events for date: {target_date}")

    events = read_events_from_datalake(target_date)
    logger.info(f"Read {len(events)} events for {target_date}")

    return events


@task
def create_daily_summary(events: list, target_date: date) -> dict:
    """Create daily summary from events."""
    logger = get_run_logger()
    logger.info(f"Aggregating summary for {target_date}")

    summary = aggregate_daily_summary(events)
    logger.info(f"Created summary for {len(summary)} vehicle-day combinations")

    return summary


@task
def write_summary_table(summary: dict, target_date: date):
    """Write summary to Iceberg table."""
    logger = get_run_logger()
    logger.info(f"Writing summary table for {target_date}")

    write_summary_to_iceberg(summary, target_date)
    logger.info(f"Successfully wrote summary table for {target_date}")


@flow(name="daily_summary_aggregation_flow", log_prints=True)
def daily_summary_aggregation_flow(target_date: str | None = None):
    """Daily Prefect Job: Create summary table from events.

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

        # Read events for target date
        events = read_events_for_date(target)

        if not events:
            logger.warning(f"No events found for {target}")
            return {"status": "no_events", "date": str(target)}

        # Create summary
        summary = create_daily_summary(events, target)

        if not summary:
            logger.warning(f"No summary data created for {target}")
            return {"status": "no_summary", "date": str(target)}

        # Write to Iceberg table
        write_summary_table(summary, target)

        logger.info(f"Successfully completed summary aggregation for {target}")
        return {
            "status": "success",
            "date": str(target),
            "summary_records": len(summary),
        }

    except Exception as e:
        logger.error(f"Error in daily summary aggregation flow: {e}")
        raise


if __name__ == "__main__":
    daily_summary_aggregation_flow()

