"""Utilities for daily summary aggregation."""

import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, date
from data_utils.aws.s3_driver import s3_driver
from prefect_jobs.daily_summary_aggregation.config import (
    DATALAKE_EVENTS_TABLE,
    DATALAKE_SUMMARY_TABLE,
)


def read_events_from_datalake(target_date: date = None) -> List[Dict[str, Any]]:
    """Read events from datalake for a specific date.

    In production, this would query Iceberg table.
    For local dev, reads Parquet files from local datalake structure.

    Args:
        target_date: Date to read events for. If None, reads all dates.

    Returns:
        List of event records.
    """
    all_events = []

    try:
        import pyarrow.parquet as pq
        import pyarrow as pa
    except ImportError:
        # Fallback to DuckDB if pyarrow not available
        try:
            import duckdb
            use_duckdb = True
        except ImportError:
            raise ImportError("Either pyarrow or duckdb must be installed to read Parquet files")

    if target_date:
        # Read specific date partition
        partition_path = DATALAKE_EVENTS_TABLE / f"date={target_date.strftime('%Y-%m-%d')}"
        parquet_files = list(partition_path.glob("*.parquet"))
        
        if not parquet_files:
            return []
        
        try:
            import pyarrow.parquet as pq
            # Read all Parquet files in partition
            for parquet_file in parquet_files:
                table = pq.read_table(str(parquet_file))
                # Convert to list of dicts
                events = table.to_pylist()
                all_events.extend(events)
        except ImportError:
            # Fallback to DuckDB
            import duckdb
            conn = duckdb.connect()
            file_paths = [str(f) for f in parquet_files]
            file_list = "', '".join(file_paths)
            result = conn.execute(f"SELECT * FROM read_parquet(['{file_list}'])").fetchall()
            columns = [desc[0] for desc in conn.execute(f"SELECT * FROM read_parquet(['{file_list}']) LIMIT 0").description]
            all_events = [dict(zip(columns, row)) for row in result]
            conn.close()
    else:
        # Read all partitions (for local dev)
        base_path = DATALAKE_EVENTS_TABLE
        if base_path.exists():
            parquet_files = list(base_path.glob("**/*.parquet"))
            
            if not parquet_files:
                return []
            
            try:
                import pyarrow.parquet as pq
                # Read all Parquet files
                for parquet_file in parquet_files:
                    table = pq.read_table(str(parquet_file))
                    events = table.to_pylist()
                    all_events.extend(events)
            except ImportError:
                # Fallback to DuckDB
                import duckdb
                conn = duckdb.connect()
                file_paths = [str(f) for f in parquet_files]
                file_list = "', '".join(file_paths)
                result = conn.execute(f"SELECT * FROM read_parquet(['{file_list}'])").fetchall()
                columns = [desc[0] for desc in conn.execute(f"SELECT * FROM read_parquet(['{file_list}']) LIMIT 0").description]
                all_events = [dict(zip(columns, row)) for row in result]
                conn.close()

    return all_events


def aggregate_daily_summary(events: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Aggregate events into daily summary.

    Args:
        events: List of event records.

    Returns:
        Dictionary keyed by (vehicle_id, day) with last event info.
    """
    summary = {}

    for event in events:
        vehicle_id = event.get("vehicle_id")
        event_time_str = event.get("event_time", "")
        event_type = event.get("event_type")

        if not vehicle_id or not event_time_str:
            continue

        try:
            # Parse event_time
            event_time = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
            event_date = event_time.date()

            key = f"{vehicle_id}_{event_date}"

            if key not in summary:
                summary[key] = {
                    "vehicle_id": vehicle_id,
                    "day": event_date,
                    "last_event_time": event_time,
                    "last_event_type": event_type,
                }
            else:
                # Update if this event is later
                if event_time > summary[key]["last_event_time"]:
                    summary[key]["last_event_time"] = event_time
                    summary[key]["last_event_type"] = event_type

        except Exception as e:
            print(f"Error processing event: {e}")
            continue

    return summary


def write_summary_to_iceberg(summary: Dict[str, Dict[str, Any]], target_date: date = None):
    """Write summary to Iceberg table.

    In production, uses PyIceberg to write to Iceberg table.
    For local dev, writes Parquet files to local datalake structure.

    Args:
        summary: Summary dictionary.
        target_date: Target date for summary.
    """
    if target_date:
        partition_path = DATALAKE_SUMMARY_TABLE / f"date={target_date.strftime('%Y-%m-%d')}"
    else:
        partition_path = DATALAKE_SUMMARY_TABLE / "latest"

    partition_path.mkdir(parents=True, exist_ok=True)

    # Convert summary to list
    summary_list = list(summary.values())
    
    if not summary_list:
        return

    # Write as Parquet file (matching datalake format)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        # Convert to PyArrow table
        # Ensure all datetime objects are converted to strings for Parquet
        for record in summary_list:
            if isinstance(record.get("day"), date):
                record["day"] = record["day"].isoformat()
            if isinstance(record.get("last_event_time"), datetime):
                record["last_event_time"] = record["last_event_time"].isoformat()
        
        table = pa.Table.from_pylist(summary_list)
        
        # Write Parquet file
        output_file = partition_path / f"daily_summary_{target_date.strftime('%Y-%m-%d') if target_date else 'latest'}.parquet"
        pq.write_table(table, str(output_file))
    except ImportError:
        # Fallback to JSON if pyarrow not available
        output_file = partition_path / f"daily_summary_{target_date.strftime('%Y-%m-%d') if target_date else 'latest'}.json"
        s3_driver.write_file(str(output_file), json.dumps(summary_list, default=str).encode("utf-8"))


# Production Iceberg implementation (commented for reference)
# ============================================================
# def write_summary_to_iceberg_production(summary: Dict[str, Dict[str, Any]]):
#     """Write summary to Iceberg table using PyIceberg."""
#     from pyiceberg.catalog import load_catalog
#     from pyarrow import Table
#     import pandas as pd
#
#     catalog = load_catalog(
#         "s3",
#         uri=ICEBERG_CATALOG_URI,
#         warehouse=ICEBERG_WAREHOUSE
#     )
#
#     table = catalog.load_table(ICEBERG_SUMMARY_TABLE)
#
#     # Convert to DataFrame
#     df = pd.DataFrame(list(summary.values()))
#
#     # Convert to PyArrow table
#     pa_table = Table.from_pandas(df)
#
#     # Append to Iceberg table
#     table.append(pa_table)
# ============================================================

