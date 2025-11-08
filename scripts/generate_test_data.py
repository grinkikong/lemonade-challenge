#!/usr/bin/env python3
"""Test data generator for the data ingestion pipeline.

Generates realistic JSON files for vehicles_events and vehicles_status.
Creates all necessary metadata in database for happy flow.
Can run continuously to generate files every X seconds.

Usage:
    # Generate 5 files of each type and setup metadata (default)
    python scripts/generate_test_data.py

    # Generate 10 files of each type
    python scripts/generate_test_data.py --num-files 10

    # Generate files continuously every 10 seconds
    python scripts/generate_test_data.py --interval 10 --continuous
"""

import sys
import json
import argparse
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import random
import uuid

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_utils.aws.s3_driver import s3_driver
from data_utils.database.drivers import db_driver
from project_utils import ENV

# Sample data
EVENT_TYPES = [
    "start_drive",
    "end_drive",
    "door_open",
    "door_close",
    "accident",
    "parking",
    "speed_limit_exceeded",
    "engine_start",
    "engine_stop",
]

EVENT_SOURCES = ["mobile", "device", "sensor"]
STATUS_TYPES = ["driving", "parked", "accident", "garage", "maintenance", "offline"]
STATUS_SOURCES = ["mobile", "device", "police", "garage"]


def generate_vehicle_id() -> str:
    """Generate a random vehicle ID."""
    return str(uuid.uuid4()).replace("-", "")


# Global list to track vehicle IDs for reuse
_reusable_vehicle_ids = []


def generate_event(vehicle_id: str, event_time: datetime = None) -> dict:
    """Generate a single vehicle event."""
    if event_time is None:
        event_time = datetime.now()

    event_type = random.choice(EVENT_TYPES)
    event_source = random.choice(EVENT_SOURCES)

    if event_type in ["start_drive", "end_drive", "parking"]:
        event_value = f"{random.uniform(-180, 180):.7f},{random.uniform(-90, 90):.7f}"
    elif event_type == "door_open":
        event_value = random.choice(["driver_door", "passenger_door", "trunk"])
    elif event_type == "speed_limit_exceeded":
        event_value = str(random.randint(80, 120))
    else:
        event_value = None

    event_extra_data = {
        "note": f"Generated test event: {event_type}",
        "boot_time": random.randint(5, 30) if event_source == "device" else None,
    }

    if event_type == "accident":
        event_extra_data["emergency_call"] = True
        event_extra_data["severity"] = random.choice(["low", "medium", "high"])

    return {
        "vehicle_id": vehicle_id,
        "event_time": event_time.isoformat() + "Z",
        "event_source": event_source,
        "event_type": event_type,
        "event_value": event_value,
        "event_extra_data": event_extra_data,
    }


def generate_status(vehicle_id: str, report_time: datetime = None) -> dict:
    """Generate a single vehicle status."""
    if report_time is None:
        report_time = datetime.now()

    status = random.choice(STATUS_TYPES)
    status_source = random.choice(STATUS_SOURCES)

    return {
        "vehicle_id": vehicle_id,
        "report_time": report_time.isoformat() + "Z",
        "status_source": status_source,
        "status": status,
    }


def generate_filename(file_type: str, timestamp: datetime = None) -> str:
    """Generate filename with timestamp."""
    if timestamp is None:
        timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d%H%M%S")
    return f"{file_type}_{timestamp_str}.json"


def cleanup_all_data(clean_datalake=False):
    """Clean all data folders and optionally datalake, but NOT database (metadata preserved).
    
    Args:
        clean_datalake: If True, also cleans the datalake. Default: False.
    """
    print("Cleaning all existing data (keeping database)...")
    
    # Clean data folders (use absolute paths from project root)
    project_root = Path(__file__).parent.parent
    data_folders = [
        project_root / "data" / "source_data" / "incoming",
        project_root / "data" / "source_data" / "processing",
        project_root / "data" / "source_data" / "failed",
    ]
    
    for folder in data_folders:
        if folder.exists():
            shutil.rmtree(folder)
            print(f"✓ Removed {folder}")
        folder.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created {folder}")
    
    # Clean datalake only if requested
    if clean_datalake:
        datalake_path = project_root / "data" / "datalake"
        
        if datalake_path.exists():
            shutil.rmtree(datalake_path)
            print(f"✓ Removed datalake: {datalake_path}")
        datalake_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created datalake: {datalake_path}")
    else:
        print("✓ Datalake kept (not cleaned)")
    
    # Database cleanup removed - keep existing metadata
    print("✓ Database kept (metadata preserved)")
    print("✓ All data cleaned (except database)\n")


def setup_metadata():
    """Create metadata entries in database for happy flow."""
    print("Setting up metadata in database...")
    
    # Ensure database tables exist
    if db_driver.database_url.startswith("sqlite"):
        db_path = Path(db_driver.database_url.replace("sqlite:///", ""))
        db_path.parent.mkdir(parents=True, exist_ok=True)
    db_driver.create_tables()
    print("✓ Database tables ready")
    
    # Vehicle events metadata
    vehicle_events_schema = {
        "type": "object",
        "properties": {
            "vehicle_id": {"type": "string"},
            "event_time": {"type": "string", "format": "date-time"},
            "event_source": {"type": "string"},
            "event_type": {"type": "string"},
            "event_value": {"type": ["string", "null"]},
            "event_extra_data": {"type": ["object", "null"]}
        },
        "required": ["vehicle_id", "event_time", "event_source", "event_type"]
    }
    
    db_driver.get_or_create_table_metadata(
        table_name="vehicle_events",
        file_type="vehicle_events",
        schema_definition=vehicle_events_schema,
        partition_column="date",
        processing_config={"mode": "append", "format": "parquet"},
        description="Vehicle events table - stores all vehicle events",
        status="active"
    )
    print("✓ Created metadata for vehicle_events")
    
    # Vehicle status metadata
    vehicle_status_schema = {
        "type": "object",
        "properties": {
            "vehicle_id": {"type": "string"},
            "report_time": {"type": "string", "format": "date-time"},
            "status_source": {"type": "string"},
            "status": {"type": "string"}
        },
        "required": ["vehicle_id", "report_time", "status_source", "status"]
    }
    
    db_driver.get_or_create_table_metadata(
        table_name="vehicle_status",
        file_type="vehicle_status",
        schema_definition=vehicle_status_schema,
        partition_column="date",
        processing_config={"mode": "append", "format": "parquet"},
        description="Vehicle status table - stores vehicle status reports",
        status="active"
    )
    print("✓ Created metadata for vehicle_status")
    print("Metadata setup complete!\n")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Generate test data files")
    parser.add_argument(
        "--interval", type=int, default=10, help="Interval in seconds (default: 10)"
    )
    parser.add_argument("--continuous", action="store_true", help="Generate continuously")
    parser.add_argument("--retroactive", action="store_true", help="Generate retroactive files")
    parser.add_argument("--skip-metadata", action="store_true", help="Skip metadata setup")
    parser.add_argument("--cleanup", action="store_true", help="Clean source data folders before generating (default: no cleanup)")
    parser.add_argument("--clean-datalake", action="store_true", help="Also clean datalake when using --cleanup (default: datalake is preserved)")
    parser.add_argument("--num-files", type=int, default=5, help="Number of files to generate for each type (default: 5)")
    parser.add_argument("--date", type=str, help="Generate data for specific date (YYYY-MM-DD). Default: today")
    parser.add_argument("--days-ago", type=int, help="Generate data for N days ago (e.g., --days-ago 1 for yesterday)")
    parser.add_argument("--type", type=str, choices=["events", "status", "both"], default="both", 
                        help="Type of files to generate: 'events', 'status', or 'both' (default: both)")
    parser.add_argument("--same-vehicle", action="store_true",
                        help="Generate multiple events for the same vehicle_id (useful for testing daily summary 'last_event_type')")

    args = parser.parse_args()

    # Clean all existing data before generating new data (only if --cleanup is provided)
    if args.cleanup:
        cleanup_all_data(clean_datalake=args.clean_datalake)
    else:
        print("ℹ️  No cleanup requested - existing data will be preserved")
        print("   Use --cleanup to clean source data folders, --clean-datalake to also clean datalake\n")

    # Setup metadata for happy flow
    if not args.skip_metadata:
        setup_metadata()

    # Use absolute path from project root
    project_root = Path(__file__).parent.parent
    # Use absolute path to ensure we're in project root, not scripts folder
    incoming_folder = project_root / "data" / "source_data" / "incoming"
    incoming_folder.mkdir(parents=True, exist_ok=True)

    # Determine target date for data generation (default: today)
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            base_timestamp = datetime.combine(target_date, datetime.now().time())
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD")
            return
    elif args.days_ago:
        target_date = datetime.now().date() - timedelta(days=args.days_ago)
        base_timestamp = datetime.combine(target_date, datetime.now().time())
    else:
        # Default to today
        target_date = datetime.now().date()
        base_timestamp = datetime.now()
    
    # Determine which file types to generate
    should_generate_events = args.type in ["events", "both"]
    should_generate_status = args.type in ["status", "both"]
    
    # Initialize reusable vehicle IDs if same-vehicle mode
    if args.same_vehicle:
        # Create a small pool of vehicle IDs (2-3) so they repeat across files
        # This ensures the same vehicle appears in multiple files for testing aggregation
        num_vehicles = min(3, max(2, args.num_files // 2))  # Use 2-3 vehicles so they repeat
        global _reusable_vehicle_ids
        _reusable_vehicle_ids = [generate_vehicle_id() for _ in range(num_vehicles)]
        print(f"🔁 Same-vehicle mode: Will reuse {len(_reusable_vehicle_ids)} vehicle IDs across {args.num_files} files")
        print(f"   Each vehicle will appear in multiple files to test 'last_event_type' aggregation\n")
    
    if args.continuous:
        print(f"Generating files every {args.interval} seconds. Press Ctrl+C to stop.")
        try:
            while True:
                timestamp = base_timestamp if not args.retroactive else datetime.now()
                if args.retroactive and random.random() < 0.3:
                    timestamp = timestamp - timedelta(days=random.randint(1, 7))

                    if should_generate_events:
                        # Generate events file
                        filename = generate_filename("vehicles_events", timestamp)
                        file_path = incoming_folder / filename
                        # Use same vehicle IDs if same-vehicle mode, otherwise generate new ones
                        if args.same_vehicle and _reusable_vehicle_ids:
                            # Cycle through vehicle IDs for continuous mode too
                            vehicle_id = random.choice(_reusable_vehicle_ids)  # For continuous, random is OK
                            events = [generate_event(vehicle_id, timestamp) for _ in range(random.randint(1, 10))]
                        else:
                            events = [generate_event(generate_vehicle_id(), timestamp) for _ in range(random.randint(1, 10))]
                        s3_driver.write_file(str(file_path), json.dumps({"vehicles_events": events}, indent=2).encode())
                        print(f"✓ Generated: {filename} ({len(events)} events)")

                if should_generate_status:
                    # Generate status file
                    filename = generate_filename("vehicles_status", timestamp)
                    file_path = incoming_folder / filename
                    statuses = [generate_status(generate_vehicle_id(), timestamp) for _ in range(random.randint(1, 5))]
                    s3_driver.write_file(str(file_path), json.dumps({"vehicle_status": statuses}, indent=2).encode())
                    print(f"✓ Generated: {filename} ({len(statuses)} statuses)")

                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nStopped generating files.")
    else:
        # Generate multiple files with different timestamps
        target_date_str = base_timestamp.strftime("%Y-%m-%d")
        type_str = args.type
        print(f"Generating {args.num_files} {type_str} file(s) for date: {target_date_str}...")
        
        for i in range(args.num_files):
            # Vary timestamps throughout the target date (spread over the day)
            # In same-vehicle mode, spread events across the day to test "last_event_type"
            if args.same_vehicle:
                # Spread events evenly across the day for better testing
                hours_offset = int((i * 24) / args.num_files)  # Distribute evenly
                minutes_offset = random.randint(0, 59)
            else:
                hours_offset = random.randint(0, 23)
                minutes_offset = random.randint(0, 59)
            timestamp = base_timestamp.replace(hour=hours_offset, minute=minutes_offset, second=0, microsecond=0)
            
            if should_generate_events:
                # Generate events file with varying number of events
                filename = generate_filename("vehicles_events", timestamp)
                file_path = incoming_folder / filename
                num_events = random.randint(3, 15)
                # Use same vehicle IDs if same-vehicle mode, otherwise generate new ones
                if args.same_vehicle and _reusable_vehicle_ids:
                    # Cycle through vehicle IDs so each appears in multiple files
                    # This ensures the same vehicle has events across different times of the day
                    vehicle_id = _reusable_vehicle_ids[i % len(_reusable_vehicle_ids)]
                    events = [generate_event(vehicle_id, timestamp) for _ in range(num_events)]
                    print(f"✓ Generated: {filename} ({len(events)} events for vehicle {vehicle_id[:8]}... at {timestamp.strftime('%H:%M')})")
                else:
                    events = [generate_event(generate_vehicle_id(), timestamp) for _ in range(num_events)]
                    print(f"✓ Generated: {filename} ({len(events)} events)")
                s3_driver.write_file(str(file_path), json.dumps({"vehicles_events": events}, indent=2).encode())
            
            if should_generate_status:
                # Generate status file with varying number of statuses
                filename = generate_filename("vehicles_status", timestamp)
                file_path = incoming_folder / filename
                num_statuses = random.randint(2, 8)
                statuses = [generate_status(generate_vehicle_id(), timestamp) for _ in range(num_statuses)]
                s3_driver.write_file(str(file_path), json.dumps({"vehicle_status": statuses}, indent=2).encode())
                print(f"✓ Generated: {filename} ({len(statuses)} statuses)")
        
        total_files = args.num_files * (should_generate_events + should_generate_status)
        print(f"\n✓ Happy flow setup complete! Generated {total_files} file(s) in {incoming_folder}")


if __name__ == "__main__":
    main()

