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


def cleanup_all_data():
    """Clean all data folders, datalake, and database before generating new data."""
    print("Cleaning all existing data...")
    
    # Clean data folders (use absolute paths from project root)
    project_root = Path(__file__).parent.parent
    data_folders = [
        project_root / "data" / "incoming",
        project_root / "data" / "source_data" / "processing",
        project_root / "data" / "source_data" / "failed",
        project_root / "data" / "source_data" / "error",
        project_root / "data" / "processed",
    ]
    
    # Clean datalake (recursive - contains partitioned data)
    datalake_path = project_root / "data" / "datalake"
    
    for folder in data_folders:
        if folder.exists():
            shutil.rmtree(folder)
            print(f"✓ Removed {folder}")
        folder.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created {folder}")
    
    # Clean datalake (recursive - contains partitioned data)
    if datalake_path.exists():
        shutil.rmtree(datalake_path)
        print(f"✓ Removed datalake: {datalake_path}")
    datalake_path.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created datalake: {datalake_path}")
    
    # Clean database
    if db_driver.database_url.startswith("sqlite"):
        db_path = Path(db_driver.database_url.replace("sqlite:///", ""))
        if db_path.exists():
            db_path.unlink()
            print(f"✓ Removed database: {db_path}")
    
    print("✓ All data cleaned\n")


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
    parser.add_argument("--no-cleanup", action="store_true", help="Skip cleanup of existing data")
    parser.add_argument("--num-files", type=int, default=5, help="Number of files to generate for each type (default: 5)")

    args = parser.parse_args()

    # Clean all existing data before generating new data
    if not args.no_cleanup:
        cleanup_all_data()

    # Setup metadata for happy flow
    if not args.skip_metadata:
        setup_metadata()

    # Use absolute path from project root
    project_root = Path(__file__).parent.parent
    incoming_folder = project_root / "data" / "incoming"
    incoming_folder.mkdir(parents=True, exist_ok=True)

    if args.continuous:
        print(f"Generating files every {args.interval} seconds. Press Ctrl+C to stop.")
        try:
            while True:
                timestamp = datetime.now()
                if args.retroactive and random.random() < 0.3:
                    timestamp = timestamp - timedelta(days=random.randint(1, 7))

                # Generate events file
                filename = generate_filename("vehicles_events", timestamp)
                file_path = incoming_folder / filename
                events = [generate_event(generate_vehicle_id(), timestamp) for _ in range(random.randint(1, 10))]
                s3_driver.write_file(str(file_path), json.dumps({"vehicles_events": events}, indent=2).encode())
                print(f"✓ Generated: {filename} ({len(events)} events)")

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
        # Generate multiple files of each type with different timestamps and events
        print(f"Generating {args.num_files} files of each type...")
        
        base_timestamp = datetime.now()
        
        for i in range(args.num_files):
            # Vary timestamps slightly (spread over last hour)
            timestamp_offset = timedelta(minutes=random.randint(-60, 0))
            timestamp = base_timestamp + timestamp_offset
            
            # Generate events file with varying number of events
            filename = generate_filename("vehicles_events", timestamp)
            file_path = incoming_folder / filename
            num_events = random.randint(3, 15)
            events = [generate_event(generate_vehicle_id(), timestamp) for _ in range(num_events)]
            s3_driver.write_file(str(file_path), json.dumps({"vehicles_events": events}, indent=2).encode())
            print(f"✓ Generated: {filename} ({len(events)} events)")
            
            # Generate status file with varying number of statuses
            filename = generate_filename("vehicles_status", timestamp)
            file_path = incoming_folder / filename
            num_statuses = random.randint(2, 8)
            statuses = [generate_status(generate_vehicle_id(), timestamp) for _ in range(num_statuses)]
            s3_driver.write_file(str(file_path), json.dumps({"vehicle_status": statuses}, indent=2).encode())
            print(f"✓ Generated: {filename} ({len(statuses)} statuses)")
        
        print(f"\n✓ Happy flow setup complete! Generated {args.num_files * 2} files in {incoming_folder}")


if __name__ == "__main__":
    main()

