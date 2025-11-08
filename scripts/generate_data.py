#!/usr/bin/env python3
"""Test data generator for the data ingestion pipeline.

Generates realistic JSON files for vehicles_events and vehicles_status.
Can run continuously to generate files every X seconds.

Usage:
    # Generate 5 files of each type (default)
    python scripts/generate_data.py

    # Generate 10 files of each type
    python scripts/generate_data.py --num-files 10

    # Generate files continuously every 10 seconds
    python scripts/generate_data.py --interval 10 --continuous
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

# Global counter for generating new vehicle IDs when needed
_vehicle_id_counter = 0


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


def main():
    """Main function."""
    # Declare global variables at the very top
    global _reusable_vehicle_ids, _vehicle_id_counter
    
    parser = argparse.ArgumentParser(description="Generate test data files")
    parser.add_argument(
        "--interval", type=int, default=10, help="Interval in seconds (default: 10)"
    )
    parser.add_argument("--continuous", action="store_true", help="Generate continuously")
    parser.add_argument("--retroactive", action="store_true", help="Generate retroactive files")
    parser.add_argument("--cleanup", action="store_true", help="Clean source data folders before generating (default: no cleanup)")
    parser.add_argument("--clean-datalake", action="store_true", help="Also clean datalake when using --cleanup (default: datalake is preserved)")
    parser.add_argument("--num-files", type=int, default=5, help="Number of files to generate for each type (default: 5)")
    parser.add_argument("--date", type=str, help="Generate data for specific date (YYYY-MM-DD). Default: today")
    parser.add_argument("--days-ago", type=int, help="Generate data for N days ago (e.g., --days-ago 1 for yesterday)")
    parser.add_argument("--type", type=str, choices=["events", "status", "both"], default="both", 
                        help="Type of files to generate: 'events', 'status', or 'both' (default: both)")
    parser.add_argument("--same-vehicle", action="store_true",
                        help="Generate multiple events for the same vehicle_id (default behavior, this flag is kept for compatibility)")
    parser.add_argument("--no-same-vehicle", action="store_true",
                        help="Disable reusing vehicle IDs across files (each file will have unique vehicle IDs)")
    parser.add_argument("--include-late-arrival", action="store_true",
                        help="Generate late-arriving data: files with today's timestamp but event_time from yesterday (for testing 2-day lookback)")

    args = parser.parse_args()

    # Clean all existing data before generating new data (only if --cleanup is provided)
    if args.cleanup:
        cleanup_all_data(clean_datalake=args.clean_datalake)
    else:
        print("ℹ️  No cleanup requested - existing data will be preserved")
        print("   Use --cleanup to clean source data folders, --clean-datalake to also clean datalake\n")


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
    
    # Initialize reusable vehicle IDs (default behavior for testing aggregation)
    # By default, reuse vehicle IDs across files so we can test daily summary aggregation
    # Users can disable this by using --no-same-vehicle flag if needed
    use_same_vehicle = not args.no_same_vehicle  # Default to True (reuse vehicle IDs)
    
    if use_same_vehicle:
        # Create a pool of vehicle IDs that will be reused across files
        # Generate enough vehicles so we have variety (one new vehicle per 5 events)
        # But still reuse them across files for aggregation testing
        # Estimate: if we have 5 files with ~10 events each = 50 events
        # With 1 vehicle per 5 events = 10 vehicles, but we'll reuse them across files
        estimated_events_per_file = 10
        total_estimated_events = args.num_files * estimated_events_per_file
        # Generate vehicle pool: one vehicle per 5 events, but reuse across files
        # So we need enough vehicles to cover all events, but they'll repeat across files
        num_vehicles = max(5, (total_estimated_events // 5))  # At least 5 vehicles, more if needed
        _reusable_vehicle_ids = [generate_vehicle_id() for _ in range(num_vehicles)]
        _vehicle_id_counter = 0
        print(f"🔁 Created pool of {len(_reusable_vehicle_ids)} vehicle IDs for reuse across files")
        print(f"   New vehicle ID every ~5 events, but vehicles will repeat across files")
        print(f"   This ensures variety while still testing 'last_event_type' aggregation")
        print(f"   Use --no-same-vehicle to disable this behavior\n")
    else:
        _reusable_vehicle_ids = []
        _vehicle_id_counter = 0
    
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
                        num_events = random.randint(1, 10)
                        # Use same vehicle IDs if same-vehicle mode, otherwise generate new ones
                        if use_same_vehicle and _reusable_vehicle_ids:
                            # Generate events with new vehicle ID every 5 events
                            events = []
                            vehicle_id = None
                            for j in range(num_events):
                                # New vehicle ID every 5 events
                                if j % 5 == 0:
                                    vehicle_id = _reusable_vehicle_ids[_vehicle_id_counter % len(_reusable_vehicle_ids)]
                                    _vehicle_id_counter += 1
                                # Add small time offset (seconds) to each event in the same file
                                event_timestamp = timestamp + timedelta(seconds=j * random.randint(1, 10))
                                events.append(generate_event(vehicle_id, event_timestamp))
                        else:
                            events = [generate_event(generate_vehicle_id(), timestamp) for _ in range(num_events)]
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
        
        # Generate regular files for target date
        for i in range(args.num_files):
            # Vary timestamps throughout the target date (spread over the day)
            # In same-vehicle mode, spread events across the day to test "last_event_type"
            if use_same_vehicle:
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
                if use_same_vehicle and _reusable_vehicle_ids:
                    # Generate events with new vehicle ID every 5 events
                    # This ensures variety while still reusing vehicle IDs across files
                    events = []
                    unique_vehicles_in_file = set()
                    vehicle_id = None
                    for j in range(num_events):
                        # New vehicle ID every 5 events (or at start of file)
                        if j % 5 == 0:
                            vehicle_id = _reusable_vehicle_ids[_vehicle_id_counter % len(_reusable_vehicle_ids)]
                            _vehicle_id_counter += 1
                            unique_vehicles_in_file.add(vehicle_id)
                        # Add small time offset (seconds) to each event in the same file
                        event_timestamp = timestamp + timedelta(seconds=j * random.randint(1, 10))
                        events.append(generate_event(vehicle_id, event_timestamp))
                    vehicles_str = f"{len(unique_vehicles_in_file)} unique vehicle(s)" if len(unique_vehicles_in_file) > 1 else "1 vehicle"
                    print(f"✓ Generated: {filename} ({len(events)} events, {vehicles_str} at {timestamp.strftime('%H:%M')})")
                else:
                    # For non-same-vehicle mode, use different vehicle IDs
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
        
        # Generate late-arriving data if requested
        num_late_files = 0
        if args.include_late_arrival:
            yesterday = base_timestamp - timedelta(days=1)
            today = datetime.now()
            yesterday_date = yesterday.date()
            today_date = today.date()
            print(f"\n📦 Generating late-arriving data:")
            print(f"   Today's date: {today_date.strftime('%Y-%m-%d')}")
            print(f"   Yesterday's date: {yesterday_date.strftime('%Y-%m-%d')}")
            print(f"   Files will have today's timestamp ({today.strftime('%Y-%m-%d %H:%M')})")
            print(f"   But event_time will be from yesterday ({yesterday_date.strftime('%Y-%m-%d')})")
            print(f"   This simulates late-arriving data for testing 2-day lookback\n")
            
            # Generate 2-3 late-arriving files
            num_late_files = random.randint(2, 3)
            for i in range(num_late_files):
                # File timestamp is today (when file arrives)
                file_timestamp = today.replace(
                    hour=random.randint(0, today.hour if today.hour > 0 else 23),
                    minute=random.randint(0, 59),
                    second=0,
                    microsecond=0
                )
                
                # Event times are from yesterday (late-arriving data)
                if should_generate_events:
                    filename = generate_filename("vehicles_events", file_timestamp)
                    file_path = incoming_folder / filename
                    num_events = random.randint(3, 8)
                    
                    events = []
                    if use_same_vehicle and _reusable_vehicle_ids:
                        # Reuse vehicle IDs from the pool
                        vehicle_id = _reusable_vehicle_ids[_vehicle_id_counter % len(_reusable_vehicle_ids)]
                        _vehicle_id_counter += 1
                    else:
                        vehicle_id = generate_vehicle_id()
                    
                    # Generate events with event_time from yesterday
                    for j in range(num_events):
                        # Event time is from yesterday, spread throughout the day
                        hours_offset = random.randint(0, 23)
                        minutes_offset = random.randint(0, 59)
                        event_time = yesterday.replace(hour=hours_offset, minute=minutes_offset, second=random.randint(0, 59))
                        events.append(generate_event(vehicle_id, event_time))
                    
                    s3_driver.write_file(str(file_path), json.dumps({"vehicles_events": events}, indent=2).encode())
                    print(f"✓ Generated late-arrival: {filename} ({len(events)} events from yesterday, file timestamp: {file_timestamp.strftime('%Y-%m-%d %H:%M')})")
                
                if should_generate_status:
                    filename = generate_filename("vehicles_status", file_timestamp)
                    file_path = incoming_folder / filename
                    num_statuses = random.randint(1, 3)
                    
                    statuses = []
                    for j in range(num_statuses):
                        # Report time is from yesterday
                        hours_offset = random.randint(0, 23)
                        minutes_offset = random.randint(0, 59)
                        report_time = yesterday.replace(hour=hours_offset, minute=minutes_offset, second=random.randint(0, 59))
                        statuses.append(generate_status(generate_vehicle_id(), report_time))
                    
                    s3_driver.write_file(str(file_path), json.dumps({"vehicle_status": statuses}, indent=2).encode())
                    print(f"✓ Generated late-arrival: {filename} ({len(statuses)} statuses from yesterday, file timestamp: {file_timestamp.strftime('%Y-%m-%d %H:%M')})")
        
        total_files = args.num_files * (should_generate_events + should_generate_status)
        if args.include_late_arrival:
            total_files += num_late_files * (should_generate_events + should_generate_status)
        print(f"\n✓ Happy flow setup complete! Generated {total_files} file(s) in {incoming_folder}")


if __name__ == "__main__":
    main()

