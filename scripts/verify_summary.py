#!/usr/bin/env python3
"""Verify daily summary table structure matches requirements."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow.parquet as pq

# Read the summary table
summary_file = Path("data/datalake/dwh/daily_summary/date=2025-11-07/daily_summary_2025-11-07.parquet")

if not summary_file.exists():
    print(f"❌ Summary file not found: {summary_file}")
    sys.exit(1)

table = pq.read_table(str(summary_file))

# Required columns from requirements.pdf
required_columns = ["vehicle_id", "day", "last_event_time", "last_event_type"]

print("=" * 60)
print("Daily Summary Table Verification")
print("=" * 60)
print(f"\n✓ File: {summary_file}")
print(f"✓ Total rows: {len(table)}")
print(f"\n✓ Columns found: {table.column_names}")

# Check required columns
print("\n✓ Required columns check:")
all_present = True
for col in required_columns:
    present = col in table.column_names
    status = "✓" if present else "❌"
    print(f"  {status} {col}: {present}")
    if not present:
        all_present = False

if all_present:
    print("\n✅ All required columns are present!")
else:
    print("\n❌ Missing required columns!")
    sys.exit(1)

# Show sample data
print("\n✓ Sample records (first 3):")
for i in range(min(3, len(table))):
    print(f"\n  Row {i+1}:")
    for col in table.column_names:
        val = table[col][i].as_py()
        print(f"    {col}: {val}")

print("\n" + "=" * 60)
print("✅ Daily summary table structure matches requirements!")
print("=" * 60)

