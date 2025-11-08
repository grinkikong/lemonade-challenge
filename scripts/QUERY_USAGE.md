# Interactive SQL Query CLI Usage

## Quick Start

```bash
poetry run python scripts/query_interactive.py
```

## How to Use

1. **Start the CLI:**
   ```bash
   poetry run python scripts/query_interactive.py
   ```

2. **Type SQL queries:**
   ```sql
   SQL> SELECT * FROM vehicle_events LIMIT 10;
   SQL> SELECT COUNT(*) FROM vehicle_events;
   SQL> SELECT event_type, COUNT(*) FROM vehicle_events GROUP BY event_type;
   ```

3. **Exit:**
   ```sql
   SQL> exit
   ```
   Or press `Ctrl+C`

## Available Tables

- `vehicle_events` - Vehicle event data
- `vehicle_status` - Vehicle status data

## Example Queries

```sql
-- Get all events
SELECT * FROM vehicle_events LIMIT 10;

-- Count total events
SELECT COUNT(*) FROM vehicle_events;

-- Group by event type
SELECT event_type, COUNT(*) as count 
FROM vehicle_events 
GROUP BY event_type 
ORDER BY count DESC;

-- Filter by date
SELECT * FROM vehicle_events 
WHERE date = '2025-11-07';

-- Join tables (if needed)
SELECT e.vehicle_id, e.event_type, s.status
FROM vehicle_events e
JOIN vehicle_status s ON e.vehicle_id = s.vehicle_id
LIMIT 10;
```

## Commands

- `help` - Show help
- `exit` or `quit` or `q` - Exit the CLI
- `Ctrl+C` - Exit the CLI

