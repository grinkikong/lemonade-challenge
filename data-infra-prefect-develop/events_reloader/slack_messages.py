from datetime import datetime
import json
from typing import Dict

def create_events_reloader_message(success_count: int, total_count: int) -> Dict:

    return {
        "title": "Missing Events Reloader Completed",
        "text": (
            f"• Total events processed: {total_count}\n"
            f"• Successfully sent: {success_count}\n"
            f"• Failed: {total_count - success_count}"
        ),
        "footer": "prefect_flow.events.reloader",
        "ts": json.dumps(datetime.now(), default=str),
    }
