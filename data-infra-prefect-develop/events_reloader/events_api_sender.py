import json
from typing import List, Dict, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import decimal

class EventAPISender:
    def __init__(self, api_url: str, max_retries: int, backoff_factor: float, status_forcelist: List[int], num_threads: int, logger):
        self.api_url = api_url
        self.num_threads = num_threads
        self.logger = logger
        self.session = requests.Session()
        retries = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _safe_json_field(self, event: Dict, key: str) -> dict:
        value = event.get(key)
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        try:
            return json.loads(value)
        except Exception:
            return {}

    def to_int(self, val):
        if isinstance(val, decimal.Decimal):
            return int(val)
        return val

    def transform_event(self, event: Dict) -> Dict:
        try:
            timestamp = self.to_int(event['TIMESTAMP'])
            kafka_timestamp_source = self.to_int(event.get('KAFKA_TIMESTAMP_SOURCE', timestamp))

            # Get context and payload
            context = self._safe_json_field(event, 'CONTEXT')
            payload = self._safe_json_field(event, 'PAYLOAD')
            original_payload = self._safe_json_field(event, 'ORIGINAL_PAYLOAD')

            transformed = {
                'eventId': event['EVENTID'],
                'eventName': event['EVENTNAME'],
                'timestamp': timestamp,
                'context': context,
                'payload': original_payload,
                'kafka_timestamp_source': kafka_timestamp_source
            }

            # Extract session IDs from context and add to root level
            if payload.get('browserSessionId'):
                transformed['browserSessionId'] = payload['browserSessionId']
            if payload.get('userSessionId'):
                transformed['userSessionId'] = payload['userSessionId']
            if payload.get('tabSessionId'):
                transformed['tabSessionId'] = payload['tabSessionId']
            if payload.get('playSessionId'):
                transformed['playSessionId'] = payload['playSessionId']

            self.logger.info(f"Transformed event: {json.dumps(transformed, indent=2)}")
            return transformed
        except KeyError as e:
            self.logger.error(f"Missing required field in event: {str(e)}")
            raise

    def send_single_event(self, event: Dict) -> Tuple[str, bool, str]:
        try:
            transformed_event = self.transform_event(event)
            self.logger.info(f"Sending event to API: {self.api_url}/eventflow/handleEvent")
            response = self.session.post(f"{self.api_url}/eventflow/handleEvent", json=transformed_event)
            self.logger.info(f"API Response status: {response.status_code}")
            self.logger.info(f"API Response content: {response.text}")
            if response.status_code == 200:
                return event['EVENTID'], True, ""
            else:
                return event['EVENTID'], False, f"API returned status code {response.status_code}"
        except Exception as e:
            self.logger.error(f"Error sending event {event['EVENTID']}: {str(e)}")
            return event.get('EVENTID', 'unknown'), False, str(e)

    def send_batch_events(self, events: List[Dict]) -> Tuple[int, List[Dict]]:
        failed_events = []
        count, report_interval = 0, 1000
        success_count = 0

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_event = {executor.submit(self.send_single_event, event): event for event in events}
            for future in as_completed(future_to_event):
                event_id, ok, reason = future.result()
                if ok:
                    success_count += 1
                else:
                    failed_events.append({'event_id': event_id, 'reason': reason})

                count += 1
                if count % report_interval == 0:
                    self.logger.info(f'sent {count} events to API so far')

        if failed_events:
            self.logger.error(f'Failed to send {len(failed_events)} events:')
            for failed in failed_events:
                self.logger.error(f"Event {failed['event_id']} failed: {failed['reason']}")
        return success_count, failed_events
