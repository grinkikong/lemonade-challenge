import json
import os
import traceback
from typing import List, Dict, Optional, Tuple
import fastavro
import pandas as pd

from config import EVENT_ENTITY, GITHUB_SCHEMAS_LOCAL_PATH, SCHEMA_VERSION


class BatchEventValidator:
    def __init__(self, schemas_local_path: str = GITHUB_SCHEMAS_LOCAL_PATH, event_schema_name: str = None, dlq=None):
        self.schemas_local_path = schemas_local_path
        self.event_schema_name = event_schema_name
        self.dlq = dlq
        self.event_base_schema = self._load_event_base_schema()
        self.event_specific_schema = self._load_event_specific_schema() if event_schema_name else None
        self.required_context_fields = self._get_required_context_fields()

    def _handle_processing_error(self, err_msg, event_payload):
        print(err_msg)
        self.dlq.produce(
            error_message=err_msg,
            traceback=traceback.format_exc(),
            original_payload=event_payload,
        )

    def _load_event_base_schema(self) -> Dict:
        base_schema_path = os.path.join(self.schemas_local_path, "event_base_v2.avsc")
        print(f"Loading base schema from: {base_schema_path}")
        
        with open(base_schema_path, "r") as f:
            event_base_schema = json.load(f)
        
        print(f"Base schema fields: {[f['name'] for f in event_base_schema['fields']]}")
        
        payload_schema = self._load_payload_schema()
        
        for field in event_base_schema['fields']:
            if field['name'] == 'payload':
                field['type'] = payload_schema
                print(f"Replaced payload field type with: {payload_schema}")
        
        parsed_schema = fastavro.parse_schema(event_base_schema)
        print("Base schema loaded and parsed successfully")
        return parsed_schema

    def _load_payload_schema(self) -> Dict:
        path = os.path.join(self.schemas_local_path, f"{EVENT_ENTITY}/v3/base_payload.avsc")
        with open(path, "r") as f:
            payload_schema = json.load(f)
        return payload_schema

    def _load_event_specific_schema(self) -> Optional[Dict]:
        if not self.event_schema_name:
            return None
            
        path = os.path.join(self.schemas_local_path, f"{EVENT_ENTITY}/{SCHEMA_VERSION}/{self.event_schema_name}")
        try:
            with open(path, "r") as f:
                specific_schema = json.load(f)
            print(f"Loaded specific schema: {self.event_schema_name}")
            return specific_schema
        except FileNotFoundError:
            print(f"Specific schema file not found: {path}")
            return None

    def _get_required_context_fields(self) -> List[str]:
        if not self.event_specific_schema:
            return []
        
        for field in self.event_specific_schema['fields']:
            if field['name'] == 'context':
                return field.get('default', [])
        return []

    def validate_event(self, event: Dict) -> Dict:
        parsed_event = event.copy()
        if isinstance(event.get('context'), str):
            try:
                parsed_event['context'] = json.loads(event['context'])
            except json.JSONDecodeError as e:
                print(f"Failed to parse context JSON: {e}")
                parsed_event['context'] = {}
        
        if isinstance(event.get('payload'), str):
            try:
                parsed_event['payload'] = json.loads(event['payload'])
            except json.JSONDecodeError as e:
                print(f"Failed to parse payload JSON: {e}")
                parsed_event['payload'] = {}

        validation_results = {
            'is_valid': True,
            'errors': [],
            'event_id': parsed_event.get('eventId', 'unknown'),
            'event_name': parsed_event.get('eventName', 'unknown'),
            'schema_name': self.event_schema_name or 'base_only'
        }

        base_validation = self._validate_base_schema(parsed_event)
        if not base_validation['is_valid']:
            validation_results['is_valid'] = False
            validation_results['errors'].extend(base_validation['errors'])

        if self.event_specific_schema:
            specific_validation = self._validate_specific_schema(parsed_event)
            if not specific_validation['is_valid']:
                validation_results['is_valid'] = False
                validation_results['errors'].extend(specific_validation['errors'])

        return validation_results

    def _validate_base_schema(self, event: Dict) -> Dict:
        try:
            fastavro.validation.validate(event, self.event_base_schema)
            return {'is_valid': True, 'errors': []}
        except fastavro.validation.ValidationError as e:
            print(f"Base schema validation failed: {str(e)}")
            return {
                'is_valid': False, 
                'errors': [f'Base schema validation failed: {str(e)}']
            }

    def _validate_specific_schema(self, event: Dict) -> Dict:
        try:
            fastavro.validation.validate(event, self.event_specific_schema)
            return {'is_valid': True, 'errors': []}
        except fastavro.validation.ValidationError as e:
            print(f"Specific schema validation failed: {str(e)}")
            return {
                'is_valid': False, 
                'errors': [f'Specific schema validation failed: {str(e)}']
            }

    def validate_batch(self, events_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        validation_results = []
        summary_stats = {
            'total_events': len(events_df),
            'valid_events': 0,
            'invalid_events': 0,
            'validation_errors': {},
            'schema_name': self.event_schema_name or 'base_only'
        }

        for idx, row in events_df.iterrows():
            try:
                event = json.loads(row['EVENT_PAYLOAD'])
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse JSON for row {idx}: {e}"
                print(error_msg)
                
                # Send to dead letter queue
                self._handle_processing_error(error_msg, row['EVENT_PAYLOAD'])
                
                validation_result = {
                    'is_valid': False,
                    'errors': [f'JSON parsing failed: {str(e)}'],
                    'event_id': f'row_{idx}',
                    'event_name': 'unknown',
                    'schema_name': self.event_schema_name or 'base_only'
                }
                validation_results.append(validation_result)
                summary_stats['invalid_events'] += 1
                continue

            validation_result = self.validate_event(event)
            validation_results.append(validation_result)
            
            if validation_result['is_valid']:
                summary_stats['valid_events'] += 1
            else:
                summary_stats['invalid_events'] += 1
                
                # Send failed validation event to dead letter queue
                error_msg = f"Schema validation failed for event {validation_result['event_id']} ({validation_result['event_name']}): {', '.join(validation_result['errors'])}"
                self._handle_processing_error(error_msg, row['EVENT_PAYLOAD'])
                
                for error in validation_result['errors']:
                    error_type = error.split(':')[0] if ':' in error else 'unknown'
                    summary_stats['validation_errors'][error_type] = summary_stats['validation_errors'].get(error_type, 0) + 1

        results_df = pd.DataFrame(validation_results)
        
        return results_df, summary_stats
