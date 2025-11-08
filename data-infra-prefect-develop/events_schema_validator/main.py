from datetime import datetime, timedelta
from typing import Optional, List, Dict
import pandas as pd

from prefect import flow, task, get_run_logger

from drivers import init_drivers
from config import (
    ENV, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE,
    DEFAULT_HOURS_BACK, EVENT_TOPIC_MAPPING
)
from schema_manager import SchemaManager
from schema_validator import BatchEventValidator
from validation_reporter import ValidationResultsReporter


ENV = 'staging' if ENV in ['staging', 'local'] else ENV


@task
def fetch_events_from_snowflake(snowflake_driver, hour: str, topic: str) -> pd.DataFrame:
    logger = get_run_logger()
    
    query = f"""
    SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} 
    WHERE timestamp >= '{hour}'::timestamp
    AND timestamp < '{hour}'::timestamp + INTERVAL '1 HOUR'
    AND topic = '{ENV}.{topic}'
    """
    
    logger.info(f"Executing query for hour {hour}, topic {topic}")
    logger.info(f"Query: {query}")
    
    try:
        events_df = snowflake_driver.get_query_results_as_df(query)
        logger.info(f"Successfully fetched {len(events_df)} events from Snowflake")
        return events_df
    except Exception as e:
        logger.error(f"Failed to fetch events from Snowflake: {e}")
        raise


@task
def validate_events_batch(events_df: pd.DataFrame, schemas_local_path: str, event_schema_name: str, dlq) -> tuple:
    logger = get_run_logger()
    
    if events_df.empty:
        logger.warning("No events to validate")
        return pd.DataFrame(), {
            'total_events': 0,
            'valid_events': 0,
            'invalid_events': 0,
            'validation_errors': {},
            'schema_name': event_schema_name
        }
    
    logger.info(f"Starting validation of {len(events_df)} events using schema: {event_schema_name}")

    try:
        events_validator = BatchEventValidator(schemas_local_path, event_schema_name, dlq)
        validation_results_df, summary_stats = events_validator.validate_batch(events_df)
        
        logger.info(f"Validation completed: {summary_stats['valid_events']} valid, {summary_stats['invalid_events']} invalid")
        
        return validation_results_df, summary_stats
        
    except Exception as e:
        logger.error(f"Failed to validate events batch: {e}")
        raise


@task
def get_schemas(github_driver, schemas_local_path: str) -> str:
    logger = get_run_logger()
    
    try:
        schema_manager = SchemaManager(github_driver, schemas_local_path)
        success = schema_manager.download_schemas()
        if success:
            schema_info = schema_manager.get_schema_info()
            logger.info(f"Schemas available: {schema_info}")
            return schemas_local_path
        else:
            raise Exception("Failed to ensure schemas are available")
            
    except Exception as e:
        logger.error(f"Failed to ensure schemas are available: {e}")
        raise


@task
def get_available_event_schemas(schema_manager: SchemaManager) -> List[str]:
    """Get all available event schemas from the backend v4 directory"""
    return schema_manager.get_available_event_schemas()


@task
def log_validation_summary(summary_stats: dict, target_date: str, event_schema_name: str) -> None:
    logger = get_run_logger()
    logger.info(f"VALIDATION SUMMARY FOR {target_date} - {event_schema_name}\n")
    logger.info(f"Total Events Processed: {summary_stats['total_events']}")
    logger.info(f"Valid Events: {summary_stats['valid_events']}")
    logger.info(f"Invalid Events: {summary_stats['invalid_events']}")
    
    if summary_stats['validation_errors']:
        logger.info("Validation Errors Breakdown:")
        for error_type, count in summary_stats['validation_errors'].items():
            logger.info(f"  {error_type}: {count}")
    else:
        logger.info("No validation errors found")



def _get_target_hour(hours_back: Optional[int] = None) -> str:
    """Get target hour for validation (looks back N hours)"""
    if hours_back is None:
        hours_back = DEFAULT_HOURS_BACK
    
    # Get the previous complete hour
    # If it's 16:17, we want to validate 15:00-16:00
    now = datetime.utcnow()
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    target_hour = current_hour - timedelta(hours=hours_back)
    return target_hour.strftime('%Y-%m-%d %H:%M:%S')

@task
def process_single_event(
    snowflake_driver,
    schemas_local_path: str,
    hour: str,
    event_schema_name: str,
    dlq
) -> Dict:
    logger = get_run_logger()
    
    topic = EVENT_TOPIC_MAPPING[event_schema_name]
    logger.info(f"Processing event schema: {event_schema_name} with topic: {topic}")
    
    try:
        events_df = fetch_events_from_snowflake(snowflake_driver, hour, topic)
        
        if events_df.empty:
            logger.warning(f"No events found for hour {hour}, topic {topic}, schema {event_schema_name}")
            return {
                'event_schema_name': event_schema_name,
                'topic': topic,
                'target_date': hour,
                'total_events': 0,
                'valid_events': 0,
                'invalid_events': 0,
                'validation_errors': {},
                'status': 'no_events_found'
            }
        
        validation_results_df, summary_stats = validate_events_batch(events_df, schemas_local_path, event_schema_name, dlq)
        log_validation_summary(summary_stats, hour, event_schema_name)

        # Extract detailed error information for reporting
        detailed_errors = []
        if not validation_results_df.empty:
            invalid_events = validation_results_df[~validation_results_df['is_valid']]
            for _, event in invalid_events.iterrows():
                detailed_errors.append({
                    'event_id': event['event_id'],
                    'event_name': event['event_name'],
                    'errors': event['errors']
                })

        return {
            'event_schema_name': event_schema_name,
            'topic': topic,
            'target_date': hour,
            'total_events': summary_stats['total_events'],
            'valid_events': summary_stats['valid_events'],
            'invalid_events': summary_stats['invalid_events'],
            'validation_errors': summary_stats['validation_errors'],
            'detailed_errors': detailed_errors,
            'status': 'completed_successfully'
        }
        
    except Exception as e:
        logger.error(f"Failed to process event {event_schema_name}: {e}")
        return {
            'event_schema_name': event_schema_name,
            'topic': topic,
            'target_date': hour,
            'total_events': 0,
            'valid_events': 0,
            'invalid_events': 0,
            'validation_errors': {'processing_error': str(e)},
            'detailed_errors': [],
            'status': 'failed'
        }


@flow()
def events_schema_validator_flow(
    hours_back: int = 1,
    hour=None
) -> dict:
    logger = get_run_logger()
    
    if hour is None:
        hour = _get_target_hour(hours_back)
    
    logger.info(f"Starting events schema validation flow for hour: {hour}")

    try:
        snowflake_driver, slack_driver, github_driver, dlq = init_drivers()
        schemas_local_path = get_schemas(github_driver, schemas_local_path="schemas")
        schema_manager = SchemaManager(github_driver, schemas_local_path)
        event_schemas = get_available_event_schemas(schema_manager)

        # Process each event schema
        results = []
        for event_schema_name in event_schemas:
            result = process_single_event(
                snowflake_driver,
                schemas_local_path,
                hour,
                event_schema_name,
                dlq
            )
            results.append(result)
        
        # Send consolidated validation report
        validation_reporter = ValidationResultsReporter(slack_driver)
        validation_reporter.handle_consolidated_validation_results(results, hour)
        
        # Aggregate results
        total_events = sum(r['total_events'] for r in results)
        total_valid = sum(r['valid_events'] for r in results)
        total_invalid = sum(r['invalid_events'] for r in results)
        
        # Combine validation errors
        combined_errors = {}
        for result in results:
            for error_type, count in result['validation_errors'].items():
                combined_errors[error_type] = combined_errors.get(error_type, 0) + count
        
        overall_status = 'completed_successfully' if all(r['status'] != 'failed' for r in results) else 'partial_failure'
        
        final_result = {
            'target_date': hour,
            'total_events': total_events,
            'total_valid_events': total_valid,
            'total_invalid_events': total_invalid,
            'combined_validation_errors': combined_errors,
            'status': overall_status,
            'event_results': results
        }
        
        logger.info(f"Flow completed. Processed {len(event_schemas)} event schemas.")
        logger.info(f"Overall: {total_valid} valid, {total_invalid} invalid out of {total_events} total events sampled")
        
        # Log event counts per schema for visibility
        logger.info("Event counts by schema:")
        for result in results:
            schema_name = result['event_schema_name'].replace('.avsc', '')
            event_count = result['total_events']
            logger.info(f"  {schema_name}: {event_count} events sampled")
        return final_result
        
    except Exception as e:
        logger.error(f"Flow failed: {e}")
        raise


if __name__ == "__main__":
    events_schema_validator_flow(hours_back=1)
