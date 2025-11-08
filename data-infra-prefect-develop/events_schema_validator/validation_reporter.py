import logging
from typing import Dict, List
import pandas as pd

from config import (
    SLACK_CHANNEL, ENV
)

logger = logging.getLogger('events-schema-validator.validation_reporter')


class ValidationResultsReporter:
    def __init__(self, slack_driver):
        self.slack_driver = slack_driver

    def handle_validation_results(self, validation_results_df: pd.DataFrame, summary_stats: Dict, target_date: str, event_schema_name: str):
        logger.info(f"Processing validation results for {target_date} - {event_schema_name}")
        logger.info(f"Summary: {summary_stats['valid_events']} valid, {summary_stats['invalid_events']} invalid out of {summary_stats['total_events']} total events")

        if summary_stats['invalid_events'] > 0:
            self._handle_invalid_events(validation_results_df, summary_stats, target_date, event_schema_name)
        else:
            self._handle_all_valid_events(target_date, event_schema_name)

    def handle_consolidated_validation_results(self, all_results: List[Dict], target_date: str):
        """Handle consolidated validation results for all event schemas"""
        logger.info(f"Processing consolidated validation results for {target_date}")
        
        # Separate successful and failed event schemas
        successful_schemas = []
        failed_schemas = []
        total_events = 0
        total_valid = 0
        total_invalid = 0
        
        for result in all_results:
            event_schema_name = result['event_schema_name']
            total_events += result['total_events']
            total_valid += result['valid_events']
            total_invalid += result['invalid_events']
            
            # Only process schemas that have events or actual validation failures
            if result['status'] == 'completed_successfully' and result['invalid_events'] == 0 and result['total_events'] > 0:
                # True success: events found and all valid
                successful_schemas.append({
                    'schema_name': event_schema_name,
                    'total_events': result['total_events'],
                    'valid_events': result['valid_events']
                })
            elif result['total_events'] > 0 and (result['invalid_events'] > 0 or result['status'] != 'completed_successfully'):
                # Failure: actual validation errors or processing failures (only for schemas with events)
                failed_schemas.append({
                    'schema_name': event_schema_name,
                    'total_events': result['total_events'],
                    'valid_events': result['valid_events'],
                    'invalid_events': result['invalid_events'],
                    'validation_errors': result['validation_errors'],
                    'detailed_errors': result.get('detailed_errors', []),
                    'reason': 'validation_failures' if result['invalid_events'] > 0 else 'processing_error'
                })
            # Ignore schemas with 0 events - no processing, no storage, no alerts
        
        # Send success message if there are successful schemas
        # DISABLED: Uncomment line below to enable success notifications
        # if successful_schemas:
        #     self._send_success_slack_report(target_date, successful_schemas)
        
        # Send failure message if there are actual validation failures
        if failed_schemas:
            self._send_failure_slack_report(target_date, failed_schemas, total_events, total_valid, total_invalid)

    def _handle_invalid_events(self, validation_results_df: pd.DataFrame, summary_stats: Dict, target_date: str, event_schema_name: str):
        invalid_events = validation_results_df[~validation_results_df['is_valid']]
        
        error_groups = {}
        for _, event in invalid_events.iterrows():
            for error in event['errors']:
                error_type = error.split(':')[0] if ':' in error else 'unknown'
                if error_type not in error_groups:
                    error_groups[error_type] = []
                error_groups[error_type].append({
                    'event_id': event['event_id'],
                    'event_name': event['event_name'],
                    'error': error
                })

        self._send_slack_validation_report(target_date, summary_stats, error_groups, event_schema_name)

    def _handle_all_valid_events(self, target_date: str, event_schema_name: str):
        logger.info(f"All events for {target_date} - {event_schema_name} passed validation successfully")
        self._send_slack_success_notification(target_date, event_schema_name)

    def _send_slack_validation_report(self, target_date: str, summary_stats: Dict, error_groups: Dict, event_schema_name: str):
        try:
            error_summary = []
            for error_type, events in error_groups.items():
                error_summary.append(f"• *{error_type}*: {len(events)} events")
                for event in events[:3]:
                    error_summary.append(f"  - {event['event_id']}: {event['error']}")
                if len(events) > 3:
                    error_summary.append(f"  ... and {len(events) - 3} more")

            message_content = f"*Schema:* {event_schema_name}\n"
            message_content += f"*Summary:*\n"
            message_content += f"• Total Events: {summary_stats['total_events']}\n"
            message_content += f"• Valid Events: {summary_stats['valid_events']}\n"
            message_content += f"• Invalid Events: {summary_stats['invalid_events']}\n\n"

            if error_groups:
                message_content += f"*Validation Errors:*\n"
                message_content += "\n".join(error_summary)

            msg_payload = {
                "title": f"Schema Validation Report for {target_date} - {event_schema_name}",
                "text": message_content,
                "color": "#FF9933" if summary_stats['invalid_events'] > 0 else "#36C5F0",
                "footer": "data.validation",
            }

            self.slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg_payload)
            logger.info(f"Sent validation report to Slack for {target_date} - {event_schema_name}")
        except Exception as e:
            logger.error(f"Error sending validation report to Slack: {e}")

    def _send_slack_success_notification(self, target_date: str, event_schema_name: str):
        try:
            msg_payload = {
                "title": f"✅ Schema Validation Success for {target_date} - {event_schema_name[:-5]}",
                "text": "All events passed validation successfully!",
                "color": "#36a64f",
                "footer": "data.validation",
            }

            self.slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg_payload)
            logger.info(f"Sent success notification to Slack for {target_date} - {event_schema_name}")

        except Exception as e:
            logger.error(f"Failed to send Slack success notification: {e}")

    def _send_success_slack_report(self, target_date: str, successful_schemas: List[Dict]):
        """Send a success Slack report for validated event schemas"""
        try:
            content = "Successfully validated event types:\n"
            
            total_success_events = 0
            for schema in successful_schemas:
                schema_display_name = schema['schema_name'].replace('.avsc', '')
                event_count = schema['total_events']
                total_success_events += event_count
                content += f"• `{schema_display_name}`: ({event_count} events)\n"
            
            content += f"\nTotal: {len(successful_schemas)} event types passed validation ({total_success_events})"
            
            msg_payload = {
                "title": f"✅ Schema Validation Success for {target_date}",
                "text": content,
                "color": "#36a64f",  # Green for success
                "footer": "data.validation",
            }
            
            self.slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg_payload)
            logger.info(f"Sent success validation report to Slack for {target_date}")
            
        except Exception as e:
            logger.error(f"Error sending success validation report to Slack: {e}")

    def _send_warning_slack_report(self, target_date: str, warning_schemas: List[Dict]):
        """Send a warning Slack report for schemas with no events found"""
        try:
            content = ""
            for schema in warning_schemas:
                schema_display_name = schema['schema_name'].replace('.avsc', '')
                content += f"• `{schema_display_name}`: 0 events\n"
            
            msg_payload = {
                "title": f"⚠️ Schema Validation Warning for {target_date}",
                "text": content,
                "color": "#FFCC00",  # Yellow/amber for warnings
                "footer": "data.validation",
            }
            
            self.slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg_payload)
            logger.info(f"Sent warning validation report to Slack for {target_date}")
            
        except Exception as e:
            logger.error(f"Error sending warning validation report to Slack: {e}")

    def _send_failure_slack_report(self, target_date: str, failed_schemas: List[Dict], 
                                 total_events: int, total_valid: int, total_invalid: int):
        """Send a failure Slack report for failed event schemas with detailed errors"""
        try:
            content = f"*Overall Summary:*\n"
            content += f"• Total Events: {total_events}\n"
            content += f"• Valid Events: {total_valid}\n"
            content += f"• Invalid Events: {total_invalid}\n"
            
            if total_events > 0:
                failure_percentage = (total_invalid / total_events) * 100
                content += f"• Failure Rate: {failure_percentage:.1f}%\n"
            
            content += "\n*Validation Errors by Event Type:*\n"
            for failed_schema in failed_schemas:
                schema_display_name = failed_schema['schema_name'].replace('.avsc', '')
                total_events_schema = failed_schema['total_events']
                invalid_events_schema = failed_schema['invalid_events']
                
                if total_events_schema > 0:
                    schema_failure_rate = (invalid_events_schema / total_events_schema) * 100
                    content += f"• `{schema_display_name}`: {total_events_schema} events, {invalid_events_schema} failed ({schema_failure_rate:.1f}%) - "
                else:
                    content += f"• `{schema_display_name}`: 0 events - "
                
                if failed_schema['validation_errors']:
                    error_summary = []
                    for error_type, count in failed_schema['validation_errors'].items():
                        error_summary.append(f"{error_type}")
                    content += ", ".join(error_summary)
                else:
                    content += "No specific validation errors"
                content += "\n"
            
            # Show error details with smart truncation
            content += "\n*Error Details:*\n"
            for failed_schema in failed_schemas:
                schema_display_name = failed_schema['schema_name'].replace('.avsc', '')
                detailed_errors = failed_schema.get('detailed_errors', [])
                
                if detailed_errors:
                    content += f"\n*{schema_display_name}:*\n"
                    
                    # Sort errors by frequency (most common first)
                    error_frequency = {}
                    for error_detail in detailed_errors:
                        error_key = ', '.join(error_detail['errors'])
                        error_frequency[error_key] = error_frequency.get(error_key, 0) + 1
                    
                    # Sort by frequency (descending) then by event_id for consistency
                    sorted_errors = sorted(detailed_errors, 
                                         key=lambda x: (-error_frequency[', '.join(x['errors'])], x['event_id']))
                    
                    # Show up to 15 most important errors
                    max_errors_to_show = 15
                    errors_to_show = sorted_errors[:max_errors_to_show]
                    
                    for error_detail in errors_to_show:
                        event_id = error_detail['event_id']
                        errors = error_detail['errors']
                        
                        # Clean up and format error messages
                        cleaned_errors = []
                        for error in errors:
                            # Remove event_validator_service namespace and clean up formatting
                            cleaned_error = error.replace('event_validator_service.', '')
                            # Remove array brackets and quotes for better readability
                            cleaned_error = cleaned_error.replace('["', '').replace('"]', '').replace('"', '')
                            # Clean up common Avro validation patterns
                            cleaned_error = cleaned_error.replace('Field(', '').replace(') is None expected', ' is None, expected')
                            cleaned_error = cleaned_error.replace('Specific schema validation failed: ', '')
                            cleaned_errors.append(cleaned_error)
                        
                        content += f"  Event ID: `{event_id}` - {', '.join(cleaned_errors)}\n"
                    
                    # Add truncation message if there are more errors
                    if len(detailed_errors) > max_errors_to_show:
                        remaining_count = len(detailed_errors) - max_errors_to_show
                        content += f"  ... and {remaining_count} more errors (check logs for full details)\n"
                else:
                    content += f"\n*{schema_display_name}:* No detailed error information available\n"
            
            # Final safety check for message length (Slack limit is ~40,000 characters)
            max_content_length = 35000  # Leave some buffer
            if len(content) > max_content_length:
                logger.warning(f"Message content too long ({len(content)} chars), truncating...")
                content = content[:max_content_length] + "\n\n... (message truncated due to length - check logs for full details)"
            
            # Smart color coding based on failure severity
            failure_rate = (total_invalid / total_events) * 100 if total_events > 0 else 0
            
            if failure_rate >= 15:
                color = "#FF0000"  # Red for critical failure rate (>=15%)
                severity_emoji = "🚨"
                severity_text = "CRITICAL"
            elif failure_rate >= 5:
                color = "#FF4500"  # Orange-red for high failure rate (5-15%)
                severity_emoji = "⚠️"
                severity_text = "HIGH"
            elif failure_rate >= 1:
                color = "#FFA500"  # Orange for medium failure rate (1-5%)
                severity_emoji = "⚠️"
                severity_text = "MEDIUM"
            else:
                color = "#FFD700"  # Gold for low failure rate (<1%)
                severity_emoji = "⚠️"
                severity_text = "LOW"
            
            msg_payload = {
                "title": f"{severity_emoji} Schema Validation Failures ({severity_text}) for {target_date}",
                "text": content,
                "color": color,
                "footer": "data.validation",
            }
            
            self.slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg_payload)
            logger.info(f"Sent {severity_text} failure validation report to Slack for {target_date} (content length: {len(content)} chars)")
            
        except Exception as e:
            logger.error(f"Error sending failure validation report to Slack: {e}")
