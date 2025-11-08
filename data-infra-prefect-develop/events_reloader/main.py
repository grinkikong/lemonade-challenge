import os
from typing import List, Dict, Tuple
from prefect import flow, task, get_run_logger
from data_utils.snowflake_utils import SnowflakeDriver
from data_utils.slack_utils import SlackDriver
from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from events_reloader.config import TARGET_DB, SLACK_CHANNEL, EVENTS_HANDLER_API_URL, ENV, NUM_THREADPOOL_EXECUTORS, SEND_SLACK_THRESHOLD
from events_reloader.config import MAX_RETRIES, BACKOFF_FACTOR, STATUS_FORCELIST
from events_reloader.slack_messages import create_events_reloader_message
from events_reloader.events_api_sender import EventAPISender

# Load credentials
secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)
snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')
sf_api_user = snowflake_secrets['username']
sf_api_password = snowflake_secrets['password']

# Initialize drivers
sf_driver = SnowflakeDriver(
    username=sf_api_user,
    password=sf_api_password,
    database=TARGET_DB
)
slack_secrets = secrets_manager_driver.get_secret('slack/ludata_bot')
slack_driver = SlackDriver(slack_secrets['token'])

@task
def fetch_missing_events(snowflake_driver: SnowflakeDriver, hours_back: int, date_from: str = None, date_to: str = None) -> List[Dict]:
    logger = get_run_logger()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        if date_from and date_to:
            logger.info(f"Fetching missing events from {date_from} to {date_to}")
            path = os.path.join(current_dir, 'queries', 'missing_events_by_dates.sql')
            with open(path, 'r') as f:
                query = f.read()
            formatted_query = query.format(
                target_db=TARGET_DB,
                date_from=date_from,
                date_to=date_to
            )
        else:
            logger.info(f"Fetching missing events with {hours_back} hours lookback")
            path = os.path.join(current_dir, 'queries', 'missing_events.sql')
            with open(path, 'r') as f:
                query = f.read()
            formatted_query = query.format(
                target_db=TARGET_DB,
                hours_back=hours_back
            )

        logger.info(f"formatted_query:\n {formatted_query}")
        df = snowflake_driver.get_query_results_as_df(formatted_query)
        if df is None or df.empty:
            logger.info("No results returned from Snowflake query.")
            return []
        events = df.to_dict('records')
        logger.info(f"Found {len(events)} missing events")
        return events

    except Exception as e:
        logger.error(f"Error fetching missing events: {e}")
        return []

@task
def send_events_to_api(events: List[Dict]) -> Tuple[int, List[Dict]]:
    logger = get_run_logger()
    api_sender = EventAPISender(
        api_url=EVENTS_HANDLER_API_URL,
        max_retries=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=STATUS_FORCELIST,
        num_threads=NUM_THREADPOOL_EXECUTORS,
        logger=logger
    )
    success_count, failed_events = api_sender.send_batch_events(events)
    return success_count, failed_events

@task
def notify_slack(success_count: int, total_count: int):
    msg_payload = create_events_reloader_message(success_count, total_count)
    slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg_payload)

@flow()
def events_reloader_flow(hours_back: int, date_from=None, date_to=None):
    logger = get_run_logger()
    logger.info(f"Running Events reloader flow")
    logger.info(f"Using hours_back: {hours_back}, date_from: {date_from}, date_to: {date_to}")
    events = fetch_missing_events(sf_driver, hours_back, date_from, date_to)
    if events:
        success_count, failed_events = send_events_to_api(events)
        total_count = len(events)
        if total_count >= SEND_SLACK_THRESHOLD:
            notify_slack(success_count, total_count)
        else:
            logger.info(f"Processed {total_count} events, below alert threshold ({SEND_SLACK_THRESHOLD}), not sending Slack notification.")
    else:
        logger.info("No missing events found")

if __name__ == "__main__":
    events_reloader_flow(hours_back=1)
