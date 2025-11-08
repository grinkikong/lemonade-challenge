import logging
import os
from datetime import datetime
from typing import List
from kafka import KafkaConsumer
import pandas as pd
from prefect import flow, task, get_run_logger

from data_utils.datadog_utils import init_datadog, log_timing_and_increment
from data_utils.pythonic_utils import read_file_content
from drivers import ENV, KAFKA_BROKER, snowflake_driver, s3_driver, slack_driver
from crud import get_events_metadata_df, insert_backend_events, insert_client_events

S3_BACKUP_BUCKET = f'{"prod" if ENV == "production" else "stg"}-use1-data-backups'
SF_DATABASE = 'DATALAKE' if ENV == 'production' else 'DATALAKE_STAGING'
SLACK_CHANNEL = '#data-notifications' if ENV == 'production' else '#monitoring-data-staging'

TOPICS_TO_EXCLUDE = [
    'eventflow.raw-events', 'eventflow.enriched-events', 'collector-service.client-events', 'ludeo-player.ludeo-Lose', 'eventflow.system-events'
]
TOPICS_TO_INCLUDE = [
    'data.dead-letter-queue'
]

init_datadog(os.environ['DD_AGENT_HOST'])
DATADOG_METRIC_NAME = 'eventflow.update-events-metadata.run'


# Helper functions (not tasks)
def _remove_env_prefix(topic_name: str) -> str:
    topic_parts = topic_name.split('.')
    if len(topic_parts) == 3:
        return f'{topic_parts[1]}.{topic_parts[2]}'
    else:
        if topic_name in TOPICS_TO_INCLUDE:
            return topic_name


def _beautify_event_name(event_name):
    return event_name.replace('-', '_')


def _generate_events_list(events: List[str]) -> str:
    final_msg = ''
    for event in events:
        final_msg += f'`{event}`\n'
    return final_msg


def _log_missing_events(entity: str, missing_events: set, logger):
    logger.info(f'found missing {entity} events ({len(missing_events)}): {missing_events}')


# Prefect Tasks
@task()
def backup_existing_table_to_s3(existing_events_df: pd.DataFrame):
    logger = get_run_logger()
    logger.info('backing up events_metadata table to s3')
    today = datetime.utcnow().strftime('%Y-%m-%d')
    now = datetime.utcnow().strftime('%H:%M:%S')
    folder_prefix = 'postgres/events_metadata'
    # save current
    s3_driver.save_df_as_csv(
        existing_events_df, S3_BACKUP_BUCKET, folder=f'{folder_prefix}/{today}', file=f'{now}.csv'
    )
    # delete old
    logger.info('deleting old backups from s3')
    seven_days_ago = (datetime.utcnow() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
    s3_driver.delete_directory(S3_BACKUP_BUCKET, f'{folder_prefix}/{seven_days_ago}')


def get_all_topics() -> List[str]:
    logger = get_run_logger()
    logger.info('getting list of all kafka topics (=backend events)')
    output = []
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER)
    all_topics = consumer.topics()
    for topic in all_topics:
        topic_name = _remove_env_prefix(topic)
        if topic_name is not None and topic_name not in TOPICS_TO_EXCLUDE:
            if 'eventflow' in topic_name:
                print(topic_name)
            output.append(topic_name)
    consumer.close()
    return output


@task()
def get_missing_backend_events(existing_events_df: pd.DataFrame) -> List:
    logger = get_run_logger()
    existing_backend_events = existing_events_df[existing_events_df['entity'] == 'backend']
    topics = get_all_topics()
    missing_events = set(topics) - set(existing_backend_events['source_name'])
    if missing_events:
        _log_missing_events('backend', missing_events, logger)
    else:
        logger.info('no new backend events found')
    return list(missing_events)


@task()
def get_missing_client_events(existing_events_df: pd.DataFrame) -> List:
    logger = get_run_logger()
    existing_client_events = existing_events_df[existing_events_df['entity'] == 'client']

    recent_events_query = read_file_content('update_postgres_events_metadata/queries/get_recent_client_events_from_snowflake.sql').format(
        database=SF_DATABASE)
    recent_client_events = snowflake_driver.get_query_results_as_df(recent_events_query)['EVENT_NAME']

    recent_client_events_set = {_beautify_event_name(event) for event in recent_client_events}
    missing_events = recent_client_events_set - set(existing_client_events['event_name'])
    if missing_events:
        _log_missing_events('client', missing_events, logger)
    else:
        logger.info('no new client events found')
    return list(missing_events)


@task()
def insert_missing_events(missing_backend_events: List, missing_client_events: List):
    if len(missing_backend_events) > 0:
        insert_backend_events(missing_backend_events)
    if len(missing_client_events) > 0:
        insert_client_events(missing_client_events)


@task()
def send_slack_notification(missing_backend_events: List, missing_client_events: List):
    logger = get_run_logger()
    logger.info('sending slack notification')

    # backend
    if missing_backend_events:
        msg = slack_driver.generate_slack_message_payload(
            title="Datalake - New Backend Events",
            text=f"{len(missing_backend_events)} events were inserted to events_metadata table with IS_ACTIVE=FALSE\n"
                 f"{_generate_events_list(missing_backend_events)}",
            footer='eventflow.update-events-metadata job'
        )
        slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg)

    # client
    if missing_client_events:
        msg = slack_driver.generate_slack_message_payload(
            title="Datalake - New Client Events",
            text=f"{len(missing_client_events)} events were inserted to events_metadata table with IS_ACTIVE=TRUE\n"
                 f"{_generate_events_list(missing_client_events)}",
            footer='eventflow.update-events-metadata job'
        )
        slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg)


@flow()
def update_postgres_events_metadata_flow() -> None:
    start_timestamp = datetime.utcnow()
    logger = get_run_logger()
    logger.info('running update events_metadata table job')

    s3transfer_logger = logging.getLogger('s3transfer')
    s3transfer_logger.setLevel(logging.WARNING)

    existing_events_df = get_events_metadata_df()
    backup_existing_table_to_s3(existing_events_df)
    missing_backend_events = get_missing_backend_events(existing_events_df)
    missing_client_events = get_missing_client_events(existing_events_df)

    if missing_backend_events or missing_client_events:
        insert_missing_events(missing_backend_events, missing_client_events)
        send_slack_notification(missing_backend_events, missing_client_events)

    log_timing_and_increment(
        start_timestamp,
        DATADOG_METRIC_NAME,
        tags=[f'new_be_events:{len(missing_backend_events)},new_client_events:{len(missing_client_events)}']
    )


if __name__ == '__main__':
    update_postgres_events_metadata_flow()
