from typing import List

import pandas as pd
from data_utils.logger import init_logger

from events_metadata import EventsMetadataTable
from drivers import postgres_driver

logger = init_logger('events_metadata_updater.crud')


def get_events_metadata_df() -> pd.DataFrame:
    logger.info('getting events_metadata postgres table rows')
    events = postgres_driver.session.query(
        EventsMetadataTable.event_name, EventsMetadataTable.entity, EventsMetadataTable.source_name
    ).all()
    events_as_dicts = [e._asdict() for e in events]
    return pd.DataFrame(events_as_dicts)


def insert_backend_events(topic_names: List) -> None:
    logger.info(f'inserting {len(topic_names)} backend events')
    for topic in topic_names:
        if topic is not None:
            event_name = topic.split('.')[1].replace('-', '_').lower()
            source_type = 'kafka_data' if topic.startswith('data.') else 'kafka_rnd'
            payload = {
                "event_name": event_name,
                "entity": "backend",
                "is_active": False,
                "source_type": source_type,
                "source_name": topic,
                "target_topic": "data.eventflow.raw-events",
                "is_mixpanel": False,
            }
            row = EventsMetadataTable(**payload)
            postgres_driver.session.add(row)
            postgres_driver.session.commit()
            logger.info(f'inserted event: {topic}')


def insert_client_events(events: List) -> None:
    logger.info(f'inserting {len(events)} client events')
    for event in events:
        payload = {
            "event_name": event,
            "entity": "client",
            "is_active": True,
            "source_type": "kafka_rnd",
            "source_name": 'collector-service.client-events',
            "target_topic": "data.eventflow.raw-events",
            "is_mixpanel": False,
        }
        row = EventsMetadataTable(**payload)
        postgres_driver.session.add(row)
        postgres_driver.session.commit()
        logger.info(f'inserted event: {event}')
