from sqlalchemy import Column, Integer, String, TIMESTAMP, Boolean, Enum
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

ENUM_ENTITY = Enum("backend", "sdk", "client", name="entity_enum")
ENUM_SOURCE_TYPE = Enum("kafka_rnd", "kafka_data", "third_party", "data_api", name="source_type_enum")
ENUM_TARGET_TOPIC = Enum("data.eventflow.raw-events", "data.eventflow.system-events", name="target_topic_enum")

Base = declarative_base()


class EventsMetadataTable(Base):
    __tablename__ = 'events_metadata'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_name = Column(String)
    entity = Column(ENUM_ENTITY, comment='what is the entity that the event is related to')
    is_active = Column(Boolean, default=False)

    # map columns
    event_group = Column(String, nullable=True)
    source_type = Column(ENUM_SOURCE_TYPE, comment='where did the event come from')
    source_name = Column(String, comment='name of the source. for example the name of the kafka topic or third party service')
    target_topic = Column(ENUM_TARGET_TOPIC)
    enrichment_config = Column(String, nullable=True)

    # description columns
    owner = Column(String, comment='the team responsible for creating the event', nullable=True)
    description = Column(String, nullable=True)
    is_business_event = Column(Boolean, default=False)
    is_mixpanel = Column(Boolean, default=False)

    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())

    def _asdict(self):
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}


SPECIAL_TOPIC_NAMES_MAPPING = {
    'users.created': 'user_created',
    'users.complete-registration': 'user_complete_registration',
    'users.deleted': 'user_deleted',
    'users.updated': 'user_updated',

    'games.created': 'game_created',
    'games.deleted': 'game_deleted',
    'games.updated': 'game_updated'
}
