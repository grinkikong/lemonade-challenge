import os

ENV = os.environ['ENV']

# EVENTS
EVENT_ENTITY = 'backend'
SCHEMA_VERSION = 'v4'
EVENT_TOPIC_MAPPING = {
    'browser_session_created.avsc': 'session.browser-session-created',
    'cloud_session_created.avsc': 'cloud-sessions.cloud-session-created',
    'cloud_session_sent_to_client.avsc': 'cloud-session.cloud-session-sent-to-client',
    'ludeo_play_started.avsc': 'gameplays.ludeo-play-started',
    'ludeo_play_ended.avsc': 'ludeo-player.ludeo-play-ended',
    'ludeo_play_aborted.avsc': 'ludeo-player.ludeo-play-aborted',
    'tab_session_created.avsc': 'session.tab-session-created',
    'user_session_created.avsc': 'session.user-session-created',
}

# SNOWFLAKE PARAMETERS
SNOWFLAKE_DATABASE = 'DATALAKE_STAGING' if ENV in ['staging', 'local'] else 'DATALAKE'
SNOWFLAKE_SCHEMA = 'SPARK_EMR'
SNOWFLAKE_TABLE = 'RAW_EVENTS'

# GITHUB SCHEMA REPO
GITHUB_EVENTS_SCHEMA_REPO = 'events-schemas'
GITHUB_SCHEMAS_LOCAL_PATH = 'schemas'
GITHUB_SCHEMAS_BRANCH = 'main'

# SLACK
SLACK_CHANNEL = '#monitoring-data-staging' if ENV in ['staging', 'local'] else '#events-validation'

# KAFKA
KAFKA_BROKER = os.environ['KAFKA_BROKER']

DEFAULT_HOURS_BACK = 1
