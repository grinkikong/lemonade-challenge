from enum import Enum
import os

from health_checker.tasks.recommendations_api_health import health_check_recommendations_apis
from health_checker.tasks.datalake_freshness import health_check_datalake_freshness
from health_checker.tasks.snowpipe_freshness import health_check_snowpipe_freshness

ENV = os.environ['ENV']
SLACK_CHANNEL = '#monitoring-data-infra' if ENV == 'production' else '#monitoring-data-staging'

TASKS_METADATA = {
    # 'recommendations_api': {
    #     'health_check_func': health_check_recommendations_apis,
    #     'alert_title': 'Recommendations API failure',
    #     'still_alert_title': 'Recommendations API still fails',
    #     'success_title': 'Recommendations API back to normal'
    # },
    'datalake_freshness': {
        'health_check_func': health_check_datalake_freshness,
        'alert_title': 'datalake table is not fresh',
        'still_alert_title': 'datalake table is still not fresh',
        'success_title': 'datalake table freshness back to normal'
    },
    'snowpipe_freshness': {
        'health_check_func': health_check_snowpipe_freshness,
        'alert_title': 'Snowpipe auto refresh is not active',
        'still_alert_title': 'Snowpipe auto refresh is still not active',
        'success_title': 'Snowpipe auto refresh back to normal'
    },
}

SLACK_MSG_FOOTER = 'health checker'
TASK_AIRFLOW_VAR_TEMPLATE = 'health_check::{task_id}::last_state'
TIME_BETWEEN_ALERT_MESSAGES_MINUTES = 60


class States(Enum):
    SUCCESS = 'success'
    ALERT = 'alert'
