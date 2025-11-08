"""
See tasks configuration in config.py
"""

from datetime import datetime, timedelta
from typing import List, Dict
import json
from prefect import get_run_logger
from prefect.blocks.system import JSON

from data_utils.slack_utils import SlackDriver
from data_utils.redis_utils import RedisDriver

from project_config import PREFECT_REDIS_DB

from health_checker.config import States, SLACK_MSG_FOOTER, \
    TASK_AIRFLOW_VAR_TEMPLATE, SLACK_CHANNEL, TIME_BETWEEN_ALERT_MESSAGES_MINUTES
from project_utils import secrets_manager_driver

REDIS_HOST = JSON.load("databases-hosts").value['redis_host']

slack_secrets = secrets_manager_driver.get_secret('slack/ludata_bot')
redis_secrets = secrets_manager_driver.get_secret('redis/data')

slack_driver = SlackDriver(slack_secrets['token'])
redis_driver = RedisDriver(host=REDIS_HOST, password=redis_secrets['password'], db=PREFECT_REDIS_DB)


class HealthChecker:
    def __init__(self):
        pass

    def handle_failures(self, task_id: str, task_metadata: Dict, failures_messages: List):
        """
        we know that cur_state = alert
        if last_state == alert -> check if updated_at > 1 hour -> send "still" alert message
        if last_state == success -> send first alert message
        """
        logger = get_run_logger()
        logger.info(f'handling {len(failures_messages)} failures')
        messages_to_send = []
        last_state = self._get_last_state(task_id)
        if not last_state:  # if key not exists in redis - create it
            self._set_state(task_id, States.SUCCESS.value)
            last_state = self._get_last_state(task_id)

        if last_state['state'] == States.SUCCESS.value:
            logger.info('first alert identified. sending slack message')
            messages_to_send = self._generate_first_failure_messages(task_metadata, failures_messages)

        else:
            # send "still alert" message
            time_elapsed_from_last_msg_sent = datetime.utcnow() - datetime.fromisoformat(last_state['updated_at'])
            if time_elapsed_from_last_msg_sent > timedelta(minutes=TIME_BETWEEN_ALERT_MESSAGES_MINUTES):
                messages_to_send = self._generate_still_failure_message(task_metadata, failures_messages)
                logger.info('"still alert" identified. sending slack message')
            else:
                logger.info(f'time elapsed from last sent ({time_elapsed_from_last_msg_sent.total_seconds() / 60})\n'
                            f'which is smaller from the minutes threshold ({TIME_BETWEEN_ALERT_MESSAGES_MINUTES})\n'
                            f'not sending "still alert" message')
        if messages_to_send:
            for msg in messages_to_send:
                logger.info(f'sending slack message {msg.get("title")}')
                slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg)
                self._set_state(task_id, States.ALERT.value)

    def handle_success(self, task_id, task_metadata):
        """
        we know that cur_state = success
        if last_state == alert -> send back to normal message
        if last_state == success -> do nothing
        """
        logger = get_run_logger()
        last_state = self._get_last_state(task_id)

        if not last_state:  # if key not exists in redis - create it
            self._set_state(task_id, States.SUCCESS.value)
            last_state = self._get_last_state(task_id)

        if last_state['state'] == States.SUCCESS.value:
            logger.info(f'no need to send success message for {task_id}')
        else:
            back_to_normal_msg = self._generate_back_to_normal_message(task_metadata)
            logger.info('sending back to normal message')
            slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=back_to_normal_msg)
            self._set_state(task_id, States.SUCCESS.value)

    @staticmethod
    def _generate_first_failure_messages(task_metadata: Dict, failures_messages: List) -> List:
        output = []
        for msg in failures_messages:
            output.append(
                slack_driver.generate_slack_message_payload(
                    title=task_metadata['alert_title'],
                    text=msg,
                    color='error',
                    footer=SLACK_MSG_FOOTER
                )
            )
        return output

    @staticmethod
    def _generate_still_failure_message(task_metadata: Dict, failures_messages: List) -> List:
        output = []
        for msg in failures_messages:
            output.append(
                slack_driver.generate_slack_message_payload(
                    title=task_metadata['still_alert_title'],
                    text=msg,
                    color='error',
                    footer=SLACK_MSG_FOOTER
                )
            )
        return output

    @staticmethod
    def _generate_back_to_normal_message(task_metadata: Dict) -> Dict:
        return slack_driver.generate_slack_message_payload(
            title=task_metadata['success_title'],
            text='Back to normal',
            color='success',
            footer=SLACK_MSG_FOOTER
        )

    @staticmethod
    def _get_last_state(task_id: str) -> [Dict, None]:
        try:
            redis_key = TASK_AIRFLOW_VAR_TEMPLATE.format(task_id=task_id)
            return json.loads(redis_driver.get(redis_key))
        except KeyError:
            return {'state': States.ALERT.value, 'updated_at': datetime.utcnow().isoformat()}
        except Exception as e:
            return None

    @staticmethod
    def _set_state(task_id: str, state: str):
        logger = get_run_logger()
        redis_key = TASK_AIRFLOW_VAR_TEMPLATE.format(task_id=task_id)
        data = {"state": state, "updated_at": datetime.utcnow().isoformat()}
        try:
            redis_driver.set(redis_key, json.dumps(data))
        except Exception as e:
            logger.info(f'failed to set state for {task_id} due to {e}')
