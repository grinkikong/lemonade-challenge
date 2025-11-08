from typing import List
import requests
import json
from prefect import get_run_logger

from health_checker.tasks.tasks_config import API_URLS


def call_api_post(**kwargs):
    headers = {'accept': 'application/json'}
    return requests.post(kwargs['url'], headers=headers, data=json.dumps(kwargs['payload']))


def _generate_failure_messages(failures: list) -> list:
    failures_to_send = []
    for f in failures:
        msg = f'API request to `{f["url"]}` failed\n' \
              f'payload: {f["payload"]}'
        failures_to_send.append(msg)
    return failures_to_send


def health_check_recommendations_apis() -> List:
    logger = get_run_logger()
    logger.info('-- checking API responses')
    failures = []
    for api in API_URLS:
        payload = api['payload']
        count = payload['args'].get('count')
        results = call_api_post(url=api['url'], payload=payload)

        if results.status_code == 200 and len(json.loads(results.content)) == count:
            logger.info(f'API {api["name"]} is healthy')
        else:
            logger.info(f'API {api["name"]} failed. response: {json.loads(results.content)}')
            failures.append(api)

    if len(failures) > 0:
        return _generate_failure_messages(failures)
    else:
        logger.info(f'{"*" * 4}  All APIs are healthy  {"*" * 4}')
        return []
