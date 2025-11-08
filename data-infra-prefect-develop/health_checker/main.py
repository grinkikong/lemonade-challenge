from prefect import flow, task, get_run_logger

from health_checker.config import TASKS_METADATA
from health_checker.utils import HealthChecker

health_checker = HealthChecker()


@task
def success(task_id, task_metadata):
    logger = get_run_logger()
    logger.info('handling success check')
    health_checker.handle_success(task_id, task_metadata)


@task
def fail(task_id, task_metadata, failures_messages):
    logger = get_run_logger()
    logger.info('handling failed check')
    health_checker.handle_failures(task_id, task_metadata, failures_messages)


@task
def run_check(task_id, task_metadata):
    logger = get_run_logger()
    health_check_func = task_metadata['health_check_func']
    logger.info(f'@@@@   running check: {task_id}  @@@@')
    failures_messages = health_check_func()

    if len(failures_messages) == 0:
        success(task_id, task_metadata)
    else:
        fail(task_id, task_metadata, failures_messages)


@flow
def health_checker_flow():
    logger = get_run_logger()

    for task_id, task_metadata in TASKS_METADATA.items():
        try:
            run_check(task_id, task_metadata)
        except Exception as err:
            logger.error(f'failed to run health check for {task_id} due to {err}')


if __name__ == "__main__":
    health_checker_flow()
