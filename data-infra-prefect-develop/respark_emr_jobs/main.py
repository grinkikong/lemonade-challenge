from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
import json
from datetime import datetime
from data_utils.slack_utils import SlackDriver
from data_utils.s3_utils import S3Driver
from data_utils.emr_utils import EMRContainersDriver, EMRJobState
from project_utils import secrets_manager_driver
from config import (
    ICEBERG_TABLES_JOBS,
    SLACK_CHANNEL,
    EMR_SPARK_S3_BUCKET,
    CLUSTER_NAME,
)

slack_secrets = secrets_manager_driver.get_secret('slack/ludata_bot')
EMR_VIRTUAL_CLUSTER_ID = JSON.load("respark-emr-jobs").value["EMR_VIRTUAL_CLUSTER_ID"]
slack_driver = SlackDriver(slack_secrets['token'])
emr_client = EMRContainersDriver(EMR_VIRTUAL_CLUSTER_ID)
s3_client = S3Driver()


def _send_slack_message(job: str, error: bool = False):
    image_tag = job + "-latest"
    datadog_url = (
        f"https://app.datadoghq.eu/dashboard/u33-ysf-5xf?"
        f"fromUser=false&refresh_mode=sliding"
        f"&tpl_var_cluster_name={CLUSTER_NAME}"
        f"&tpl_var_image_tag={image_tag}"
        f"&from_ts=1739015160601&to_ts=1739101560601&live=true"
    )
    emr_cluster_url = f"https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/eks/clusters/{EMR_VIRTUAL_CLUSTER_ID}"
    msg_payload = {
        "title": f"Respark EMR Job {'Triggered' if not error else 'Failed'}",
        "text": f"`{job}`"
                f"\n{'Check Monitoring on Datadog: <' + datadog_url + '|Datadog Dashboard>'}" 
                f"\n{'Check EMR Cluster: <' + emr_cluster_url + '|EMR Cluster>'}",
        "footer": 'emr.spark',
        "ts": json.dumps(datetime.now(), default=str),
    }

    slack_driver.send_message(channel=SLACK_CHANNEL, msg_payload=msg_payload)


def _get_spark_s3_config(job_name: str):
    json_file_name = f"spark-job-config-{job_name}.json"
    s3_file_path = f"emr-on-eks/job_config_jsons/{json_file_name}"
    s3_object = s3_client.get_object(bucket=EMR_SPARK_S3_BUCKET, file_path=s3_file_path)
    return json.loads(s3_object['Body'].read().decode('utf-8'))


@task
def start_emr_job_and_send_slack_msg(job: str):
    job_config = _get_spark_s3_config(job)
    emr_client.trigger_spark_emr_job(job_config)
    _send_slack_message(job)


def _get_active_emr_jobs():
    active_jobs = emr_client.get_spark_emr_jobs(
        [
            EMRJobState.RUNNING.value,
            EMRJobState.PENDING.value,
            EMRJobState.SUBMITTED.value,
        ]
    )

    return active_jobs

@task
def get_not_active_emr_jobs():
    output = []
    active_jobs = _get_active_emr_jobs()
    jobs_names = [job['name'] for job in active_jobs]
    for job in ICEBERG_TABLES_JOBS:
        if job in jobs_names:
            pass
        else:
            output.append(job)
    return output

@flow
def respark_emr_jobs_flow():
    logger = get_run_logger()
    logger.info('Running Respark EMR jobs flow')
    try:
        not_active_jobs = get_not_active_emr_jobs()
    except Exception as err:
        logger.error(f'Failed to get not active emr jobs due to {str(err)}')
        raise

    if not_active_jobs:
        for job in not_active_jobs:
            try:
                start_emr_job_and_send_slack_msg(job)
                logger.info(f'Respark Triggered EMR job {job}')
            except Exception as err:
                logger.error(f'Failed to run spark emr job for {job} due to {str(err)}')
                _send_slack_message(job, error=True)
    else:
        logger.info('All Spark EMR Jobs are Active')


if __name__ == "__main__":
    respark_emr_jobs_flow()
