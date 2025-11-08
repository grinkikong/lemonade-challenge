from datetime import datetime
import os
from typing import List, Dict
from croniter import croniter
from prefect import flow, task, get_run_logger


from sql_reports.config import (
    KAFKA_PRODUCE_TOPIC, KAFKA_PRODUCE_EVENT_NAME, LOCAL_CSV_DIR_PATH
)
from sql_reports.models.models import SqlReportsMng
from drivers import (
    postgres_driver, snowflake_driver, slack_driver, kafka_producer
)
from sql_reports.utils import create_tmp_dir, delete_tmp_dir, generate_csv_file


@task
def get_sql_reports_to_run() -> List[SqlReportsMng]:
    logger = get_run_logger()
    logger.info('fetching active SQL reports that need to run')

    reports_to_run = []
    current_time = datetime.utcnow()

    active_reports = _get_active_reports()
    for report in active_reports:
        if report.last_successful_run:
            cron = croniter(report.cron, report.last_successful_run)
            next_run = cron.get_next(datetime)
            if next_run <= current_time:
                reports_to_run.append(report)
        else:
            # if never ran before, run it now
            reports_to_run.append(report)

    logger.info(f'found {len(reports_to_run)} reports to run')
    return reports_to_run


def _get_active_reports():
    return postgres_driver.session.query(SqlReportsMng).filter(
        SqlReportsMng.is_active == True
    ).all()


@task
def get_query(report: SqlReportsMng) -> str:
    # TODO move queries to github instead of in DB
    logger = get_run_logger()
    logger.info(f'fetching SQL query for report: {report.report_name}')
    return report.query


@task
def run_snowflake_query(query: str) -> List[Dict]:
    logger = get_run_logger()
    logger.info(f'executing query in Snowflake:\n{"#"*10}\n{query}\n{"#"*10}')
    results = snowflake_driver.get_query_results(query)
    logger.info(f'query returned {len(results)} rows')
    return results


@task
def generate_output(report: SqlReportsMng, results: List[Dict]) -> Dict:
    logger = get_run_logger()
    logger.info('generating output')
    title = 'SQL Reports'
    text = (
        f'{len(results)} results returned for report: `{report.report_display_name}`\n'
        f'find attached CSV below and the query in comment'
            )
    msg = slack_driver.generate_slack_message_payload(title=title, text=text)
    output_csv_file_path = generate_csv_file(LOCAL_CSV_DIR_PATH, results, report.report_name)
    return {'output_msg': msg, 'output_csv_file_path': output_csv_file_path}


@task
def send_output_to_recipients(report: SqlReportsMng, output: Dict):
    logger = get_run_logger()

    if report.slack_recipients:
        logger.info(f'sending to slack recipients: {report.slack_recipients}')
        output_msg = output['output_msg']
        output_csv_file_path = output['output_csv_file_path']
        output_query = f'Query:\n```{report.query}```'

        for recipient in report.slack_recipients:
            send_message_results = slack_driver.send_message(channel=recipient, msg_payload=output_msg)
            slack_message_thread_ts = send_message_results.data['ts']
            slack_driver.send_thread_reply(channel=recipient, thread_ts=slack_message_thread_ts, text=output_query)

            with open(output_csv_file_path, 'r') as file:
                file_content = file.read()
                slack_driver.upload_file(
                    file_content=file_content,
                    file_name=os.path.basename(output_csv_file_path),
                    channels=recipient,
                    title=f"{report.report_display_name} - Full Results.csv"
                )

    if report.email_recipients:
        logger.info(f'sending emails not implemented yet')


@task
def update_last_successful_run(report: SqlReportsMng):
    report.last_successful_run = datetime.utcnow()
    postgres_driver.session.commit()


@task
def log_report_run_to_kafka(report: SqlReportsMng, num_results: int, query_time_taken: float, is_notified: bool):
    """used for analytics"""
    logger = get_run_logger()
    payload = {
        'report_id': report.id,
        'report_name': report.report_name,
        'number_of_rows': num_results,
        'is_notified': is_notified,
        'query_time_taken': query_time_taken
    }
    logger.info(f'logging report execution')
    kafka_producer.produce_ludeo_event(KAFKA_PRODUCE_TOPIC, KAFKA_PRODUCE_EVENT_NAME, payload=payload)


def _run_single_report(report: SqlReportsMng):
    logger = get_run_logger()
    query = get_query(report)
    start_time = datetime.utcnow()
    results = run_snowflake_query(query)
    query_time_taken = (datetime.utcnow() - start_time).total_seconds()

    if results:
        output = generate_output(report, results)
        send_output_to_recipients(report, output)
        is_notified = True
    else:
        logger.info(f'no results returned. skipping notification.')
        results = []
        is_notified = False

    update_last_successful_run(report)
    log_report_run_to_kafka(report, len(results), query_time_taken, is_notified)


@flow
def sql_reports_flow():
    logger = get_run_logger()
    logger.info('starting SQL Reports flow')

    create_tmp_dir(LOCAL_CSV_DIR_PATH)

    try:
        reports_to_run = get_sql_reports_to_run()
        if len(reports_to_run) == 0:
            logger.info('no reports to run. exiting')
            return

        logger.info(f'running on {len(reports_to_run)} reports')
        for report in reports_to_run:
            try:
                logger.info(f'-- processing report: {report.report_name}')
                _run_single_report(report)
                logger.info(f'-- finished processing report: {report.report_name}')

            except Exception as err:
                logger.error(f'error processing report {report.report_name}: {err}')
                continue

    except Exception as err:
        logger.error(f'error in SQL Reports flow: {err}')

    finally:
        delete_tmp_dir(LOCAL_CSV_DIR_PATH)
        pass

    logger.info('SQL Reports flow completed')


if __name__ == '__main__':
    sql_reports_flow()
