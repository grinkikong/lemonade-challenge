from datetime import datetime
from prefect import flow, task, get_run_logger
from prefect.futures import PrefectFuture
from prefect_utils import trigger_and_wait_prefect_job
from data_utils.pythonic_utils import read_file_content

from recommendations.jobs.snowflake_to_dynamo.config import MODELS_TO_DYNAMO, MODELS_TO_KAFKA, KafkaRecommendationMessage, KafkaAllGamesMessage, KAFKA_TOPIC
from recommendations.jobs.snowflake_to_dynamo.drivers import (
    snowflake_driver, dynamodb_driver, kafka_producer,
    ENV, SNOWFLAKE_DATABASE, KAFKA_SNOWFLAKE_DATABASE, PREFECT_DBT_JOB_NAME, PREFECT_DBT_FLOW_NAME
)


@task
def trigger_and_wait_dbt_recommendations_job():
    logger = get_run_logger()
    logger.info('triggering dbt recommendations job')
    trigger_and_wait_prefect_job(prefect_flow_name=PREFECT_DBT_FLOW_NAME, prefect_job_name=PREFECT_DBT_JOB_NAME)


@task
def get_results_from_snowflake(model, model_params):
    logger = get_run_logger()
    logger.info(f'getting results from snowflake for model: {model}')
    query = read_file_content('recommendations/jobs/snowflake_to_dynamo/queries/get_model_results_template.sql')
    query = query.format(
            snowflake_list_column=model_params['snowflake_list_column'],
            database=SNOWFLAKE_DATABASE,
            table=model_params['sf_table_name']
        )
    return snowflake_driver.get_query_results(query)


def upsert_to_dynamo(model_params, results):
    logger = get_run_logger()
    table_name = model_params['dynamo_table_name']
    logger.info(f'dynamo table name: {table_name}')
    dynamo_table = dynamodb_driver.get_table_object(table_name)
    now_timestamp = datetime.utcnow()
    for row in results:
        obj = _prepare_dynamo_object(row, now_timestamp, table_name)
        logger.info(f'inserting to dynamo table: {dynamo_table} : {obj}')
        dynamodb_driver.insert_item(dynamo_table, obj)


@task
def upsert_to_dynamo_task():
    logger = get_run_logger()
    logger.info('starting DynamoDB upsert task')
    
    # Original DynamoDB flow logic
    trigger_and_wait_dbt_recommendations_job()
    futures = []
    for model, model_params in MODELS_TO_DYNAMO.items():
        results_future: PrefectFuture = get_results_from_snowflake.submit(model, model_params)
        future = results_future.result()
        upsert_to_dynamo(model_params, future)
    
    logger.info('completed DynamoDB upsert task')


def insert_to_kafka(results, model_kind='most_popular'):
    logger = get_run_logger()
    logger.info(f'inserting {len(results)} messages to kafka topic: {KAFKA_TOPIC} with type: {model_kind}')
    for row in results:
        sorted_ludeos_key = 'sorted_ludeo_ids' if 'sorted_ludeo_ids' in row else list(MODELS_TO_DYNAMO.values())[0]['snowflake_list_column']
        
        if model_kind == 'most_popular_all_games':
            message = KafkaAllGamesMessage(
                model_kind=model_kind,
                sorted_ludeos=row[sorted_ludeos_key].split(',')
            )
        else:
            message = KafkaRecommendationMessage(
                model_kind=model_kind,
                game_id=row['game_id'],
                game_name=row['game_name'],
                sorted_ludeos=row[sorted_ludeos_key].split(',')
            )
        
        kafka_producer.send(KAFKA_TOPIC, value=message.model_dump())
        logger.info(f'sent message to kafka: {message.model_dump()}')
    kafka_producer.flush()


@task
def get_results_from_kafka_query(query_name, query_params):
    logger = get_run_logger()
    logger.info(f'executing Kafka query for: {query_name}')
    
    try:
        base_query = read_file_content('recommendations/jobs/snowflake_to_dynamo/queries/base_ludeo_data.sql')
        
        games_filter = 'gr.is_relevant = TRUE' if ENV == 'production' else 'gr.is_staging = TRUE'
        
        base_query = base_query.format(
            database=KAFKA_SNOWFLAKE_DATABASE,
            game_for_reports_filter=games_filter
        )
        
        query = read_file_content(f'recommendations/jobs/snowflake_to_dynamo/queries/{query_params["query_file"]}')
        final_query = query.format(base_query=base_query)
        
        logger.info(f'executing query for {query_name}')
        results = snowflake_driver.get_query_results(final_query)
        logger.info(f'query returned {len(results)} results for {query_name}')
        
        return results

    except Exception as e:
        logger.error(f'error in Kafka query for {query_name}: {str(e)}')
        raise


@task
def insert_to_kafka_task():
    logger = get_run_logger()
    logger.info('starting Kafka insert task')
    
    # Use MODELS_TO_KAFKA for the new Kafka flow
    for query_name, query_params in MODELS_TO_KAFKA.items():
        results_future: PrefectFuture = get_results_from_kafka_query.submit(query_name, query_params)
        results = results_future.result()
        insert_to_kafka(results, query_params['model_kind'])
    
    logger.info('completed Kafka insert task')


def _prepare_dynamo_object(row, timestamp, table_name):
    return {
        'game_id': row['game_id'],
        'game_name': row['game_name'],
        'model_run_id': row['model_run_id'],
        MODELS_TO_DYNAMO[table_name]['entity']:
            row[MODELS_TO_DYNAMO[table_name]['snowflake_list_column']].split(','),
        'inserted_at': str(timestamp)
    }


@flow
def recommendations_snowflake_to_dynamo_flow():
    logger = get_run_logger()
    
    dynamo_future = upsert_to_dynamo_task.submit()
    dynamo_future.result()
    logger.info('DynamoDB flow completed successfully')
    
    kafka_future = insert_to_kafka_task.submit()
    kafka_future.result()
    logger.info('Kafka flow completed successfully')


if __name__ == '__main__':
    recommendations_snowflake_to_dynamo_flow()
