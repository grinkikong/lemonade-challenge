from prefect import get_run_logger

from auto_scoring.functions.drivers import snowflake_driver


def fetch_game_events(game_id, max_date, db_names):

    path = 'auto_scoring/queries/gameplay_data.sql'
    logger = get_run_logger()

    datalake_db = db_names["datalake_db"]
    data_warehouse_db = db_names["data_warehouse_db"]

    with open(path, 'r') as f:
        query = f.read()

    logger.info("Query:")
    logger.info(query.format(game_id=game_id, max_date=max_date, datalake_db=datalake_db, data_warehouse_db=data_warehouse_db))
    df = snowflake_driver.get_query_results_as_df(query.format(game_id=game_id,
                                                               max_date=max_date,
                                                               datalake_db=datalake_db,
                                                               data_warehouse_db=data_warehouse_db))

    logger.info("data stats")
    logger.info(f"Number of rows for {game_id} is: {len(df)}")
    return df






