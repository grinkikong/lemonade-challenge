from datetime import datetime
from prefect.blocks.system import Secret, JSON

from auto_scoring.functions.drivers import snowflake_driver


database = JSON.load("auto-scoring").value["database"]
schema = 'external_api'


def _load_to_snowflake(df, table_name):
    snowflake_driver.write_df_to_table(df, database=database, schema=schema, table_name=table_name)


def load_correlation_to_snowflake(df, game_id, process_id, file_id):
    colleration_df = df.reset_index(drop=False).melt(id_vars='EVENT_KEY', var_name='EVENT_KEY_2',
                                                     value_name='CORRELATION')

    # change the column names
    colleration_df = colleration_df.rename(columns={'EVENT_KEY': 'EVENT_KEY_1',
                                                    'EVENT_KEY_2': 'EVENT_KEY_2',
                                                    'CORRELATION': 'VALUE'})

    # filter NaN values
    colleration_df = colleration_df[colleration_df['VALUE'].notna()]

    colleration_df['INSERTED_AT'] = datetime.utcnow()
    colleration_df['FILE_TYPE'] = 'correlation'
    colleration_df['FILE_ID'] = file_id
    colleration_df['GAME_ID'] = game_id
    colleration_df['PROCESS_ID'] = str(process_id)

    _load_to_snowflake(colleration_df, 'CORRELATION_MATRIX')
