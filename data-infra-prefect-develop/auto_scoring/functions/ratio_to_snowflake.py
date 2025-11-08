from datetime import datetime
from prefect.blocks.system import JSON

from auto_scoring.functions.drivers import snowflake_driver


database = JSON.load("auto-scoring").value["database"]
schema = 'external_api'


def _snowflake_inserts(df, table_name):
    snowflake_driver.write_df_to_table(df, database=database, schema=schema, table_name=table_name)


def _split_event_aggfunc(column):
    return column.str.split('-').str[-2], column.str.split('-').str[-1]


def load_ratio_to_snowflake(df, game_id, process_id, file_id):

    ratio_df = df

    # Splitting the columns and removing the prefix
    ratio_df['AGG_FUNC_1'], ratio_df['EVENT_KEY_1'] = _split_event_aggfunc(df['NAME_AGGFUNC_EVENTKEY1'])
    ratio_df['AGG_FUNC_2'], ratio_df['EVENT_KEY_2'] = _split_event_aggfunc(df['NAME_AGGFUNC_EVENTKEY2'])

    #print columns

    matrix = ratio_df.drop(columns=['NAME_AGGFUNC_EVENTKEY1', 'NAME_AGGFUNC_EVENTKEY2', 'pair'])

    matrix = matrix.rename(columns={'mean_ratio': 'VALUE', 'distinct_gameplays': 'DISTINCT_GAMEPLAYS'})

    matrix.columns = map(str.upper, matrix.columns)

    # Add inserted_at with current datetime
    matrix['INSERTED_AT'] = datetime.utcnow()
    matrix['FILE_TYPE'] = 'ratio'
    matrix['FILE_ID'] = file_id
    matrix['GAME_ID'] = game_id
    matrix['PROCESS_ID'] = str(process_id)


    _snowflake_inserts(matrix, 'RATIO_MATRIX')
