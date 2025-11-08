from functions.s3_utils import S3Utils
import pandas as pd
from prefect import get_run_logger


def calculate_value_for_event_key(group, event_key):
    original_name_aggfunc_eventkey = group['NAME_AGGFUNC_EVENTKEY'].iloc[0]
    parts = original_name_aggfunc_eventkey.split('-')
    if len(parts) >= 3:
        name, aggfunc, eventkey = parts[0], parts[1], parts[2]
    else:
        # Handle cases where the format is not as expected
        name, aggfunc, eventkey = original_name_aggfunc_eventkey, 'UNKNOWN', event_key

    # Initialize result dictionary
    result = {'GAMEPLAY_ID': group['GAMEPLAY_ID'].iloc[0], 'EVENT_KEY': eventkey,
              'NAME_AGGFUNC_EVENTKEY': original_name_aggfunc_eventkey}

    # Check if 'message_type' == 'action'
    if (group['MESSAGE_TYPE'] == 'action').any():
        # Count unique timestamps and update NAME_AGGFUNC_EVENTKEY
        result['value'] = group[group['MESSAGE_TYPE'] == 'action']['TIMESTAMP'].nunique()
        result['NAME_AGGFUNC_EVENTKEY'] = f"{name}-nunique-{eventkey}"


    # Check if 'message_type' == 'state' and ('aggregationFunction' == 'last' or is null)
    elif (group['MESSAGE_TYPE'] == 'state').any() and (
            (group['AGGREGATION_FUNCTION'] == 'LAST') | group['AGGREGATION_FUNCTION'].isnull()).any():
        # Get the last state value and update NAME_AGGFUNC_EVENTKEY
        last_state_group = group[(group['MESSAGE_TYPE'] == 'state') &
                                 ((group['AGGREGATION_FUNCTION'] == 'LAST') |
                                  (group['AGGREGATION_FUNCTION'].isnull()))]
        if len(last_state_group) > 0:
            result['value'] = last_state_group.sort_values(by='TIMESTAMP', ascending=False).iloc[0]['EVENT_VALUE']
            result['NAME_AGGFUNC_EVENTKEY'] = f"{name}-last-{eventkey}"

    # Lastly, check for 'LAST_MINUS_FIRST' aggregation function
    else:
        last_minus_first_group = group[group['AGGREGATION_FUNCTION'] == 'LAST_MINUS_FIRST']
        if len(last_minus_first_group) > 0:
            # Calculate last minus first value and update NAME_AGGFUNC_EVENTKEY
            last_value = int(float(last_minus_first_group.sort_values(by='TIMESTAMP', ascending=False).iloc[0]['EVENT_VALUE']))
            first_value = int(float(last_minus_first_group.sort_values(by='TIMESTAMP', ascending=True).iloc[0]['EVENT_VALUE']))
            result['value'] = last_value - first_value
            result['NAME_AGGFUNC_EVENTKEY'] = f"{name}-last_minus_first-{eventkey}"

    return result



def calculate_event_ratio(df):
    """
    in the future, I can run this function and check the ratio between scorable events and all events
    :param df:
    :return:
    """
    all_ratios = []
    df['value'] = df['value'].astype(float)

    # Filter out event keys with 'nan' in NAME_AGGFUNC_EVENTKEY for the numerator
    numerator_df = df[df['NAME_AGGFUNC_EVENTKEY'].str.split('-').str[0] != 'nan']
    numerator_event_keys = numerator_df['NAME_AGGFUNC_EVENTKEY'].unique()

    # Get all unique event keys for the denominator
    denominator_event_keys = df['NAME_AGGFUNC_EVENTKEY'].unique()


    # Iterate through each pair of event keys
    for key1 in numerator_event_keys:
        for key2 in denominator_event_keys:
            if key1 != key2:  # Ensure we don't divide by the same event
                value1 = df[df['NAME_AGGFUNC_EVENTKEY'] == key1]['value'].iloc[0]
                value2 = df[df['NAME_AGGFUNC_EVENTKEY'] == key2]['value'].iloc[0]


                # Calculate ratio key1/key2
                if value2 != 0:  # Avoid division by zero
                    ratio = value1 / value2
                    pair = f"{key1}/{key2}"
                    all_ratios.append({'pair': pair, 'ratio': ratio, 'NAME_AGGFUNC_EVENTKEY1': key1, 'NAME_AGGFUNC_EVENTKEY2': key2})

    return pd.DataFrame(all_ratios)

def calc_ratio_matrix(df, game_id, bucket_name):

    logger = get_run_logger()

    logger.info(f"Processing game_id: {game_id}")

    # filter the data for the game_id from df
    data = df[df['GAME_ID'] == game_id]

    logger.info(f'manage to load game_events_data for game_id: {game_id}')


    all_gameplay_ratios = []  # List to store all gameplay_ratios

    # Group by 'GAMEPLAY_ID' and iterate over each group
    for gameplay_id, group_df in data.groupby('GAMEPLAY_ID'):

        results = []
        # Iterate over each 'NAME_AGGFUNC_EVENTKEY' for the current gameplay_id
        for NAME_AGGFUNC_EVENTKEY in group_df['NAME_AGGFUNC_EVENTKEY'].unique():
            event_group = group_df[group_df['NAME_AGGFUNC_EVENTKEY'] == NAME_AGGFUNC_EVENTKEY]
            value_row = calculate_value_for_event_key(event_group, NAME_AGGFUNC_EVENTKEY)
            results.append(value_row)

        # Convert results to DataFrame and handle gameplay_id specific logic
        if results:
            results_df = pd.DataFrame(results)
            gameplay_ratio = calculate_event_ratio(results_df)
            gameplay_ratio['GAMEPLAY_ID'] = gameplay_id  # Add gameplay_id to the gameplay_ratio DataFrame
            all_gameplay_ratios.append(gameplay_ratio)
        else:
            logger.info(f"The DataFrame for gameplay_id {gameplay_id} is empty.")

    # Combine all gameplay_ratios into a single dataframe
    if all_gameplay_ratios:
        final_ratio_df = pd.concat(all_gameplay_ratios, ignore_index=True)
    else:
        final_ratio_df = pd.DataFrame()
        logger.info("No data available for any gameplay_id.")

    # try download the file or set the correlation_old_df to None
    try:
        history_prep_ratio_df = S3Utils.pickle_to_df(f'{game_id}/prep_ratio_matrix.pkl', bucket_name=bucket_name)
    except:
        history_prep_ratio_df = None

    if history_prep_ratio_df is not None:
        dfs = (history_prep_ratio_df, final_ratio_df)
        final_ratio_df = pd.concat(dfs, ignore_index=True)

    # save the dataframe to a pickle file in s3
    S3Utils.df_to_pickle(final_ratio_df, f'{game_id}/prep_ratio_matrix.pkl', bucket_name=bucket_name)

    stats = final_ratio_df.groupby(['pair', 'NAME_AGGFUNC_EVENTKEY1', 'NAME_AGGFUNC_EVENTKEY2']).agg(
        mean_ratio=('ratio', 'mean'),
        distinct_gameplays=('GAMEPLAY_ID', pd.Series.nunique)
    ).reset_index()

    S3Utils.df_to_pickle(stats, f'{game_id}/ratio_matrix.pkl', bucket_name=bucket_name)

    logger.info(f'finished processing game_id:{game_id}')