import pandas as pd
import numpy as np
import os
from decimal import Decimal
from prefect import get_run_logger
from functions.s3_utils import S3Utils


def initialize_statistics(data):
    data = data.astype(float)
    means = data.mean()
    variances = data.var(ddof=1)
    counts = data.count()
    covariance_matrix = data.cov()
    return means, variances, counts, covariance_matrix


def update_variance(variances, new_data, all_events, weights):
    # Convert the variances to float, ensuring all data is of the same type
    variances_float = {k: float(v) if isinstance(v, Decimal) else v for k, v in variances.items()}
    variances_series = pd.Series(variances_float)

    # Convert all Decimal objects to float in the DataFrame
    new_data = new_data.applymap(lambda x: float(x) if isinstance(x, Decimal) else x)

    # Calculate the variances for the new data
    updated_variances = new_data.var(ddof=1)

    # Reindex the variances to include all possible events
    reindexed_variances = updated_variances.reindex(all_events)

    # Fill in the missing values with those from variances_series
    reindexed_variances_filled = reindexed_variances.fillna(variances_series)

    # Perform the weighted update of variances
    final_variances = reindexed_variances_filled * weights + updated_variances * (1 - weights)

    return final_variances


def update_statistics(new_data, means, variances, counts, covariance_matrix):
    # Initialize statistics from new data
    new_means, new_variances, new_counts, new_covariance = initialize_statistics(new_data)

    # Identify all unique event names from both new data and existing statistics
    all_events = set(means.index).union(new_means.index)

    # Prepare for integration by reindexing to include all possible events
    means = means.reindex(all_events, fill_value=0)
    variances = variances.reindex(all_events, fill_value=0)
    counts = counts.reindex(all_events, fill_value=0)
    covariance = covariance_matrix.reindex(index=all_events, columns=all_events, fill_value=0)

    # Update counts
    total_counts = counts + new_counts.reindex(all_events, fill_value=0)

    # Calculate weights based on old counts to handle division by zero
    weights = counts / total_counts.replace(0, 1)  # Avoid division by zero by replacing zero counts with one

    # Calculate new means and variances using weighted averages
    updated_means = means * weights + new_means.reindex(all_events, fill_value=0) * (1 - weights)
    updated_variances = variances * weights + new_variances.reindex(all_events, fill_value=0) * (1 - weights)

    # Update covariance matrix using a weighted approach
    updated_covariance = covariance.copy()
    for col in all_events:
        for row in all_events:
            if col == row:
                updated_covariance.at[col, row] = updated_variances[col]
            else:
                # Compute the updated covariance for each pair of events
                new_cov_col_row = new_covariance.at[
                    col, row] if col in new_covariance.columns and row in new_covariance.index else 0
                updated_covariance.at[col, row] = (covariance.at[col, row] * counts.get(col, 0) +
                                                   new_cov_col_row * new_counts.get(col, 0)) / total_counts[col]

    return updated_means, updated_variances, total_counts, updated_covariance


def calculate_correlation_matrix(covariance_matrix):
    # Diagonal elements are the variances
    variances = np.diag(covariance_matrix)

    # Ensure variances are positive to avoid division by zero or negative under root
    std_dev = np.sqrt(np.maximum(variances, 0))  # Use maximum to avoid negative values

    # Outer product of standard deviations to form denominator matrix for correlation calculation
    denominator = np.outer(std_dev, std_dev)

    # Element-wise division of covariance matrix by the denominator
    correlation_matrix = pd.DataFrame(
        np.divide(covariance_matrix.values, denominator, out=np.zeros_like(denominator), where=denominator != 0),
        index=covariance_matrix.index,
        columns=covariance_matrix.columns
    )

    # Clamp the values between -1 and 1 to handle any possible numerical precision issues
    correlation_matrix = correlation_matrix.clip(-1, 1)

    # Set correlation of zero variance cases to NaN since correlation is undefined there
    zero_variance = (variances <= 0)
    correlation_matrix.loc[zero_variance, :] = np.nan
    correlation_matrix.loc[:, zero_variance] = np.nan

    return correlation_matrix


def calc_correlation_matrix(df, game_id, bucket_name):
    logger = get_run_logger()
    logger.info(f'processing game_id:{game_id}')

    # Filter data and create a clean copy
    data = df[df['GAME_ID'] == game_id].copy()
    logger.info(f'manage to load game_events_data for game_id: {game_id}')

    # Sort and deduplicate in one step
    data_sorted = data.sort_values(by=['GAMEPLAY_ID', 'TIMESTAMP'], ascending=[True, True])
    data_unique = data_sorted.drop_duplicates(subset=['GAMEPLAY_ID', 'TIMESTAMP', 'EVENT_KEY'])
    del data_sorted  # Free memory
    del data  # Free memory

    # Initialize an empty list to store the pivoted DataFrames
    pivoted_dfs = []

    # Group by 'gameplay_id' and process each subset
    for gameplay_id, group_df in data_unique.groupby('GAMEPLAY_ID'):
        # Pivot the DataFrame
        pivot_df = group_df.pivot(index='TIMESTAMP', columns='EVENT_KEY', values='EVENT_VALUE')

        # Forward fill and backward fill to handle NaN values
        pivot_df = pivot_df.ffill().bfill()

        # Store the gameplay_id as a column before appending to the list
        pivot_df['GAMEPLAY_ID'] = gameplay_id
        pivoted_dfs.append(pivot_df)

        # Free memory
        del pivot_df

    # Free memory
    del data_unique

    # Concatenate all pivoted DataFrames
    combined_df = pd.concat(pivoted_dfs, axis=0, sort=False)
    del pivoted_dfs  # Free memory

    # Reset the index to ensure 'timestamp' is not in the index
    combined_df = combined_df.reset_index()

    # Drop the 'timestamp' column
    combined_df = combined_df.drop(columns='TIMESTAMP')

    # Remove duplicate rows based on the new index (if any)
    combined_df = combined_df[~combined_df.index.duplicated(keep='first')]

    # Drop the 'gameplay_id' column if it's not needed for correlation calculation
    combined_df = combined_df.drop(columns='GAMEPLAY_ID')

    logger.info('finished working on the new_data')

    try:
        # Load historical data
        correlation_stats = S3Utils.pickle_to_df(f'{game_id}/correlation_stats_hist.pkl', bucket_name=bucket_name)
        logger.info('loaded correlation_stats_hist.pkl successfully')

        # Extract statistics efficiently
        stats_dict = correlation_stats.to_dict('index')
        means = pd.Series({k: v['mean'] for k, v in stats_dict.items()})
        variances = pd.Series({k: v['variance'] for k, v in stats_dict.items()})
        counts = pd.Series({k: v['count'] for k, v in stats_dict.items()})
        del correlation_stats, stats_dict  # Free memory

        covariance_matrix = S3Utils.pickle_to_df(f'{game_id}/covariance_matrix_hist.pkl', bucket_name=bucket_name)
        logger.info('loaded covariance_matrix_hist.pkl successfully')

        # Update statistics
        means, variances, counts, covariance_matrix = update_statistics(combined_df, means, variances, counts,
                                                                        covariance_matrix)
    except Exception as e:
        means, variances, counts, covariance_matrix = initialize_statistics(combined_df)

    # Create correlation statistics DataFrame efficiently
    df_correlation = pd.DataFrame({
        'mean': means,
        'variance': variances,
        'count': counts
    })

    # Save results
    S3Utils.df_to_pickle(df_correlation, f'{game_id}/correlation_stats_hist.pkl', bucket_name=bucket_name)
    S3Utils.df_to_pickle(covariance_matrix, f'{game_id}/covariance_matrix_hist.pkl', bucket_name=bucket_name)

    # Calculate and save correlation matrix
    correlation_matrix = calculate_correlation_matrix(covariance_matrix)
    S3Utils.df_to_pickle(correlation_matrix, f'{game_id}/correlation_matrix.pkl', bucket_name=bucket_name)

    logger.info(f'finished processing game_id:{game_id}')
    return correlation_matrix