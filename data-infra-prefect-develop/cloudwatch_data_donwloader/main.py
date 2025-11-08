import datetime
from config import NAMESPACES, STATISTICS
import boto3
from data_utils.logger import init_logger

session = boto3.Session(region_name='eu-central-1')
cloudwatch = session.client('cloudwatch')
logger = init_logger('CloudWatch.DataDownloader')

def get_services_info():
    namespaces_info = []
    for namespace, info in NAMESPACES.items():
        namespace_info = {
            'name': namespace,
            'metrics': info['metrics'],
            'period': info['period']
        }
        namespaces_info.append(namespace_info)
    return namespaces_info


def generate_timestamp_pairs(start_date=None, end_date=None):

    start_date, end_date = generate_start_n_end_dates(start_date, end_date)
    date_pairs = []
    current_date = start_date

    while current_date < end_date:
        next_date = current_date + datetime.timedelta(days=1)
        if next_date > end_date:
            break
        start_datetime = datetime.datetime.combine(current_date, datetime.time.min)  # Midnight of the current day
        end_datetime = datetime.datetime.combine(next_date, datetime.time.min)  # Midnight of the next day
        date_pairs.append((start_datetime, end_datetime))
        current_date = next_date

    return date_pairs


def generate_start_n_end_dates(start_date,end_date):
    if start_date is None and end_date is not None:
        raise ValueError("missing start_date")

    if end_date is None and start_date is not None:
        raise ValueError("missing end_date")

    if start_date is None:
        start_date = (datetime.datetime.utcnow() - datetime.timedelta(days=2)).date()
    else:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()

    if end_date is None:
        end_date = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).date()
    else:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    return start_date, end_date

def get_cloudwatch_metric(namespace, metric_name, dimensions, start_time, end_time, period, statistics):
    response = cloudwatch.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=statistics
    )
    return response


def list_metrics(namespace, metrics_list):
    paginator = cloudwatch.get_paginator('list_metrics')
    metrics_iterator = paginator.paginate(Namespace=namespace)
    all_metrics = []
    for page in metrics_iterator:
        all_metrics.extend(page['Metrics'])

    filtered_list = [d for d in all_metrics if d['MetricName'] in metrics_list]
    return filtered_list


def format_metric_datapoint(metric_name, dimensions, datapoint):
    return {
        'metric_name': metric_name,
        'dimensions': dimensions,
        'timestamp': datapoint['Timestamp'],
        'sum': datapoint.get('Sum'),
        'sample_count': datapoint.get('SampleCount'),
        'average': datapoint.get('Average'),
        'min': datapoint.get('Minimum'),
        'max': datapoint.get('Maximum')
    }



def get_cloudwatch_parameters(metric,dates,service):
    namespace = metric['Namespace']
    dimensions = metric['Dimensions']
    metric_name = metric['MetricName']
    start_time = dates[0]
    end_time = dates[1]
    period = service['period']
    return namespace, dimensions, metric_name, start_time, end_time, period

if __name__ == '__main__':
    logger.info('Starting cloudwatch metrics process')
    output = []
    services_info_list = get_services_info()
    dates_list = generate_timestamp_pairs()
    for service in services_info_list:
        logger.info(f'Running Service {service["name"]}: fetching {len(service["metrics"])} metrics')
        metrics = list_metrics(service['name'], service['metrics'])
        for dates in dates_list:
            logger.info(f'Running on dates {dates}')
            for metric in metrics:
                namespace, dimensions, metric_name, start_time, end_time, period  = get_cloudwatch_parameters(metric, dates ,service)
                statistics = STATISTICS
                metrics_results = get_cloudwatch_metric(namespace, metric_name, dimensions, start_time, end_time, period, statistics)
                for datapoint in metrics_results['Datapoints']:
                    formatted_result = format_metric_datapoint(metric_name, dimensions, datapoint)
                    print(f'cloudwatch_metrics: {formatted_result}')
                    output.append(formatted_result)

