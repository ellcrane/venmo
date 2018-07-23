import requests
import pandas as pd
import boto3
import time
import datetime
import calendar
from sys import argv
import json


def get_unix_timestamp(input_time):
    '''
    INPUT: list in following format: [year, month, day, hours (24), minutes, seconds] **for UTC**
    OUTPUT: Int of unix timestamp
    '''
    dt = datetime.datetime(input_time[0], input_time[1], input_time[2],
                           input_time[3], input_time[4], input_time[5])
    return int(time.mktime(dt.timetuple()))


def get_venmo_url(start_unix_timestamp, end_unix_timestamp, limit=1000000):
    return f'https://venmo.com/api/v5/public?since={start_unix_timestamp}&until={end_unix_timestamp}&limit={limit}'


def get_venmo_data(url):
    raw_data = requests.get(url)
    json_data = raw_data.json()
    return json_data


def get_unix_timestamp(time):
    '''
    INPUT: list in following format: [year, month, day, hours (24), minutes, seconds] **for UTC**
    OUTPUT: Int of unix timestamp
    '''
    dt = datetime.datetime(time[0], time[1], time[2], time[3], time[4], time[5])
    return calendar.timegm(dt.utctimetuple())


def get_s3_keys(bucket):
    """Get a list of keys in an S3 bucket."""
    keys = []
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket)
    for obj in response['Contents']:
        keys.append(obj['Key'])
    return keys


def get_df_from_aws_keys(bucket, key_list):
    s3 = boto3.client('s3')
    data = list()
    for i in range(len(key_list)):
        if i % 20 == 0:
            print(len(key_list) - i)
        response = s3.get_object(Bucket=bucket, Key=key_list[i])
        body = json.loads((response['Body'].read()))
        data.extend(body)
    return pd.DataFrame(data)


def scrape(start_date, end_date, interval, limit=1000000):
    '''
    INPUT: start_date and end_date, each as list in format: [year, month, day, hours (24), minutes, seconds]
           Int of min 10 representing the number of second-long intervals to scrape
    OUTPUT: none, will upload to s3 bucket
    '''
    keys = get_s3_keys('transaction-data-2018')
    links = [get_venmo_url(key.split('_')[0], key.split('_')[1],
                           key.split('_')[2]).split('.json')[0] for key in keys]
    s3 = boto3.resource('s3')
    if interval < 10:
        print('Interval must be greater than 10 seconds')
    else:
        start_unix_ts = get_unix_timestamp(start_date)
        end_unix_ts = get_unix_timestamp(end_date)
        scrape_count = int((end_unix_ts - start_unix_ts) / interval)
        print(
            f'With your parameters, this function will scrape the public venmo API {scrape_count} times.')
        for i in range(start_unix_ts, end_unix_ts, interval):
            url = get_venmo_url(i, i + interval, limit)
            if url in links:
                print('Already scraped this!')
                print(f'already scraped: {url}')
            else:
                print(f'scraping: {url}')
                data = get_venmo_data(url)['data']
                file_name = f'{i}_{i + interval}_{limit}'
                obj = s3.Object('transaction-data-2018', f'{file_name}.json')
                obj.put(Body=json.dumps(data))
                time.sleep(1)


if __name__ == '__main__':
    print("This is the name of the script: "), argv[0]
    start_date = [int(argv[1][1:-1]),
                  int(argv[2][:-1]),
                  int(argv[3][:-1]),
                  int(argv[4][:-1]),
                  int(argv[5][:-1]),
                  int(argv[6][:-1])]
    print(f'Start Date: {start_date}')
    end_date = [int(argv[7][1:-1]),
                int(argv[8][:-1]),
                int(argv[9][:-1]),
                int(argv[10][:-1]),
                int(argv[11][:-1]),
                int(argv[12][:-1])]
    print(f'End Date: {end_date}')
    interval = int(argv[13])
    print(f'Interval: {interval} seconds')

    scrape(start_date, end_date, interval)
