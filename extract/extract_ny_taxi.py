"""
This file downloads public NY Taxi data from s3 for a given duration.
"""

import os
import boto3
import pandas as pd
from utils import config

conf = config.get_config()

def requires_update(obj):
    """
    Returns true if an s3 object should be downloaded.
    :param obj: s3 Object
    :return: boolean
    """
    duration = int(conf['etl']['duration'])
    valid_files = ('trip data/green', 'trip data/yellow')
    months = pd.date_range(end=pd.to_datetime('today'),
                           periods=duration,
                           freq='MS').strftime("%Y-%m").to_list()
    return str(obj.key).startswith(valid_files) & \
           (obj.key[-11:-4] in months)


def extract_from_s3():
    """
    Downloads taxi data from s3.
    :return:
    """
    bucket_name = conf['source']['s3']['bucket']
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(name=bucket_name)

    for obj in bucket.objects.all():
        if requires_update(obj):
            s3.meta.client.download_file(bucket_name,
                                         obj.key,
                                         os.path.join('temp', os.path.split(obj.key)[-1]))


def main():
    """
    Main function.
    :return:
    """
    extract_from_s3()


if __name__ == "__main__":
    main()
