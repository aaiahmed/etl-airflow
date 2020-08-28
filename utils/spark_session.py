"""
Creates a spark session to send jobs.
"""
from pyspark.sql import SparkSession
from utils import config


def get_spark_session():
    """
    Returns a spark session.
    :return:
    """
    conf = config.get_config()
    master = conf['spark']['master']
    app_name = conf['spark']['app']
    deploy_mode = conf['spark']['deploy_mode']
    jar = conf['spark']['jar']

    return SparkSession\
        .builder \
        .master(master) \
        .appName(app_name) \
        .config("spark.submit.deployMode", deploy_mode) \
        .config("spark.jars", jar) \
        .getOrCreate()
