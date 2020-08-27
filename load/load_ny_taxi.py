"""
Loads transformed rdd into data warehouse.
"""
import os
from utils import config, spark_session


def load_taxi_data():
    """
    Reads taxi data and writes to destination.
    :return:
    """
    conf = config.get_config()
    host = conf['dw']['postgresql']['host']
    port = conf['dw']['postgresql']['port']
    db = conf['dw']['postgresql']['db']
    user = conf['dw']['postgresql']['user']
    password = os.getenv("DW_PASSWORD", "")
    schema = conf['dw']['postgresql']['schema']
    table = conf['dw']['postgresql']['table']
    driver = conf['dw']['postgresql']['driver']
    url = "jdbc:postgresql://{host}:{port}/{db}"\
        .format(host=host, port=port, db=db)
    db_table = "{schema}.{table}"\
        .format(schema=schema, table=table)

    spark = spark_session.get_spark_session()
    spark \
        .read \
        .csv("temp/transformed",header=True,sep=",")\
        .write \
        .mode("overwrite") \
        .format('jdbc') \
        .option("url", url) \
        .option("dbtable", db_table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .save()


def main():
    """
    Main function.
    :return:
    """
    load_taxi_data()


if __name__ == "__main__":
    main()
