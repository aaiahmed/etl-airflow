"""
Loads transformed rdd into data warehouse.
"""
import os
from airflow.models import Variable
from utils import config, spark_session


def get_dw_password():
    """
    Returns the password to connect to data warehouse.
    :return: password
    """

    if os.getenv("DW_PASSWORD") is not None:
        return os.getenv("DW_PASSWORD")
    return Variable.get("DW_PASSWORD")


def load():
    """
    Reads taxi data and writes to destination.
    :return:
    """
    conf = config.get_config()
    host = conf['dw']['postgresql']['host']
    port = conf['dw']['postgresql']['port']
    db = conf['dw']['postgresql']['db']
    user = conf['dw']['postgresql']['user']
    password = get_dw_password()
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
        .csv("temp/transformed", header=True, sep=",")\
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
    load()


if __name__ == "__main__":
    main()
