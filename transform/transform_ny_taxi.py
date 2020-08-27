"""
Reads the extracted taxi data, transforms rdd.
"""
from pyspark.sql.functions import lit, monotonically_increasing_id
from utils import spark_session


def transform_taxi_data():
    """
    Transforms data and writes into temp.
    :return:
    """
    spark = spark_session.get_spark_session()
    green_df = spark\
        .read\
        .csv("temp/green_*.csv",header=True,sep=",")\
        .withColumn("taxi_type", lit('1'))\
        .drop("ehail_fee")\
        .drop("trip_type")\
        .withColumnRenamed('lpep_pickup_datetime', 'tpep_pickup_datetime')\
        .withColumnRenamed('lpep_dropoff_datetime', 'tpep_dropoff_datetime')

    yellow_df = spark\
        .read\
        .csv("temp/yellow_*.csv",header=True,sep=",")\
        .withColumn('taxi_type', lit('2'))

    green_df\
        .union(yellow_df)\
        .withColumn('id', monotonically_increasing_id())\
        .withColumnRenamed('VendorID', 'vendor_id')\
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')\
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')\
        .withColumnRenamed('RatecodeID', 'rate_code_id')\
        .withColumnRenamed('PULocationID', 'pickup_location_id')\
        .withColumnRenamed('DOLocationID', 'dropoff_location_id')\
        .select(["id", "vendor_id", "pickup_datetime", "dropoff_datetime",
                 "pickup_location_id", "dropoff_location_id", "passenger_count",
                 "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
                 "tolls_amount", "improvement_surcharge", "total_amount",
                 "payment_type", "congestion_surcharge", "taxi_type"]) \
        .write \
        .mode("overwrite") \
        .csv(path='temp/transformed', sep=',', header=True)


def main():
    """
    Main function.
    :return:
    """
    transform_taxi_data()


if __name__ == "__main__":
    main()
