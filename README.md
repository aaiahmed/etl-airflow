# etl-airflow
This is a sample ETL application that extracts open data on AWS 
for [New York City Taxi and Limousine Commission (TLC) Trip Record Data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/). 

It uses [Apache Spark](https://spark.apache.org/) to transform data into applicable schema and 
loads into a destination data warehouse in PostgreSQL. 

[Apache Airflow](https://airflow.apache.org/) is used to create DAGs to 
orchestrate the data pipelines. 