"""
DAG for NY Taxi ETL
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


path = '~/IdeaProjects/etl-airflow'

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'start_date': days_ago(2)
}

dag = DAG(
    'ny-taxi-etl',
    default_args=default_args,
    description='NY Taxi ETL',
    schedule_interval=timedelta(days=1)
)

extract = BashOperator(
    task_id='extract_ny_taxi',
    bash_command='{path}/{command}/{command}.sh '
        .format(path=path, command='extract'),
    dag=dag
)

transform = BashOperator(
    task_id='transform_ny_taxi',
    bash_command='{path}/{command}/{command}.sh '
        .format(path=path, command='transform'),
    dag=dag
)

load = BashOperator(
    task_id='load_ny_taxi',
    bash_command='{path}/{command}/{command}.sh '
        .format(path=path, command='load'),
    dag=dag
)

extract >> transform >> load
