from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from time import sleep

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 17),
    'email': ['caixiuhong2009@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    dag_id='solar_power_marketing_dag',
    default_args=default_args,
    schedule_interval='14 04 * * *'
)

ingest_to_s3 = BashOperator(
    task_id='ingest_to_s3',
    bash_command="/home/ubuntu/src/spark/submit-ingest.sh ",
    dag=dag)

process = BashOperator(
    task_id='process',
    bash_command='/home/ubuntu/src/spark/submit-process.sh ',
    dag=dag)

ingest_to_s3 >> process