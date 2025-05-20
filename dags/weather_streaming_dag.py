# dags/weather_streaming_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import sys
sys.path.append('/opt/airflow/scripts')
from weather_ingestion import main as weather_ingestion
from weather_transformation import main as weather_transformation

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_streaming_processing',
    default_args=default_args,
    description='A DAG for processing weather data',
    schedule_interval=timedelta(hours=1),
)

ingest_task = PythonOperator(
    task_id='ingest_weather_data',
    python_callable=weather_ingestion,
    op_kwargs={'api_key': '951d8917fa8b154afd44712c1c73ac4c'},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=weather_transformation,
    dag=dag,
)

dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/dbt_project && dbt run --models source_dim_weather trip_enriched trip_summary_per_hour',
    dag=dag,
)

ingest_task >> transform_task >> dbt_run