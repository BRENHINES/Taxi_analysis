# dags/taxi_batch_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import sys
sys.path.append('/opt/airflow/scripts')
from taxi_ingestion import main as taxi_ingestion
from taxi_transformation import main as taxi_transformation

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
    'taxi_batch_processing',
    default_args=default_args,
    description='A DAG for processing taxi data',
    schedule_interval=timedelta(days=1),
)

ingest_task = PythonOperator(
    task_id='ingest_taxi_data',
    python_callable=taxi_ingestion,
    op_kwargs={'year': 2022, 'month': 1},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_taxi_data',
    python_callable=taxi_transformation,
    dag=dag,
)

dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/dbt_project && dbt run --models source_fact_taxi_trips trip_enriched trip_summary_per_hour high_value_customers',
    dag=dag,
)

ingest_task >> transform_task >> dbt_run