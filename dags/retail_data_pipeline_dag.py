from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from log_config import setup_logger
from scripts.create_snowflake_resources import create_snowflake_resources
from scripts.data_analysis import main as analyze_data
from scripts.etl_process import etl_task

logger = setup_logger(__name__, 'logs/airflow_dag.log')


def on_success_callback(context):
    logger.info(f"Task {context['task_instance'].task_id} completed successfully")


def on_failure_callback(context):
    logger.error(f"Task {context['task_instance'].task_id} failed")


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': on_success_callback,
    'on_failure_callback': on_failure_callback,
}

with DAG(
    'retail_data_pipeline',
    default_args=default_args,
    description='A comprehensive retail data pipeline DAG',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['retail', 'etl', 'spark'],
) as dag:
    start = EmptyOperator(task_id='start')

    create_resources = PythonOperator(
        task_id='create_snowflake_resources',
        python_callable=create_snowflake_resources,
    )

    etl = PythonOperator(
        task_id='etl_task',
        python_callable=etl_task,
    )

    analyze = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_data,
    )

    end = EmptyOperator(task_id='end')

    start >> create_resources >> etl >> analyze >> end

logger.info("DAG 'retail_data_pipeline' has been defined")
