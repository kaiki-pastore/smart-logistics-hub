from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    '02_silver_transformation',
    default_args=default_args,
    description='Validates and transforms raw Bronze data into typed Silver Parquet',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['silver', 'pyspark', 'data_quality'],
) as dag:

    run_quality_checks = BashOperator(
        task_id='run_data_quality_checks',
        bash_command='python /opt/airflow/jobs/quality_checks.py'
    )

    run_silver_spark_job = BashOperator(
        task_id='run_silver_transformation_pyspark',
        bash_command='python /opt/airflow/jobs/silver_transformation.py'
    )

    run_quality_checks >> run_silver_spark_job