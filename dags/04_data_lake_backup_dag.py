from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '04_data_lake_backup',
    default_args=default_args,
    description='Exports D-1 Silver and Gold data from Postgres to MinIO Parquet (Cold Storage)',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['enterprise', 'backup', 'parquet', 'minio', 'gold', 'silver'],
) as dag:

    task_run_backup = BashOperator(
        task_id='run_spark_data_lake_backup',
        bash_command='python /opt/airflow/jobs/data_lake_backup.py'
    )

    task_run_backup