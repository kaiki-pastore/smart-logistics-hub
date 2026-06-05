from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '03_gold_transformation_dbt',
    default_args=default_args,
    description='Runs dbt to build the Star Schema (Gold Layer)',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'dbt', 'analytics'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    )

    dbt_run >> dbt_test