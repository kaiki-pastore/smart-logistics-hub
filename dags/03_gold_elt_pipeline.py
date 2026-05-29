from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    '03_gold_elt_pipeline',
    default_args=default_args,
    description='Loads Silver data to Postgres and runs dbt transformations for the Gold layer',
    schedule_interval=timedelta(minutes=30), # Runs after Silver
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'pyspark', 'dbt', 'elt'],
) as dag:

    load_silver_to_postgres = BashOperator(
        task_id='load_silver_to_postgres',
        bash_command='python /opt/airflow/jobs/load_silver_to_postgres.py'
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .'
    )

    test_dbt_models = BashOperator(
        task_id='test_dbt_models',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .'
    )

    load_silver_to_postgres >> run_dbt_models >> test_dbt_models