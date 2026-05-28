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
    '03_gold_aggregations',
    default_args=default_args,
    description='Runs PySpark job to aggregate Silver data into Gold Business Metrics',
    schedule_interval=timedelta(minutes=30), # Runs after Silver
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'pyspark', 'business_intelligence'],
) as dag:

    # Clears Ivy cache to prevent JAR conflicts and runs the Gold job
    run_gold_spark_job = BashOperator(
        task_id='run_gold_aggregations_pyspark',
        bash_command='rm -rf /home/airflow/.ivy2 && python /opt/airflow/jobs/gold_aggregations.py'
    )