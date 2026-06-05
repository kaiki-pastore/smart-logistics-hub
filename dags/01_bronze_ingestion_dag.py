from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    region_name='us-east-1'
)

BRONZE_BUCKET = 'bronze'
BASE_API_URL = 'http://api-mock:8000/api/v1'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def ingest_master_data(endpoint, s3_prefix):
    """Fetches master data (vehicles/drivers) and overwrites the current day's snapshot."""
    url = f"{BASE_API_URL}/master/{endpoint}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Creates a daily snapshot for master data
    date_str = datetime.now().strftime("%Y%m%d")
    file_key = f"master/{s3_prefix}/{s3_prefix}_{date_str}.json"
    
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=file_key,
        Body=json.dumps(data)
    )
    print(f"✅ Master data loaded to s3://{BRONZE_BUCKET}/{file_key} ({len(data)} records)")

def ingest_stream_data(endpoint, s3_prefix, batch_size):
    """Fetches multiple stream events and saves them as a single batch JSON file."""
    url = f"{BASE_API_URL}/stream/{endpoint}"
    batch_data = []
    
    for _ in range(batch_size):
        response = requests.get(url)
        if response.status_code == 200:
            batch_data.append(response.json())
            
    if not batch_data:
        print(f"⚠️ No data fetched for {endpoint}")
        return

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_key = f"stream/{s3_prefix}/batch_{timestamp_str}.json"
    
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=file_key,
        Body=json.dumps(batch_data)
    )
    print(f"✅ Stream data loaded to s3://{BRONZE_BUCKET}/{file_key} ({len(batch_data)} records)")

with DAG(
    '01_bronze_ingestion',
    default_args=default_args,
    description='Extracts ERP and Telemetry data from Mock API to Bronze layer',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion', 'erp', 'telemetry'],
) as dag:

    task_ingest_vehicles = PythonOperator(
        task_id='ingest_master_vehicles',
        python_callable=ingest_master_data,
        op_kwargs={'endpoint': 'vehicles', 's3_prefix': 'vehicles'}
    )

    task_ingest_drivers = PythonOperator(
        task_id='ingest_master_drivers',
        python_callable=ingest_master_data,
        op_kwargs={'endpoint': 'drivers', 's3_prefix': 'drivers'}
    )

    task_ingest_telemetry = PythonOperator(
        task_id='ingest_stream_telemetry',
        python_callable=ingest_stream_data,
        op_kwargs={'endpoint': 'telemetry', 's3_prefix': 'telemetry', 'batch_size': 50}
    )

    task_ingest_orders = PythonOperator(
        task_id='ingest_stream_orders',
        python_callable=ingest_stream_data,
        op_kwargs={'endpoint': 'orders', 's3_prefix': 'orders', 'batch_size': 20}
    )

    [task_ingest_vehicles, task_ingest_drivers] >> task_ingest_telemetry
    [task_ingest_vehicles, task_ingest_drivers] >> task_ingest_orders