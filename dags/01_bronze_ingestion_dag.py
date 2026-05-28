from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json
import os

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    region_name='us-east-1'
)

BRONZE_BUCKET = 'bronze'
API_URL = 'http://api-mock:8000/telemetry'
LOCAL_DATA_DIR = '/opt/airflow/data/raw_source'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def ingest_telemetry_to_bronze():
    """Fetches real-time GPS data from API and saves to Bronze."""
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_key = f"telemetry/event_{timestamp_str}.json"
    
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=file_key,
        Body=json.dumps(data)
    )
    print(f"✅ Telemetry data loaded to s3://{BRONZE_BUCKET}/{file_key}")

def ingest_static_data_to_bronze():
    """Uploads static fleet and inventory files to Bronze."""
    for filename in os.listdir(LOCAL_DATA_DIR):
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        
        # Decide the folder structure based on file type
        if "vehicles" in filename:
            s3_key = f"static/fleet/{filename}"
        elif "inventory" in filename:
            s3_key = f"inventory/{filename}"
        else:
            continue
            
        s3_client.upload_file(local_path, BRONZE_BUCKET, s3_key)
        print(f"✅ Static data loaded to s3://{BRONZE_BUCKET}/{s3_key}")

with DAG(
    '01_bronze_ingestion',
    default_args=default_args,
    description='Extracts logistics data from API and local files to Bronze layer',
    schedule_interval=timedelta(minutes=5), # Roda a cada 5 minutos
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion', 'logistics'],
) as dag:

    task_ingest_telemetry = PythonOperator(
        task_id='ingest_telemetry',
        python_callable=ingest_telemetry_to_bronze
    )

    task_ingest_static = PythonOperator(
        task_id='ingest_static_data',
        python_callable=ingest_static_data_to_bronze
    )

    # Executa em paralelo
    [task_ingest_telemetry, task_ingest_static]