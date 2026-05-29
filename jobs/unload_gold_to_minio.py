import os
import pandas as pd
from sqlalchemy import create_engine

print("Initializing Gold Unload Process (Postgres -> MinIO)...")

MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD", "admin")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

db_engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres:5432/gold_db')

minio_options = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": "http://minio:9000"
    }
}

def export_table_to_parquet(table_name, s3_path):
    print(f"Exporting {table_name} to {s3_path}...")
    df = pd.read_sql_table(table_name, db_engine)
    
    if df.empty:
        print(f"Warning: Table {table_name} is empty.")
        return

    s3_url = f"s3://{s3_path}"
    df.to_parquet(s3_url, storage_options=minio_options, engine="pyarrow", index=False)
    print(f"✅ Successfully exported {table_name} to Parquet.")

if __name__ == "__main__":
    try:
        export_table_to_parquet("dim_vehicles", "gold/dim_vehicles/data.parquet")
        export_table_to_parquet("delivery_fact", "gold/delivery_fact/data.parquet")
    except Exception as e:
        print(f"Critical error during unload: {e}")
        exit(1)