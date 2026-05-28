from contextlib import asynccontextmanager
import duckdb
from fastapi import FastAPI
import os

con = duckdb.connect()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup event: Installs and configures the 'httpfs' extension 
    allowing DuckDB to natively read Parquet files directly from MinIO.
    """
    print("Configuring DuckDB S3/MinIO connection...")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("SET s3_endpoint='minio:9000';")
    con.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ACCESS_KEY', 'admin')}';")
    con.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_SECRET_KEY', 'admin')}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    yield
    print("Shutting down Analytics API...")
    con.close()

app = FastAPI(
    title="Analytics API - Smart Logistics Hub",
    description="Serves Gold layer business metrics directly from the Data Lake.",
    lifespan=lifespan
)

@app.get("/api/v1/metrics/fleet")
def get_fleet_metrics():
    """Returns the average daily cargo temperature for each vehicle."""
    query = """
        SELECT * FROM read_parquet('s3://gold/daily_fleet_metrics/*.parquet') 
        ORDER BY date DESC, vehicle_id
    """
    result = con.execute(query).fetchdf()
    return result.to_dict(orient="records")

@app.get("/api/v1/metrics/inventory")
def get_inventory_summary():
    """Returns the total weight and order count grouped by export date."""
    query = """
        SELECT * FROM read_parquet('s3://gold/daily_inventory_summary/*.parquet') 
        ORDER BY export_date DESC
    """
    result = con.execute(query).fetchdf()
    return result.to_dict(orient="records")