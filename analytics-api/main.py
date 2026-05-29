from contextlib import asynccontextmanager
import duckdb
from fastapi import FastAPI, Query
import os

con = duckdb.connect()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup Configuration: Installs and configures the 'httpfs' extension
    allowing DuckDB to natively read Parquet files directly from MinIO/S3.
    """
    print("🚀 Setting up DuckDB connection to S3/MinIO...")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("SET s3_endpoint='minio:9000';")
    con.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ACCESS_KEY', 'admin')}';")
    con.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_SECRET_KEY', 'admin')}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    yield
    print("🛑 Disconnecting Analytics API...")
    con.close()

app = FastAPI(
    title="Analytics API - Smart Logistics Hub",
    description="Provides business metrics from the Gold layer directly from the Data Lake using DuckDB.",
    lifespan=lifespan
)

@app.get("/api/v1/metrics/fleet")
def get_fleet_metrics(status: str = Query(None)):
    """
    Returns consolidated fleet performance and temperature metrics
    by performing an in-memory JOIN between the Fact and Vehicle Dimension tables.
    """
    query = """
        SELECT 
            TRIM(UPPER(v.vehicle_id)) as vehicle_id,
            v.capacity_kg,
            COUNT(f.event_timestamp) as total_telemetry_events,
            ROUND(AVG(f.cargo_temp_c), 2) as avg_cargo_temperature
        FROM read_parquet('s3://gold/delivery_fact/*.parquet') f
        JOIN read_parquet('s3://gold/dim_vehicles/*.parquet') v 
          ON TRIM(UPPER(f.vehicle_id)) = TRIM(UPPER(v.vehicle_id))
    """
    
    if status:
        query += f" WHERE TRIM(UPPER(f.delivery_status)) = TRIM(UPPER('{status}'))"
            
        query += " GROUP BY TRIM(UPPER(v.vehicle_id)), v.capacity_kg ORDER BY total_telemetry_events DESC"
        
        result = con.execute(query).fetchdf()
        return result.to_dict(orient="records")

@app.get("/api/v1/metrics/alerts")
def get_temperature_alerts():
    """
    Returns vehicles that operated above the safe 
    temperature limit to ensure cargo quality control.
    """
    query = """
        SELECT 
            vehicle_id,
            COUNT(*) as total_alerts,
            ROUND(AVG(cargo_temp_c), 2) as alert_avg_temperature
        FROM read_parquet('s3://gold/delivery_fact/*.parquet')
        WHERE cargo_temp_c > 12.0
        GROUP BY vehicle_id
        ORDER BY total_alerts DESC
    """
    result = con.execute(query).fetchdf()
    return result.to_dict(orient="records")