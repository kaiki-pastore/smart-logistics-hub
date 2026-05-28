from fastapi import FastAPI
from faker import Faker
from datetime import datetime, timezone
import random

app = FastAPI(title="Logistics Telemetry API")
fake = Faker('en_US')

@app.get("/")
def health_check():
    return {"status": "Logistics Telemetry API is running!"}

@app.get("/telemetry")
def generate_telemetry():
    """Generates a simulated GPS event for a delivery truck."""
    status_list = ["IN_TRANSIT", "STOPPED", "DELIVERING", "DELAYED"]
    
    return {
        "vehicle_id": f"V-{random.randint(1000, 9999)}",
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": float(fake.latitude()),
        "longitude": float(fake.longitude()),
        "delivery_status": random.choice(status_list),
        "cargo_temperature": round(random.uniform(2.0, 8.0), 2) # Simulated refrigerated cargo (2ºC to 8ºC)
    }