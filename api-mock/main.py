from fastapi import FastAPI
from faker import Faker
from datetime import datetime, timezone
import random
from data_registry import registry

app = FastAPI(title="Logistics ERP Mock API")
fake = Faker('en_US')

@app.get("/")
def health_check():
    return {"status": "Logistics ERP API is running!"}

@app.get("/api/v1/master/vehicles")
def get_vehicles():
    """Returns the complete fleet master data."""
    return registry.get_all_vehicles()

@app.get("/api/v1/master/drivers")
def get_drivers():
    """Returns the complete drivers master data."""
    return registry.get_all_drivers()

@app.get("/api/v1/stream/telemetry")
def generate_telemetry():
    """Generates a GPS and sensor event for a valid fleet vehicle."""
    vehicle = registry.get_random_vehicle()
    status_list = ["IN_TRANSIT", "STOPPED", "DELIVERING", "DELAYED"]
    
    return {
        "event_id": f"EVT-{random.randint(100000, 999999)}",
        "vehicle_id": vehicle["vehicle_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        # Simulated coordinates within the Rio de Janeiro metropolitan area
        "latitude": round(random.uniform(-23.0, -22.5), 6),
        "longitude": round(random.uniform(-43.5, -43.1), 6),
        "delivery_status": random.choice(status_list),
        "cargo_temp_c": round(random.uniform(2.0, 8.0), 2),
        "speed_kmh": round(random.uniform(0.0, 90.0), 1)
    }

@app.get("/api/v1/stream/orders")
def generate_order():
    """Generates a new warehouse dispatch order."""
    return {
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "product_name": fake.word().capitalize(),
        "weight_kg": round(random.uniform(5.0, 500.0), 2),
        "destination_lat": round(random.uniform(-23.0, -22.5), 6),
        "destination_lon": round(random.uniform(-43.5, -43.1), 6),
        "status": "READY_FOR_DISPATCH",
        "created_at": datetime.now(timezone.utc).isoformat()
    }