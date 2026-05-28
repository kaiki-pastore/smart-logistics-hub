import pandas as pd
from faker import Faker
import random
import os
import json
from datetime import datetime

fake = Faker('en_US')

OUTPUT_DIR = "raw_source"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_vehicles_parquet(num_vehicles=50):
    """Generates a Parquet file containing static fleet master data."""
    data = []
    vehicle_types = ["Van", "Light Truck", "Heavy Duty"]
    
    for _ in range(num_vehicles):
        data.append({
            "vehicle_id": f"V-{random.randint(1000, 9999)}",
            "driver_name": fake.name(),
            "vehicle_type": random.choice(vehicle_types),
            "capacity_kg": random.choice([1500, 3000, 8000])
        })
    
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=['vehicle_id'])
    
    file_path = os.path.join(OUTPUT_DIR, "vehicles.parquet")
    df.to_parquet(file_path, index=False)
    print(f"✅ Fleet file generated: {file_path} ({len(df)} records)")

def generate_inventory_json(num_orders=100):
    """Generates a JSON file simulating warehouse dispatch orders."""
    data = []
    
    for _ in range(num_orders):
        data.append({
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "product_name": fake.word().capitalize(),
            "weight_kg": round(random.uniform(5.0, 500.0), 2),
            "warehouse_location": f"Aisle {random.randint(1, 15)}-Shelf {random.choice(['A','B','C','D'])}",
            "status": "READY_FOR_DISPATCH",
            "export_date": datetime.now().strftime("%Y-%m-%d")
        })
    
    file_path = os.path.join(OUTPUT_DIR, f"inventory_{datetime.now().strftime('%Y%m%d')}.json")
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
        
    print(f"✅ Inventory file generated: {file_path} ({len(data)} records)")

if __name__ == "__main__":
    print("Starting static and transactional data generation (Mock)...")
    generate_vehicles_parquet()
    generate_inventory_json()