import random
from faker import Faker

fake = Faker('en_US')

class MasterDataRegistry:
    def __init__(self, num_vehicles=50, num_drivers=30):
        self.vehicles = []
        self.drivers = []
        self._generate_vehicles(num_vehicles)
        self._generate_drivers(num_drivers)

    def _generate_vehicles(self, num_vehicles):
        vehicle_types = ["Van", "Light Truck", "Heavy Truck", "Semi-Trailer"]
        for i in range(num_vehicles):
            self.vehicles.append({
                "vehicle_id": f"V-{random.randint(1000, 9999)}",
                "vehicle_type": random.choice(vehicle_types),
                "capacity_kg": random.choice([1500, 3000, 8000, 15000]),
                "status": "ACTIVE"
            })

    def _generate_drivers(self, num_drivers):
        license_types = ["Class A", "Class B", "Class C"]
        for i in range(num_drivers):
            self.drivers.append({
                "driver_id": f"D-{random.randint(100, 999)}",
                "name": fake.name(),
                "license_type": random.choice(license_types),
                "status": "AVAILABLE"
            })

    def get_all_vehicles(self):
        return self.vehicles

    def get_all_drivers(self):
        return self.drivers

    def get_random_vehicle(self):
        return random.choice(self.vehicles)

    def get_random_driver(self):
        return random.choice(self.drivers)

# Singleton instance to ensure data consistency across the API
registry = MasterDataRegistry()