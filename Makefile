.PHONY: setup up down clean

# Grants permissions to the folders where Docker/Airflow needs to write
setup:
	@echo "🔧 Configuring volume permissions for Airflow/dbt..."
	mkdir -p dbt_project/logs dbt_project/target
	chmod -R 777 dbt_project/logs dbt_project/target

# Runs the setup and spins up the containers
up: setup
	@echo "🚀 Spinning up the Medallion architecture..."
	docker compose up -d

# Tears down the containers
down:
	@echo "🛑 Stopping the architecture..."
	docker compose down

# Deletes data volumes for a hard reset
clean: down
	@echo "🧹 Cleaning up data volumes..."
	docker compose down -v