import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

JARS = "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {JARS} pyspark-shell'
os.environ["_JAVA_OPTIONS"] = "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -Xmx2g"

# --- Credentials ---
MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD", "admin")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

JDBC_URL = "jdbc:postgresql://postgres:5432/postgres"

spark = SparkSession.builder \
    .appName("Logistics_Bronze_to_Postgres") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def write_to_postgres(df, table_name, mode):
    """Helper function to write DataFrames to PostgreSQL."""
    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()


def process_vehicles():
    print("Processing Vehicles (Master Data)...")
    df = spark.read.option("multiline", "true").json("s3a://bronze/master/vehicles/*.json")
    if df.isEmpty(): return

    df_clean = df \
        .withColumn("capacity_kg", col("capacity_kg").cast("integer")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropDuplicates(["vehicle_id"])

    write_to_postgres(df_clean, "raw_vehicles", "overwrite")
    print("✅ Vehicles data loaded into PostgreSQL.")

def process_drivers():
    print("Processing Drivers (Master Data)...")
    df = spark.read.option("multiline", "true").json("s3a://bronze/master/drivers/*.json")
    if df.isEmpty(): return

    df_clean = df \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropDuplicates(["driver_id"])

    write_to_postgres(df_clean, "raw_drivers", "overwrite")
    print("✅ Drivers data loaded into PostgreSQL.")

def process_telemetry():
    print("Processing Telemetry (Stream Data)...")
    df = spark.read.option("multiline", "true").json("s3a://bronze/stream/telemetry/*.json")
    if df.isEmpty(): return

    df_clean = df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("latitude", col("latitude").cast("float")) \
        .withColumn("longitude", col("longitude").cast("float")) \
        .withColumn("cargo_temp_c", col("cargo_temp_c").cast("float")) \
        .withColumn("speed_kmh", col("speed_kmh").cast("float")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["event_id", "vehicle_id"]) \
        .dropDuplicates(["event_id"])

    write_to_postgres(df_clean, "raw_telemetry", "append")
    print("✅ Telemetry stream loaded into PostgreSQL.")

def process_orders():
    print("Processing Orders (Stream Data)...")
    df = spark.read.option("multiline", "true").json("s3a://bronze/stream/orders/*.json")
    if df.isEmpty(): return

    df_clean = df \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .withColumn("weight_kg", col("weight_kg").cast("float")) \
        .withColumn("destination_lat", col("destination_lat").cast("float")) \
        .withColumn("destination_lon", col("destination_lon").cast("float")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["order_id"]) \
        .dropDuplicates(["order_id"])

    write_to_postgres(df_clean, "raw_orders", "append")
    print("✅ Orders stream loaded into PostgreSQL.")

if __name__ == "__main__":
    try:
        process_vehicles()
        process_drivers()
        process_telemetry()
        process_orders()
        print("🚀 Silver processing completed successfully!")
    except Exception as e:
        print(f"Critical error during Spark job: {e}")
        sys.exit(1)
    finally:
        spark.stop()