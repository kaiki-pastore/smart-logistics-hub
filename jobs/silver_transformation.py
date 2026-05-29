import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

# --- Configuração de Dependências Nativas ---
JARS = "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {JARS} pyspark-shell'
os.environ["_JAVA_OPTIONS"] = "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -Xmx2g"

MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD", "admin")

spark = SparkSession.builder \
    .appName("Logistics_Silver_Layer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def transform_telemetry():
    print("Starting Telemetry processing (Bronze -> Silver)...")
    df = spark.read.option("multiline", "true").json("s3a://bronze/telemetry/*.json")
    if df.isEmpty(): return
    
    df_silver = df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("latitude", col("latitude").cast("float")) \
        .withColumn("longitude", col("longitude").cast("float")) \
        .withColumn("cargo_temperature", col("cargo_temperature").cast("float")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["vehicle_id", "timestamp"]) \
        .dropDuplicates()

    df_silver.write.mode("append").parquet("s3a://silver/telemetry/")
    print("✅ Telemetry processed and saved.")

def transform_inventory():
    print("Starting Inventory processing (Bronze -> Silver)...")
    df = spark.read.option("multiline", "true").json("s3a://bronze/inventory/*.json")
    if df.isEmpty(): return

    df_silver = df \
        .withColumn("weight_kg", col("weight_kg").cast("float")) \
        .withColumn("export_date", to_timestamp(col("export_date"), "yyyy-MM-dd")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["order_id"])

    df_silver.write.mode("overwrite").parquet("s3a://silver/inventory/")
    print("✅ Inventory processed and saved.")

def transform_fleet():
    print("Starting Fleet processing (Bronze -> Silver)...")
    df = spark.read.parquet("s3a://bronze/static/fleet/*.parquet")
    if df.isEmpty(): return

    df_silver = df \
        .withColumn("capacity_kg", col("capacity_kg").cast("integer")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["vehicle_id"])

    df_silver.write.mode("overwrite").parquet("s3a://silver/fleet/")
    print("✅ Fleet processed and saved.")

if __name__ == "__main__":
    try:
        transform_telemetry()
        transform_inventory()
        transform_fleet()
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
    finally:
        spark.stop()