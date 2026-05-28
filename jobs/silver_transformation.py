from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
import sys
import os

os.environ["_JAVA_OPTIONS"] = "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -Xmx2g"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

# 2. Fetch credentials securely
MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD")

spark = SparkSession.builder \
    .appName("Logistics_Silver_Layer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def transform_telemetry():
    """Reads telemetry JSONs from Bronze and saves them as Parquet in Silver."""
    print("Starting Telemetry processing (Bronze -> Silver)...")
    
    df = spark.read.option("multiline", "true").json("s3a://bronze/telemetry/*.json")
    
    if df.isEmpty():
        print("No new telemetry data found in Bronze.")
        return

    df_silver = df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("latitude", col("latitude").cast("float")) \
        .withColumn("longitude", col("longitude").cast("float")) \
        .withColumn("cargo_temperature", col("cargo_temperature").cast("float")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["vehicle_id", "timestamp"]) \
        .dropDuplicates()

    df_silver.write.mode("append").parquet("s3a://silver/telemetry/")
    print(f"✅ Telemetry processed and saved to s3a://silver/telemetry/")

def transform_inventory():
    """Reads inventory JSONs from Bronze and saves them as Parquet in Silver."""
    print("Starting Inventory processing (Bronze -> Silver)...")
    
    df = spark.read.option("multiline", "true").json("s3a://bronze/inventory/*.json")
    
    if df.isEmpty():
        return

    df_silver = df \
        .withColumn("weight_kg", col("weight_kg").cast("float")) \
        .withColumn("export_date", to_timestamp(col("export_date"), "yyyy-MM-dd")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["order_id"])

    df_silver.write.mode("overwrite").parquet("s3a://silver/inventory/")
    print(f"✅ Inventory processed and saved to s3a://silver/inventory/")

def transform_fleet():
    """Reads fleet Parquet from Bronze and saves optimized Parquet in Silver."""
    print("Starting Fleet processing (Bronze -> Silver)...")
    
    df = spark.read.parquet("s3a://bronze/static/fleet/*.parquet")
    
    if df.isEmpty():
        return

    df_silver = df \
        .withColumn("capacity_kg", col("capacity_kg").cast("integer")) \
        .withColumn("ingestion_date", current_timestamp()) \
        .dropna(subset=["vehicle_id"])

    df_silver.write.mode("overwrite").parquet("s3a://silver/fleet/")
    print(f"✅ Fleet processed and saved to s3a://silver/fleet/")

if __name__ == "__main__":
    try:
        transform_telemetry()
        transform_inventory()
        transform_fleet()
    except Exception as e:
        print(f"Critical error during Silver processing: {e}")
        sys.exit(1)
    finally:
        spark.stop()