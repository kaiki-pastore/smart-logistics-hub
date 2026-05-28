from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, date_format
import sys
import os

os.environ["_JAVA_OPTIONS"] = "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -Xmx2g"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD")

spark = SparkSession.builder \
    .appName("Logistics_Gold_Layer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def build_inventory_summary():
    """Aggregates inventory to show total weight and orders per export date."""
    print("Building Daily Inventory Summary (Silver -> Gold)...")
    
    df_inventory = spark.read.parquet("s3a://silver/inventory/")
    
    df_gold_inventory = df_inventory.groupBy("export_date") \
        .agg(
            _sum("weight_kg").alias("total_weight_kg"),
            count("order_id").alias("total_orders")
        ) \
        .orderBy("export_date")

    df_gold_inventory.write.mode("overwrite").parquet("s3a://gold/daily_inventory_summary/")
    print("✅ Inventory Summary saved to s3a://gold/daily_inventory_summary/")

def build_fleet_metrics():
    """Aggregates telemetry to show average daily temperature per vehicle."""
    print("Building Daily Fleet Metrics (Silver -> Gold)...")
    
    df_telemetry = spark.read.parquet("s3a://silver/telemetry/")
    
    df_gold_fleet = df_telemetry \
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .groupBy("vehicle_id", "date") \
        .agg(
            avg("cargo_temperature").alias("avg_temperature_celsius"),
            count("timestamp").alias("telemetry_pings")
        ) \
        .orderBy("date", "vehicle_id")

    df_gold_fleet.write.mode("overwrite").parquet("s3a://gold/daily_fleet_metrics/")
    print("✅ Fleet Metrics saved to s3a://gold/daily_fleet_metrics/")

if __name__ == "__main__":
    try:
        build_inventory_summary()
        build_fleet_metrics()
    except Exception as e:
        print(f"Critical error during Gold processing: {e}")
        sys.exit(1)
    finally:
        spark.stop()