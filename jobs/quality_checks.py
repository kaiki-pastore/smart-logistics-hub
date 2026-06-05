import great_expectations as gx
from pyspark.sql import SparkSession
import sys
import os

JARS = "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {JARS} pyspark-shell'

MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD", "admin")

spark = SparkSession.builder \
    .appName("Logistics_Data_Quality") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "512m") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def validate_telemetry_bronze():
    """Validates raw Bronze telemetry data before allowing Silver processing."""
    print("Initializing Data Quality Checks for Telemetry...")
    
    df_raw = spark.read.option("multiline", "true").json("s3a://bronze/telemetry/*.json")
    
    if df_raw.isEmpty():
        print("No telemetry data to validate.")
        return
    
    from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
    gx_df = SparkDFDataset(df_raw)
    
    print("Running Expectation 1: Latitude and Longitude must not be null")
    res_lat = gx_df.expect_column_values_to_not_be_null("latitude")
    res_lon = gx_df.expect_column_values_to_not_be_null("longitude")
    
    print("Running Expectation 2: Cargo temperature must be physically possible (-50 to 100)")
    res_temp = gx_df.expect_column_values_to_be_between("cargo_temperature", -50, 100)

    if not res_lat["success"] or not res_lon["success"] or not res_temp["success"]:
        print("❌ CRITICAL: Bad data detected in Bronze layer.")
        raise ValueError("Data Quality Gates failed! Pipeline execution aborted to protect the Data Lake.")
        
    print("✅ All Data Quality Checks Passed. Ready for Silver Transformation.")

if __name__ == "__main__":
    try:
        validate_telemetry_bronze()
    except Exception as e:
        print(f"Quality Check Failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()