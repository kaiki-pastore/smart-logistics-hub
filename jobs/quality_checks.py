import great_expectations as gx
from pyspark.sql import SparkSession
import sys
import os

os.environ["_JAVA_OPTIONS"] = "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -Xmx2g"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD", "admin")

spark = SparkSession.builder \
    .appName("Logistics_Data_Quality") \
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

    context = gx.get_context(mode="ephemeral")
    
    from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
    gx_df = SparkDFDataset(df_raw)

    # 3.