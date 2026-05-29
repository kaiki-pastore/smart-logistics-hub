import os
import shutil
import glob
import sys
from sqlalchemy import create_engine

try:
    for spark_folder in glob.glob("/tmp/spark-*"):
        shutil.rmtree(spark_folder, ignore_errors=True)
except Exception:
    pass

import urllib.request
from pyspark.sql import SparkSession

JARS_DIR = "/opt/airflow/jars"
os.makedirs(JARS_DIR, exist_ok=True)

HADOOP_AWS_URL = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
AWS_SDK_URL = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

HADOOP_AWS_JAR = os.path.join(JARS_DIR, "hadoop-aws-3.3.4.jar")
AWS_SDK_JAR = os.path.join(JARS_DIR, "aws-java-sdk-bundle-1.12.262.jar")

def download_jar(url, dest):
    if not os.path.exists(dest):
        print(f"⬇️ Downloading {os.path.basename(dest)}...")
        urllib.request.urlretrieve(url, dest)

download_jar(HADOOP_AWS_URL, HADOOP_AWS_JAR)
download_jar(AWS_SDK_URL, AWS_SDK_JAR)

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {HADOOP_AWS_JAR},{AWS_SDK_JAR} pyspark-shell'
os.environ["_JAVA_OPTIONS"] = "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -Xmx2g"

MINIO_ACCESS_KEY = os.getenv("SPARK_MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("SPARK_MINIO_PASSWORD", "admin")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

db_engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres:5432/gold_db')

spark = SparkSession.builder \
    .appName("Logistics_Silver_To_Postgres") \
    .config("spark.jars", f"{HADOOP_AWS_JAR},{AWS_SDK_JAR}") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def save_to_postgres(df, table_name):
    """Converts Spark DataFrame to Pandas and writes it to PostgreSQL."""
    pandas_df = df.toPandas()
    pandas_df.to_sql(table_name, db_engine, schema='public', if_exists='replace', index=False)
    print(f"✅ Data successfully loaded into Postgres table: {table_name}")

def load_silver_data():
    """Reads clean Silver Parquet files and loads them raw into Postgres for dbt staging."""
    print("Extracting Silver Inventory...")
    df_inventory = spark.read.parquet("s3a://silver/inventory/")
    save_to_postgres(df_inventory, "raw_inventory")

    print("Extracting Silver Telemetry...")
    df_telemetry = spark.read.parquet("s3a://silver/telemetry/")
    save_to_postgres(df_telemetry, "raw_telemetry")

if __name__ == "__main__":
    try:
        load_silver_data()
    except Exception as e:
        print(f"Critical error loading data to Postgres: {e}")
        sys.exit(1)
    finally:
        spark.stop()