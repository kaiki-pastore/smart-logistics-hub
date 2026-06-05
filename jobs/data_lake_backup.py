import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

JARS = "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {JARS} pyspark-shell'
os.environ["_JAVA_OPTIONS"] = "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -Xmx2g"

def create_spark_session():
    """Initializes Spark with S3/MinIO and Postgres JDBC configurations."""
    return SparkSession.builder \
        .appName("DataLakeBackupJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "admin")) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .getOrCreate()

def backup_table_to_parquet(spark, table_name, bucket, target_folder, yesterday_str):
    """Reads from Postgres and writes as partitioned Parquet in MinIO."""
    print(f"📦 Starting backup for table '{table_name}' to bucket '{bucket}'...")
    
    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    properties = {
        "user": os.getenv("POSTGRES_USER", "admin"),
        "password": os.getenv("POSTGRES_PASSWORD", "admin"),
        "driver": "org.postgresql.Driver"
    }
    
    try:
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
        
        if "ingestion_date" in df.columns:
            df_filtered = df.filter(col("ingestion_date") == yesterday_str)
        else:
            df_filtered = df.withColumn("snapshot_date", lit(yesterday_str))
        
        if df_filtered.isEmpty():
            print(f"⚠️ No records found for date {yesterday_str} in table {table_name}. Skipping export.")
            return
            
        target_path = f"s3a://{bucket}/{target_folder}/"
        
        partition_col = "ingestion_date" if "ingestion_date" in df.columns else "snapshot_date"
        
        df_filtered.write \
            .mode("append") \
            .partitionBy(partition_col) \
            .parquet(target_path)
            
        print(f"✅ Successfully exported {table_name} to {target_path}")
        
    except Exception as e:
        print(f"❌ Failed to backup {table_name}: {e}")

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    print(f"🗓️ Target Backup Date (D-1): {yesterday_str}")
    
    try:
        backup_table_to_parquet(spark, "fact_orders", "silver", "orders_history", yesterday_str)
        backup_table_to_parquet(spark, "fact_telemetry", "silver", "telemetry_history", yesterday_str)
        
        backup_table_to_parquet(spark, "dim_vehicles", "gold", "dim_vehicles_snapshots", yesterday_str)
        backup_table_to_parquet(spark, "dim_drivers", "gold", "dim_drivers_snapshots", yesterday_str)
        
    finally:
        spark.stop()
        print("🚀 Data Lake Backup Job Finished.")