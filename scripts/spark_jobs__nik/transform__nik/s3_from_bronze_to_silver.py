import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

spark = SparkSession.builder \
    .config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,"
    "org.apache.hadoop:hadoop-aws:3.3.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
) \
    .appName('nbainin_2') \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"]) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.credentials.provider", "true") \
    .getOrCreate()


logging.info("Succeeded connect spark app")

BUCKET_NAME = 'dev'
S3_PATH_BRONZE_APPLICATIONS = 'bronze/ods.applications/*/*.parquet'
S3_PATH_BRONZE_PRODUCTS = 'bronze/ods.products/*/*.parquet'

S3_PATH_SILVER_SUM = 'silver/applications_summary/'

df_apps = spark.read.parquet(f"s3a://{BUCKET_NAME}/{S3_PATH_BRONZE_APPLICATIONS}")
df_prods = spark.read.parquet(f"s3a://{BUCKET_NAME}/{S3_PATH_BRONZE_PRODUCTS}")

df_prods = df_prods.dropDuplicates(["bank_name"])
df_enriched = df_apps.join(df_prods.hint("BROADCAST"), on='product_id', how='inner')

SALT_PARTITIONS = 10

df_salted = df_enriched.withColumn("salt", F.floor(F.rand() * SALT_PARTITIONS))
df_stage_1 = df_salted.groupBy("product_id", "bank_name", "status", "salt") \
    .agg(
    F.count("app_id").alias("partial_count"),
    F.sum("requested_amount").alias("partial_amount")
)
df_sum = df_stage_1.groupBy("product_id", "bank_name", "status") \
    .agg(
    F.sum("partial_count").alias("total_applications"),
    F.sum("partial_amount").alias("total_amount")
)
df_sum.write.mode("overwrite").parquet(f"s3a://{BUCKET_NAME}/{S3_PATH_SILVER_SUM}")