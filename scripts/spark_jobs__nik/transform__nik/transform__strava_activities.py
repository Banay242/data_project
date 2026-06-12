import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from datetime import datetime, timedelta

parser = argparse.ArgumentParser()
parser.add_argument("--ch_user", required=True)
parser.add_argument("--ch_password", required=True)
parser.add_argument("--ch_table", required=True)

args = parser.parse_args()

spark = SparkSession.builder \
    .appName("nbainin") \
    .getOrCreate()

try:
    bronze_path = "s3a://dev/raw/strava_batch/*/*/*/data.json"
    df_bronze = spark.read.json(bronze_path)

    df_silver = df_bronze.select(
        col("id").cast("long").alias("activity_id"),
        col("name").alias("activity_name"),
        col("sport_type").alias("activity_type"),
        to_timestamp(col("start_date_local")).alias("start_datetime_local"),
        col("moving_time").cast("long").alias("moving_time_local_sec"),
        col("elapsed_time").cast("long").alias("elapsed_time_local_sec"),
        col("distance").cast("double").alias("distance_meters"),
        col("total_elevation_gain").cast("double").alias("elevation_gain_meters"),
        col("average_speed").cast("double").alias("average_speed_mps"),
        col("max_speed").cast("double").alias("max_speed_mps"),
        col("start_latlng")[0].cast("double").alias("start_lat"),
        col("start_latlng")[1].cast("double").alias("start_lon"),
        col("end_latlng")[0].cast("double").alias("end_lat"),
        col("end_latlng")[1].cast("double").alias("end_lon"),
        col("map.summary_polyline").alias("summary_polyline"),
        col("has_heartrate").cast("boolean"),
        col("device_name"),
        col("kudos_count").cast("long")
    )
    df_silver = df_silver.fillna({"device_name": "Unknown"}).coalesce(1)

    silver_path = "s3a://dev/silver/strava_activities"
    df_silver \
        .write \
        .mode("overwrite") \
        .partitionBy("activity_type") \
        .parquet(silver_path)

    logging.info(f"Successfully created {silver_path} into minio")

    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
    ch_user = args.ch_user
    ch_password = args.ch_password
    ch_table = args.ch_table

    df_silver.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", clickhouse_url) \
        .option("dbtable", ch_table) \
        .option("user", ch_user) \
        .option("password", ch_password) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("batchsize", "10000") \
        .option("isolationLevel", "NONE") \
        .save()

    logging.info("Successfully loaded data to CH")
except Exception as e:
    logging.error(f"Error: {e}")
    sys.exit(1)
finally:
    spark.stop()