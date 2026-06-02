import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--target_date", required=True)
parser.add_argument("--jdbc-url", required=True)
parser.add_argument("--db-user", required=True)
parser.add_argument("--db-password", required=True)
parser.add_argument("--table-name", required=True)
args = parser.parse_args()

dt = args.target_date
dt_formatted = dt.replace("-", "/")

spark = SparkSession.builder \
    .appName(f"spark__earthquake_processing__{dt}") \
    .getOrCreate()

try:
    s3_path = f"s3a://dev/raw-data/earthquakes/{dt_formatted}/data.json"
    raw_df = spark.read.json(s3_path)

    clean_df = raw_df.selectExpr("explode(features) as feature") \
        .select(
            F.col("feature.id").alias("event_id"),
            F.col("feature.properties.mag").cast("float").alias("magnitude"),
            F.col("feature.properties.place").alias("place"),
            F.from_unixtime(F.col("feature.properties.time") / 1000).cast('timestamp').alias("event_time"),
            F.col("feature.properties.url").alias("url"),
            F.col("feature.geometry.coordinates").getItem(0).alias("longitude"),
            F.col("feature.geometry.coordinates").getItem(1).alias("latitude"),
    )

    clean_df.write \
        .format("jdbc") \
        .option("url", args.jdbc_url) \
        .option("dbtable", args.table_name) \
        .option("user", args.db_user) \
        .option("password", args.db_password) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

finally:
    spark.stop()