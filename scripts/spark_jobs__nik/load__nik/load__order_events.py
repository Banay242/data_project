import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

parser = argparse.ArgumentParser()
parser.add_argument("--target_date", required=True)
parser.add_argument("--s3_path", required=True)
parser.add_argument("--kafka_topic", required=True)
parser.add_argument("--kafka_bootstrap", required=True)

args = parser.parse_args()

spark = SparkSession.builder \
    .appName("OrdersEvents") \
    .getOrCreate()

spark = SparkSession.builder \
    .appName("OrdersEvents") \
    .config("spark.ui.port", 4041) \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", args.kafka_bootstrap) \
    .option("subscribe", args.kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .limit(10)

df.printSchema()

schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("ts", LongType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("ts", LongType(), True)
    ]), True),
    StructField("source", StructType([]), True),  # если не используешь, можно пустым
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

json_df = df.selectExpr("CAST(value as string) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .where(""" data.after is not null""") \
    .select("data.after.*")

processed_df = json_df \
    .withColumn("ts_sec", (col("ts") / 1000000).cast("double")) \
    .withColumn("ts_utc", from_unixtime(col("ts_sec")).cast("timestamp")) \
    .withColumn("event_date", to_date(col("ts_utc"))) \
    .drop("ts", "ts_sec")

processed_df.writeStream \
    .format("parquet") \
    .queryName("order_events") \
    .option("path", args.s3_path) \
    .option("checkpointLocation", args.s3_path + "/_checkpoint/") \
    .partitionBy("event_date") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
