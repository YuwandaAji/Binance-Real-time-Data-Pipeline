import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, avg, max as _max, min as _min
from pyspark.sql.types import *

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("BinancePipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session started!")

# === SCHEMA ===

trades_schema = StructType([
    StructField("symbol",    StringType()),
    StructField("price",     DoubleType()),
    StructField("quantity",  DoubleType()),
    StructField("side",      StringType()),
    StructField("timestamp", LongType()),
    StructField("trade_id",  LongType())
])

ticker_schema = StructType([
    StructField("symbol",      StringType()),
    StructField("price",       DoubleType()),
    StructField("change_pct",  DoubleType()),
    StructField("high_24h",    DoubleType()),
    StructField("low_24h",     DoubleType()),
    StructField("volume_24h",  DoubleType()),
    StructField("timestamp",   LongType())
])

# === BACA DARI KAFKA ===

def read_kafka(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

trades_df = read_kafka("trades", trades_schema)
ticker_df = read_kafka("ticker", ticker_schema)

# === TRANSFORMASI TRADES ===
# Agregasi: volume & harga rata-rata per coin per 1 menit

from pyspark.sql.functions import from_unixtime, to_timestamp

trades_agg = trades_df \
    .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "1 minute"), "symbol", "side") \
    .agg(
        _sum("quantity").alias("total_volume"),
        avg("price").alias("avg_price"),
        _max("price").alias("max_price"),
        _min("price").alias("min_price")
    )

# === TRANSFORMASI TICKER ===
# Simpan ticker terbaru per coin

ticker_latest = ticker_df \
    .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) \
    .withWatermark("event_time", "10 minutes")

# === TULIS KE POSTGRESQL ===

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce"
JDBC_PROPS = {
    "user": "postgres",
    "password": "secret",
    "driver": "org.postgresql.Driver"
}

def write_trades(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("side"),
        col("total_volume"),
        col("avg_price"),
        col("max_price"),
        col("min_price")
    ).write \
     .jdbc(JDBC_URL, "trades_summary", mode="append", properties=JDBC_PROPS)
    print(f"[TRADES] Batch {batch_id} written")

def write_ticker(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.select(
        "symbol", "price", "change_pct",
        "high_24h", "low_24h", "volume_24h", "event_time"
    ).write \
     .jdbc(JDBC_URL, "ticker_latest", mode="append", properties=JDBC_PROPS)
    print(f"[TICKER] Batch {batch_id} written")

# === START STREAMING ===

trades_query = trades_agg.writeStream \
    .foreachBatch(write_trades) \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

ticker_query = ticker_latest.writeStream \
    .foreachBatch(write_ticker) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Spark Streaming started! Menunggu data...")
spark.streams.awaitAnyTermination()