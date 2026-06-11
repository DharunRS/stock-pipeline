from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as spark_sum,
    stddev, count, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)

spark = SparkSession.builder \
    .appName("StockStreamingPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "io.delta:delta-spark_2.12:3.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SCHEMA = StructType([
    StructField("symbol",    StringType(),    True),
    StructField("price",     DoubleType(),    True),
    StructField("volume",    LongType(),      True),
    StructField("timestamp", StringType(),    True),
    StructField("bid",       DoubleType(),    True),
    StructField("ask",       DoubleType(),    True),
])

# ── Read from Kafka ──────────────────────────────────────────
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-ticks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

ticks = raw.select(
    from_json(col("value").cast("string"), SCHEMA).alias("data")
).select("data.*") \
 .withColumn("event_time", col("timestamp").cast(TimestampType()))

# ── Bronze layer: raw ticks ──────────────────────────────────
bronze_query = ticks.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/tmp/checkpoints/bronze") \
    .start("C:/tmp/delta/bronze/trades")

# ── Silver layer: 1-min VWAP with watermark ──────────────────
vwap = ticks \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window("event_time", "1 minute"),
        col("symbol")
    ).agg(
        (spark_sum(col("price") * col("volume")) / spark_sum("volume")).alias("vwap"),
        spark_sum("volume").alias("total_volume"),
        count("*").alias("tick_count"),
        avg("price").alias("avg_price"),
        stddev("price").alias("price_stddev"),
        expr("max(price) - min(price)").alias("price_range")
    )

silver_query = vwap.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/tmp/checkpoints/silver") \
    .start("C:/tmp/delta/silver/vwap_1min")

# ── Console sink for monitoring ──────────────────────────────
monitor = vwap.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()