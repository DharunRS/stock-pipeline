from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, max, min, sum as spark_sum

spark = SparkSession.builder \
    .appName("LakehouseManager") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BRONZE_PATH = "/tmp/delta/bronze/trades"
SILVER_PATH = "/tmp/delta/silver/vwap_1min"
GOLD_PATH   = "/tmp/delta/gold"

def build_gold_layer():
    silver = spark.read.format("delta").load(SILVER_PATH)

    # Gold 1: Daily VWAP summary per symbol
    daily_vwap = silver.groupBy("symbol").agg(
        avg("vwap").alias("avg_daily_vwap"),
        spark_sum("total_volume").alias("total_daily_volume"),
        max("vwap").alias("high_vwap"),
        min("vwap").alias("low_vwap"),
    )
    daily_vwap.write.format("delta") \
        .mode("overwrite").save(f"{GOLD_PATH}/daily_vwap")

    # Gold 2: Volatility metrics
    volatility = silver.groupBy("symbol").agg(
        stddev("vwap").alias("vwap_volatility"),
        avg("price_range").alias("avg_price_range"),
        avg("tick_count").alias("avg_ticks_per_min")
    )
    volatility.write.format("delta") \
        .mode("overwrite").save(f"{GOLD_PATH}/volatility")

    print("Gold layer built successfully")

def time_travel_query(path, version=0):
    """Delta Lake time travel — query any historical version"""
    return spark.read.format("delta") \
        .option("versionAsOf", version) \
        .load(path)

def show_history(path):
    dt = DeltaTable.forPath(spark, path)
    dt.history().show(truncate=False)

def optimize_table(path):
    """Compact small files for faster queries"""
    dt = DeltaTable.forPath(spark, path)
    dt.optimize().executeCompaction()
    print(f"Optimized: {path}")

if __name__ == "__main__":
    build_gold_layer()
    show_history(SILVER_PATH)
    optimize_table(f"{GOLD_PATH}/daily_vwap")