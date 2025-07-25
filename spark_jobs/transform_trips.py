"""PySpark job: Clean, transform, and partition NYC taxi trip data."""

import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# Configuration
RAW_PATH = sys.argv[1] if len(sys.argv) > 1 else "gs://taxi-raw/yellow/*.parquet"
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else "gs://taxi-processed/yellow_trips/"

spark = SparkSession.builder.appName("NYC Taxi Transform").getOrCreate()
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

print(f"Reading from: {RAW_PATH}")
df = spark.read.parquet(RAW_PATH)
print(f"Raw records: {df.count()}")

# === SCHEMA ENFORCEMENT ===
df = (
    df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
    .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
    .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
    .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
)

# === FILTERING (remove bad records) ===
df = df.filter(
    (F.col("trip_distance") > 0)
    & (F.col("trip_distance") < 200)
    & (F.col("fare_amount") > 0)
    & (F.col("fare_amount") < 1000)
    & (F.col("passenger_count") > 0)
    & (F.col("passenger_count") <= 9)
    & (F.col("tpep_pickup_datetime").isNotNull())
    & (F.col("tpep_dropoff_datetime").isNotNull())
    & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
)

# === DERIVED COLUMNS ===
df = (
    df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
    )
    .withColumn(
        "avg_speed_mph",
        F.when(
            F.col("trip_duration_min") > 0,
            F.col("trip_distance") / (F.col("trip_duration_min") / 60),
        ).otherwise(0),
    )
    .withColumn(
        "fare_per_mile",
        F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance")).otherwise(0),
    )
    .withColumn(
        "tip_percentage",
        F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount") * 100).otherwise(0),
    )
    .withColumn(
        "time_of_day",
        F.when(F.hour("tpep_pickup_datetime").between(6, 9), "morning_rush")
        .when(F.hour("tpep_pickup_datetime").between(10, 15), "midday")
        .when(F.hour("tpep_pickup_datetime").between(16, 19), "evening_rush")
        .when(F.hour("tpep_pickup_datetime").between(20, 23), "evening")
        .otherwise("late_night"),
    )
    .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
)

# Filter unreasonable derived values
df = df.filter(
    (F.col("trip_duration_min") > 0.5)
    & (F.col("trip_duration_min") < 360)
    & (F.col("avg_speed_mph") < 100)
)

# === PARTITIONING COLUMNS ===
df = (
    df.withColumn("year", F.year("tpep_pickup_datetime"))
    .withColumn("month", F.month("tpep_pickup_datetime"))
)

print(f"Cleaned records: {df.count()}")

# === WRITE OPTIMIZED PARQUET ===
(
    df.repartition("year", "month", "PULocationID")
    .write.mode("overwrite")
    .partitionBy("year", "month")
    .option("compression", "snappy")
    .parquet(OUTPUT_PATH)
)

print(f"Written to {OUTPUT_PATH}")
spark.stop()
