"""PySpark job: Zone-level aggregations for analytics."""

import sys
from pyspark.sql import SparkSession, functions as F

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "gs://taxi-processed/yellow_trips/"
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else "gs://taxi-processed/zone_aggregates/"

spark = SparkSession.builder.appName("NYC Taxi Zone Aggregates").getOrCreate()

df = spark.read.parquet(INPUT_PATH)

zone_agg = (
    df.groupBy("PULocationID", "year", "month")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("pickup_date").alias("active_days"),
    )
    .withColumnRenamed("PULocationID", "pickup_zone_id")
)

zone_agg.write.mode("overwrite").partitionBy("year", "month").parquet(OUTPUT_PATH)
print(f"Zone aggregates written to {OUTPUT_PATH}")
spark.stop()
