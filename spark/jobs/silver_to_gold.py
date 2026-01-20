from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, sum as sql_sum, to_date
)

SILVER_PATH = "data/silver/orders"
GOLD_CITY_HOURLY = "data/gold/revenue_by_city_hour"
GOLD_PRODUCT_DAILY = "data/gold/revenue_by_product_day"

spark = SparkSession.builder.appName("SilverToGoldOrders").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(SILVER_PATH)

# 1) Revenue by city per hour
city_hourly = (
    silver
    .groupBy(
        window(col("event_ts_ts"), "1 hour").alias("hour_window"),
        col("city")
    )
    .agg(
        sql_sum("total_amount").alias("total_revenue"),
        sql_sum("quantity").alias("total_units")
    )
)

# Flatten window columns
city_hourly_flat = (
    city_hourly
    .withColumn("hour_start", col("hour_window").start)
    .withColumn("hour_end", col("hour_window").end)
    .drop("hour_window")
)

# 2) Revenue by product per day
product_daily = (
    silver
    .withColumn("event_date", to_date(col("event_ts_ts")))
    .groupBy("event_date", "product")
    .agg(
        sql_sum("total_amount").alias("total_revenue"),
        sql_sum("quantity").alias("total_units")
    )
)

# Write Gold outputs
city_hourly_flat.write.mode("overwrite").parquet(GOLD_CITY_HOURLY)
product_daily.write.mode("overwrite").parquet(GOLD_PRODUCT_DAILY)

print("Gold written:")
print(" -", GOLD_CITY_HOURLY)
print(" -", GOLD_PRODUCT_DAILY)

spark.stop()
