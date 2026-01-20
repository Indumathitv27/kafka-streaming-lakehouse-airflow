from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, abs as sql_abs, row_number
)
from pyspark.sql.window import Window

BRONZE_PATH = "data/bronze/orders"
SILVER_PATH = "data/silver/orders"
QUARANTINE_PATH = "data/silver/quarantine_orders"

spark = SparkSession.builder.appName("BronzeToSilverOrders").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet(BRONZE_PATH)

# 1) Parse timestamp
df = bronze.withColumn("event_ts_ts", to_timestamp(col("event_ts")))

# 2) Data quality rules
expected_total = col("quantity") * col("unit_price")

is_good = (
    col("event_id").isNotNull()
    & col("order_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("product").isNotNull()
    & col("city").isNotNull()
    & col("event_ts_ts").isNotNull()
    & (col("quantity") > 0)
    & (col("unit_price") > 0)
    & (col("total_amount") > 0)
    & (sql_abs(col("total_amount") - expected_total) < 0.01)
)

good = df.filter(is_good)
bad = df.filter(~is_good)

# 3) Deduplicate (keep latest record per event_id)
w = Window.partitionBy("event_id").orderBy(col("event_ts_ts").desc())

good_dedup = (
    good.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
)

# 4) Write outputs
good_dedup.write.mode("overwrite").parquet(SILVER_PATH)
bad.write.mode("overwrite").parquet(QUARANTINE_PATH)

print("Silver written to:", SILVER_PATH)
print("Quarantine written to:", QUARANTINE_PATH)
print("Counts -> bronze:", bronze.count(), "good:", good_dedup.count(), "bad:", bad.count())

spark.stop()
