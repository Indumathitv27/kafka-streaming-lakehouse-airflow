from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "orders_raw"

BRONZE_PATH = "data/bronze/orders"
CHECKPOINT_PATH = "data/checkpoints/orders"

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("total_amount", DoubleType(), True),
])

spark = (
    SparkSession.builder
    .appName("KafkaToBronzeOrders")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")

parsed_df = (
    json_df
    .withColumn("data", from_json(col("json_str"), schema))
    .select("data.*")
)

query = (
    parsed_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("Streaming started. Writing Bronze data to:", BRONZE_PATH)
query.awaitTermination()

