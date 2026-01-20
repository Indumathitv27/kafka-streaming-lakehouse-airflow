from pyspark.sql import SparkSession

BRONZE_PATH = "data/bronze/orders"

spark = SparkSession.builder.appName("PreviewBronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(BRONZE_PATH)

print("\n===== BRONZE SCHEMA =====")
df.printSchema()

print("\n===== SAMPLE ROWS =====")
df.orderBy(df.event_ts.desc()).show(10, truncate=False)

print("\n===== COUNT =====")
print(df.count())

spark.stop()
