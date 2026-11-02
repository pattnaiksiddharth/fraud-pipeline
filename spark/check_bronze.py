from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("check-bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/data/bronze/transactions")

print("Total rows:", df.count())
df.orderBy(F.col("kafka_offset").desc()).show(5, truncate=60)

print("Rows per Kafka partition:")
df.groupBy("kafka_partition").count().orderBy("kafka_partition").show()

print("Rows containing injected fraud:",
      df.filter(F.col("raw_json").contains('"is_injected_fraud": true')).count())