from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("bronze-transactions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "3")   # small local data, 3 matches topic partitions
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Read the Kafka topic as a stream
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")   # only applies on the very first run
    .option("failOnDataLoss", "false")
    .load()
)

# 2. Keep the payload raw. Add Kafka metadata and an ingestion timestamp.
bronze = raw.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_json"),
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.current_timestamp().alias("ingested_at"),
)

# 3. Append to a Delta table, with a checkpoint for restart safety
query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/data/checkpoints/bronze")
    .trigger(processingTime="10 seconds")    # one micro-batch every 10 seconds
    .start("/data/bronze/transactions")
)

query.awaitTermination()