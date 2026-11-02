from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("gold-to-postgres")

GOLD = "/data/gold/scored_transactions"
CHECKPOINT = "/data/checkpoints/postgres_sink"

JDBC_URL = "jdbc:postgresql://postgres:5432/frauddb"
JDBC_PROPS = {"user": "fraud", "password": "fraud", "driver": "org.postgresql.Driver"}

# Must match the column order of flagged_transactions
FLAGGED_COLS = [
    "transaction_id", "card_id", "amount", "currency", "merchant",
    "merchant_category", "city", "latitude", "longitude", "device_id",
    "event_time", "txn_count_5m", "txn_count_10s", "distance_km", "speed_kmh",
    "avg_amount_prior", "amount_ratio", "is_new_device", "triggered_rules",
    "risk_score", "scored_at", "label_is_fraud", "label_fraud_type",
]

gold = (
    spark.readStream.format("delta")
    .option("maxFilesPerTrigger", 20)
    .load(GOLD)
)


def write_batch(batch_df, batch_id):
    batch_df = batch_df.persist()

    first_minute = batch_df.agg(
        F.min(F.date_trunc("minute", "event_time"))).first()[0]
    if first_minute is None:
        batch_df.unpersist()
        return

    # 1. Flagged transactions -> ingest_flagged (trigger upserts + creates alerts)
    flagged = (
        batch_df.filter("is_flagged")
        .withColumn("triggered_rules", F.array_join("triggered_rules", ","))
        .select(*FLAGGED_COLS)
    )
    flagged.write.jdbc(JDBC_URL, "ingest_flagged", mode="append",
                       properties=JDBC_PROPS)

    # 2. Per-minute stats. Recompute FULL totals for every minute touched,
    #    straight from gold, so a replayed batch writes identical numbers.
    stats = (
        spark.read.format("delta").load(GOLD)
        .filter(F.col("event_time") >= F.lit(first_minute))
        .groupBy(F.date_trunc("minute", "event_time").alias("minute"))
        .agg(F.count("*").alias("txns"),
             F.sum(F.col("is_flagged").cast("int")).alias("flagged_txns"),
             F.sum("amount").alias("total_amount"),
             F.max("risk_score").alias("max_risk_score"))
    )
    stats.write.jdbc(JDBC_URL, "ingest_stats", mode="append",
                     properties=JDBC_PROPS)

    batch_df.unpersist()


query = (
    gold.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()