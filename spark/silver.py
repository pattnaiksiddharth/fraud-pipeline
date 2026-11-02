from datetime import timedelta

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, StringType,
                               StructField, StructType)

from common import get_spark

spark = get_spark("silver-transactions")

BRONZE = "/data/bronze/transactions"
SILVER = "/data/silver/transactions"
QUARANTINE = "/data/silver/quarantine"
CHECKPOINT = "/data/checkpoints/silver"
HISTORY_LOOKBACK = timedelta(days=1)

# ---------- Schema of the JSON payload ----------
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant", StringType()),
    StructField("merchant_category", StringType()),
    StructField("city", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("device_id", StringType()),
    StructField("event_time", StringType()),
    StructField("is_injected_fraud", BooleanType()),
    StructField("fraud_type", StringType()),
])

# Columns stored in silver before feature columns are added
BASE_COLS = [
    "transaction_id", "card_id", "amount", "currency", "merchant",
    "merchant_category", "city", "latitude", "longitude", "device_id",
    "event_time", "label_is_fraud", "label_fraud_type",
    "kafka_partition", "kafka_offset", "ingested_at",
]


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two points (Spark Column expressions)."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlmb = F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dlmb / 2) ** 2
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(F.least(a, F.lit(1.0))))


# ---------- 1. Stream bronze, parse JSON, flag validity ----------
bronze = (
    spark.readStream.format("delta")
    .option("maxFilesPerTrigger", 20)      # keeps the first catch-up batch from being huge
    .load(BRONZE)
)

parsed = (
    bronze.withColumn("j", F.from_json("raw_json", schema))
    .select("j.*", "raw_json", "kafka_partition", "kafka_offset", "ingested_at")
    .withColumn("event_time", F.col("event_time").cast("timestamp"))
    # Ground-truth labels are renamed so nobody mistakes them for features
    .withColumnRenamed("is_injected_fraud", "label_is_fraud")
    .withColumnRenamed("fraud_type", "label_fraud_type")
    .withColumn(
        "is_valid",
        F.coalesce(
            F.col("transaction_id").isNotNull()
            & F.col("card_id").isNotNull()
            & (F.col("amount") > 0)
            & F.col("event_time").isNotNull()
            & F.col("latitude").between(-90, 90)
            & F.col("longitude").between(-180, 180),
            F.lit(False),
        ),
    )
)


# ---------- 2. Process each micro-batch ----------
def process_batch(batch_df, batch_id):
    batch_df = batch_df.persist()

    # 2a. Quarantine invalid records (keep the raw payload for debugging)
    invalid = batch_df.filter(~F.col("is_valid"))
    if invalid.limit(1).count() > 0:
        (invalid.select("raw_json", "kafka_partition", "kafka_offset", "ingested_at")
         .withColumn("quarantined_at", F.current_timestamp())
         .write.format("delta").mode("append").save(QUARANTINE))

    # 2b. Dedupe inside the batch
    new = (batch_df.filter(F.col("is_valid"))
           .select(*BASE_COLS)
           .dropDuplicates(["transaction_id"]))

    min_t = new.agg(F.min("event_time")).first()[0]
    if min_t is None:                      # batch had no valid rows
        batch_df.unpersist()
        return

    # 2c. Load recent history and drop rows already written (replay safety)
    if DeltaTable.isDeltaTable(spark, SILVER):
        hist = (spark.read.format("delta").load(SILVER)
                .filter(F.col("event_time") >= F.lit(min_t - HISTORY_LOOKBACK))
                .select(*BASE_COLS))
        new = new.join(hist.select("transaction_id"), "transaction_id", "left_anti")
        combined = (hist.withColumn("is_new", F.lit(False))
                    .unionByName(new.withColumn("is_new", F.lit(True))))
    else:
        combined = new.withColumn("is_new", F.lit(True))

    # 2d. Window features over history + new rows
    combined = combined.withColumn("event_epoch", F.col("event_time").cast("double"))

    w_range = (Window.partitionBy("card_id").orderBy("event_epoch")
               .rangeBetween(-300, 0))                           # last 5 minutes incl. current row
    w_range_10s = (Window.partitionBy("card_id").orderBy("event_epoch")
               .rangeBetween(-10, 0))                       
    w_rows = Window.partitionBy("card_id").orderBy("event_epoch", "transaction_id")
    w_prior = w_rows.rowsBetween(Window.unboundedPreceding, -1)   # earlier rows only
    w_dev = (Window.partitionBy("card_id", "device_id")
             .orderBy("event_epoch", "transaction_id")
             .rowsBetween(Window.unboundedPreceding, -1))

    feats = (
        combined
        .withColumn("txn_count_5m", F.count("*").over(w_range))
        .withColumn("txn_count_10s", F.count("*").over(w_range_10s))
        .withColumn("spend_5m", F.sum("amount").over(w_range))
        .withColumn("prev_lat", F.lag("latitude").over(w_rows))
        .withColumn("prev_lon", F.lag("longitude").over(w_rows))
        .withColumn("prev_epoch", F.lag("event_epoch").over(w_rows))
        .withColumn("avg_amount_prior", F.avg("amount").over(w_prior))
        .withColumn("prior_txn_count", F.count("*").over(w_prior))
        .withColumn("device_prior_txns", F.count("*").over(w_dev))
        .withColumn("distance_km",
                    F.when(F.col("prev_lat").isNotNull(),
                           haversine_km(F.col("prev_lat"), F.col("prev_lon"),
                                        F.col("latitude"), F.col("longitude"))))
        .withColumn("secs_since_prev", F.col("event_epoch") - F.col("prev_epoch"))
        .withColumn("speed_kmh",
                    F.when(F.col("distance_km").isNotNull(),
                           F.col("distance_km")
                           / (F.greatest(F.col("secs_since_prev"), F.lit(1.0)) / 3600.0)))
        .withColumn("amount_ratio", F.col("amount") / F.col("avg_amount_prior"))
        .withColumn("is_new_device",
                    (F.col("device_prior_txns") == 0) & (F.col("prior_txn_count") > 0))
    )

    # 2e. Keep only the new rows and write them to silver
    out = (feats.filter("is_new")
           .drop("is_new", "event_epoch", "prev_lat", "prev_lon",
                 "prev_epoch", "device_prior_txns")
           .withColumn("processed_at", F.current_timestamp()))

    out.write.format("delta").mode("append").save(SILVER)
    batch_df.unpersist()


query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()