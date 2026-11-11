import os
import time
from datetime import timedelta

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, StringType,
                               StructField, StructType)

from common import get_spark

TRIGGER = f"{os.getenv('TRIGGER_SECONDS', '5')} seconds"
spark = get_spark("fraud-pipeline")

BRONZE = "/data/bronze/transactions"
SILVER = "/data/silver/transactions"
QUARANTINE = "/data/silver/quarantine"
GOLD = "/data/gold/scored_transactions"
CP = "/data/checkpoints"
HISTORY_LOOKBACK = timedelta(days=1)

JDBC_URL = "jdbc:postgresql://postgres:5432/frauddb"
JDBC_PROPS = {"user": "fraud", "password": "fraud", "driver": "org.postgresql.Driver"}


def wait_for(path, name):
    """Block until a Delta table exists and has a schema (it appears after the first write)."""
    while True:
        try:
            if DeltaTable.isDeltaTable(spark, path) and \
                    len(spark.read.format("delta").load(path).schema) > 0:
                print(f"[ready] {name}")
                return
        except Exception:
            pass
        print(f"[waiting] {name} has no data yet (is the generator running?)")
        time.sleep(5)


# ====================== BRONZE ======================
def start_bronze():
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    bronze = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )
    return (bronze.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", f"{CP}/bronze")
            .trigger(processingTime=TRIGGER).queryName("bronze").start(BRONZE))


# ====================== SILVER ======================
SCHEMA = StructType([
    StructField("transaction_id", StringType()), StructField("card_id", StringType()),
    StructField("amount", DoubleType()), StructField("currency", StringType()),
    StructField("merchant", StringType()), StructField("merchant_category", StringType()),
    StructField("city", StringType()), StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()), StructField("device_id", StringType()),
    StructField("event_time", StringType()), StructField("is_injected_fraud", BooleanType()),
    StructField("fraud_type", StringType()),
])

BASE_COLS = [
    "transaction_id", "card_id", "amount", "currency", "merchant",
    "merchant_category", "city", "latitude", "longitude", "device_id",
    "event_time", "label_is_fraud", "label_fraud_type",
    "kafka_partition", "kafka_offset", "ingested_at",
]


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi, dlmb = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dlmb / 2) ** 2
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(F.least(a, F.lit(1.0))))


def process_silver(batch_df, batch_id):
    batch_df = batch_df.persist()

    invalid = batch_df.filter(~F.col("is_valid"))
    if invalid.limit(1).count() > 0:
        (invalid.select("raw_json", "kafka_partition", "kafka_offset", "ingested_at")
         .withColumn("quarantined_at", F.current_timestamp())
         .write.format("delta").mode("append").save(QUARANTINE))

    new = (batch_df.filter(F.col("is_valid")).select(*BASE_COLS)
           .dropDuplicates(["transaction_id"]))
    min_t = new.agg(F.min("event_time")).first()[0]
    if min_t is None:
        batch_df.unpersist()
        return

    if DeltaTable.isDeltaTable(spark, SILVER):
        hist = (spark.read.format("delta").load(SILVER)
                .filter(F.col("event_time") >= F.lit(min_t - HISTORY_LOOKBACK))
                .select(*BASE_COLS))
        new = new.join(hist.select("transaction_id"), "transaction_id", "left_anti")
        combined = (hist.withColumn("is_new", F.lit(False))
                    .unionByName(new.withColumn("is_new", F.lit(True))))
    else:
        combined = new.withColumn("is_new", F.lit(True))

    combined = combined.withColumn("event_epoch", F.col("event_time").cast("double"))
    w_5m = Window.partitionBy("card_id").orderBy("event_epoch").rangeBetween(-300, 0)
    w_10s = Window.partitionBy("card_id").orderBy("event_epoch").rangeBetween(-10, 0)
    w_rows = Window.partitionBy("card_id").orderBy("event_epoch", "transaction_id")
    w_prior = w_rows.rowsBetween(Window.unboundedPreceding, -1)
    w_dev = (Window.partitionBy("card_id", "device_id")
             .orderBy("event_epoch", "transaction_id")
             .rowsBetween(Window.unboundedPreceding, -1))

    feats = (
        combined
        .withColumn("txn_count_5m", F.count("*").over(w_5m))
        .withColumn("txn_count_10s", F.count("*").over(w_10s))
        .withColumn("spend_5m", F.sum("amount").over(w_5m))
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

    out = (feats.filter("is_new")
           .drop("is_new", "event_epoch", "prev_lat", "prev_lon",
                 "prev_epoch", "device_prior_txns")
           .withColumn("processed_at", F.current_timestamp()))
    out.write.format("delta").mode("append").save(SILVER)
    batch_df.unpersist()


def start_silver():
    bronze = (spark.readStream.format("delta")
              .option("maxFilesPerTrigger", 20).load(BRONZE))
    parsed = (
        bronze.withColumn("j", F.from_json("raw_json", SCHEMA))
        .select("j.*", "raw_json", "kafka_partition", "kafka_offset", "ingested_at")
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .withColumnRenamed("is_injected_fraud", "label_is_fraud")
        .withColumnRenamed("fraud_type", "label_fraud_type")
        .withColumn("is_valid", F.coalesce(
            F.col("transaction_id").isNotNull() & F.col("card_id").isNotNull()
            & (F.col("amount") > 0) & F.col("event_time").isNotNull()
            & F.col("latitude").between(-90, 90)
            & F.col("longitude").between(-180, 180), F.lit(False)))
    )
    return (parsed.writeStream.foreachBatch(process_silver)
            .option("checkpointLocation", f"{CP}/silver")
            .trigger(processingTime=TRIGGER).queryName("silver").start())


# ====================== GOLD ======================
DEFAULTS = {"BURST_COUNT": 4, "TRAVEL_MIN_KM": 100, "TRAVEL_MAX_SPEED": 900,
            "AMOUNT_MULTIPLIER": 5, "MIN_PRIOR_TXNS": 3}


def load_thresholds(path="/data/config/thresholds.json"):
    import json
    t = dict(DEFAULTS)
    if os.path.exists(path):
        with open(path) as f:
            t.update(json.load(f)["thresholds"])
    return t


def start_gold():
    T = load_thresholds()
    print("Gold thresholds in use:", T)
    silver = spark.readStream.format("delta").load(SILVER)

    burst = F.coalesce(F.col("txn_count_10s") >= T["BURST_COUNT"], F.lit(False))
    travel = F.coalesce((F.col("distance_km") > T["TRAVEL_MIN_KM"])
                        & (F.col("speed_kmh") > T["TRAVEL_MAX_SPEED"]), F.lit(False))
    large = F.coalesce((F.col("prior_txn_count") >= T["MIN_PRIOR_TXNS"])
                       & (F.col("amount") > T["AMOUNT_MULTIPLIER"] * F.col("avg_amount_prior")),
                       F.lit(False))
    rules = F.array(F.when(burst, F.lit("BURST")),
                    F.when(travel, F.lit("IMPOSSIBLE_TRAVEL")),
                    F.when(large, F.lit("LARGE_AMOUNT")))
    scored = (
        silver
        .withColumn("triggered_rules", F.filter(rules, lambda x: x.isNotNull()))
        .withColumn("risk_score", F.least(F.lit(100),
                    F.when(burst, 40).otherwise(0) + F.when(travel, 50).otherwise(0)
                    + F.when(large, 40).otherwise(0)))
        .withColumn("is_flagged", F.size("triggered_rules") > 0)
        .withColumn("scored_at", F.current_timestamp())
    )
    return (scored.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", f"{CP}/gold")
            .trigger(processingTime=TRIGGER).queryName("gold").start(GOLD))


# ====================== POSTGRES SINK ======================
FLAGGED_COLS = [
    "transaction_id", "card_id", "amount", "currency", "merchant",
    "merchant_category", "city", "latitude", "longitude", "device_id",
    "event_time", "txn_count_5m", "txn_count_10s", "distance_km", "speed_kmh",
    "avg_amount_prior", "amount_ratio", "is_new_device", "triggered_rules",
    "risk_score", "scored_at", "label_is_fraud", "label_fraud_type",
]


def write_postgres(batch_df, batch_id):
    batch_df = batch_df.persist()
    first_minute = batch_df.agg(F.min(F.date_trunc("minute", "event_time"))).first()[0]
    if first_minute is None:
        batch_df.unpersist()
        return

    flagged = (batch_df.filter("is_flagged")
               .withColumn("triggered_rules", F.array_join("triggered_rules", ","))
               .select(*FLAGGED_COLS))
    flagged.write.jdbc(JDBC_URL, "ingest_flagged", mode="append", properties=JDBC_PROPS)

    stats = (spark.read.format("delta").load(GOLD)
             .filter(F.col("event_time") >= F.lit(first_minute))
             .groupBy(F.date_trunc("minute", "event_time").alias("minute"))
             .agg(F.count("*").alias("txns"),
                  F.sum(F.col("is_flagged").cast("int")).alias("flagged_txns"),
                  F.sum("amount").alias("total_amount"),
                  F.max("risk_score").alias("max_risk_score")))
    stats.write.jdbc(JDBC_URL, "ingest_stats", mode="append", properties=JDBC_PROPS)
    batch_df.unpersist()


def start_sink():
    gold = spark.readStream.format("delta").option("maxFilesPerTrigger", 20).load(GOLD)
    return (gold.writeStream.foreachBatch(write_postgres)
            .option("checkpointLocation", f"{CP}/postgres_sink")
            .trigger(processingTime=TRIGGER).queryName("postgres_sink").start())


# ====================== MAIN ======================
if __name__ == "__main__":
    print(f"Trigger interval: {TRIGGER}")
    start_bronze()
    wait_for(BRONZE, "bronze")
    start_silver()
    wait_for(SILVER, "silver")
    start_gold()
    wait_for(GOLD, "gold")
    start_sink()
    print("All four streams are running. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()     # raises if any stream fails