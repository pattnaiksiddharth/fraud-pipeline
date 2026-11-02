from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("gold-scoring")

SILVER = "/data/silver/transactions"
GOLD = "/data/gold/scored_transactions"
CHECKPOINT = "/data/checkpoints/gold"

# ---------- Thresholds (Airflow will refresh these from history in Step 7) ----------
import json
import os

DEFAULTS = {
    "BURST_COUNT": 4, "TRAVEL_MIN_KM": 100, "TRAVEL_MAX_SPEED": 900,
    "AMOUNT_MULTIPLIER": 5, "MIN_PRIOR_TXNS": 3,
}


def load_thresholds(path="/data/config/thresholds.json"):
    t = dict(DEFAULTS)
    if os.path.exists(path):
        with open(path) as f:
            t.update(json.load(f)["thresholds"])
    return t


T = load_thresholds()
print("Gold thresholds in use:", T)
BURST_COUNT = T["BURST_COUNT"]
TRAVEL_MIN_KM = T["TRAVEL_MIN_KM"]
TRAVEL_MAX_SPEED = T["TRAVEL_MAX_SPEED"]
AMOUNT_MULTIPLIER = T["AMOUNT_MULTIPLIER"]
MIN_PRIOR_TXNS = T["MIN_PRIOR_TXNS"]

silver = spark.readStream.format("delta").load(SILVER)

burst = F.coalesce(F.col("txn_count_10s") >= BURST_COUNT, F.lit(False))
travel = F.coalesce((F.col("distance_km") > TRAVEL_MIN_KM)
                    & (F.col("speed_kmh") > TRAVEL_MAX_SPEED), F.lit(False))
large = F.coalesce((F.col("prior_txn_count") >= MIN_PRIOR_TXNS)
                   & (F.col("amount") > AMOUNT_MULTIPLIER * F.col("avg_amount_prior")),
                   F.lit(False))

rules = F.array(
    F.when(burst, F.lit("BURST")),
    F.when(travel, F.lit("IMPOSSIBLE_TRAVEL")),
    F.when(large, F.lit("LARGE_AMOUNT")),
)

scored = (
    silver
    .withColumn("triggered_rules", F.filter(rules, lambda x: x.isNotNull()))
    .withColumn("risk_score", F.least(F.lit(100),
                F.when(burst, 40).otherwise(0)
                + F.when(travel, 50).otherwise(0)
                + F.when(large, 40).otherwise(0)))
    .withColumn("is_flagged", F.size("triggered_rules") > 0)
    .withColumn("scored_at", F.current_timestamp())
)

query = (
    scored.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start(GOLD)
)
query.awaitTermination()