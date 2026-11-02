from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("check-silver")
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/data/silver/transactions")

total = df.count()
distinct = df.select("transaction_id").distinct().count()
print(f"Silver rows: {total}, distinct transaction_ids: {distinct}  (must be equal)")

print("Burst events: txn_count_5m should climb within each burst")
(df.filter("label_fraud_type = 'burst'")
   .select("card_id", "amount", "txn_count_5m", "event_time")
   .orderBy("card_id", "event_time").show(12, truncate=False))

print("Impossible travel: distance and speed should be huge")
(df.filter("label_fraud_type = 'impossible_travel'")
   .select("card_id", "city", "distance_km", "secs_since_prev", "speed_kmh")
   .show(8, truncate=False))

print("Large amount: amount_ratio should be roughly 7x or more")
(df.filter("label_fraud_type = 'large_amount'")
   .select("amount", "avg_amount_prior", "prior_txn_count", "amount_ratio")
   .show(8, truncate=False))

print("Normal transactions for comparison")
(df.filter(F.col("label_is_fraud") == False)
   .select("card_id", "amount", "txn_count_5m", "distance_km", "speed_kmh", "amount_ratio")
   .show(5, truncate=False))

try:
    q = spark.read.format("delta").load("/data/silver/quarantine")
    print("Quarantined rows:", q.count())
    q.select("raw_json").show(5, truncate=80)
except Exception:
    print("No quarantine table yet (no bad records seen)")