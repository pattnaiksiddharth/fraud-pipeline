from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("check-gold")
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/data/gold/scored_transactions")

print("Flagged vs not flagged:")
df.groupBy("is_flagged").count().show()

print("Rule frequency:")
(df.select(F.explode("triggered_rules").alias("rule"))
   .groupBy("rule").count().show())

print("Sample flagged rows:")
(df.filter("is_flagged")
   .select("card_id", "amount", "txn_count_5m", "distance_km",
           "amount_ratio", "triggered_rules", "risk_score", "label_fraud_type")
   .show(10, truncate=False))