from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("evaluate")
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.format("delta").load("/data/gold/scored_transactions")
      .withColumn("label_fraud_type", F.coalesce("label_fraud_type", F.lit("none"))))

print("Total scored:", df.count())

print("Flag rate per injected fraud type:")
(df.groupBy("label_fraud_type")
   .agg(F.count("*").alias("txns"),
        F.sum(F.col("is_flagged").cast("int")).alias("flagged"))
   .orderBy("label_fraud_type").show())

tp = df.filter("label_is_fraud AND is_flagged").count()
fp = df.filter("NOT label_is_fraud AND is_flagged").count()
fn = df.filter("label_is_fraud AND NOT is_flagged").count()
precision = tp / (tp + fp) if (tp + fp) else 0.0
recall = tp / (tp + fn) if (tp + fn) else 0.0
print(f"TP={tp} FP={fp} FN={fn}  precision={precision:.2%}  recall={recall:.2%}")

print("False positives by rule:")
(df.filter("NOT label_is_fraud AND is_flagged")
   .select(F.explode("triggered_rules").alias("rule"))
   .groupBy("rule").count().show())