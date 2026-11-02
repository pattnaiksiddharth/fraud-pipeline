from common import get_spark

spark = get_spark("check-counts")
spark.sparkContext.setLogLevel("ERROR")

for name, path in [("bronze", "/data/bronze/transactions"),
                   ("silver", "/data/silver/transactions"),
                   ("gold", "/data/gold/scored_transactions")]:
    try:
        print(f"{name}: {spark.read.format('delta').load(path).count()} rows")
    except Exception as e:
        print(f"{name}: ERROR {str(e)[:150]}")