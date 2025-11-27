import joblib, time, pandas as pd
from delta import DeltaTable
from pyspark.sql import SparkSession

model = joblib.load("../models/rf_pm_v1.joblib")

spark = SparkSession.builder \
    .appName("scoring-batch") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

while True:
    # read gold
    df = spark.read.format("delta").load("/opt/delta/gold")
    # convert to pandas and score
    pdf = df.select("machine_id","avg_temp_5min","avg_vibration_5min","avg_rpm_5min","window").toPandas()
    if not pdf.empty:
        feats = ["avg_temp_5min","avg_vibration_5min","avg_rpm_5min"]
        pdf["score"] = model.predict_proba(pdf[feats])[:,1]
        pdf["pred_label"] = (pdf["score"] > 0.5).astype(int)
        print(pdf[["machine_id","window","pred_label","score"]].tail(10))
    time.sleep(60)
