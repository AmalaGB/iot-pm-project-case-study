import time, requests
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("alerter") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

TELEGRAM_BOT_TOKEN = ""    # optional
TELEGRAM_CHAT_ID = ""      # optional

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("TELEGRAM not configured. Alert:", msg)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

while True:
    try:
        gold = spark.read.format("delta").load("/opt/delta/gold")
        anomalies = gold.filter("anomaly_flag = 1").select("machine_id","window","avg_vibration_5min","avg_temp_5min").collect()
        for row in anomalies:
            msg = f"Anomaly detected machine {row['machine_id']} window {row['window']}: vib={row['avg_vibration_5min']}, temp={row['avg_temp_5min']}"
            send_telegram(msg)
    except Exception as e:
        print("Error reading gold table:", e)
    time.sleep(30)
