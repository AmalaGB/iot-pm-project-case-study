from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr, window, avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# 1) Spark session with Delta support
spark = SparkSession.builder \
    .appName("IoTPredictiveMaintenance") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2) Kafka source
kafka_bootstrap = "localhost:9092"
topic = "sensor-data"
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# 3) Parse value as JSON
schema = StructType([
    StructField("machine_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("rpm", IntegerType()),
    StructField("timestamp", StringType())
])

json_df = raw.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_received_ts")
parsed = json_df.select(from_json(col("json_str"), schema).alias("data"), col("kafka_received_ts"))
bronze = parsed.selectExpr("data.*", "kafka_received_ts")

# 4) Write Bronze (raw) to Delta
bronze_path = "/opt/delta/bronze"
bronze.writeStream.format("delta") \
    .option("checkpointLocation", "/opt/delta/checkpoints/bronze") \
    .outputMode("append") \
    .start(bronze_path)

# 5) Silver - basic validation and type conversion
from pyspark.sql.functions import to_timestamp, when, lit
silver = bronze.withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("is_valid", 
                when((col("temperature").isNotNull()) &
                     (col("vibration").isNotNull()) &
                     (col("rpm").isNotNull()) & (col("timestamp").isNotNull()), lit(True)).otherwise(lit(False))
               )

silver_path = "/opt/delta/silver"
silver.writeStream.format("delta") \
    .option("checkpointLocation", "/opt/delta/checkpoints/silver") \
    .outputMode("append") \
    .start(silver_path)

# 6) Gold - aggregated 5-minute windows and anomaly flag
agg = silver.withWatermark("timestamp", "10 minutes") \
    .groupBy(
        col("machine_id"),
        window(col("timestamp"), "5 minutes")
    ).agg(
        avg("temperature").alias("avg_temp_5min"),
        avg("vibration").alias("avg_vibration_5min"),
        avg("rpm").alias("avg_rpm_5min")
    )

from pyspark.sql.functions import expr
# simple rule-based anomaly
gold = agg.withColumn("anomaly_flag", expr("CASE WHEN avg_vibration_5min > 80 OR avg_temp_5min > 100 THEN 1 ELSE 0 END"))

gold_path = "/opt/delta/gold"
query = gold.writeStream.format("delta") \
    .option("checkpointLocation", "/opt/delta/checkpoints/gold") \
    .outputMode("append") \
    .start(gold_path)

query.awaitTermination()
