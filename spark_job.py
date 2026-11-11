"""Spark Structured Streaming: Kafka -> anomaly detection -> Kafka.
Run: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_job.py
"""
import json, math
from kafka import KafkaProducer
from dgim import DGIM
from bloom import BloomFilter
from pyspark.sql import SparkSession, functions as F, types as T

BOOT = "localhost:9092"
spark = (SparkSession.builder.appName("IoT-Anomaly-Detection")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("sensor_id", T.StringType()), T.StructField("type", T.StringType()),
    T.StructField("location", T.StringType()), T.StructField("unit", T.StringType()),
    T.StructField("value", T.DoubleType()), T.StructField("ts", T.LongType()),
])

raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
       .option("subscribe", "sensor-readings").option("startingOffsets", "latest").load())
parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
          .select("d.*").withColumn("event_time", (F.col("ts") / 1000).cast("timestamp")))

# ---- Query 1: per-reading anomaly detection (EWMA z-score + hard limits) ----
LIMITS = {"temperature": (0, 45), "pm25": (0, 100), "power": (0, 11), "humidity": (5, 95)}
MIN_STD = {"temperature": 0.3, "humidity": 0.8, "pm25": 1.5, "power": 0.2}   # avoids tiny-variance false alarms
ALPHA, Z_WARN, Z_CRIT, RESET_AFTER = 0.1, 3.5, 6.0, 10
DGIM_WINDOW = 100            # count anomalies among the last 100 readings of each sensor
dgims = {}
FAULTY_AFTER = 5            # 5+ anomalies in the last 100 readings (DGIM) -> known faulty
bloom = BloomFilter(capacity=1000, error_rate=0.01)
state, producer = {}, None   # sensor_id -> [mean, variance, count, anomaly_streak]

def process_batch(df, batch_id):
    global producer
    if producer is None:
        producer = KafkaProducer(bootstrap_servers=BOOT, value_serializer=lambda v: json.dumps(v).encode())
    for r in df.orderBy("ts").collect():
        st = state.setdefault(r.sensor_id, [r.value, 1.0, 0, 0])
        mean, var, n, streak = st
        std = max(math.sqrt(var), MIN_STD.get(r.type, 0.1))
        z = (r.value - mean) / std if n >= 20 else 0.0     # warm-up period
        lo, hi = LIMITS.get(r.type, (-1e9, 1e9))
        out_of_range = not (lo <= r.value <= hi)
        anomaly = abs(z) > Z_WARN or out_of_range
        if not anomaly:                                    # learn only from normal data
            d = r.value - mean
            st[0] = mean + ALPHA * d
            st[1] = (1 - ALPHA) * (var + ALPHA * d * d)
            st[3] = 0
        else:
            st[3] = streak + 1
            if st[3] >= RESET_AFTER and not out_of_range:  # sustained shift: re-learn baseline
                st[0], st[1], st[3] = r.value, 1.0, 0
        st[2] = n + 1
        dg = dgims.setdefault(r.sensor_id, DGIM(DGIM_WINDOW))
        dg.add(1 if anomaly else 0)
        if anomaly and dg.count() >= FAULTY_AFTER:
            bloom.add(r.sensor_id)                      # blacklist repeat offenders
        producer.send("sensor-processed", {
            "sensor_id": r.sensor_id, "type": r.type, "location": r.location, "unit": r.unit,
            "value": r.value, "ts": r.ts, "z": round(z, 2), "anomaly": anomaly,
            "dgim_count": dg.count(), "dgim_window": DGIM_WINDOW,
            "known_faulty": r.sensor_id in bloom, "bloom_n": bloom.count, "bloom_fp": round(bloom.fp_rate(), 6),
            "bloom_bits": bloom.m, "bloom_k": bloom.k,
            "severity": "critical" if abs(z) > Z_CRIT or out_of_range else ("warning" if anomaly else "ok"),
        })
    producer.flush()

q1 = (parsed.writeStream.foreachBatch(process_batch)
      .option("checkpointLocation", "./checkpoints/proc").trigger(processingTime="1 second").start())

# ---- Query 2: 30s sliding-window aggregates per sensor type ----
agg = (parsed.withWatermark("event_time", "10 seconds")
       .groupBy(F.window("event_time", "30 seconds", "10 seconds"), "type")
       .agg(F.avg("value").alias("avg"), F.min("value").alias("min"),
            F.max("value").alias("max"), F.count("*").alias("n")))
out = agg.select(F.to_json(F.struct(
    F.col("window.end").cast("string").alias("window_end"), "type",
    F.round("avg", 2).alias("avg"), "min", "max", "n")).alias("value"))
q2 = (out.writeStream.outputMode("update").format("kafka")
      .option("kafka.bootstrap.servers", BOOT).option("topic", "sensor-agg")
      .option("checkpointLocation", "./checkpoints/agg").start())

spark.streams.awaitAnyTermination()