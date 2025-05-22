from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window, udf
from pyspark.sql.types import StructType, StringType, IntegerType

# Setup Spark session
spark = SparkSession.builder \
    .appName("MonitoringGudang") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema untuk masing-masing topik
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# Stream suhu
suhu_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema_suhu).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Stream kelembaban
kelembaban_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema_kelembaban).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Windowed aggregation (10 detik)
windowed_suhu = suhu_df.withWatermark("timestamp", "20 seconds") \
    .groupBy(window("timestamp", "10 seconds"), "gudang_id") \
    .agg({"suhu": "max"}).withColumnRenamed("max(suhu)", "suhu")

windowed_kelembaban = kelembaban_df.withWatermark("timestamp", "20 seconds") \
    .groupBy(window("timestamp", "10 seconds"), "gudang_id") \
    .agg({"kelembaban": "max"}).withColumnRenamed("max(kelembaban)", "kelembaban")

# Gabungkan dua stream berdasarkan gudang_id dan window
joined = windowed_suhu.join(windowed_kelembaban, on=["window", "gudang_id"])

# UDF untuk status lengkap
def evaluate_status(gudang_id, suhu, kelembaban):
    if suhu > 80 and kelembaban > 70:
        return f"[PERINGATAN KRITIS] Gudang {gudang_id}: - Suhu: {suhu}°C - Kelembaban: {kelembaban}% - Status: Bahaya tinggi! Barang berisiko rusak"
    elif suhu > 80:
        return f"Gudang {gudang_id}: - Suhu: {suhu}°C - Kelembaban: {kelembaban}% - Status: Suhu tinggi, kelembaban normal"
    elif kelembaban > 70:
        return f"Gudang {gudang_id}: - Suhu: {suhu}°C - Kelembaban: {kelembaban}% - Status: Kelembaban tinggi, suhu aman"
    else:
        return f"Gudang {gudang_id}: - Suhu: {suhu}°C - Kelembaban: {kelembaban}% - Status: Aman"

status_udf = udf(evaluate_status, StringType())

# Tambahkan kolom status
result = joined.withColumn("status", status_udf(col("gudang_id"), col("suhu"), col("kelembaban")))

# Tampilkan hasil status saja (bisa dimodifikasi jika perlu menampilkan kolom lain)
query = result.select("status") \
    .writeStream.outputMode("append").format("console").option("truncate", False).start()

query.awaitTermination()
