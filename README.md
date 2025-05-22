## Real-Time Monitoring Gudang dengan Kafka dan PySpark

Proyek ini mensimulasikan sistem monitoring real-time suhu dan kelembaban di gudang menggunakan **Apache Kafka**, **Docker**, dan **PySpark Streaming**.

## Arsitektur

- **Kafka + Zookeeper**: Menyediakan saluran komunikasi real-time
- **Kafka Producer**: Mengirim data sensor suhu dan kelembaban setiap detik
- **PySpark Consumer**: Mengolah dan menampilkan data sensor sebagai peringatan

---

## Prasyarat

- Docker & Docker Compose
- Python 3.8+
- Java (untuk Spark)
- Apache Spark
- Apache Kafka image (Confluent)

---

##  Langkah-Langkah

### 1. Jalankan Kafka dan Zookeeper (Docker)

```bash
docker run -d --name zoo1 -p 2181:2181 confluentinc/cp-zookeeper:7.8.0

docker run -d --name kafka1 -p 9092:9092 -p 29092:29092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zoo1:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --link zoo1 \
  confluentinc/cp-kafka:7.8.0
```

jika belum ada docker image untuk kafka dan zookeeper bisa install dengan 

```bash
git clone https://github.com/conduktor/kafka-stack-docker-compose.git

docker-compose -f zk-single-kafka-single.yml up -d
```
![Screenshot 2025-05-22 145749](https://github.com/user-attachments/assets/1111393c-4bfe-4f55-aed6-0fb81f0df72f)

### 2. Buatlah topic di kafka1 container

```bash
docker exec -it kafka1 bash

kafka-topics --bootstrap-server localhost:9092 --create --topic sensor-suhu-gudang --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --topic sensor-kelembaban-gudang --partitions 1 --replication-factor 1

# Cek topik berhasil dibuat
kafka-topics --bootstrap-server localhost:9092 --list
```


### 3. Buat kafka producer python

Buka terminal lokal (bukan dalam container), aktifkan virtual environment (opsional), lalu install:
```bash
pip install kafka-python
```

producer suhu

```python
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudangs = ['G1', 'G2', 'G3']

while True:
    for gudang in gudangs:
        data = {'gudang_id': gudang, 'suhu': random.randint(60, 70)}
        producer.send('sensor-suhu-gudang', value=data)
    time.sleep(1)
```

producer kelembaban

```python
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudangs = ['G1', 'G2', 'G3']

while True:
    for gudang in gudangs:
        data = {'gudang_id': gudang, 'kelembaban': random.randint(50, 60)}
        producer.send('sensor-kelembaban-gudang', value=data)
    time.sleep(1)
```

jalankan dua script ini di dua terminal yang berbeda
```bash
python producer_suhu.py
python producer_kelembaban.py

```

### 4. Buat pyspark streaming consumer

Buka terminal lokal (bukan dalam container), aktifkan virtual environment (opsional), lalu install:
```bash
pip install pyspark
```

streaming consumer dengan pyspark
```python
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
```

jalankan script ini:
```bash
python spark_streaming_monitor.py
```
![Screenshot 2025-05-22 145539](https://github.com/user-attachments/assets/60b2454d-eab0-4f10-98a7-c81a408d45e0)
