# 🚚 Real-Time Monitoring Gudang dengan Kafka dan PySpark

Proyek ini mensimulasikan sistem monitoring real-time suhu dan kelembaban di gudang menggunakan **Apache Kafka**, **Docker**, dan **PySpark Streaming**.

## 🧱 Arsitektur

- **Kafka + Zookeeper**: Menyediakan saluran komunikasi real-time
- **Kafka Producer**: Mengirim data sensor suhu dan kelembaban setiap detik
- **PySpark Consumer**: Mengolah dan menampilkan data sensor sebagai peringatan

---

## 🧰 Prasyarat

- Docker & Docker Compose
- Python 3.8+
- Java (untuk Spark)
- Apache Spark
- Apache Kafka image (Confluent)

---

## 🔧 Langkah-Langkah

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