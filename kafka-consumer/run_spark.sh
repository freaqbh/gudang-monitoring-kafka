#!/bin/bash

# Ganti sesuai path ke file Python-mu
PYSPARK_SCRIPT="spark_streaming_monitor.py"

# Ganti versi jika Spark kamu berbeda (cek dengan `spark.version`)
SPARK_VERSION="3.5.5"
SCALA_VERSION="2.12"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION}"

# Jalankan spark-submit dengan konektor Kafka
$SPARK_HOME/bin/spark-submit \
  --packages "$KAFKA_PACKAGE" \
  "$PYSPARK_SCRIPT"
