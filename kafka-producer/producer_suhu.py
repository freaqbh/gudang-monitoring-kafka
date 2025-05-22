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
