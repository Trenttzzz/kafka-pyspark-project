import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:29092', # Terhubung ke port yang diekspos ke host
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'sensor-kelembaban-gudang'
GUDANG_IDS = ['G1', 'G2', 'G3']

print(f"Memulai producer kelembaban untuk topik: {TOPIC_NAME}...")
try:
    while True:
        for gudang_id in GUDANG_IDS:
            kelembaban = round(random.uniform(65.0, 75.0), 1) # Kelembaban antara 65 dan 75
            data = {
                "gudang_id": gudang_id,
                "kelembaban": kelembaban,
                # Menggunakan ISO format string untuk timestamp agar mudah diparsing di Spark
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            print(f"Mengirim data kelembaban: {data}")
            producer.send(TOPIC_NAME, value=data)
        producer.flush()
        time.sleep(1) # Kirim data setiap detik
except KeyboardInterrupt:
    print("Producer kelembaban dihentikan.")
finally:
    producer.close()
    print("Koneksi producer kelembaban ditutup.")