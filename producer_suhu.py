import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:29092', # Terhubung ke port yang diekspos ke host
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'sensor-suhu-gudang'
GUDANG_IDS = ['G1', 'G2', 'G3']

print(f"Memulai producer suhu untuk topik: {TOPIC_NAME}...")
try:
    while True:
        for gudang_id in GUDANG_IDS:
            suhu = round(random.uniform(75.0, 85.0), 1) # Suhu antara 75 dan 85
            data = {
                "gudang_id": gudang_id,
                "suhu": suhu,
                # Menggunakan ISO format string untuk timestamp agar mudah diparsing di Spark
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            print(f"Mengirim data suhu: {data}")
            producer.send(TOPIC_NAME, value=data)
        producer.flush() # Pastikan semua pesan terkirim
        time.sleep(1) # Kirim data setiap detik (total 3 pesan per detik, satu per gudang)
except KeyboardInterrupt:
    print("Producer suhu dihentikan.")
finally:
    producer.close()
    print("Koneksi producer suhu ditutup.")