# Proyek Pemrosesan Data Sensor Gudang Real-time

**Nama:** Azza Farichi Tjahjono  
**NRP:** 5027231071  
**Tanggal:** 21 Mei 2025  

## Ringkasan Proyek

Proyek ini mendemonstrasikan sistem pemrosesan data sensor secara real-time menggunakan Apache Kafka dan Apache Spark (PySpark). Sistem ini dirancang untuk memantau kondisi suhu dan kelembaban di beberapa gudang. Data dari sensor disimulasikan oleh dua producer Kafka yang mengirimkan metrik suhu dan kelembaban setiap detik. Sebuah consumer PySpark kemudian mengonsumsi data ini, melakukan filtering untuk mendeteksi kondisi abnormal (suhu > 80°C atau kelembaban > 70%), dan menggabungkan kedua stream data untuk mengidentifikasi peringatan kritis jika kedua kondisi tersebut terjadi bersamaan di gudang yang sama dalam rentang waktu tertentu.

Tujuan utama proyek ini adalah untuk menunjukkan alur kerja end-to-end dari pengumpulan data sensor, pengiriman melalui message broker, hingga pemrosesan dan analisis real-time untuk pengambilan keputusan cepat.

## Komponen Utama

*   **Apache Kafka:** Digunakan sebagai message broker untuk menangani aliran data sensor yang masuk.
*   **Python Kafka Producers (`producer_suhu.py`, `producer_kelembapan.py`):** Mensimulasikan pengiriman data suhu dan kelembaban dari tiga gudang (G1, G2, G3) ke topik Kafka yang sesuai.
*   **Apache Spark (PySpark) (`pyspark_consumer.py`):** Aplikasi Structured Streaming yang mengonsumsi data dari Kafka, melakukan transformasi, filtering, dan join pada stream data untuk menghasilkan peringatan.
*   **Docker & Docker Compose:** Digunakan untuk mengelola dan menjalankan lingkungan Kafka (Zookeeper & Kafka broker) dan Spark (Jupyter Notebook dengan PySpark).

## Cara Menjalankan Proyek

Pastikan Docker dan Docker Compose sudah terinstal di sistem Anda.

### 1. Jalankan Layanan Kafka dan Spark

Buka terminal di direktori root proyek dan jalankan perintah berikut untuk memulai Zookeeper, Kafka, dan container Spark:

```bash
docker-compose up -d
```

Tunggu beberapa saat hingga semua container berjalan dengan baik. Anda dapat memeriksa status container dengan `docker ps`.

### 2. Jalankan Producer Kafka

Producer akan mengirimkan data sensor ke topik Kafka. Anda perlu menjalankan kedua script producer di terminal terpisah (atau sebagai background process) dari direktori root proyek Anda.

**Producer Suhu:**

```bash
python3 producer_suhu.py
```

**Producer Kelembaban:**

```bash
python3 producer_kelembapan.py
```

Biarkan kedua producer ini berjalan. Mereka akan terus mengirim data setiap detik.

### 3. Jalankan Consumer PySpark

Consumer PySpark akan memproses data dari Kafka. Script ini dijalankan di dalam container Spark.

Buka terminal baru dan masuk ke dalam container Spark:

```bash
docker exec -it spark_pyspark /bin/bash
```

Setelah berada di dalam shell container (`jovyan@<container_id>:/opt/spark/work$`), jalankan script consumer menggunakan `spark-submit`:

```bash
spark-submit \\
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
/home/jovyan/work/pyspark_consumer.py
```
*Catatan: Path `/home/jovyan/work/pyspark_consumer.py` adalah path di dalam container Spark yang telah di-mount melalui `docker-compose.yml`.*

Consumer akan mulai memproses data dan menampilkan output peringatan di konsol terminal tersebut.

### 4. Menghentikan Proyek

Untuk menghentikan semua layanan:

1.  Hentikan producer Python (Ctrl+C di terminal masing-masing).
2.  Hentikan layanan Docker Compose:

    ```bash
    docker compose down ; jika menggunakan docker compose V2

    docker-compose down ; jika menggunakan docker compose dibawah V2
    ```

## Hasil dan Screenshot

Berikut adalah tempat untuk menyertakan screenshot yang menunjukkan berbagai tahapan dan hasil dari proyek ini.

### 1. Output Producer Suhu


![Output Producer Suhu](./assets/producer_suhu.png)


### 2. Output Producer Kelembaban

![Output Producer Kelembapan](./assets/producer_kelembapan.png)

### 3. Output Consumer PySpark - Peringatan Individual


### 4. Output Consumer PySpark - Peringatan Gabungan (Kritis dan Status Lain)


