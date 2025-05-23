from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SensorStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Mengurangi verbosity log Spark

    # Skema untuk data sensor suhu
    schema_suhu = StructType([
        StructField("gudang_id", StringType(), True),
        StructField("suhu", FloatType(), True),
        StructField("timestamp", StringType(), True) # Baca sebagai string dulu, lalu cast
    ])

    # Skema untuk data sensor kelembaban
    schema_kelembaban = StructType([
        StructField("gudang_id", StringType(), True),
        StructField("kelembaban", FloatType(), True),
        StructField("timestamp", StringType(), True) # Baca sebagai string dulu, lalu cast
    ])

    # Konsumsi data dari topik sensor-suhu-gudang ---
    df_suhu_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "sensor-suhu-gudang") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    df_suhu = df_suhu_raw \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema_suhu).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp_suhu", col("timestamp").cast(TimestampType()))

    # --- Konsumsi data dari topik sensor-kelembaban-gudang ---
    df_kelembaban_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "sensor-kelembaban-gudang") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    df_kelembaban = df_kelembaban_raw \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema_kelembaban).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp_kelembaban", col("timestamp").cast(TimestampType()))

    # --- Filtering: Suhu > 80°C ---
    df_peringatan_suhu = df_suhu.filter(col("suhu") > 80)

    query_peringatan_suhu = df_peringatan_suhu \
        .selectExpr("CAST(gudang_id AS STRING) as gudang_id_str", "CAST(suhu AS STRING) as suhu_str") \
        .selectExpr("'[Peringatan Suhu Tinggi] Gudang ' || gudang_id_str || ': Suhu ' || suhu_str || '°C' as message") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- Filtering: Kelembaban > 70% ---
    df_peringatan_kelembaban = df_kelembaban.filter(col("kelembaban") > 70)

    query_peringatan_kelembaban = df_peringatan_kelembaban \
        .selectExpr("CAST(gudang_id AS STRING) as gudang_id_str", "CAST(kelembaban AS STRING) as kelembaban_str") \
        .selectExpr("'[Peringatan Kelembaban Tinggi] Gudang ' || gudang_id_str || ': Kelembaban ' || kelembaban_str || '%' as message") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- Gabungkan Stream dari Dua Sensor (Join) ---
    # Watermark mendefinisikan seberapa lama data bisa "terlambat" sebelum dianggap usang untuk operasi stateful.
    df_suhu_watermarked = df_suhu.withWatermark("timestamp_suhu", "20 seconds")
    df_kelembaban_watermarked = df_kelembaban.withWatermark("timestamp_kelembaban", "20 seconds")

    
    joined_df = df_suhu_watermarked.alias("s").join(
        df_kelembaban_watermarked.alias("k"),
        expr("""
            s.gudang_id = k.gudang_id AND
            s.timestamp_suhu >= k.timestamp_kelembaban - interval 10 seconds AND
            s.timestamp_suhu <= k.timestamp_kelembaban + interval 10 seconds
        """),
        "inner" # Atau 'leftOuter', 'rightOuter' jika ingin menyimpan data bahkan jika tidak ada pasangan
    ).select(
        col("s.gudang_id").alias("gudang_id"),
        col("s.suhu").alias("suhu"),
        col("k.kelembaban").alias("kelembaban"),
        # Pilih salah satu timestamp atau rata-ratanya jika perlu
        # Di sini kita bisa memilih timestamp suhu sebagai referensi
        col("s.timestamp_suhu").alias("event_time")
    )

    # --- Buat Peringatan Gabungan ---
    df_status_gudang = joined_df.withColumn(
        "status",
        when((col("suhu") > 80) & (col("kelembaban") > 70), "Bahaya tinggi! Barang berisiko rusak")
        .when((col("suhu") > 80) & (col("kelembaban") <= 70), "Suhu tinggi, kelembaban normal")
        .when((col("suhu") <= 80) & (col("kelembaban") > 70), "Kelembaban tinggi, suhu aman")
        .otherwise("Aman")
    )

    # Fungsi untuk format output gabungan
    def format_output_gabungan(batch_df, batch_id):
        print(f"--- Batch Peringatan Gabungan (ID: {batch_id}) ---")
        if batch_df.count() > 0:
            for row in batch_df.collect():
                gudang = row["gudang_id"]
                suhu = row["suhu"]
                kelembaban = row["kelembaban"]
                status = row["status"]

                if status == "Bahaya tinggi! Barang berisiko rusak":
                    print("[PERINGATAN KRITIS]")
                elif status == "Suhu tinggi, kelembaban normal":
                    print("[PERINGATAN SUHU]") # Sesuai contoh output tugas
                elif status == "Kelembaban tinggi, suhu aman":
                    print("[PERINGATAN KELEMBABAN]") # Sesuai contoh output tugas
                else: # Aman
                    print("[STATUS AMAN]")

                print(f"Gudang {gudang}:")
                print(f"  - Suhu: {suhu}°C")
                print(f"  - Kelembaban: {kelembaban}%")
                print(f"  - Status: {status}")
                print("-" * 30)
        # else:
            # print("Tidak ada data gabungan yang memenuhi kriteria pada batch ini.")

    query_gabungan = df_status_gudang \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(format_output_gabungan) \
        .trigger(processingTime="15 seconds") \
        .start()

    # Tunggu semua query streaming selesai (tidak akan pernah kecuali dihentikan manual)
    spark.streams.awaitAnyTermination()