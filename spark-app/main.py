from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, explode
from pyspark.sql.types import *
from clickhouse_driver import Client

# Инициализация Spark
spark = SparkSession.builder \
    .appName("MusicDataPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# Определение схемы для Debezium
inner_schema = StructType([
    StructField("before", StructType([
        StructField("user_id", IntegerType(), True),
        StructField("track_id", StringType(), True),
        StructField("genre", ArrayType(StringType()), True),
        StructField("artists", ArrayType(StringType()), True),
        StructField("timestamp", LongType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("user_id", IntegerType(), True),
        StructField("track_id", StringType(), True),
        StructField("genre", ArrayType(StringType()), True),
        StructField("artists", ArrayType(StringType()), True),
        StructField("timestamp", LongType(), True)
    ]), True),
    StructField("op", StringType(), True)
])

root_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", inner_schema)
])

# Чтение из Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "pgserver.public.messages") \
    .option("startingOffsets", "earliest") \
    .load()

# Обработка данных
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), root_schema).alias("data")
).select(
    col("data.payload.after.user_id").alias("user_id"),
    col("data.payload.after.track_id").alias("track_id"),
    col("data.payload.after.genre").alias("genre"),
    col("data.payload.after.artists").alias("artists"),
    expr("timestamp_micros(data.payload.after.timestamp)").alias("timestamp")
).withColumn(
    "genre", explode("genre")
).withColumn(
    "artist", explode("artists")
).select(
    "user_id",
    "track_id",
    "genre",
    "artist",
    "timestamp"
).filter(
    col("genre").isNotNull() & col("artist").isNotNull()
)

# Функция записи в ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    client = None
    try:
        if batch_df.rdd.isEmpty():
            return
        client = Client('clickhouse', port=9000, database='test')
        rows = batch_df.collect()
        inserts = [{
            "user_id": row.user_id,
            "track_id": row.track_id,
            "genre": row.genre,
            "artist": row.artist,  # Исправлено на "artist"
            "timestamp": row.timestamp
        } for row in rows]
        client.execute("INSERT INTO test.messages (user_id, track_id, genre, artist, timestamp) VALUES", inserts)
        print(f"Batch {batch_id}: Inserted {len(inserts)} rows")
    except Exception as e:
        print(f"Error in batch {batch_id}: {str(e)}")
    finally:
        if client is not None:
            client.disconnect()

# Запуск стриминга
query = parsed_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .start()

query.awaitTermination()