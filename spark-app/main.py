from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType
)
from clickhouse_driver import Client

# 1. Инициализация Spark Session
spark = SparkSession.builder \
    .appName("DebeziumClickHouse") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# 2. Определяем схему для Debezium Envelope внутри поля payload
inner_schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("message", StringType(), True),
        StructField("created_at", LongType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("message", StringType(), True),
        StructField("created_at", LongType(), True)
    ]), True),
    StructField("op", StringType(), True)
])

root_schema = StructType([
    StructField("schema", StringType(), True),  # ignore schema field
    StructField("payload", inner_schema, True)
])

# 3. Чтение из Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "pgserver.public.messages") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Парсинг JSON, приводим payload.after.created_at к Timestamp
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), root_schema).alias("data")
).select(
    col("data.payload.after.message").alias("message"),
    expr("timestamp_micros(data.payload.after.created_at)").alias("timestamp"),
    col("data.payload.op").alias("operation_type")
).filter(
    col("operation_type").isin("c", "u")
).filter(
    col("message").isNotNull()
)

# 5. Функция для записи в ClickHouse с логированием

def write_to_clickhouse(batch_df, batch_id):
    try:
        count = batch_df.count()
        print(f"→ Batch {batch_id}: got {count} rows")
        batch = batch_df.collect()
        for row in batch:
            print(f"   row: msg={row.message!r}, ts={row.timestamp}")

        if count == 0:
            return

        client = Client(
            host='clickhouse', port=9000,
            user='default', password='', database='test'
        )

        inserts = [{"message": r.message, "timestamp": r.timestamp} for r in batch]

        print(f"Batch {batch_id}: will insert {len(inserts)} rows")
        try:
            client.execute(
                "INSERT INTO test.messages (message, timestamp) VALUES",
                inserts,
                types_check=True
            )
            print(f"Batch {batch_id}: successfully inserted {len(inserts)} records")
        except Exception as e:
            print(f"Batch {batch_id}: insert failed: {e}")

    except Exception as e:
        print(f"Error in batch {batch_id}: {e}")
    finally:
        try:
            client.disconnect()
        except:
            pass

# 6. Запуск стриминга
query = parsed_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .start()

query.awaitTermination()
