from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, TimestampType
from clickhouse_driver import Client

# 1. Инициализация Spark Session
spark = SparkSession.builder \
    .appName("KafkaClickhouse") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 2. Схема для JSON данных
schema = StructType() \
    .add("message", StringType()) \
    .add("timestamp", TimestampType())

# 3. Чтение из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Парсинг JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Запись в ClickHouse
def write_to_clickhouse(df, epoch_id):
    client = Client(
        host='clickhouse',
        user='default',
        password='',
        database='test'
    )
    data = [row.asDict() for row in df.collect()]
    client.execute("INSERT INTO test.messages VALUES", data)

query = parsed_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .start()

query.awaitTermination()