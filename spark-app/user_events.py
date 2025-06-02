from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import *
from clickhouse_driver import Client

def get_user_schema():
    return StructType([
        StructField("schema", StringType(), nullable=True),
        StructField("payload", StructType([
            StructField("before", StructType([
                StructField("event_id", IntegerType(), nullable=False),
                StructField("event_type", StringType(), nullable=False),
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", LongType(), nullable=False)
            ]), nullable=True),
            StructField("after", StructType([
                StructField("event_id", IntegerType(), nullable=False),
                StructField("event_type", StringType(), nullable=False),
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", LongType(), nullable=False)
            ]), nullable=True),
            StructField("op", StringType(), nullable=True)
        ]), nullable=True)
    ])

def process_user_stream(spark):
    user_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "pgserver.public.users_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        user_raw_df
        .select(from_json(col("value").cast("string"), get_user_schema()).alias("data"))
        .select(
            col("data.payload.after.event_id").alias("event_id"),
            col("data.payload.after.event_type").alias("event_type"),
            col("data.payload.after.user_id").alias("user_id"),
            expr("timestamp_micros(data.payload.after.timestamp)").alias("timestamp")
        )
        .filter(col("event_id").isNotNull())
    )

def write_user_to_clickhouse(batch_df, batch_id):
    client = None
    try:
        if batch_df.rdd.isEmpty():
            return

        rows = batch_df.collect()
        client = Client('clickhouse', port=9000, database='test')

        inserts = []
        for row in rows:
            inserts.append({
                "event_id": row.event_id,
                "event_type": row.event_type,
                "user_id": row.user_id or "",
                "timestamp": row.timestamp
            })

        client.execute(
            "INSERT INTO test.users_events VALUES",
            inserts
        )
    except Exception as e:
        print(f"User batch error: {e}")
    finally:
        if client:
            client.disconnect()