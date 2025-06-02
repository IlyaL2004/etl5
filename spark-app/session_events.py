from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import *
from clickhouse_driver import Client

def get_session_schema():
    return StructType([
        StructField("schema", StringType(), nullable=True),
        StructField("payload", StructType([
            StructField("before", StructType([
                StructField("event_id", IntegerType(), nullable=False),
                StructField("event_type", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("user_id", StringType(), nullable=True),
                StructField("track_id", StringType(), nullable=True),
                StructField("bitrate", StringType(), nullable=True),
                StructField("new_bitrate", IntegerType(), nullable=True),
                StructField("acked_chunk_count", IntegerType(), nullable=True),
                StructField("total_chunks_sent", IntegerType(), nullable=True),
                StructField("new_chunk_offset", IntegerType(), nullable=True),
                StructField("old_chunk_offset", IntegerType(), nullable=True),
                StructField("timestamp", LongType(), nullable=False)
            ]), nullable=True),
            StructField("after", StructType([
                StructField("event_id", IntegerType(), nullable=False),
                StructField("event_type", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("user_id", StringType(), nullable=True),
                StructField("track_id", StringType(), nullable=True),
                StructField("bitrate", StringType(), nullable=True),
                StructField("new_bitrate", IntegerType(), nullable=True),
                StructField("acked_chunk_count", IntegerType(), nullable=True),
                StructField("total_chunks_sent", IntegerType(), nullable=True),
                StructField("new_chunk_offset", IntegerType(), nullable=True),
                StructField("old_chunk_offset", IntegerType(), nullable=True),
                StructField("timestamp", LongType(), nullable=False)
            ]), nullable=True),
            StructField("op", StringType(), nullable=True)
        ]), nullable=True)
    ])

def process_session_stream(spark):
    session_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "pgserver.public.session_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        session_raw_df
        .select(from_json(col("value").cast("string"), get_session_schema()).alias("data"))
        .select(
            col("data.payload.after.event_id").alias("event_id"),
            col("data.payload.after.event_type").alias("event_type"),
            col("data.payload.after.session_id").alias("session_id"),
            col("data.payload.after.user_id").alias("user_id"),
            col("data.payload.after.track_id").alias("track_id"),
            col("data.payload.after.bitrate").alias("bitrate"),
            col("data.payload.after.new_bitrate").alias("new_bitrate"),
            col("data.payload.after.acked_chunk_count").alias("acked_chunk_count"),
            col("data.payload.after.total_chunks_sent").alias("total_chunks_sent"),
            col("data.payload.after.new_chunk_offset").alias("new_chunk_offset"),
            col("data.payload.after.old_chunk_offset").alias("old_chunk_offset"),
            expr("timestamp_micros(data.payload.after.timestamp)").alias("timestamp")
        )
        .filter(col("event_id").isNotNull())
    )

def write_session_to_clickhouse(batch_df, batch_id):
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
                "session_id": row.session_id or "",
                "user_id": row.user_id or "",
                "track_id": row.track_id or "",
                "bitrate": row.bitrate or "",
                "new_bitrate": row.new_bitrate if row.new_bitrate is not None else 0,
                "acked_chunk_count": row.acked_chunk_count if row.acked_chunk_count is not None else 0,
                "total_chunks_sent": row.total_chunks_sent if row.total_chunks_sent is not None else 0,
                "new_chunk_offset": row.new_chunk_offset if row.new_chunk_offset is not None else 0,
                "old_chunk_offset": row.old_chunk_offset if row.old_chunk_offset is not None else 0,
                "timestamp": row.timestamp
            })

        client.execute(
            "INSERT INTO test.session_events VALUES",
            inserts
        )
    except Exception as e:
        print(f"Session batch error: {e}")
    finally:
        if client:
            client.disconnect()