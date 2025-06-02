from pyspark.sql import SparkSession
from session_events import process_session_stream, write_session_to_clickhouse
from user_events import process_user_stream, write_user_to_clickhouse
from playlist_events import process_playlist_stream, write_playlist_to_clickhouse

spark = (
    SparkSession.builder
    .appName("MultiTopicMusicDataPipeline")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# Session events stream
session_stream = process_session_stream(spark)
session_query = (
    session_stream.writeStream
    .foreachBatch(write_session_to_clickhouse)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints_session_events")
    .start()
)

# User events stream
user_stream = process_user_stream(spark)
user_query = (
    user_stream.writeStream
    .foreachBatch(write_user_to_clickhouse)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints_users_events")
    .start()
)

# Playlist events stream
playlist_stream = process_playlist_stream(spark)
playlist_query = (
    playlist_stream.writeStream
    .foreachBatch(write_playlist_to_clickhouse)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints_playlist_events")
    .start()
)

spark.streams.awaitAnyTermination()