CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.session_events (
    event_id          Int32,
    event_type        String,
    session_id        String,
    user_id           String,
    track_id          String,
    bitrate           String,
    new_bitrate       Int32,
    acked_chunk_count Int32,
    total_chunks_sent Int32,
    new_chunk_offset  Int32,
    old_chunk_offset  Int32,
    timestamp         DateTime
) ENGINE = MergeTree()
ORDER BY (session_id, timestamp);

CREATE TABLE IF NOT EXISTS test.playlist_events (
    event_id    Int32,
    event_type  String,
    playlist_id Int32,
    track_id    Int32,
    user_id     Int32,
    timestamp   DateTime
) ENGINE = MergeTree()
ORDER BY (playlist_id, timestamp);


CREATE TABLE IF NOT EXISTS test.users_events (
    event_id   Int32,
    event_type String,
    user_id    String,
    timestamp  DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp);