CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.messages (
    user_id   Int32,
    track_id  String,
    genre     String,
    artist    String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp);
