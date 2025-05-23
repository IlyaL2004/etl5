CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.messages
(
    message String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY timestamp;