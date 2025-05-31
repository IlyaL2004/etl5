-- Подключаемся к системной базе 'postgres'
\c postgres

-- Удаляем базу 'mydb', если она существует
DROP DATABASE IF EXISTS mydb;

-- Создаем новую базу 'mydb'
CREATE DATABASE mydb;

-- Переключаемся на созданную базу
\c mydb

-- Создаем таблицу 'messages'
CREATE TABLE messages (
    user_id INT,
    track_id VARCHAR(255),
    genre TEXT[],
    artists TEXT[],
    timestamp TIMESTAMP DEFAULT NOW()
);


-- Новые таблицы для событий
CREATE TABLE IF NOT EXISTS user_events (
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    username VARCHAR(255),
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS session_events (
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255),
    track_id VARCHAR(255),
    bitrate VARCHAR(50),
    new_bitrate INT,
    acked_chunk_count INT,
    total_chunks_sent INT,
    new_chunk_offset INT,
    old_chunk_offset INT,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS playlist_events (
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    playlist_id INT NOT NULL,
    track_id INT NOT NULL,
    user_id INT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);